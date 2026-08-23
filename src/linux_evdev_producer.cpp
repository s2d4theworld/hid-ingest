// src/linux_evdev_producer.cpp — spec section 5 (Linux only).
// SCHED_FIFO pinned thread, libudev discovery, edge-triggered epoll,
// EV_SYN-accumulated samples. Compiled only on Linux.
#if defined(__linux__)

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <libevdev/libevdev.h>
#include <libudev.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <cstdint>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <vector>

#include "hid_ingest/spsc_ring.h"

namespace hid::evdev {

struct EvdevProducerConfig {
    int fifo_priority = 80;
    int core = -1;              // -1 = OS choice
};

class EvdevProducer {
public:
    explicit EvdevProducer(SpscRing<>& ring, EvdevProducerConfig cfg = {})
        : ring_(ring), cfg_(cfg) {}
    ~EvdevProducer() { stop(); }

    /// Returns true only if the producer thread is up AND at least one device
    /// was captured. A false return means the caller should not expect data.
    bool start() {
        try { thread_ = std::thread([this] { run(); }); }
        catch (...) { return false; }
        // Wait briefly for run(): it sets running_ after discovery.
        for (int i = 0; i < 100 && !running_.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return running_.load(std::memory_order_acquire);
    }
    void stop() {
        running_.store(false);
        // Break the blocked epoll_wait (timeout is infinite): a write to the
        // eventfd makes it return with the wake fd ready.
        if (wake_fd_ != -1) {
            const uint64_t one = 1;
            ssize_t r = write(wake_fd_, &one, sizeof(one));
            (void)r;
        }
        if (thread_.joinable()) thread_.join();
    }

private:
    void run();
    bool discover_devices();
    void handle_sync_loss(struct libevdev* dev);

    struct CapturedDevice {
        int fd;
        struct libevdev* dev;
        // Last known absolute position (24.8), per device. Pressure-only or
        // button-only SYN frames reuse it instead of emitting (0,0).
        int32_t last_x = 0;
        int32_t last_y = 0;
    };

    SpscRing<>& ring_;
    EvdevProducerConfig cfg_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    int epoll_fd_ = -1;
    int wake_fd_ = -1;   // eventfd: written by stop() to break epoll_wait
    std::vector<CapturedDevice> devices_;
    // Persistent button state: survives EV_SYN resets of the per-sample
    // accumulator, so mid-drag samples report the held buttons.
    uint16_t button_state_ = 0;
};

bool EvdevProducer::discover_devices() {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) return false;

    // Self-pipe: stop() writes here to break an otherwise-infinite epoll_wait.
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) { close(epoll_fd_); epoll_fd_ = -1; return false; }
    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = wake_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_ev) < 0) {
        close(wake_fd_); wake_fd_ = -1;
        close(epoll_fd_); epoll_fd_ = -1;
        return false;
    }

    struct udev* udev = udev_new();
    if (!udev) {
        // Full teardown: wake fd was already created above.
        close(wake_fd_); wake_fd_ = -1;
        close(epoll_fd_); epoll_fd_ = -1;
        return false;
    }
    struct udev_enumerate* enumerate = udev_enumerate_new(udev);
    udev_enumerate_add_match_subsystem(enumerate, "input");
    udev_enumerate_scan_devices(enumerate);
    struct udev_list_entry* entries = udev_enumerate_get_list_entry(enumerate);
    struct udev_list_entry* entry;

    udev_list_entry_foreach(entry, entries) {
        const char* path = udev_list_entry_get_name(entry);
        struct udev_device* dev = udev_device_new_from_syspath(udev, path);
        if (!dev) continue;
        const char* devnode = udev_device_get_devnode(dev);
        if (!devnode || !strstr(devnode, "event")) { udev_device_unref(dev); continue; }

        // Filter to mice (ID_INPUT_MOUSE) and tablets (ID_INPUT_TABLET).
        const char* mouse = udev_device_get_property_value(dev, "ID_INPUT_MOUSE");
        const char* tablet = udev_device_get_property_value(dev, "ID_INPUT_TABLET");
        if ((!mouse || strcmp(mouse, "1") != 0) && (!tablet || strcmp(tablet, "1") != 0)) {
            udev_device_unref(dev);
            continue;
        }

        int fd = open(devnode, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
        if (fd < 0) { udev_device_unref(dev); continue; }

        struct libevdev* evdev = nullptr;
        if (libevdev_new_from_fd(fd, &evdev) < 0) { close(fd); udev_device_unref(dev); continue; }

        printf("[producer] captured %s (%s)\n", devnode, libevdev_get_name(evdev));

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;      // edge-triggered
        ev.data.fd = fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            fprintf(stderr, "[producer] epoll_ctl add failed for %s\n", devnode);
            libevdev_free(evdev);
            close(fd);
            udev_device_unref(dev);
            continue;
        }
        devices_.push_back({fd, evdev});
        udev_device_unref(dev);
    }
    udev_enumerate_unref(enumerate);
    udev_unref(udev);
    return !devices_.empty();
}

void EvdevProducer::handle_sync_loss(struct libevdev* dev) {
    input_event ev;
    int rc;
    do {
        rc = libevdev_next_event(dev, LIBEVDEV_READ_FLAG_SYNC, &ev);
    } while (rc == LIBEVDEV_READ_STATUS_SYNC || rc == LIBEVDEV_READ_STATUS_SUCCESS);
}

void EvdevProducer::run() {
    sched_param sp{};
    sp.sched_priority = cfg_.fifo_priority;
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp) != 0)
        // Common without root/CAP_SYS_NICE; latency targets may not hold.
        fprintf(stderr, "[producer] SCHED_FIFO not granted (no privilege?)\n");

    if (cfg_.core >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cfg_.core, &set);
        pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    }

    if (!discover_devices()) {
        // Early-out with full fd cleanup: discover_devices() may have created
        // epoll/wake fds before failing to capture any device.
        fprintf(stderr, "[producer] no HID devices found\n");
        if (wake_fd_ != -1) { close(wake_fd_); wake_fd_ = -1; }
        if (epoll_fd_ != -1) { close(epoll_fd_); epoll_fd_ = -1; }
        return;
    }
    running_.store(true);

    epoll_event events[16];
    input_event ev;

    while (running_.load()) {
        const int nfds = epoll_wait(epoll_fd_, events, 16, -1);
        if (nfds < 0) { if (errno == EINTR) continue; break; }

        for (int i = 0; i < nfds; ++i) {
            // Wake fd: shutdown signal — drain it and let the loop check exit.
            if (events[i].data.fd == wake_fd_) {
                uint64_t drain = 0;
                ssize_t r = read(wake_fd_, &drain, sizeof(drain));
                (void)r;
                continue;
            }

            struct libevdev* dev = nullptr;
            CapturedDevice* captured = nullptr;
            for (auto& d : devices_) if (d.fd == events[i].data.fd) { dev = d.dev; captured = &d; }
            if (!dev) continue;

            HidSample acc{};
            bool has_motion = false;
            bool has_button_change = false;   // press OR release this packet
            bool has_pressure = false;        // pressure arrived this packet
            int rc;

            while ((rc = libevdev_next_event(dev, LIBEVDEV_READ_FLAG_NORMAL, &ev))
                   == LIBEVDEV_READ_STATUS_SUCCESS) {
                switch (ev.type) {
                    case EV_REL:
                        if (ev.code == REL_X) { acc.dx += ev.value; has_motion = true; }
                        if (ev.code == REL_Y) { acc.dy += ev.value; has_motion = true; }
                        break;
                    case EV_ABS:
                        // Clamp to int32 range BEFORE narrowing: the int64
                        // shift alone does not prevent truncation on assign
                        // to acc.dx (int32_t). Coordinates beyond ~8.3M after
                        // <<8 are out of spec for a 24.8 field.
                        if (ev.code == ABS_X) {
                            const int64_t v = static_cast<int64_t>(ev.value) << 8;
                            acc.dx = static_cast<int32_t>(
                                v > INT32_MAX ? INT32_MAX : (v < INT32_MIN ? INT32_MIN : v));
                            captured->last_x = acc.dx;   // remember per-device position
                            has_motion = true;
                        }
                        if (ev.code == ABS_Y) {
                            const int64_t v = static_cast<int64_t>(ev.value) << 8;
                            acc.dy = static_cast<int32_t>(
                                v > INT32_MAX ? INT32_MAX : (v < INT32_MIN ? INT32_MIN : v));
                            captured->last_y = acc.dy;
                            has_motion = true;
                        }
                        if (ev.code == ABS_PRESSURE) {
                            // Clamp: some devices report negative/odd values;
                            // pressure must stay in [0, max] before scaling.
                            const int max_p =
                                libevdev_get_abs_maximum(dev, ABS_PRESSURE) ?: 1023;
                            const int v = ev.value < 0 ? 0
                                        : (ev.value > max_p ? max_p : ev.value);
                            acc.pressure =
                                static_cast<uint16_t>(v * 65535 / max_p);
                            has_pressure = true;   // pressure-only samples are reportable
                        }
                        break;
                    case EV_KEY:
                        // Persistent state: update button_state_ (survives
                        // EV_SYN accumulator reset), then copy into acc.
                        if (ev.code == BTN_LEFT)
                            button_state_ = ev.value ? (button_state_ | 0x01)
                                                     : (button_state_ & static_cast<uint16_t>(~0x01));
                        if (ev.code == BTN_RIGHT)
                            button_state_ = ev.value ? (button_state_ | 0x02)
                                                     : (button_state_ & static_cast<uint16_t>(~0x02));
                        if (ev.code == BTN_MIDDLE)
                            button_state_ = ev.value ? (button_state_ | 0x04)
                                                     : (button_state_ & static_cast<uint16_t>(~0x04));
                        // Only tracked buttons (LEFT/RIGHT/MIDDLE) count as
                        // reportable changes. BTN_TOOL_* (pen/finger
                        // proximity) and untracked side buttons do not push a
                        // sample by themselves.
                        if (ev.code == BTN_LEFT || ev.code == BTN_RIGHT ||
                            ev.code == BTN_MIDDLE)
                            has_button_change = true;
                        break;
                    case EV_SYN:
                        if (ev.code == SYN_REPORT &&
                            (has_motion || has_button_change || has_pressure ||
                             button_state_)) {
                            acc.timestamp = static_cast<uint32_t>(
                                (uint64_t)ev.input_event_sec * 1000000 + ev.input_event_usec);
                            // Absolute devices that did not resend X/Y this
                            // frame (pressure-only, button-only) reuse the
                            // last known position instead of emitting (0,0).
                            if (!has_motion && captured) {
                                acc.dx = captured->last_x;
                                acc.dy = captured->last_y;
                            }
                            acc.buttons = button_state_;   // always current state
                            ring_.push(acc);
                            acc = {};
                            has_motion = false;
                            has_button_change = false;
                            has_pressure = false;
                        }
                        break;
                    default: break;
                }
            }
            if (rc == LIBEVDEV_READ_STATUS_SYNC) handle_sync_loss(dev);
            if (rc == -ENODEV) {
                // Device removed.
                for (size_t d = 0; d < devices_.size(); ++d) {
                    if (devices_[d].dev == dev) {
                        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, devices_[d].fd, nullptr);
                        libevdev_free(devices_[d].dev);
                        close(devices_[d].fd);
                        devices_.erase(devices_.begin() + d);
                        printf("[producer] device removed, %zu remain\n", devices_.size());
                        break;
                    }
                }
            }
        }
    }

    for (auto& d : devices_) {
        libevdev_free(d.dev);
        close(d.fd);
    }
    if (wake_fd_ != -1) close(wake_fd_);
    close(epoll_fd_);
}

} // namespace hid::evdev

#endif // __linux__
