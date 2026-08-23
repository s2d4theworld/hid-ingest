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
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <thread>
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

    bool start() {
        try { thread_ = std::thread([this] { run(); }); }
        catch (...) { return false; }
        return true;
    }
    void stop() {
        running_.store(false);
        if (thread_.joinable()) thread_.join();
    }

private:
    void run();
    bool discover_devices();
    void handle_sync_loss(struct libevdev* dev);

    SpscRing<>& ring_;
    EvdevProducerConfig cfg_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    int epoll_fd_ = -1;
    // Persistent button state: survives EV_SYN resets of the per-sample
    // accumulator, so mid-drag samples report the held buttons.
    uint16_t button_state_ = 0;
};

bool EvdevProducer::discover_devices() {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) return false;

    struct udev* udev = udev_new();
    if (!udev) { close(epoll_fd_); epoll_fd_ = -1; return false; }
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
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
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
    pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp);

    if (cfg_.core >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cfg_.core, &set);
        pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    }

    if (!discover_devices()) { fprintf(stderr, "[producer] no HID devices found\n"); return; }
    running_.store(true);

    epoll_event events[16];
    input_event ev;

    while (running_.load()) {
        const int nfds = epoll_wait(epoll_fd_, events, 16, -1);
        if (nfds < 0) { if (errno == EINTR) continue; break; }

        for (int i = 0; i < nfds; ++i) {
            struct libevdev* dev = nullptr;
            for (auto& d : devices_) if (d.fd == events[i].data.fd) dev = d.dev;
            if (!dev) continue;

            HidSample acc{};
            bool has_motion = false;
            bool has_button_change = false;   // press OR release this packet
            int rc;

            while ((rc = libevdev_next_event(dev, LIBEVDEV_READ_FLAG_NORMAL, &ev))
                   == LIBEVDEV_READ_STATUS_SUCCESS) {
                switch (ev.type) {
                    case EV_REL:
                        if (ev.code == REL_X) { acc.dx += ev.value; has_motion = true; }
                        if (ev.code == REL_Y) { acc.dy += ev.value; has_motion = true; }
                        break;
                    case EV_ABS:
                        if (ev.code == ABS_X) { acc.dx = ev.value << 8; has_motion = true; }  // -> 24.8
                        if (ev.code == ABS_Y) { acc.dy = ev.value << 8; has_motion = true; }
                        if (ev.code == ABS_PRESSURE)
                            acc.pressure = static_cast<uint16_t>(
                                ev.value * 65535 /
                                (libevdev_get_abs_maximum(dev, ABS_PRESSURE) ?: 1023));
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
                        if (ev.value) has_motion = true;  // press is a reportable event
                        else has_button_change = true;    // release too (unstuck drag)
                        break;
                    case EV_SYN:
                        if (ev.code == SYN_REPORT &&
                            (has_motion || has_button_change || button_state_)) {
                            acc.timestamp = static_cast<uint32_t>(
                                (uint64_t)ev.input_event_sec * 1000000 + ev.input_event_usec);
                            acc.buttons = button_state_;   // always current state
                            ring_.push(acc);
                            acc = {};
                            has_motion = false;
                            has_button_change = false;
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
    close(epoll_fd_);
}

} // namespace hid::evdev

#endif // __linux__
