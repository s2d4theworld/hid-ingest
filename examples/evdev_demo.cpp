// examples/evdev_demo.cpp — Linux smoke demo.
// Starts the evdev producer and prints live stats while you move the mouse.
// Ctrl+C triggers a clean shutdown (producer.stop()).
// Requires read access to /dev/input/event* (often the 'input' group).
//
// NOTE: EvdevProducer is defined inside src/linux_evdev_producer.cpp
// (namespace hid::evdev, class name EvdevProducer). This demo #includes the
// .cpp directly so it works without exposing a producer header — acceptable
// for an example target.
#define HID_INGEST_DEMO 1
#include <csignal>
#include "../src/linux_evdev_producer.cpp"

int main() {
    using namespace hid;
    SpscRing<> ring;

    evdev::EvdevProducerConfig cfg;
    cfg.fifo_priority = 80;   // needs root/CAP_SYS_NICE; warning printed if denied
    evdev::EvdevProducer producer(ring, cfg);
    if (!producer.start()) {
        fprintf(stderr,
                "failed to start evdev producer — check device permissions "
                "(add user to 'input' group) and that a mouse/tablet is plugged in\n");
        return 1;
    }
    std::signal(SIGINT, [](int) { std::signal(SIGINT, SIG_DFL); std::raise(SIGINT); });
    printf("evdev producer running — move your mouse (Ctrl+C to quit)\n");

    alignas(64) HidSample batch[1024];
    uint64_t frames = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(8));  // ~120 Hz consumer
        const size_t n = ring.pop_batch(batch, 1024);
        frames += n;
        if (n == 0) continue;

        // Latest sample wins for display; values are fixed-point 24.8
        // (absolute coords for tablets, last-frame delta for REL mice).
        const HidSample& s = batch[n - 1];
        printf("\rframe %6llu | drained %4zu | dxy (%d,%d)/256 px | btns %04x | dropped %llu   ",
               (unsigned long long)frames, n, s.dx, s.dy, s.buttons,
               (unsigned long long)ring.dropped_approx());
        fflush(stdout);
    }
}
