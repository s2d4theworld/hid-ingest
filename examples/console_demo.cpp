// examples/console_demo.cpp — end-to-end smoke demo.
// Windows: starts the real Raw Input producer and prints live stats while
// you move the mouse. Ctrl+C triggers a clean shutdown (producer.stop()).
// On other platforms this exits with a note.
#include <chrono>
#include <csignal>
#include <cstdio>
#include <thread>

#include "hid_ingest/raw_input_producer.h"
#include "hid_ingest/spline.h"

namespace {
volatile std::sig_atomic_t g_quit = 0;
extern "C" void on_sigint(int) { g_quit = 1; }
}

int main() {
#if defined(_WIN32)
    using namespace hid;
    SpscRing<> ring;

    win32::ProducerConfig cfg;
    cfg.core_affinity_mask = 0;          // let OS pick; set e.g. 0x1000 to pin core 12
    win32::RawInputProducer producer(ring, cfg);
    if (!producer.start()) {
        fprintf(stderr, "failed to start raw input producer\n");
        return 1;
    }
    std::signal(SIGINT, on_sigint);
    printf("raw input producer running — move your mouse (Ctrl+C to quit)\n");

    alignas(64) HidSample batch[1024];
    Vec2 verts[4096];
    uint64_t frames = 0;
    while (!g_quit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(8));  // ~120 Hz consumer
        const size_t n = ring.pop_batch(batch, 1024);
        ++frames;
        if (n == 0) continue;

        const float step = 2.0f;  // adaptive-ish fixed step, px
        const size_t vcount = interpolate_batch(batch, n, step, verts, 4096);
        printf("\rframe %6llu | drained %4zu | verts %5zu | dropped %llu   ",
               (unsigned long long)frames, n, vcount,
               (unsigned long long)ring.dropped_approx());
        fflush(stdout);
    }

    // Clean shutdown: stop() wakes the message loop via WM_APP and joins.
    printf("\nshutting down...\n");
    producer.stop();
    return 0;
#else
    fprintf(stderr, "console_demo is Windows-only; build the Linux evdev producer on Linux.\n");
    return 0;
#endif
}
