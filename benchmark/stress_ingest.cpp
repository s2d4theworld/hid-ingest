// benchmark/stress_ingest.cpp — spec section 7.1 synthetic stress test.
// Injects HidSamples at exactly 8 kHz (125 us interval) from a producer
// thread while the main thread drains once per simulated frame tick.
// Reports: delivered count, dropped count, worst/avg push latency.
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "hid_ingest/spsc_ring.h"

using clock_hi = std::chrono::steady_clock;
using hid::HidSample;
using hid::SpscRing;

int main(int argc, char** argv) {
    uint64_t target = 10'000'000;   // ~21 min at 8 kHz; spec's 10^8 via argv
    if (argc > 1) target = std::strtoull(argv[1], nullptr, 10);
    const auto interval = std::chrono::microseconds(125);  // 8 kHz

    SpscRing<> ring;
    std::atomic<bool> producer_done{false};
    uint64_t pushed = 0, worst_us = 0;
    double sum_us = 0.0;

    std::thread producer([&] {
        auto next = clock_hi::now();
        for (uint64_t i = 0; i < target; ++i) {
            next += interval;
            std::this_thread::sleep_until(next);

            HidSample s{};
            s.dx = static_cast<int32_t>(i & 0xFF);
            s.dy = static_cast<int32_t>((i >> 8) & 0xFF);
            s.timestamp = static_cast<uint32_t>(i);

            const auto t0 = clock_hi::now();
            ring.push(s);
            const auto t1 = clock_hi::now();
            const uint64_t us = (uint64_t)std::chrono::duration_cast<
                std::chrono::nanoseconds>(t1 - t0).count() / 1000;
            if (us > worst_us) worst_us = us;
            sum_us += (double)us;
            ++pushed;

            // Simulate background CPU load every 4096 samples.
            if ((i & 0xFFF) == 0) {
                volatile double x = 1.0;
                for (int k = 0; k < 20000; ++k) x = x * 1.0000001 + 0.5;
            }
        }
        producer_done.store(true);
    });

    alignas(64) HidSample batch[1024];
    uint64_t drained = 0;
    const auto t_start = clock_hi::now();
    while (!producer_done.load() || ring.size_approx() > 0) {
        drained += ring.pop_batch(batch, 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(4));  // ~250 Hz frame
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        clock_hi::now() - t_start).count();

    producer.join();

    printf("target=%llu pushed=%llu drained=%llu dropped=%llu\n",
           (unsigned long long)target, (unsigned long long)pushed,
           (unsigned long long)drained, (unsigned long long)ring.dropped_approx());
    printf("push latency: avg=%.3f us worst=%llu us\n",
           sum_us / (double)pushed, (unsigned long long)worst_us);
    printf("wall time: %lld ms | effective ingest: %.1f Hz\n",
           (long long)elapsed, (double)pushed * 1000.0 / (double)elapsed);
    printf(drained == target ? "RESULT: PASS (zero loss)\n" : "RESULT: CHECK (loss/drop occurred)\n");
    return drained == target ? 0 : 1;
}
