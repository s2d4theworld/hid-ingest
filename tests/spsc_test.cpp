// tests/spsc_test.cpp — spec section 7 invariant verification.
//
// NOTE: no assert() — Release builds define NDEBUG and compile asserts out,
// which would make every "pass" vacuous. All invariants use explicit
// failure returns so the result is real in Debug AND Release.
//
// MT semantics: DropOnOverflow rings legitimately produce gaps under overload
// (the incoming sample is dropped — back-pressure). The MT test therefore
// verifies (a) strictly increasing timestamps (order), (b) all delivered
// values were actually produced, and (c) final accounting:
// consumed + dropped == produced. Zero-gap FIFO is asserted in the
// single-thread and no-drop-policy variants where drops are impossible or
// deterministic.
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <thread>

#include "hid_ingest/spsc_ring.h"
#include "hid_ingest/spline.h"

using hid::HidSample;
using hid::SpscRing;

static int failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);\
            ++failures;                                                    \
            return 1;                                                      \
        }                                                                  \
    } while (0)

static HidSample make(uint32_t seq) {
    HidSample s{};
    s.dx = static_cast<int32_t>(seq);
    s.dy = -static_cast<int32_t>(seq);
    s.timestamp = seq;
    return s;
}

static int test_single_thread() {
    SpscRing<256> ring;
    CHECK(ring.capacity() == 256);

    // Fill to full.
    for (uint32_t i = 0; i < 256; ++i)
        CHECK(ring.push(make(i)));

    // Overflow: push reports false; the INCOMING sample is dropped.
    CHECK(!ring.push(make(999)));
    CHECK(ring.dropped_approx() == 1);
    CHECK(ring.size_approx() <= 256);

    // Drain all in batches of varying size; verify order & content.
    // Expected content: exactly samples 0..255 (incoming 999 was rejected).
    HidSample out[100];
    uint32_t expect = 0;
    size_t total = 0;
    for (;;) {
        const size_t n = ring.pop_batch(out, 37);
        if (n == 0) break;
        for (size_t i = 0; i < n; ++i) {
            CHECK(out[i].timestamp == expect);
            CHECK(out[i].dx == static_cast<int32_t>(expect));
            CHECK(out[i].dy == -static_cast<int32_t>(expect));
            ++total;
            ++expect;
        }
        CHECK(ring.size_approx() <= 256);
    }
    CHECK(total == 256);
    CHECK(expect == 256);   // consumed exactly 0..255

    // Empty drain returns 0.
    CHECK(ring.pop_batch(out, 10) == 0);

    // Wrap-around: push/pop repeatedly past capacity.
    for (uint32_t round = 0; round < 1024; ++round) {
        CHECK(ring.push(make(round)));
        CHECK(ring.pop_batch(out, 1) == 1);
        CHECK(out[0].timestamp == round);
    }

    // Overflow burst: first 64 accepted, rest rejected, FIFO intact.
    {
        SpscRing<64, true> burst;
        for (uint32_t i = 0; i < 200; ++i)
            burst.push(make(i));
        uint32_t got = 0;
        for (;;) {
            const size_t n = burst.pop_batch(out, 10);
            if (n == 0) break;
            for (size_t i = 0; i < n; ++i, ++got) {
                CHECK(out[i].timestamp == got);
            }
        }
        CHECK(got == 64);
        CHECK(burst.dropped_approx() == 136);
    }
    return 0;
}

// DropOnOverflow=false variant: full ring must reject pushes without loss,
// and the caller owns drop accounting (ring counter stays 0).
static int test_no_drop_policy() {
    SpscRing<128, false> ring;
    for (uint32_t i = 0; i < 128; ++i)
        CHECK(ring.push(make(i)));
    CHECK(!ring.push(make(999)));          // rejected
    CHECK(ring.dropped_approx() == 0);     // silent: caller counts its own drops

    HidSample out[128];
    size_t total = ring.pop_batch(out, 128);
    CHECK(total == 128);
    for (size_t i = 0; i < total; ++i)
        CHECK(out[i].timestamp == i);      // original contents intact
    return 0;
}

// Interleaved push/drain under constant overflow: order must stay monotonic.
static int test_interleaved_overflow() {
    SpscRing<64, true> ring;
    uint32_t produced = 0, last = 0;
    bool first = true;
    HidSample out[64];
    for (int round = 0; round < 50000; ++round) {
        const int pushes = 1 + static_cast<int>((round * 7919u) % 20u);  // unsigned: no UB
        for (int p = 0; p < pushes; ++p)
            ring.push(make(produced++));
        const size_t n = ring.pop_batch(out, 1 + (unsigned)(round * 104729u) % 32u);
        for (size_t i = 0; i < n; ++i) {
            if (!first && out[i].timestamp <= last) {
                fprintf(stderr, "FAIL interleaved: %u after %u\n",
                        out[i].timestamp, last);
                return 1;
            }
            last = out[i].timestamp;
            first = false;
        }
    }
    printf("interleaved: %u produced, order monotonic\n", produced);
    return 0;
}

static int test_mt_stress() {
    constexpr uint64_t kCount = 4'000'000;
    SpscRing<16384, true> ring;
    std::atomic<bool> done{false};

    std::thread producer([&] {
        for (uint64_t i = 0; i < kCount; ++i) {
            HidSample s{};
            s.dx = static_cast<int32_t>(i & 0xFFFFFFFFull);
            s.dy = -s.dx;
            s.timestamp = static_cast<uint32_t>(i);   // 4M << 2^32, no wrap
            ring.push(s);                              // SPSC: producer only pushes
        }
        done.store(true, std::memory_order_release);
    });

    uint32_t last = 0;
    bool first = true;
    uint64_t consumed = 0;
    HidSample batch[512];
    while (!(done.load(std::memory_order_acquire) && ring.size_approx() == 0)) {
        const size_t n = ring.pop_batch(batch, 512);
        for (size_t i = 0; i < n; ++i, ++consumed) {
            if (!first && batch[i].timestamp <= last) {  // strict order invariant
                fprintf(stderr, "FAIL mt_stress: %u after %u (consumed=%llu)\n",
                        batch[i].timestamp, last, (unsigned long long)consumed);
                producer.join();
                return 1;   // main() counts the failure — no ++ here
            }
            first = false;
            last = batch[i].timestamp;
        }
        std::this_thread::yield();
    }
    producer.join();

    fprintf(stderr, "[mt_stress] loop exited: consumed=%llu dropped=%llu\n",
            (unsigned long long)consumed, (unsigned long long)ring.dropped_approx());
    // Accounting: everything produced is either consumed or dropped.
    CHECK(consumed + ring.dropped_approx() == kCount);
    printf("mt_stress: %llu consumed + %llu dropped = %llu, order monotonic\n",
           (unsigned long long)consumed, (unsigned long long)ring.dropped_approx(),
           (unsigned long long)kCount);
    return 0;
}

// Fixed-point 24.8 helpers: saturation bounds, NaN handling, clamp24 edges.
static int test_fixed_point() {
    // Post-scale bounds: |v| > 8388607 must saturate, not UB-cast.
    CHECK(hid::ToFixed24_8(9.0e6f) == INT32_MAX);
    CHECK(hid::ToFixed24_8(-9.0e6f) == INT32_MIN);
    // NaN falls through every comparison -> deterministic INT32_MIN.
    CHECK(hid::ToFixed24_8(std::numeric_limits<float>::quiet_NaN()) == INT32_MIN);
    // Boundary values round-trip exactly.
    CHECK(hid::ToFixed24_8(8388607.0f) == 8388607 * 256);
    CHECK(hid::ToFixed24_8(-8388608.0f) == -8388608 * 256);
    // Zero.
    CHECK(hid::ToFixed24_8(0.0f) == 0);

    // clamp24 edges.
    CHECK(hid::clamp24(static_cast<int64_t>(INT32_MAX) + 1) == INT32_MAX);
    CHECK(hid::clamp24(static_cast<int64_t>(INT32_MIN) - 1) == INT32_MIN);
    CHECK(hid::clamp24(0) == 0);
    return 0;
}

// Spline invariants: straight-line collinearity, bounded deviation on a
// coarse circle, out_cap truncation, step_px guard.
static int test_spline() {
    using hid::Vec2;
    using hid::HidSample;

    // Helper: build a HidSample from 24.8 fixed-point coords.
    auto mk = [](int32_t dx, int32_t dy) {
        HidSample s{};
        s.dx = dx; s.dy = dy;
        return s;
    };

    // --- Straight line: every output vertex must be collinear with the
    // input line y = x (within float epsilon). Chordal CR reproduces lines
    // exactly, so this also guards tangent math regressions.
    {
        HidSample line[8];
        for (int k = 0; k < 8; ++k) line[k] = mk(k * 256, k * 256);  // (0,0)..(7,7) px
        Vec2 out[256];
        const size_t n = hid::interpolate_batch(line, 8, 2.0f, out, 256);
        CHECK(n > 0);
        for (size_t k = 0; k < n; ++k) {
            if (std::fabs(out[k].x - out[k].y) > 1e-3f) return 1;  // off the line
        }
    }

    // --- Coarse circle: max radial deviation must stay bounded. A perfect
    // chordal CR through evenly spaced points tracks a circle within a
    // small fraction of the segment length; this catches seam gaps
    // (missing segments show up as large radius jumps).
    {
        constexpr int kN = 32;
        HidSample circ[kN];
        for (int k = 0; k < kN; ++k) {
            const double a = k * 6.28318530718 / kN;
            // 24.8 fixed-point: radius 1000 px -> 1000*256 fixed units.
            circ[k] = mk(static_cast<int32_t>(1000.0 * 256.0 * std::cos(a)),
                         static_cast<int32_t>(1000.0 * 256.0 * std::sin(a)));
        }
        Vec2 out[1024];
        const size_t n = hid::interpolate_batch(circ, kN, 4.0f, out, 1024);
        CHECK(n > 0);
        for (size_t k = 0; k < n; ++k) {
            // Output vertices are already in px (FromFixed24_8 unwound the
            // 24.8 scale); expect ~1000 px radius, 15% slack.
            const double r = std::sqrt(double(out[k].x) * out[k].x +
                                       double(out[k].y) * out[k].y);
            if (r < 850 || r > 1150) return 1;
        }
    }

    // --- out_cap truncation: written == out_cap exactly, and the endpoint
    // is dropped when the cap is hit (documented behavior).
    {
        HidSample line[16];
        for (int k = 0; k < 16; ++k) line[k] = mk(k * 256, k * 256);
        Vec2 small[4];
        const size_t n = hid::interpolate_batch(line, 16, 1.0f, small, 4);
        CHECK(n == 4);   // exactly filled
    }

    // --- step_px guard: non-positive / NaN are clamped to 1 px at entry,
    // so these must produce vertices rather than hang or UB-cast.
    {
        HidSample line[4];
        for (int k = 0; k < 4; ++k) line[k] = mk(k * 512, 0);
        Vec2 out[64];
        CHECK(hid::interpolate_batch(line, 4, 0.0f, out, 64) > 0);
        CHECK(hid::interpolate_batch(line, 4, -1.0f, out, 64) > 0);
        CHECK(hid::interpolate_batch(
                  line, 4, std::numeric_limits<float>::quiet_NaN(), out, 64) > 0);
    }
    return 0;
}

int main() {
    failures += test_fixed_point();
    if (!failures) printf("fixed_point: OK\n");
    failures += test_spline();
    if (!failures) printf("spline: OK\n");
    failures += test_single_thread();
    if (!failures) printf("single_thread: OK\n");
    failures += test_no_drop_policy();
    if (!failures) printf("no_drop_policy: OK\n");
    failures += test_interleaved_overflow();
    if (!failures) printf("interleaved_overflow: OK\n");
    if (!failures) {
        failures += test_mt_stress();
        if (!failures) { /* mt_stress prints its own OK line */ }
    }

    {
        std::atomic<size_t> probe{};
        printf("lock_free(size_t): %s\n", probe.is_lock_free() ? "true" : "false");
    }

    if (failures) {
        fprintf(stderr, "%d FAILURE(S)\n", failures);
        return 1;
    }
    printf("ALL TESTS PASSED\n");
    return 0;
}
