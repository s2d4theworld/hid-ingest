// hid_ingest/core/spsc_ring.h
// Lock-free SPSC ring buffer with cache-line isolation — spec section 2.
//
// Template params:
//   Capacity       - power of two slot count (default 16384)
//   DropOnOverflow - true  (default): full ring drops the INCOMING sample
//                    (back-pressure) and bumps the drop counter.
//                    false : full ring rejects the sample silently; caller
//                    detects via false return and counts drops itself.
//
// WHY NOT drop-OLDEST: dropping the oldest requires the producer to advance
// the consumer index (tail_), which races with the consumer's own advances —
// a lost-update on a shared atomic that cannot be resolved without a CAS or
// per-slot sequence numbers. A correct drop-oldest needs a Vyukov-style
// seq-per-slot design (see README "Future Work"). For input ingestion drained
// every frame, an overflowing 16384-deep ring means the consumer stalled for
// ~65 ms at 8 kHz — back-pressure is the honest failure mode and the drop
// counter surfaces it.
//
// Memory ordering:
//   push: payload write -> release store of head_ (publish).
//   pop : acquire load of head_ (observe) -> payload reads ->
//         release store of tail_ (free slots).
#pragma once

// MSVC C4324 ("structure was padded due to alignment specifier") fires on
// every SpscRing instantiation. The padding is the deliberate cache-line
// isolation this header exists to provide — silence it scoped.
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4324)
#endif

#include <atomic>
#include <cstddef>
#include <new>
#include <type_traits>

#include "hid_ingest/hid_sample.h"

namespace hid {

template <size_t Capacity = 16384, bool DropOnOverflow = true>
class SpscRing {
    static_assert((Capacity & (Capacity - 1)) == 0 && Capacity != 0,
                  "Capacity must be a power of two.");

public:
    constexpr SpscRing() noexcept = default;

    SpscRing(const SpscRing&)            = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    /// Producer side: push one sample.
    /// Returns true when appended normally; false when the ring was full and
    /// the incoming sample was dropped. With DropOnOverflow=true the drop is
    /// counted in dropped_approx(); with false the caller must count it.
    bool push(const HidSample& sample) noexcept {
        const size_t head = head_.load(std::memory_order_relaxed);
        size_t tail = tail_cached_;

        if (head - tail == Capacity) {
            tail = tail_.load(std::memory_order_acquire);
            if (head - tail == Capacity) {          // genuinely full
                if constexpr (DropOnOverflow) {
                    // back-pressure: incoming sample dropped (see header note)
                    drop_counter_.fetch_add(1, std::memory_order_relaxed);
                }
                return false;
            }
            tail_cached_ = tail;
        }

        const size_t idx = head & (Capacity - 1);
        ring_[idx] = sample;                                  // write payload
        head_.store(head + 1, std::memory_order_release);     // publish
        return true;
    }

    /// Consumer side: drain up to max_count samples into out[].
    /// Returns number of samples drained (0 when empty).
    size_t pop_batch(HidSample* out, size_t max_count) noexcept {
        const size_t tail = tail_.load(std::memory_order_relaxed);  // own index
        const size_t head = head_.load(std::memory_order_acquire);

        if (head <= tail) return 0;                    // empty

        const size_t available = head - tail;
        const size_t count = (available < max_count) ? available : max_count;

        for (size_t i = 0; i < count; ++i) {
            const size_t idx = (tail + i) & (Capacity - 1);
            out[i] = ring_[idx];
        }

        tail_.store(tail + count, std::memory_order_release); // free slots
        return count;
    }

    size_t size_approx() const noexcept {
        // Clamp: called off-thread, head/tail loads can interleave so the
        // unsigned subtraction transiently underflows.
        const size_t h = head_.load(std::memory_order_acquire);
        const size_t t = tail_.load(std::memory_order_acquire);
        return h > t ? (h - t) : 0;
    }
    uint64_t dropped_approx() const noexcept {
        return drop_counter_.load(std::memory_order_relaxed);
    }
    static constexpr size_t capacity() noexcept { return Capacity; }

private:
    alignas(64) HidSample ring_[Capacity]{};

    // Producer-owned cache line.
    alignas(64) std::atomic<size_t> head_{0};
    size_t tail_cached_ = 0;
    // Atomic: read cross-thread via dropped_approx(); producer writes with
    // relaxed stores — counter is telemetry, exact snapshot not required,
    // but the standard forbids unsynchronized concurrent access.
    std::atomic<uint64_t> drop_counter_{0};

    // Consumer-owned cache line.
    alignas(64) std::atomic<size_t> tail_{0};
};

static_assert(std::is_standard_layout_v<SpscRing<>>);

} // namespace hid

#ifdef _MSC_VER
#pragma warning(pop)
#endif
