// hid_ingest/core/spsc_ring.h
// Lock-free SPSC ring buffer with cache-line isolation — spec section 2.
//
// Template params:
//   Capacity      - power of two slot count (default 16384)
//   DropOnOverflow - when true, push() on a full ring advances the consumer
//                    tail to drop the OLDEST sample (latest-wins semantics).
#pragma once

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

    // Non-copyable, non-movable (atomics + shared state).
    SpscRing(const SpscRing&)            = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    /// Producer side: push one sample. Returns false if the ring was full
    /// and DropOnOverflow == false. With DropOnOverflow == true it drops
    /// the oldest sample and still returns false for the rejected write
    /// (the incoming sample takes the newest slot).
    bool push(const HidSample& sample) noexcept {
        const size_t head = head_.load(std::memory_order_relaxed);
        size_t tail = tail_cached_;

        if (head - tail == Capacity) {
            tail = tail_.load(std::memory_order_acquire);
            if (head - tail == Capacity) {
                if constexpr (DropOnOverflow) {
                    ++tail_cached_;          // drop oldest sample
                    ++drop_counter_;
                    // Overwrite oldest slot with the new sample.
                    const size_t idx = head & (Capacity - 1);
                    ring_[idx] = sample;
                    head_.store(head + 1, std::memory_order_release);
                    return false;
                }
                ++drop_counter_;
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
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t head = head_cached_;

        if (head == tail) {
            head = head_.load(std::memory_order_acquire);
            if (head == tail) return 0;
            head_cached_ = head;
        }

        const size_t available = head - tail;
        const size_t count = (available < max_count) ? available : max_count;

        for (size_t i = 0; i < count; ++i) {
            const size_t idx = (tail + i) & (Capacity - 1);
            out[i] = ring_[idx];
        }

        tail_.store(tail + count, std::memory_order_release); // release consumed
        return count;
    }

    // Approximate sizes (relaxed; for telemetry only).
    size_t size_approx() const noexcept {
        return head_.load(std::memory_order_acquire) - tail_.load(std::memory_order_acquire);
    }
    uint64_t dropped_approx() const noexcept { return drop_counter_; }

    static constexpr size_t capacity() noexcept { return Capacity; }

private:
    alignas(64) HidSample ring_[Capacity]{};

    // Producer-owned cache line.
    alignas(64) std::atomic<size_t> head_{0};
    size_t tail_cached_ = 0;
    uint64_t drop_counter_ = 0;

    // Consumer-owned cache line.
    alignas(64) std::atomic<size_t> tail_{0};
    size_t head_cached_ = 0;
};

static_assert(std::is_standard_layout_v<SpscRing<>>);

} // namespace hid
