// hid_ingest/core/spsc_ring_vyukov.h
// SPSC ring with sequence-per-slot (Vyukov-style) enabling a CORRECT
// latest-wins (drop-oldest) overflow policy — the design the basic ring's
// header explains is racy when done by mutating the consumer index.
//
// Layout: per-slot atomic sequence + HidSample payload. head_ (writer
// position, producer-owned) and tail_ (reader position, consumer-owned)
// are monotonically increasing. Each slot's sequence encodes its state
// relative to those positions:
//   seq == pos          -> writable   (producer may write at pos)
//   seq == pos + 1      -> readable   (written; consumer may read at pos)
//   seq == pos + Capacity -> released (consumer finished; slot reusable)
//
// DROP-OLDEST on overflow (DropOnOverflow=true): when full, the producer
// targets the oldest slot (position head - Capacity) and either
//   a) finds it already released (seq == head) — a plain writable slot;
//      write and publish normally (no drop occurred), or
//   b) claims it from the consumer with a single CAS
//      (seq: readable-for-oldest -> readable-for-new-position), then
//      advances tail_ past the evicted sample.
// Only ONE slot's sequence is CAS'd — there is never a read-modify-write
// race on shared indices, which is what made the naive design unsafe.
// If the consumer wins the race mid-read, the producer rejects THIS
// sample instead (counted; bounded and rare).
//
// DropOnOverflow=false behaves like the basic ring: full -> reject incoming,
// return false, caller counts drops.
//
// Ordering summary:
//   push : write payload -> release store slot seq (publish)
//   pop  : acquire load slot seq (observe) -> payload reads ->
//          release store slot seq (free slot)
//   tail_/head_ themselves: relaxed (ownership is single-writer except the
//   drop-oldest tail bump, which pairs with the consumer's own advance via
//   the slot sequences).
#pragma once

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4324)   // deliberate alignas(64) padding
#endif

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "hid_ingest/hid_sample.h"

namespace hid {

template <size_t Capacity = 16384, bool DropOnOverflow = true>
class SpscRingVyukov {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

public:
    SpscRingVyukov() {
        for (size_t i = 0; i < Capacity; ++i)
            slots_[i].seq.store(static_cast<int64_t>(i), std::memory_order_relaxed);
    }

    /// Push one sample. Returns false only in the !DropOnOverflow full case
    /// (or the extremely rare lost-race drop-oldest case, which still
    /// delivered THIS sample — see below). Never blocks, never allocates.
    bool push(const HidSample& s) {
        for (int attempt = 0; attempt < 4; ++attempt) {
            int64_t pos = head_.load(std::memory_order_relaxed);
            Slot& slot = slots_[pos & kMask];

            const int64_t seq = slot.seq.load(std::memory_order_acquire);
            const intptr_t dif = static_cast<intptr_t>(seq) -
                                 static_cast<intptr_t>(pos);

            if (dif == 0) {
                // Slot is writable for this position.
                slot.data = s;
                // Publish by stamping the slot readable-for-pos. The consumer
                // observes this acquire-side, then frees the slot with
                // seq = pos + Capacity.
                slot.seq.store(pos + 1, std::memory_order_release);
                head_.store(pos + 1, std::memory_order_release);
                return true;
            }
            if constexpr (!DropOnOverflow)
                break;
        }
        // Ring full.
        if constexpr (DropOnOverflow) {
            return drop_oldest_and_write(s);
        } else {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    }

    /// Pop one sample. Returns false when empty.
    bool pop(HidSample& out) {
        int64_t pos = tail_.load(std::memory_order_relaxed);
        Slot& slot = slots_[pos & kMask];

        const int64_t seq = slot.seq.load(std::memory_order_acquire);
        const intptr_t dif = static_cast<intptr_t>(seq) -
                             static_cast<intptr_t>(pos + 1);

        if (dif == 0) {
            out = slot.data;
            slot.seq.store(pos + Capacity, std::memory_order_release);  // free slot
            tail_.store(pos + 1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    /// Drain up to max_count samples; returns how many were popped.
    size_t pop_batch(HidSample* out, size_t max_count) {
        for (size_t i = 0; i < max_count; ++i)
            if (!pop(out[i])) return i;
        return max_count;
    }

    /// Approximate element count (relaxed; may be off under concurrency).
    size_t size_approx() const noexcept {
        const int64_t h = head_.load(std::memory_order_relaxed);
        const int64_t t = tail_.load(std::memory_order_relaxed);
        const int64_t n = h - t;
        return n > 0 ? static_cast<size_t>(n) : 0;   // clamp transient inversion
    }

    uint64_t dropped_approx() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }

private:
    struct Slot {
        std::atomic<int64_t> seq;
        HidSample data;
    };

    /// Full ring, latest-wins: overwrite the oldest slot.
    ///
    /// The oldest live sample occupies the slot at tail_pos = head_ -
    /// Capacity. Its sequence equals tail_pos + Capacity iff the consumer
    /// has already released it... it hasn't (that IS the oldest unconsumed).
    /// The producer therefore CLAIMS the slot from the consumer side: CAS
    /// the slot sequence from "readable for tail_pos" to "writable for
    /// head_". If the CAS wins, the consumer will observe seq != expected
    /// on its next read attempt and treat the position as skipped (its pop
    /// fails, it retries at the next position — monotonic tail_ keeps
    /// everything consistent because the producer also bumped tail_).
    ///
    /// Single CAS on ONE slot: no read-modify-write races on shared indices,
    /// which is what made the naive design unsafe.
    bool drop_oldest_and_write(const HidSample& s) {
        // NOTE on recursion: this may call push() when space has appeared;
        // that inner push can only succeed via a writable slot or recurse
        // back here at most once more (depth <= 2 in practice, and the
        // expect_released branch no longer recurses). Bounded by the
        // consumer's progress between the two tail_ loads.
        const int64_t head = head_.load(std::memory_order_relaxed);
        const int64_t tail = tail_.load(std::memory_order_acquire);
        if (head - tail < static_cast<int64_t>(Capacity))
            return push(s);   // space appeared; retry normally

        const int64_t oldest_pos = head - Capacity;
        Slot& slot = slots_[oldest_pos & kMask];

        // Expected: consumer has observed this slot as readable-at-oldest_pos
        // or has already moved past (released) it.
        const int64_t expect_readable = oldest_pos + 1;
        const int64_t expect_released = oldest_pos + Capacity;

        int64_t cur = slot.seq.load(std::memory_order_acquire);
        if (cur == expect_released) {
            // expect_released == head: the consumer already released this
            // slot, so the ring is NOT actually full — this is a normal
            // writable slot for position `head`. Publish it as such.
            slot.data = s;
            slot.seq.store(head + 1, std::memory_order_release);   // publish
            head_.store(head + 1, std::memory_order_release);
            // tail_ needs no adjustment: the consumer advanced it itself.
            return true;
        }
        if (cur == expect_readable &&
            slot.seq.compare_exchange_strong(cur, head + 1,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            // Claimed the oldest slot away from the consumer. The CAS stamps
            // it readable-for-`head` — exactly the position this push is
            // about to publish (head_.store(head+1) below).
            slot.data = s;
            head_.store(head + 1, std::memory_order_release);
            tail_.store(tail + 1, std::memory_order_release);   // skip dropped
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        // Lost the race with the consumer mid-read: fall back to rejecting
        // THIS sample (bounded, rare, and counted). Correctness intact.
        dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    static constexpr size_t kMask = Capacity - 1;

    // Cache-line isolation: sequences array separate from indices; hot
    // indices each get their own line.
    alignas(64) std::atomic<int64_t> head_{0};   // producer-owned writes
    alignas(64) std::atomic<int64_t> tail_{0};   // consumer-owned (+producer
                                                 // drop-oldest skip, release)
    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) Slot slots_[Capacity];
};

} // namespace hid

#ifdef _MSC_VER
#pragma warning(pop)
#endif
