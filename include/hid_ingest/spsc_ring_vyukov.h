// hid_ingest/core/spsc_ring_vyukov.h
// SPSC ring with sequence-per-slot (Vyukov-style) and a CORRECT latest-wins
// (drop-oldest) overflow policy.
//
// DESIGN (v3 — skip watermark, no producer tail_ writes)
//
// Ownership invariants:
//   - head_ is written ONLY by the producer.
//   - tail_ is written ONLY by the consumer.
//     (v1 violated this: the producer bumped tail_ during drop-oldest,
//      desynchronizing the consumer and allowing torn copies. v2 tried to
//      encode skips in slot sequences alone — but once the producer reuses
//      a skipped slot, the consumer cannot distinguish "skipped" from "not
//      yet published" and stalls forever. v3 adds an explicit watermark.)
//   - skip_to_ is written ONLY by the producer: the position one past the
//     last evicted (dropped-oldest) sample. Monotonically non-decreasing.
//
// Slot sequence states (per-slot atomic int64):
//   seq == pos              -> writable  (producer may write at pos)
//   seq == pos + 1          -> readable  (written; consumer may read)
//   seq == released(pos)    -> free      (pos + Capacity; reusable next lap)
//
// DROP-OLDEST PROTOCOL:
// When full at position `pos`, the oldest live sample is at
// oldest = pos - Capacity. The producer CASes that slot's sequence from
// readable(oldest) directly to released(oldest) — atomically skipping the
// pop+release step — bumps skip_to_ to oldest+1, counts a drop, then writes
// its payload into its own slot at pos (which is now legitimately writable).
//
// The CONSUMER, before each pop, fast-forwards: while tail_'s slot holds a
// released stamp (seq == released(pos)), it advances tail_, counting each
// as a drop. Because released stamps are only ever created by the producer
// (drop path) or by the consumer's own pop, and the consumer checks them
// BEFORE reading, a torn copy is impossible: any slot the producer may
// overwrite has already been released by the time the producer CASes past
// it — the CAS itself is the synchronization point.
//
// Ordering summary:
//   push : write payload -> release store slot seq = readable(pos) ->
//          head_ store (release). Drop path: acq_rel CAS on the victim
//          slot + release store of skip_to_.
//   pop  : catch-up (acquire loads; release stores freeing slots) ->
//          acquire load slot seq -> payload copy -> release store
//          seq = released(pos); tail_ store relaxed.
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

    /// Push one sample. Returns false only in the !DropOnOverflow full case.
    /// With DropOnOverflow=true the push always succeeds (possibly evicting
    /// the oldest sample). Never blocks, never allocates.
    bool push(const HidSample& s) {
        const int64_t pos = head_.load(std::memory_order_relaxed);
        Slot& slot = slots_[pos & kMask];

        // Writable iff seq == pos (initial turn or fully released turn).
        int64_t seq = slot.seq.load(std::memory_order_acquire);
        if (seq != pos) {
            if constexpr (!DropOnOverflow) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            return drop_oldest_and_write(s, pos);
        }

        slot.data = s;
        // Publish: readable for pos. Release pairs with the consumer's
        // acquire load before its payload copy.
        slot.seq.store(pos + 1, std::memory_order_release);
        head_.store(pos + 1, std::memory_order_release);
        return true;
    }

    /// Pop one sample. Returns false when empty (or everything remaining
    /// was already skipped/dropped).
    bool pop(HidSample& out) {
        catch_up_drops();   // consumer-side: advance tail_ past dropped

        const int64_t pos = tail_.load(std::memory_order_relaxed);
        Slot& slot = slots_[pos & kMask];

        const int64_t seq = slot.seq.load(std::memory_order_acquire);
        if (seq != pos + 1)
            return false;   // not yet published (empty)

        out = slot.data;    // safe: producer published via release; the slot
                            // cannot be overwritten until WE release it
        slot.seq.store(released(pos), std::memory_order_release);
        tail_.store(pos + 1, std::memory_order_relaxed);
        return true;
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
        return n > 0 ? static_cast<size_t>(n) : 0;
    }

    uint64_t dropped_approx() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }

private:
    struct Slot {
        std::atomic<int64_t> seq;
        HidSample data;
    };

    static constexpr int64_t released(int64_t pos) { return pos + static_cast<int64_t>(Capacity); }

    static constexpr size_t kMask = Capacity - 1;

    /// Overflow (DropOnOverflow=true): evict the oldest live sample.
    ///
    /// oldest_pos = pos - Capacity. Its slot sequence is one of:
    ///   released(oldest_pos) — consumer already freed it: plain writable;
    ///                          nothing is being dropped. Fall back to the
    ///                          normal write attempt.
    ///   readable(oldest_pos) — still unconsumed: atomically skip it by
    ///                          CASing readable(oldest_pos) ->
    ///                          released(oldest_pos) (the exact value a
    ///                          legit pop+release would leave). Record the
    ///                          eviction in skip_to_ so the consumer knows
    ///                          to advance past it, count the drop, then
    ///                          write our payload at pos.
    /// Anything else — the consumer is mid-read on this slot; reject THIS
    /// sample (counted; bounded and rare).
    ///
    /// CRITICAL: never writes tail_. The consumer discovers the eviction
    /// through the slot's released stamp during its own catch-up pass.
    bool drop_oldest_and_write(const HidSample& s, int64_t pos) {
        const int64_t oldest_pos = pos - static_cast<int64_t>(Capacity);
        Slot& victim = slots_[oldest_pos & kMask];
        int64_t cur = victim.seq.load(std::memory_order_acquire);

        if (cur == released(oldest_pos)) {
            // Already freed — nothing to evict. Our target slot at pos must
            // be writable now; retry the normal path.
            return push(s);
        }

        if (cur == oldest_pos + 1) {
            // Atomically skip the oldest sample: readable -> released.
            if (victim.seq.compare_exchange_strong(
                    cur, released(oldest_pos),
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                // Publish the eviction watermark BEFORE advancing head_,
                // so the consumer can never observe head_ covering skipped
                // positions without also seeing skip_to_ cover them.
                skip_to_.store(oldest_pos + 1, std::memory_order_release);
                dropped_.fetch_add(1, std::memory_order_relaxed);

                // Our own target slot is now within range; write normally.
                Slot& target = slots_[pos & kMask];
                const int64_t tseq = target.seq.load(std::memory_order_acquire);
                if (tseq == pos) {
                    target.data = s;
                    target.seq.store(pos + 1, std::memory_order_release);
                    head_.store(pos + 1, std::memory_order_release);
                    return true;
                }
                return push(s);   // became writable meanwhile; normal path
            }
            // Lost the CAS: consumer popped-and-released between load and
            // CAS. Retry via normal push (the ring now has space).
            return push(s);
        }

        // Consumer mid-read on the victim slot: reject THIS sample.
        dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    /// Consumer-only: advance tail_ past every position the producer has
    /// evicted (skip_to_ watermark), counting the drops. Slots are NOT
    /// re-stamped here: the producer's drop path already left them in a
    /// valid state (either released(pos) or — if it reused the slot for a
    /// newer sample — readable-for-that-newer-position, which the consumer
    /// must not clobber).
    void catch_up_drops() {
        const int64_t t = tail_.load(std::memory_order_relaxed);
        const int64_t st = skip_to_.load(std::memory_order_acquire);
        if (t >= st)
            return;   // nothing skipped ahead of us
        // The drop was already counted by the producer at eviction time.
        tail_.store(st, std::memory_order_relaxed);
    }

    alignas(64) std::atomic<int64_t> head_{0};    // producer-owned
    alignas(64) std::atomic<int64_t> tail_{0};    // consumer-owned exclusively
    alignas(64) std::atomic<int64_t> skip_to_{0}; // producer-owned eviction
                                                  // watermark (one past the
                                                  // last evicted position)
    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) Slot slots_[Capacity];
};

} // namespace hid

#ifdef _MSC_VER
#pragma warning(pop)
#endif
