// hid_ingest/platform/win32/raw_input_producer.h
// Win32 Raw Input producer thread — spec section 4.
//
// Spawns a TIME_CRITICAL jthread pinned to an isolated core, hosts a hidden
// message-only window, registers mouse + stylus raw input devices, and
// sleeps in MsgWaitForMultipleObjectsEx (0% idle CPU) until WM_INPUT arrives.
#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <atomic>
#include <thread>

#include "hid_ingest/spsc_ring.h"

namespace hid::win32 {

struct ProducerConfig {
    DWORD   core_affinity_mask = 0;  // 0 = let OS choose; otherwise pin to these cores
    bool    time_critical_priority = true;
};

class RawInputProducer {
public:
    explicit RawInputProducer(SpscRing<>& ring, ProducerConfig cfg = {})
        : ring_(ring), cfg_(cfg) {}

    ~RawInputProducer() { stop(); }

    // LIFECYCLE: stop() latches stop_requested_ permanently — a stop()
    // before (or after) start() disables this instance until it is
    // destroyed and recreated. Single-owner lifecycle by design.
    bool start();
    void stop();

    bool running() const noexcept { return running_.load(std::memory_order_relaxed); }

    // Telemetry: complex packets parsed via heap retry / dropped as too
    // large / RIM_TYPEHID (digitizer) packets counted + dropped unparsed.
    // All producer-thread-written; read before stop() for a stable snapshot.
    uint64_t oversize_count() const noexcept { return oversize_count_; }
    uint64_t oversize_dropped() const noexcept { return oversize_dropped_; }
    uint64_t hid_unparsed() const noexcept { return hid_unparsed_; }

private:
    static LRESULT CALLBACK wnd_proc(HWND hwnd, UINT msg, WPARAM wparam, LPARAM lparam);
    void run();
    bool register_raw_input(HWND hwnd);
    void emit_mouse_sample(const RAWMOUSE& m);   // called on the producer thread

    SpscRing<>&     ring_;
    ProducerConfig  cfg_;
    std::jthread    thread_;
    // Set true by the producer lambda right before it returns; lets start()'s
    // reap branch distinguish "thread finished" (safe to join) from "thread
    // still live" (must not join — would hang). Initialized false: no thread
    // has run yet (mirrors the evdev producer's field).
    std::atomic<bool> thread_exited_{false};
    std::atomic<bool> running_{false};
    // Only ever written by stop(). Separate from running_ because run()
    // stores true into running_ after setup and would otherwise resurrect
    // the flag during a stop()-during-setup race (same pattern as the evdev
    // producer's stop_requested_).
    std::atomic<bool> stop_requested_{false};
    // Cross-thread HWND: written by run() (producer thread) on creation and
    // reset-to-null on exit; read by stop() (PostMessageW) possibly BEFORE
    // join — that pre-join read cannot be gated, so the field is atomic.
    std::atomic<HWND> hwnd_{nullptr};
    // Telemetry: atomics because they are written on the producer thread and
    // may be read cross-thread via the getters (relaxed is sufficient for
    // approximate counters; see spsc_ring drop_counter_ precedent).
    std::atomic<uint64_t> oversize_count_{0};     // complex packets parsed via heap retry
    std::atomic<uint64_t> oversize_dropped_{0};   // packets dropped (too large / retry failed)
    std::atomic<uint64_t> hid_unparsed_{0};       // RIM_TYPEHID packets counted + dropped
                                                  // (digitizer parse out of scope)
    int64_t         screen_w_ = 0;         // cached at run() start (primary screen)
    int64_t         screen_h_ = 0;
    // Mirrored HELD-STATE of L/R/M buttons (producer thread only). Raw Input
    // reports DOWN/UP transitions; the sample contract is held state.
    uint16_t        button_mirror_ = 0;
};

} // namespace hid::win32
