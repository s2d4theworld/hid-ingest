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

    bool start();
    void stop();

    bool running() const noexcept { return running_.load(std::memory_order_relaxed); }

private:
    static LRESULT CALLBACK wnd_proc(HWND hwnd, UINT msg, WPARAM wparam, LPARAM lparam);
    void run();
    bool register_raw_input(HWND hwnd);
    void emit_mouse_sample(const RAWMOUSE& m);   // called on the producer thread

    SpscRing<>&     ring_;
    ProducerConfig  cfg_;
    std::jthread    thread_;
    std::atomic<bool> running_{false};
    HWND            hwnd_ = nullptr;
    DWORD           thread_id_ = 0;
    uint64_t        oversize_count_ = 0;   // telemetry: packets needing heap retry
};

} // namespace hid::win32
