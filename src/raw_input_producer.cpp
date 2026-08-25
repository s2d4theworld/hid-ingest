// hid_ingest/platform/win32/raw_input_producer.cpp — spec section 4.
#include "hid_ingest/raw_input_producer.h"

#include <cstdio>   // fprintf (telemetry warnings) — do not rely on transitive include

namespace hid::win32 {

LRESULT CALLBACK RawInputProducer::wnd_proc(HWND hwnd, UINT msg, WPARAM wparam, LPARAM lparam) {
    // Instance is attached via GWLP_USERDATA at WM_NCCREATE (per-window state,
    // no process-wide globals — multiple producers can coexist safely).
    RawInputProducer* self = reinterpret_cast<RawInputProducer*>(
        GetWindowLongPtrW(hwnd, GWLP_USERDATA));

    if (msg == WM_NCCREATE) {
        auto* cs = reinterpret_cast<CREATESTRUCTW*>(lparam);
        SetWindowLongPtrW(hwnd, GWLP_USERDATA,
                          reinterpret_cast<LONG_PTR>(cs->lpCreateParams));
        // Canonical pattern: attach userdata, then still forward to
        // DefWindowProc so internal WM_NCCREATE init (window text storage,
        // state) completes.
        return DefWindowProcW(hwnd, msg, wparam, lparam);
    }
    if (!self)
        return DefWindowProcW(hwnd, msg, wparam, lparam);

    switch (msg) {
        case WM_INPUT: {
            // Fast path: stack buffer covers standard mice. Complex digitizer
            // HID collections can be larger; on undersize, GetRawInputData
            // returns (UINT)-1 and sets `size` to the required byte count —
            // retry once with a heap buffer (rare path, capped at 4 KB).
            constexpr UINT kStackCap = sizeof(RAWINPUT) + 64;
            constexpr UINT kHeapCap  = 4096;   // bound the rare-path allocation

            alignas(8) uint8_t raw_buffer[kStackCap];
            UINT size = kStackCap;
            UINT bytes = GetRawInputData(reinterpret_cast<HRAWINPUT>(lparam),
                                         RID_INPUT, raw_buffer, &size,
                                         sizeof(RAWINPUTHEADER));

            if (bytes == static_cast<UINT>(-1)) {
                // Packet exceeded the stack buffer. Retry on heap when the
                // required size fits kHeapCap; count as dropped only if the
                // retry is impossible (too large / OOM) or fails.
                // CAVEAT: on an unexpected failure (not undersize) size may
                // be left at kStackCap, so the packet lands in the
                // oversize_dropped_ telemetry even though it was not truly
                // oversize — telemetry is approximate for this rare path.
                const bool retryable = size > kStackCap && size <= kHeapCap;
                if (retryable) {
                    BYTE* heap_buf = static_cast<BYTE*>(malloc(size));
                    if (heap_buf) {
                        UINT heap_size = size;
                        bytes = GetRawInputData(reinterpret_cast<HRAWINPUT>(lparam),
                                                RID_INPUT, heap_buf, &heap_size,
                                                sizeof(RAWINPUTHEADER));
                        if (bytes != static_cast<UINT>(-1)) {
                            auto* raw = reinterpret_cast<RAWINPUT*>(heap_buf);
                            if (raw->header.dwType == RIM_TYPEMOUSE) {
                                // Genuinely oversized MOUSE packet parsed.
                                self->oversize_count_.fetch_add(1, std::memory_order_relaxed);
                                self->emit_mouse_sample(raw->data.mouse);
                            } else {
                                // Oversized RIM_TYPEHID (digitizer) — parse
                                // out of scope; count as HID unparsed, not
                                // "complex mouse parsed".
                                self->hid_unparsed_.fetch_add(1, std::memory_order_relaxed);
                            }
                        } else {
                            self->oversize_dropped_.fetch_add(1, std::memory_order_relaxed);  // retry failed
                        }
                        free(heap_buf);
                    } else {
                        self->oversize_dropped_.fetch_add(1, std::memory_order_relaxed);      // OOM
                    }
                } else {
                    // Deliberately dropped: >4 KB comes from exotic digitizer
                    // collections; only mice are parsed anyway. Counted so the
                    // drop stays visible in telemetry.
                    self->oversize_dropped_.fetch_add(1, std::memory_order_relaxed);
                }
                // CRITICAL: forward so the system cleans up the handle.
                return DefWindowProcW(hwnd, WM_INPUT, wparam, lparam);
            }

            {
                auto* raw = reinterpret_cast<RAWINPUT*>(raw_buffer);
                if (raw->header.dwType == RIM_TYPEMOUSE) {
                    self->emit_mouse_sample(raw->data.mouse);
                } else if (raw->header.dwType == RIM_TYPEHID) {
                    // Digitizer/stylus (UsagePage 0x0D) arrives as RIM_TYPEHID.
                    // Parsing raw HID reports is out of scope — the
                    // registration exists so the device is claimed by this
                    // window rather than another Raw Input consumer, but its
                    // packets are counted and dropped here.
                    self->hid_unparsed_.fetch_add(1, std::memory_order_relaxed);
                }
            }
            // CRITICAL: forward so the system cleans up the handle and avoids re-delivery.
            return DefWindowProcW(hwnd, WM_INPUT, wparam, lparam);
        }
        case WM_INPUT_DEVICE_CHANGE:
            // GIDC_ARRIVAL / GIDC_REMOVAL — devices are registered usage-wide,
            // so arrival needs no action; removal is handled by Raw Input
            // itself. Deliberately swallowed (no DefWindowProc): there is no
            // default processing for GIDC_* notifications.
            return 0;
        case WM_DESTROY:
            PostQuitMessage(0);
            return 0;
        default:
            return DefWindowProcW(hwnd, msg, wparam, lparam);  // incl. WM_APP wake shot
    }
}

bool RawInputProducer::register_raw_input(HWND hwnd) {
    RAWINPUTDEVICE devices[2]{};

    // Mouse: UsagePage 0x01, Usage 0x02.
    devices[0].usUsagePage = 0x01;
    devices[0].usUsage     = 0x02;
    devices[0].dwFlags     = RIDEV_INPUTSINK | RIDEV_DEVNOTIFY;
    devices[0].hwndTarget  = hwnd;

    // Stylus/digitizer: UsagePage 0x0D, Usage 0x02.
    devices[1].usUsagePage = 0x0D;
    devices[1].usUsage     = 0x02;
    devices[1].dwFlags     = RIDEV_INPUTSINK | RIDEV_DEVNOTIFY;
    devices[1].hwndTarget  = hwnd;

    return RegisterRawInputDevices(devices, 2, sizeof(RAWINPUTDEVICE)) != FALSE;
}

/// Build a HidSample from a RAWMOUSE packet and push it into the ring.
/// Runs on the producer thread only.
///
/// Coordinate units: dx/dy are fixed-point 24.8 SCREEN-space sub-pixels.
///  - Relative moves are integer pixels -> shift into 24.8.
///  - Absolute digitizer coords span 0..65535 -> scale to the virtual-screen
///    size (all monitors) and add the virtual origin, in int64 math with the
///    shift before any division. NOTE: metrics are cached at start(); monitor
///    changes and per-monitor DPI are not tracked; see README.
void RawInputProducer::emit_mouse_sample(const RAWMOUSE& m) {
    const bool button_event =
        m.usButtonFlags & (RI_MOUSE_LEFT_BUTTON_DOWN | RI_MOUSE_LEFT_BUTTON_UP |
                           RI_MOUSE_RIGHT_BUTTON_DOWN | RI_MOUSE_RIGHT_BUTTON_UP |
                           RI_MOUSE_MIDDLE_BUTTON_DOWN | RI_MOUSE_MIDDLE_BUTTON_UP);
    const bool absolute = m.usFlags & MOUSE_MOVE_ABSOLUTE;
    // Zero-delta early-out applies to RELATIVE packets only: for absolute
    // mode (digitizers, RDP sessions) (0,0) is a legitimate position — the
    // screen origin — and must be delivered. Wheel-only filtering stays
    // unconditional: HidSample has no wheel field (documented in
    // hid_sample.h).
    if (!absolute && m.lLastX == 0 && m.lLastY == 0 && !button_event)
        return;

    // Mirror button HELD STATE from the RI transition flags. Raw Input
    // reports DOWN/UP transitions per packet, but the HidSample contract is
    // held-state bits (matching the Linux evdev producer): a consumer reading
    // bit0 as "left is down" must be right on every sample, not just on the
    // transition packet.
    if (m.usButtonFlags & RI_MOUSE_LEFT_BUTTON_DOWN)   button_mirror_ |= 0x01;
    if (m.usButtonFlags & RI_MOUSE_LEFT_BUTTON_UP)     button_mirror_ &= static_cast<uint16_t>(~0x01);
    if (m.usButtonFlags & RI_MOUSE_RIGHT_BUTTON_DOWN)  button_mirror_ |= 0x02;
    if (m.usButtonFlags & RI_MOUSE_RIGHT_BUTTON_UP)    button_mirror_ &= static_cast<uint16_t>(~0x02);
    if (m.usButtonFlags & RI_MOUSE_MIDDLE_BUTTON_DOWN) button_mirror_ |= 0x04;
    if (m.usButtonFlags & RI_MOUSE_MIDDLE_BUTTON_UP)   button_mirror_ &= static_cast<uint16_t>(~0x04);

    HidSample sample{};
    if (absolute) {
        constexpr int64_t kAbsMax = 65535;
        // Sub-pixel precision: multiply and SHIFT in int64 FIRST, divide
        // LAST — dividing before the shift quantizes to whole pixels and
        // throws away the sub-pixel bits the 24.8 format exists for.
        // Multi-monitor: absolute coordinates span the entire virtual
        // desktop, so scale to the virtual screen bounds and add the origin
        // offset of the primary monitor (the virtual screen's top-left may
        // be negative when a monitor sits left/above the primary).
        // KNOWN LIMITATION: virtual_w_/virtual_h_ == 0 (headless /
        // no-monitor session, GetSystemMetrics returning 0) makes every
        // absolute sample the origin offset. Absolute input is meaningless
        // without a display — not guarded beyond this note.
        const int64_t dx = (static_cast<int64_t>(m.lLastX) * virtual_w_ << 8) / kAbsMax
                           + (virtual_x_ << 8);
        const int64_t dy = (static_cast<int64_t>(m.lLastY) * virtual_h_ << 8) / kAbsMax
                           + (virtual_y_ << 8);
        sample.dx = clamp24(dx);
        sample.dy = clamp24(dy);
        sample.buttons |= 0x8000;  // absolute-coordinate flag
    } else {
        // Same discipline: shift in int64, clamp before narrowing.
        sample.dx = clamp24(static_cast<int64_t>(m.lLastX) << 8);
        sample.dy = clamp24(static_cast<int64_t>(m.lLastY) << 8);
    }
    // Push the mirrored held state (bits 0..2), not the raw transition
    // flags. Bit 15 absolute flag was set above.
    sample.buttons |= button_mirror_;

    // Timestamp basis: raw QPC truncated to 32 bits (~71 min wrap on typical
    // 10 MHz counters). Valid for per-platform DELTAS only, not absolute
    // comparisons across platforms or long spans.
    LARGE_INTEGER qpc{};
    QueryPerformanceCounter(&qpc);
    sample.timestamp = static_cast<uint32_t>(qpc.QuadPart & 0xFFFFFFFFu);

    ring_.push(sample);   // DropOnOverflow handles full ring
}

void RawInputProducer::run() {
    // Cache once: absolute-mode scaling uses these per packet. Virtual-screen
    // metrics cover ALL monitors (SM_XVIRTUALSCREEN etc.); the origin may be
    // negative when a monitor sits left/above the primary. Documented
    // limitation: cached at start, no DPI/monitor-change tracking.
    virtual_x_ = GetSystemMetrics(SM_XVIRTUALSCREEN);
    virtual_y_ = GetSystemMetrics(SM_YVIRTUALSCREEN);
    virtual_w_ = GetSystemMetrics(SM_CXVIRTUALSCREEN);
    virtual_h_ = GetSystemMetrics(SM_CYVIRTUALSCREEN);

    // Per-instance class name: coexisting producers must not unregister each
    // other's class on teardown.
    wchar_t class_name[64];
    swprintf_s(class_name, L"RawInputSinkWndClass_%p", this);

    WNDCLASSEXW wc{};
    wc.cbSize        = sizeof(wc);
    wc.lpfnWndProc   = &RawInputProducer::wnd_proc;
    wc.hInstance     = GetModuleHandleW(nullptr);
    wc.lpszClassName = class_name;
    if (RegisterClassExW(&wc) == 0) { running_.store(false, std::memory_order_relaxed); return; }

    hwnd_ = CreateWindowExW(0, wc.lpszClassName, L"", 0, 0, 0, 0, 0,
                            HWND_MESSAGE, nullptr, wc.hInstance,
                            this);  // lpParam -> WM_NCCREATE -> GWLP_USERDATA
    if (!hwnd_) {
        UnregisterClassW(wc.lpszClassName, wc.hInstance);  // no class-name leak
        running_.store(false, std::memory_order_relaxed);
        return;
    }

    if (!register_raw_input(hwnd_)) {
        DestroyWindow(hwnd_);
        UnregisterClassW(wc.lpszClassName, wc.hInstance);
        hwnd_ = nullptr;
        running_.store(false, std::memory_order_relaxed);
        return;
    }

    if (stop_requested_.load(std::memory_order_acquire)) {
        // stop() raced during setup: tear the window/class down here (the
        // message loop never ran, so nothing else will) and leave without
        // storing running_=true. hwnd_/class are fully released; stop()
        // owns only the thread join.
        DestroyWindow(hwnd_);
        UnregisterClassW(wc.lpszClassName, wc.hInstance);
        hwnd_ = nullptr;
        return;
    }
    running_.store(true, std::memory_order_release);

    MSG msg;
    while (running_.load(std::memory_order_relaxed) &&
           !stop_requested_.load(std::memory_order_relaxed)) {
        // Sleep with zero CPU cost until a posted message arrives.
        // NOTE: MWMO_INPUTAVAILABLE is technically a no-op here (it governs
        // object-handle availability and we pass 0 handles) — the wake is
        // purely the QS_POSTMESSAGE mask. Kept harmless; the flag does not
        // imply hardware-input wake (WM_INPUT arrives via the queue).
        MsgWaitForMultipleObjectsEx(0, nullptr, INFINITE, QS_POSTMESSAGE,
                                    MWMO_INPUTAVAILABLE);

        // NOTE: WM_QUIT is thread-wide (NULL hwnd) and never passes an
        // hwnd-filtered PeekMessage. We don't use PostQuitMessage for
        // shutdown — stop() posts WM_APP and clears running_. WM_QUIT is
        // only checked here via a second unfiltered peek so an external
        // PostQuitMessage still ends the loop cleanly.
        BOOL quit = PeekMessageW(&msg, nullptr, WM_QUIT, WM_QUIT, PM_REMOVE);
        if (quit) { running_.store(false, std::memory_order_relaxed); break; }

        // Drain pending messages. Under a sustained WM_INPUT flood this
        // inner loop could starve the outer shutdown check, so re-check the
        // flags every 64 dispatched messages — shutdown latency stays
        // bounded even during input storms.
        int drained = 0;
        while (running_.load(std::memory_order_relaxed) &&
               !stop_requested_.load(std::memory_order_relaxed) &&
               PeekMessageW(&msg, hwnd_, 0, 0, PM_REMOVE)) {
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
            if (++drained % 64 == 0) break;   // yield to the outer checks
        }
    }
    // hwnd_ ownership note: written only on this (producer) thread and read
    // by stop()/start() on other threads. The thread_exited_ release/acquire
    // pairing covers those cross-thread reads — once exited is observed, the
    // final value of hwnd_ is visible and the window is already destroyed.
    DestroyWindow(hwnd_);
    UnregisterClassW(wc.lpszClassName, wc.hInstance);
    hwnd_ = nullptr;   // no dangling HWND for cross-thread readers
}

bool RawInputProducer::start() {
    // THREAD-SAFETY: start()/stop() must be serialized externally by the
    // object's owner. Concurrent start-vs-stop can interleave the
    // stop_requested_ reset (below) with a racing stop() store, losing a
    // shutdown request; the destructor is safe (it only calls stop()).
    if (running_.load(std::memory_order_acquire))
        return false;  // already live: double-start would deadlock the join below
    if (stop_requested_.load(std::memory_order_acquire))
        return false;  // shutdown already requested — do not resurrect
    stop_requested_.store(false, std::memory_order_release);

    if (thread_.joinable()) {
        // A previous run() exited (failed start, or stop() raced and left
        // the thread for us). Join is safe ONLY if the thread has actually
        // finished — thread_exited_ tells us that. If it is still live
        // (stop-during-run race where the caller ignored false), joining
        // here would hang forever; refuse instead.
        if (!thread_exited_.load(std::memory_order_acquire)) {
            fprintf(stderr,
                    "[producer] previous producer thread still live; "
                    "call stop() before start()\n");
            return false;
        }
        thread_.join();
    }

    try {
        thread_exited_.store(false, std::memory_order_release);
        thread_ = std::jthread([this] {
            if (cfg_.time_critical_priority)
                SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
            if (cfg_.core_affinity_mask)
                SetThreadAffinityMask(GetCurrentThread(), cfg_.core_affinity_mask);
            run();
            // Mark exited BEFORE the joinable flag matters to anyone; release
            // pairs with start()'s acquire load so the reap branch is safe.
            thread_exited_.store(true, std::memory_order_release);
        });
    } catch (...) {
        return false;
    }

    // Wait briefly for the message loop to come up. Exit the loop early when
    // (a) running_ observed (success), (b) stop() raced, or (c) the thread
    // already exited on its own (fast setup failure — no reason to burn the
    // remaining ~1 s of wait). If stop() raced, do NOT join here — stop()
    // (another thread) may be joining the same std::jthread concurrently,
    // which is a data race; leave the joinable thread to stop() and report
    // failure honestly.
    for (int i = 0;
         i < 100 && !running_.load(std::memory_order_acquire) &&
         !stop_requested_.load(std::memory_order_acquire) &&
         !thread_exited_.load(std::memory_order_acquire);
         ++i)
        Sleep(10);

    if (stop_requested_.load(std::memory_order_acquire)) {
        // run()'s exit path already destroyed the window and reset hwnd_
        // (its write happens-before our read via thread_exited_/stop
        // synchronization). Do NOT touch hwnd_ here — a concurrent writer
        // would be an unsynchronized data race.
        return false;
    }
    if (!running_.load(std::memory_order_acquire) &&
        thread_exited_.load(std::memory_order_acquire)) {
        // Fast setup failure: reap the dead thread so a retry can proceed.
        thread_.join();
        return false;
    }
    // TIMEOUT CASE (loop exhausted, thread still initializing): we return
    // false but the producer may become live moments later. KNOWN
    // LIMITATION: "false means no data" is not strictly guaranteed here —
    // callers should treat false as "not confirmed" and either call stop()
    // or check running()/start() again. Practically unreachable: setup is
    // register-class + create-window + register-raw-input, milliseconds at
    // worst.
    return running_.load(std::memory_order_acquire);
}

void RawInputProducer::stop() {
    // THREAD-SAFETY: stop() is not thread-safe against itself — two
    // concurrent callers can both pass the joinable check and join the
    // same thread (UB). Single-owner lifecycle: exactly one caller owns
    // shutdown. Same contract as EvdevProducer::stop().
    //
    // Set the flag FIRST so a run() still in setup bails before storing
    // running_=true (resurrection prevention — same pattern as evdev).
    stop_requested_.store(true, std::memory_order_release);
    if (!thread_.joinable()) return;
    // Wake the blocked MsgWaitForMultipleObjectsEx with a harmless posted
    // message. Safe: hwnd_ was created before running_ went true (start()
    // waits for running_), and stop() only proceeds when thread_ is live,
    // so the window exists for the whole lifetime of this call. The loop
    // exits via running_/stop_requested_, then DestroyWindow happens on the
    // producer thread.
    if (hwnd_) PostMessageW(hwnd_, WM_APP, 0, 0);
    running_.store(false, std::memory_order_relaxed);
    thread_.join();
    hwnd_ = nullptr;
}

// NOTE on cross-thread hwnd_ access: hwnd_ is std::atomic<HWND> (relaxed
// ordering) because stop() may read it via PostMessageW BEFORE join — that
// pre-join read cannot be gated behind thread_exited_/join happens-before.
// Atomicity removes the data race; relaxed is sufficient since the value is
// a self-consistent handle and the window's actual lifetime is ordered by
// the running_/stop_requested_ protocol.


} // namespace hid::win32
