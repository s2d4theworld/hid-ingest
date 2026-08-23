// hid_ingest/platform/win32/raw_input_producer.cpp — spec section 4.
#include "hid_ingest/raw_input_producer.h"

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
                const bool retryable = size > kStackCap && size <= kHeapCap;
                if (retryable) {
                    BYTE* heap_buf = static_cast<BYTE*>(malloc(size));
                    if (heap_buf) {
                        UINT heap_size = size;
                        bytes = GetRawInputData(reinterpret_cast<HRAWINPUT>(lparam),
                                                RID_INPUT, heap_buf, &heap_size,
                                                sizeof(RAWINPUTHEADER));
                        if (bytes != static_cast<UINT>(-1)) {
                            ++self->oversize_count_;  // telemetry: complex device present
                            auto* raw = reinterpret_cast<RAWINPUT*>(heap_buf);
                            if (raw->header.dwType == RIM_TYPEMOUSE)
                                self->emit_mouse_sample(raw->data.mouse);
                        } else {
                            ++self->oversize_dropped_;  // retry failed
                        }
                        free(heap_buf);
                    } else {
                        ++self->oversize_dropped_;      // OOM
                    }
                } else {
                    // Deliberately dropped: >4 KB comes from exotic digitizer
                    // collections; only mice are parsed anyway. Counted so the
                    // drop stays visible in telemetry.
                    ++self->oversize_dropped_;
                }
                // CRITICAL: forward so the system cleans up the handle.
                return DefWindowProcW(hwnd, WM_INPUT, wparam, lparam);
            }

            {
                auto* raw = reinterpret_cast<RAWINPUT*>(raw_buffer);
                if (raw->header.dwType == RIM_TYPEMOUSE) {
                    self->emit_mouse_sample(raw->data.mouse);
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
///  - Absolute digitizer coords span 0..65535 -> scale to the primary screen
///    size first (int64 math, no overflow), then shift. NOTE: primary-screen
///    scaling ignores multi-monitor virtual desktops and per-monitor DPI;
///    see README.
void RawInputProducer::emit_mouse_sample(const RAWMOUSE& m) {
    const bool button_event =
        m.usButtonFlags & (RI_MOUSE_LEFT_BUTTON_DOWN | RI_MOUSE_LEFT_BUTTON_UP |
                           RI_MOUSE_RIGHT_BUTTON_DOWN | RI_MOUSE_RIGHT_BUTTON_UP |
                           RI_MOUSE_MIDDLE_BUTTON_DOWN | RI_MOUSE_MIDDLE_BUTTON_UP);
    if (m.lLastX == 0 && m.lLastY == 0 && !button_event)
        return;   // nothing reportable in this packet

    HidSample sample{};
    if (m.usFlags & MOUSE_MOVE_ABSOLUTE) {
        constexpr int64_t kAbsMax = 65535;
        sample.dx = static_cast<int32_t>(
            static_cast<int64_t>(m.lLastX) * screen_w_ / kAbsMax) << 8;
        sample.dy = static_cast<int32_t>(
            static_cast<int64_t>(m.lLastY) * screen_h_ / kAbsMax) << 8;
        sample.buttons |= 0x8000;  // absolute-coordinate flag
    } else {
        sample.dx = m.lLastX << 8;
        sample.dy = m.lLastY << 8;
    }
    sample.buttons |= static_cast<uint16_t>(m.ulButtons & 0xFFFF);

    // Timestamp basis: raw QPC truncated to 32 bits (~71 min wrap on typical
    // 10 MHz counters). Valid for per-platform DELTAS only, not absolute
    // comparisons across platforms or long spans.
    LARGE_INTEGER qpc{};
    QueryPerformanceCounter(&qpc);
    sample.timestamp = static_cast<uint32_t>(qpc.QuadPart & 0xFFFFFFFFu);

    ring_.push(sample);   // DropOnOverflow handles full ring
}

void RawInputProducer::run() {
    // Cache once: absolute-mode scaling uses these per packet. Documented
    // limitation: primary screen only, no DPI/monitor-change tracking.
    screen_w_ = GetSystemMetrics(SM_CXSCREEN);
    screen_h_ = GetSystemMetrics(SM_CYSCREEN);

    // Per-instance class name: coexisting producers must not unregister each
    // other's class on teardown.
    wchar_t class_name[64];
    swprintf_s(class_name, L"RawInputSinkWndClass_%p", this);

    WNDCLASSEXW wc{};
    wc.cbSize        = sizeof(wc);
    wc.lpfnWndProc   = &RawInputProducer::wnd_proc;
    wc.hInstance     = GetModuleHandleW(nullptr);
    wc.lpszClassName = class_name;
    RegisterClassExW(&wc);

    hwnd_ = CreateWindowExW(0, wc.lpszClassName, L"", 0, 0, 0, 0, 0,
                            HWND_MESSAGE, nullptr, wc.hInstance,
                            this);  // lpParam -> WM_NCCREATE -> GWLP_USERDATA
    if (!hwnd_) { running_.store(false); return; }

    if (!register_raw_input(hwnd_)) { running_.store(false); return; }

    running_.store(true, std::memory_order_release);

    MSG msg;
    while (running_.load(std::memory_order_relaxed)) {
        // Sleep with zero CPU cost until a posted message arrives.
        MsgWaitForMultipleObjectsEx(0, nullptr, INFINITE, QS_POSTMESSAGE,
                                    MWMO_INPUTAVAILABLE);

        // NOTE: WM_QUIT is thread-wide (NULL hwnd) and never passes an
        // hwnd-filtered PeekMessage. We don't use PostQuitMessage for
        // shutdown — stop() posts WM_APP and clears running_. WM_QUIT is
        // only checked here via a second unfiltered peek so an external
        // PostQuitMessage still ends the loop cleanly.
        BOOL quit = PeekMessageW(&msg, nullptr, WM_QUIT, WM_QUIT, PM_REMOVE);
        if (quit) { running_.store(false, std::memory_order_relaxed); break; }

        while (PeekMessageW(&msg, hwnd_, 0, 0, PM_REMOVE)) {
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
    }
    DestroyWindow(hwnd_);
    UnregisterClassW(wc.lpszClassName, wc.hInstance);
}

bool RawInputProducer::start() {
    if (thread_.joinable()) {
        // A previous start() failed after spawning (run() bailed on
        // CreateWindow/RegisterRawInput): the thread exited but stayed
        // joinable, which would block every future start() forever. Reap it.
        thread_.join();
    }

    try {
        thread_ = std::jthread([this] {
            if (cfg_.time_critical_priority)
                SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
            if (cfg_.core_affinity_mask)
                SetThreadAffinityMask(GetCurrentThread(), cfg_.core_affinity_mask);
            run();
        });
    } catch (...) {
        return false;
    }

    // Wait briefly for the message loop to come up.
    for (int i = 0; i < 100 && !running_.load(std::memory_order_acquire); ++i)
        Sleep(10);
    return running_.load(std::memory_order_acquire);
}

void RawInputProducer::stop() {
    if (!thread_.joinable()) return;
    // Wake the blocked MsgWaitForMultipleObjectsEx with a harmless posted
    // message. Safe: hwnd_ was created before running_ went true (start()
    // waits for running_), and stop() only proceeds when thread_ is live,
    // so the window exists for the whole lifetime of this call. The loop
    // exits via running_, then DestroyWindow happens on the producer thread.
    if (hwnd_) PostMessageW(hwnd_, WM_APP, 0, 0);
    running_.store(false, std::memory_order_relaxed);
    thread_.join();
    hwnd_ = nullptr;
}

} // namespace hid::win32
