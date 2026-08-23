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
        return TRUE;
    }
    if (!self)
        return DefWindowProcW(hwnd, msg, wparam, lparam);

    switch (msg) {
        case WM_INPUT: {
            alignas(8) uint8_t raw_buffer[sizeof(RAWINPUT) + 64];  // stack, no heap
            UINT size = sizeof(raw_buffer);

            const UINT bytes = GetRawInputData(reinterpret_cast<HRAWINPUT>(lparam),
                                               RID_INPUT, raw_buffer, &size,
                                               sizeof(RAWINPUTHEADER));
            if (bytes != static_cast<UINT>(-1)) {
                auto* raw = reinterpret_cast<RAWINPUT*>(raw_buffer);
                const auto& m = raw->data.mouse;
                if (raw->header.dwType == RIM_TYPEMOUSE &&
                    (m.lLastX != 0 || m.lLastY != 0 ||
                     (m.usButtonFlags & (RI_MOUSE_LEFT_BUTTON_DOWN | RI_MOUSE_LEFT_BUTTON_UP |
                                         RI_MOUSE_RIGHT_BUTTON_DOWN | RI_MOUSE_RIGHT_BUTTON_UP |
                                         RI_MOUSE_MIDDLE_BUTTON_DOWN | RI_MOUSE_MIDDLE_BUTTON_UP)))) {
                    HidSample sample{};
                    // Coordinate units: dx/dy are fixed-point 24.8 SCREEN-space
                    // sub-pixels. Relative moves are already integer pixels ->
                    // shift into 24.8. Absolute digitizer coords span 0..65535
                    // -> scale to the primary screen size first (int64 math,
                    // no overflow), then shift.
                    if (m.usFlags & MOUSE_MOVE_ABSOLUTE) {
                        constexpr int64_t kAbsMax = 65535;
                        const int64_t scr_w = GetSystemMetrics(SM_CXSCREEN);
                        const int64_t scr_h = GetSystemMetrics(SM_CYSCREEN);
                        sample.dx = static_cast<int32_t>(
                            m.lLastX * scr_w / kAbsMax) << 8;
                        sample.dy = static_cast<int32_t>(
                            m.lLastY * scr_h / kAbsMax) << 8;
                        sample.buttons |= 0x8000;  // absolute-coordinate flag
                    } else {
                        sample.dx = m.lLastX << 8;
                        sample.dy = m.lLastY << 8;
                    }
                    sample.buttons |= static_cast<uint16_t>(m.ulButtons & 0xFFFF);
                    LARGE_INTEGER qpc{};
                    QueryPerformanceCounter(&qpc);
                    sample.timestamp = static_cast<uint32_t>(qpc.QuadPart & 0xFFFFFFFFu);
                    self->ring_.push(sample);   // DropOnOverflow handles full ring
                }
            }
            // CRITICAL: forward so the system cleans up the handle and avoids re-delivery.
            return DefWindowProcW(hwnd, WM_INPUT, wparam, lparam);
        }
        case WM_INPUT_DEVICE_CHANGE:
            // GIDC_ARRIVAL / GIDC_REMOVAL — devices are registered usage-wide,
            // so arrival needs no action; removal is handled by Raw Input itself.
            return 0;
        case WM_DESTROY:
            PostQuitMessage(0);
            return 0;
        default:
            return DefWindowProcW(hwnd, msg, wparam, lparam);
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

void RawInputProducer::run() {
    WNDCLASSEXW wc{};
    wc.cbSize        = sizeof(wc);
    wc.lpfnWndProc   = &RawInputProducer::wnd_proc;
    wc.hInstance     = GetModuleHandleW(nullptr);
    wc.lpszClassName = L"RawInputSinkWndClass";
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

        while (PeekMessageW(&msg, hwnd_, 0, 0, PM_REMOVE)) {
            if (msg.message == WM_QUIT) {
                running_.store(false, std::memory_order_relaxed);
                break;
            }
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
    }

    DestroyWindow(hwnd_);
    UnregisterClassW(wc.lpszClassName, wc.hInstance);
}

bool RawInputProducer::start() {
    if (thread_.joinable()) return false;

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
    running_.store(false, std::memory_order_relaxed);
    // hwnd_ is created/destroyed on the producer thread; PostMessageW is safe
    // on a NULL or already-destroyed HWND only if we don't touch the handle
    // afterwards — guard with a null check and rely on running_ to end the loop.
    const HWND hwnd = hwnd_;
    if (hwnd) PostMessageW(hwnd, WM_QUIT, 0, 0);
    thread_.join();
    hwnd_ = nullptr;
}

} // namespace hid::win32
