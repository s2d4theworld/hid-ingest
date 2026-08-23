// hid_ingest/platform/win32/raw_input_producer.cpp — spec section 4.
#include "hid_ingest/raw_input_producer.h"

namespace hid::win32 {

namespace {

// SPSC = single producer; a static pointer lets the static WndProc reach the ring.
std::atomic<SpscRing<>*> g_ring{nullptr};
std::atomic<uint64_t>    g_pushed_count{0};

} // namespace

LRESULT CALLBACK RawInputProducer::wnd_proc(HWND hwnd, UINT msg, WPARAM wparam, LPARAM lparam) {
    switch (msg) {
        case WM_INPUT: {
            alignas(8) uint8_t raw_buffer[sizeof(RAWINPUT) + 64];  // stack, no heap
            UINT size = sizeof(raw_buffer);

            const UINT bytes = GetRawInputData(reinterpret_cast<HRAWINPUT>(lparam),
                                               RID_INPUT, raw_buffer, &size,
                                               sizeof(RAWINPUTHEADER));
            if (bytes != static_cast<UINT>(-1)) {
                auto* raw = reinterpret_cast<RAWINPUT*>(raw_buffer);
                SpscRing<>* ring = g_ring.load(std::memory_order_relaxed);
                if (raw->header.dwType == RIM_TYPEMOUSE && ring) {
                    const auto& m = raw->data.mouse;
                    if (m.lLastX != 0 || m.lLastY != 0 ||
                        (m.usButtonFlags & (RI_MOUSE_LEFT_BUTTON_DOWN | RI_MOUSE_LEFT_BUTTON_UP |
                                            RI_MOUSE_RIGHT_BUTTON_DOWN | RI_MOUSE_RIGHT_BUTTON_UP |
                                            RI_MOUSE_MIDDLE_BUTTON_DOWN | RI_MOUSE_MIDDLE_BUTTON_UP))) {
                        HidSample sample{};
                        if (m.usFlags & MOUSE_MOVE_ABSOLUTE) {
                            // Absolute -> fixed-point 24.8 normalized to screen size.
                            sample.dx = static_cast<int32_t>(
                                (static_cast<int64_t>(m.lLastX) * 65536 /
                                 static_cast<int64_t>(GetSystemMetrics(SM_CXSCREEN))) << 8);
                            sample.dy = static_cast<int32_t>(
                                (static_cast<int64_t>(m.lLastY) * 65536 /
                                 static_cast<int64_t>(GetSystemMetrics(SM_CYSCREEN))) << 8);
                            sample.buttons |= 0x8000;  // absolute-coordinate flag
                        } else {
                            sample.dx = m.lLastX;
                            sample.dy = m.lLastY;
                        }
                        if (m.usButtonFlags & RI_MOUSE_WHEEL)
                            sample.pressure = 0;
                        sample.buttons |= static_cast<uint16_t>(m.ulButtons & 0xFFFF);
                        LARGE_INTEGER qpc{};
                        QueryPerformanceCounter(&qpc);
                        sample.timestamp = static_cast<uint32_t>(qpc.QuadPart & 0xFFFFFFFFu);
                        if (ring->push(sample))
                            g_pushed_count.fetch_add(1, std::memory_order_relaxed);
                    }
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
                            HWND_MESSAGE, nullptr, wc.hInstance, nullptr);
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
    g_ring.store(&ring_, std::memory_order_relaxed);

    try {
        thread_ = std::jthread([this] {
            if (cfg_.time_critical_priority)
                SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
            if (cfg_.core_affinity_mask)
                SetThreadAffinityMask(GetCurrentThread(), cfg_.core_affinity_mask);
            run();
        });
    } catch (...) {
        g_ring.store(nullptr, std::memory_order_relaxed);
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
    if (hwnd_) PostMessageW(hwnd_, WM_QUIT, 0, 0);
    thread_.join();
    g_ring.store(nullptr, std::memory_order_relaxed);
}

} // namespace hid::win32
