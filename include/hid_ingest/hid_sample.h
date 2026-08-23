// hid_ingest/core/hid_sample.h
// Packed 16-byte POD sample — see spec section 3.
#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>

namespace hid {

#pragma pack(push, 1)
struct HidSample {
    int32_t  dx;          // Relative X or absolute sub-pixel coordinate (Fixed-point 24.8)
    int32_t  dy;          // Relative Y or absolute sub-pixel coordinate (Fixed-point 24.8)
    uint16_t pressure;    // Normalized pressure (0-65535; 0 for standard mice)
    uint16_t buttons;     // Bits 0..2: L/R/M button HELD STATE (1 = currently
                          // down; consistent across platforms). Bit 15
                          // (0x8000): absolute coordinates flag (Win32
                          // MOUSE_MOVE_ABSOLUTE path).
    uint32_t timestamp;   // PER-PLATFORM basis, deltas only — NOT comparable
                          // across producers: Win32 = raw QPC ticks (32-bit
                          // truncation, ~100 ns @ 10 MHz); Linux = kernel
                          // event time in wall-clock us. BOTH WRAP the 32-bit
                          // space (~71 min on both platforms). Consumers must
                          // compute deltas as unsigned uint32 subtraction,
                          // which is automatically wrap-safe; never compare
                          // raw timestamps or use signed deltas across a
                          // producer's lifetime.
    // SCOPE: motion + buttons + (Linux) pressure only. Mouse wheel input is
    // deliberately not captured — there is no wheel field in this struct.
};
#pragma pack(pop)

static_assert(sizeof(HidSample) == 16, "HidSample must remain exactly 16 bytes.");
static_assert(std::is_trivially_copyable_v<HidSample>, "HidSample must be trivially copyable.");

// Fixed-point 24.8 helpers.
constexpr inline int32_t ToFixed24_8(float v) { return static_cast<int32_t>(v * 256.0f); }
constexpr inline float   FromFixed24_8(int32_t v) { return static_cast<float>(v) / 256.0f; }

/// Saturate an int64 to the int32 range before narrowing into a 24.8 field.
/// The single clamp used by every producer conversion site (Win32 ABS/REL,
/// evdev ABS/REL/sync-loss) — keeps the shift-then-narrow discipline
/// consistent and implementation-defined-free.
constexpr inline int32_t clamp24(int64_t v) {
    return v > INT32_MAX ? INT32_MAX : (v < INT32_MIN ? INT32_MIN : static_cast<int32_t>(v));
}

} // namespace hid
