// hid_ingest/spline.h
// Centripetal Catmull-Rom fitting + cubic Hermite evaluation — spec section 6.
// Fixed-point-friendly, allocation-free, operates on 24.8 coordinates.
#pragma once

#include <cmath>
#include <cstddef>

#include "hid_ingest/hid_sample.h"

namespace hid {

struct Vec2 {
    float x, y;
};

inline float dist_sq(Vec2 a, Vec2 b) {
    const float dx = a.x - b.x, dy = a.y - b.y;
    return dx * dx + dy * dy;
}

/// Chordal parameterization (alpha = 1) over points[0..n-1]: t advances by
/// the Euclidean distance between consecutive points. NOTE: this is NOT
/// centripetal (alpha = 0.5 would be pow(dist, 0.5)); the name is historical.
/// The tangent math downstream is self-consistent with any monotonic dt.
/// t must have space for n entries. Returns total curve length parameter.
inline float centripetal_times(const Vec2* pts, size_t n, float* t) {
    t[0] = 0.0f;
    for (size_t i = 1; i < n; ++i)
        t[i] = t[i - 1] + std::sqrt(dist_sq(pts[i], pts[i - 1]));
    return t[n - 1];
}

inline Vec2 hermite(Vec2 p0, Vec2 p1, Vec2 m0, Vec2 m1, float s) {
    const float s2 = s * s, s3 = s2 * s;
    const float h00 =  2*s3 - 3*s2 + 1;
    const float h10 =    s3 - 2*s2 + s;
    const float h01 = -2*s3 + 3*s2;
    const float h11 =    s3 -   s2;
    return { h00*p0.x + h10*m0.x + h01*p1.x + h11*m1.x,
             h00*p0.y + h10*m0.y + h01*p1.y + h11*m1.y };
}

/// Evaluate the centripetal Catmull-Rom segment between p1 and p2 given the
/// 4-point window [p0..p3] and their parameterization times t[0..3], at
/// u in (0,1). (Parameterization is chordal — see centripetal_times note.)
inline Vec2 catmull_rom_segment(const Vec2* p, const float* t, float u) {
    // Finite-difference tangents scaled by chord times (centripetal CR form).
    const float dt0 = t[1] - t[0], dt1 = t[2] - t[1], dt2 = t[3] - t[2];
    Vec2 m1{}, m2{};
    if (dt0 > 0 && dt1 > 0) {
        m1.x = ((p[1].x - p[0].x) / dt0 - (p[2].x - p[0].x) / (dt0 + dt1) +
                (p[2].x - p[1].x) / dt1);
        m1.y = ((p[1].y - p[0].y) / dt0 - (p[2].y - p[0].y) / (dt0 + dt1) +
                (p[2].y - p[1].y) / dt1);
        m1.x *= dt1; m1.y *= dt1;
    }
    if (dt1 > 0 && dt2 > 0) {
        m2.x = ((p[2].x - p[1].x) / dt1 - (p[3].x - p[1].x) / (dt1 + dt2) +
                (p[3].x - p[2].x) / dt2);
        m2.y = ((p[2].y - p[1].y) / dt1 - (p[3].y - p[1].y) / (dt1 + dt2) +
                (p[3].y - p[2].y) / dt2);
        m2.x *= dt1; m2.y *= dt1;
    }
    return hermite(p[1], p[2], m1, m2, u);
}

/// Interpolate a batch of samples into `out` vertices with adaptive step size.
/// Returns number of vertices written. Allocation-free.
inline size_t interpolate_batch(const HidSample* samples, size_t count,
                                float step_px, Vec2* out, size_t out_cap) {
    if (count == 0 || out_cap == 0) return 0;

    // Convert fixed 24.8 to float once into a local window buffer.
    // Max window of 64 keeps us in L1; longer batches are processed in chunks.
    constexpr size_t kWindow = 64;
    Vec2 pts[kWindow];
    float ts[kWindow];

    size_t written = 0;
    Vec2 prev{0.f, 0.f};
    bool have_prev = false;
    auto emit = [&](Vec2 v) -> bool {
        // Dedupe: chunk seams and the tail point can coincide with the last
        // emitted vertex; zero-length segments are harmless but wasteful.
        if (have_prev && v.x == prev.x && v.y == prev.y) return true;
        if (written >= out_cap) return false;
        out[written++] = v;
        prev = v;
        have_prev = true;
        return true;
    };

    // Start from first point.
    emit({ FromFixed24_8(samples[0].dx), FromFixed24_8(samples[0].dy) });

    size_t i = 0;
    while (i + 3 < count && written < out_cap) {
        const size_t n = (count - i < kWindow) ? (count - i) : kWindow;
        for (size_t j = 0; j < n; ++j) {
            pts[j] = { FromFixed24_8(samples[i + j].dx), FromFixed24_8(samples[i + j].dy) };
        }
        centripetal_times(pts, n, ts);
        // NOTE: re-parameterizing per window means tangents near chunk seams
        // use slightly different chord times than a whole-batch fit would —
        // sub-pixel discontinuities at seams are possible. Acceptable for the
        // console demo; a GPU-side full-batch pass should be used if exact
        // C1 continuity across chunks is required.

        // Subdivide each interior segment adaptively by screen-space distance.
        for (size_t seg = 1; seg + 2 < n && written < out_cap; ++seg) {
            Vec2 p[4]{ pts[seg - 1], pts[seg], pts[seg + 1], pts[seg + 2] };
            float tt[4]{ ts[seg - 1], ts[seg], ts[seg + 1], ts[seg + 2] };

            const float len = std::sqrt(dist_sq(p[1], p[2]));
            int steps = static_cast<int>(len / step_px) + 1;
            if (steps > 32) steps = 32;

            for (int s = 1; s <= steps && written < out_cap; ++s) {
                emit(catmull_rom_segment(p, tt, static_cast<float>(s) / steps));
            }
        }
        // Overlap by 3 points so the next window's first CR segment starts
        // where this one ended (seg max here is n-3: pts[n-3]->pts[n-2] was
        // the last segment splined; pts[n-2]->pts[n-1] belongs to the next
        // window as its leading segment). Only the final short window (no
        // next window follows) advances past everything.
        if (count - i > n) i += n - 3; else i += n;
    }

    // Remainder: fewer than 4 unprocessed points remain after the last
    // spline window. Emit them as straight segments so interior samples are
    // not dropped geometrically (the Catmull-Rom loop needs a 4-point window).
    if (i + 1 < count && written < out_cap) {
        Vec2 prev_pt = { FromFixed24_8(samples[i].dx), FromFixed24_8(samples[i].dy) };
        for (size_t j = i + 1; j + 1 < count && written < out_cap; ++j) {
            Vec2 pt = { FromFixed24_8(samples[j].dx), FromFixed24_8(samples[j].dy) };
            const float len = std::sqrt(dist_sq(prev_pt, pt));
            // +1 matches the Catmull-Rom loop: even short segments advance at
            // least one sub-step so the tail never visually jumps a whole
            // sample gap.
            int steps = static_cast<int>(len / step_px) + 1;
            for (int s = 1; s <= steps && written < out_cap; ++s) {
                const float t = static_cast<float>(s) / steps;
                emit({ prev_pt.x + (pt.x - prev_pt.x) * t,
                       prev_pt.y + (pt.y - prev_pt.y) * t });
            }
            prev_pt = pt;
        }
    }

    // Tail: emit last raw point so the path always ends at true input.
    // NOTE: if out_cap is exhausted before this point, remaining interior
    // samples and the endpoint are silently dropped — callers size out_cap
    // generously (see console_demo: 4096 verts for <=1024 samples); there is
    // deliberately no truncation counter to keep this function stateless.
    if (written < out_cap)
        emit({ FromFixed24_8(samples[count - 1].dx), FromFixed24_8(samples[count - 1].dy) });

    return written;
}

} // namespace hid
