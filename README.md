# hid-ingest — 8,000 Hz Raw HID Ingestion Engine

![CI](https://github.com/s2d4t/hid-ingest/actions/workflows/ci.yml/badge.svg)

Lock-free SPSC raw-input ingestion engine: up to 8,000 Hz pointer/stylus
packets, zero heap allocations on the hot path, 0% idle CPU, sub-millisecond
latency. (Originally developed against an external engineering spec; this
document is self-contained.)

## License

MIT — see [LICENSE](LICENSE).

## Layout

```
include/hid_ingest/
  hid_sample.h           packed 16-byte POD (fixed-point 24.8)
  spsc_ring.h            lock-free SPSC ring, cache-line isolated, DropOnOverflow
  spline.h               chordal Catmull-Rom + cubic Hermite interpolation
  raw_input_producer.h   Win32 producer API
src/
  raw_input_producer.cpp Win32 Raw Input producer (message-only window, WM_INPUT)
  linux_evdev_producer.cpp  Linux evdev/libevdev producer (SCHED_FIFO + epoll ET)
tests/spsc_test.cpp      ordering/wraparound/MT stress tests
benchmark/stress_ingest.cpp  synthetic 8 kHz ingestion stress test (spec 7.1)
examples/console_demo.cpp    live mouse demo printing drain stats (Windows)
examples/evdev_demo.cpp      live mouse demo printing drain stats (Linux)
```

## Build

```
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release
```

Windows: MSVC 2022+ (VS 18 2026 tested). Linux: `libevdev`, `libudev` required.

## Run

```
build\stress_ingest.exe 10000000     # 10M samples @ 8 kHz
build\console_demo.exe               # move the mouse; Ctrl+C to quit
```

## Key design points

- Ring capacity is a power of two (default 16,384); indexing via bitmask.
- Producer/consumer state on separate 64-byte cache lines (no false sharing).
- acquire/release ordering: release on publish (`head_`), acquire on observe.
- Overflow policy: drop-incoming (back-pressure) with a drop counter for telemetry.
  `DropOnOverflow=false` rejects silently — caller owns drop accounting.
  ADR — why there is no latest-wins (drop-oldest) variant: an experimental
  Vyukov seq-per-slot ring (see commit b721c88 in git history) was built and
  removed. Every encoding of a 3-phase slot lifecycle (writable/full/reading)
  plus wrap-awareness in a single atomic sequence showed either torn-copy or
  order-violation races under concurrent overflow+read. For this engine's
  delta-stream workload, drop-incoming on a 16K-slot ring (~2 s of backlog at
  8 kHz) is sufficient. If latest-wins semantics are ever genuinely needed,
  use a single-slot seqlock or atomic triple buffer instead — do not force
  FIFO ring semantics onto a latest-state problem.
- Timestamps: Windows samples use QPC, Linux samples use kernel event time. They are
  NOT comparable across platforms and wrap (Windows 32-bit truncation, ~71 min);
  consumers must treat them as per-platform deltas only.
- Absolute coordinates (Win32): digitizer/absolute-mouse input spans 0..65535 and
  is scaled to the full virtual desktop — SM_CX/CYVIRTUALSCREEN for size plus
  SM_X/YVIRTUALSCREEN origin offset (which is negative when a monitor sits
  left/above the primary). Metrics are cached at start(); per-monitor DPI and
  runtime monitor add/remove/rearrange are out of scope (restart the producer
  after changing display topology).
- Known limitation (Linux): hotplug is supported via udev_monitor — devices
  plugged in after start() are captured automatically (removals handled via
  -ENODEV). Metadata-only events may trigger a redundant rescan (deduped).
  Requires the udev netlink socket to be permitted (unavailable in some
  containers; the producer then runs with initial discovery only).
- Consumer drains once per frame into a stack buffer; no geometry rebuild when empty.
