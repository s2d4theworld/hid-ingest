# hid-ingest — 8,000 Hz Raw HID Ingestion Engine

Lock-free SPSC raw-input ingestion engine per the engineering spec
(`../hid-ingestion-engine-spec.md`): up to 8,000 Hz pointer/stylus packets,
zero heap allocations on the hot path, 0% idle CPU, sub-millisecond latency.

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

Windows: MSVC 2022+. Linux: `libevdev`, `libudev` required.

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
  `DropOnOverflow=false` rejects silently — caller owns drop accounting. A correct
  latest-wins (drop-oldest) policy requires a Vyukov seq-per-slot ring; see the ring
  header for why the naive version is racy.
- Timestamps: Windows samples use QPC, Linux samples use kernel event time. They are
  NOT comparable across platforms and wrap (Windows 32-bit truncation, ~71 min);
  consumers must treat them as per-platform deltas only.
- Known limitation (Linux): devices are discovered once at start; there is no
  udev_monitor hotplug yet. A mouse plugged in later is not captured until restart.
- Consumer drains once per frame into a stack buffer; no geometry rebuild when empty.
