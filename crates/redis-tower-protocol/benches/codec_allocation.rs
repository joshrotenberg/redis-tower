//! Reproducible allocation and copy evidence for batched RESP decoding.
//!
//! This is a benchmark target rather than production code. Its allocator
//! wrapper measures process allocations while the production crate remains
//! `forbid(unsafe_code)`.

use bytes::{Buf, BytesMut};
use redis_tower_protocol::{Frame, RespCodec};
use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_util::codec::Decoder;

struct CountingAllocator;

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: this delegates the allocation with the exact caller layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: this delegates the allocation with the exact caller layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        // SAFETY: this delegates the pointer with the allocation's layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: this delegates the pointer and sizes supplied by the caller.
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            REALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            if new_size >= layout.size() {
                let growth = new_size - layout.size();
                ALLOCATED_BYTES.fetch_add(growth as u64, Ordering::Relaxed);
                LIVE_BYTES.fetch_add(growth as i64, Ordering::Relaxed);
            } else {
                let shrink = layout.size() - new_size;
                DEALLOCATED_BYTES.fetch_add(shrink as u64, Ordering::Relaxed);
                LIVE_BYTES.fetch_sub(shrink as i64, Ordering::Relaxed);
            }
        }
        new_pointer
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    allocation_calls: u64,
    reallocation_calls: u64,
    deallocation_calls: u64,
    allocated_bytes: u64,
    deallocated_bytes: u64,
    live_bytes: i64,
}

impl AllocationSnapshot {
    fn now() -> Self {
        Self {
            allocation_calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
            reallocation_calls: REALLOCATION_CALLS.load(Ordering::Relaxed),
            deallocation_calls: DEALLOCATION_CALLS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
            live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
        }
    }

    fn delta(self, earlier: Self) -> Value {
        json!({
            "allocation_calls": self.allocation_calls - earlier.allocation_calls,
            "reallocation_calls": self.reallocation_calls - earlier.reallocation_calls,
            "deallocation_calls": self.deallocation_calls - earlier.deallocation_calls,
            "allocated_bytes": self.allocated_bytes - earlier.allocated_bytes,
            "deallocated_bytes": self.deallocated_bytes - earlier.deallocated_bytes,
            "live_bytes_delta": self.live_bytes - earlier.live_bytes,
        })
    }
}

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    wire: Vec<u8>,
    frame_lengths: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Strategy {
    HistoricalWholeBufferCopy,
    ProductionFirstFrameCopy,
    SplitSharedStorage,
}

impl Strategy {
    const ALL: [Self; 3] = [
        Self::HistoricalWholeBufferCopy,
        Self::ProductionFirstFrameCopy,
        Self::SplitSharedStorage,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::HistoricalWholeBufferCopy => "historical_whole_buffer_copy",
            Self::ProductionFirstFrameCopy => "production_first_frame_copy",
            Self::SplitSharedStorage => "split_shared_storage",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::HistoricalWholeBufferCopy => {
                "emulates BytesMut::clone().freeze() before each parse"
            }
            Self::ProductionFirstFrameCopy => {
                "calls RespCodec after bounded preflight; copies only the completed first frame"
            }
            Self::SplitSharedStorage => {
                "splits known frame extents without copying; retained frames share the receive allocation"
            }
        }
    }

    fn expected_copied_wire_bytes(self, scenario: &Scenario) -> usize {
        match self {
            Self::HistoricalWholeBufferCopy => {
                let mut remaining = scenario.wire.len();
                scenario
                    .frame_lengths
                    .iter()
                    .map(|length| {
                        let copied = remaining;
                        remaining -= length;
                        copied
                    })
                    .sum()
            }
            Self::ProductionFirstFrameCopy => scenario.wire.len(),
            Self::SplitSharedStorage => 0,
        }
    }
}

struct DecodeOutcome {
    decoded_frames: usize,
    retained_frames: Option<Vec<Frame>>,
    input: BytesMut,
}

struct FirstDecodeOutcome {
    frame: Frame,
    unread: BytesMut,
}

fn append_frame(wire: &mut Vec<u8>, lengths: &mut Vec<usize>, frame: &[u8]) {
    wire.extend_from_slice(frame);
    lengths.push(frame.len());
}

fn append_bulk(wire: &mut Vec<u8>, lengths: &mut Vec<usize>, size: usize, seed: u8) {
    let start = wire.len();
    wire.extend_from_slice(format!("${size}\r\n").as_bytes());
    wire.extend((0..size).map(|offset| seed.wrapping_add(offset as u8)));
    wire.extend_from_slice(b"\r\n");
    lengths.push(wire.len() - start);
}

fn simple_scenario() -> Scenario {
    let mut wire = Vec::new();
    let mut frame_lengths = Vec::new();
    for _ in 0..512 {
        append_frame(&mut wire, &mut frame_lengths, b"+OK\r\n");
    }
    Scenario {
        name: "simple_512",
        wire,
        frame_lengths,
    }
}

fn mixed_scenario() -> Scenario {
    let mut wire = Vec::new();
    let mut frame_lengths = Vec::new();
    for round in 0..16u8 {
        append_frame(&mut wire, &mut frame_lengths, b"+OK\r\n");
        append_bulk(&mut wire, &mut frame_lengths, 0, round);
        append_bulk(&mut wire, &mut frame_lengths, 16, round);
        append_bulk(&mut wire, &mut frame_lengths, 256, round);
        append_bulk(&mut wire, &mut frame_lengths, 4096, round);
        append_frame(&mut wire, &mut frame_lengths, b":42\r\n");
        append_frame(
            &mut wire,
            &mut frame_lengths,
            b">2\r\n+invalidate\r\n$3\r\nkey\r\n",
        );
        append_frame(
            &mut wire,
            &mut frame_lengths,
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        );
    }
    Scenario {
        name: "mixed_128",
        wire,
        frame_lengths,
    }
}

fn retained_head_scenario() -> Scenario {
    let mut wire = Vec::new();
    let mut frame_lengths = Vec::new();
    append_bulk(&mut wire, &mut frame_lengths, 16, 0x40);
    append_bulk(&mut wire, &mut frame_lengths, 256 * 1024, 0x80);
    Scenario {
        name: "retained_16_byte_head_with_256kib_unread_tail",
        wire,
        frame_lengths,
    }
}

fn fragmented_scenario() -> Scenario {
    let mut wire = Vec::new();
    let mut frame_lengths = Vec::new();
    for round in 0..16u8 {
        append_frame(&mut wire, &mut frame_lengths, b"+OK\r\n");
        append_bulk(&mut wire, &mut frame_lengths, 0, round);
        append_bulk(&mut wire, &mut frame_lengths, 16, round);
        append_bulk(&mut wire, &mut frame_lengths, 256, round);
        append_frame(&mut wire, &mut frame_lengths, b":42\r\n");
        append_frame(
            &mut wire,
            &mut frame_lengths,
            b">2\r\n+invalidate\r\n$3\r\nkey\r\n",
        );
        append_frame(
            &mut wire,
            &mut frame_lengths,
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        );
    }
    Scenario {
        name: "mixed_112_fragmented",
        wire,
        frame_lengths,
    }
}

fn decode(scenario: &Scenario, strategy: Strategy, retain: bool) -> DecodeOutcome {
    let mut input = BytesMut::from(scenario.wire.as_slice());
    let mut retained_frames = retain.then(|| Vec::with_capacity(scenario.frame_lengths.len()));
    let mut decoded_frames = 0;

    match strategy {
        Strategy::HistoricalWholeBufferCopy => {
            while !input.is_empty() {
                let before = input.len();
                let parser_input = input.clone().freeze();
                let (frame, remaining) = resp_rs::resp3::parse_frame(parser_input).unwrap();
                let consumed = before - remaining.len();
                assert_eq!(consumed, scenario.frame_lengths[decoded_frames]);
                input.advance(consumed);
                decoded_frames += 1;
                retain_or_drop(&mut retained_frames, frame);
            }
        }
        Strategy::ProductionFirstFrameCopy => {
            let mut codec = RespCodec::new();
            while let Some(frame) = codec.decode(&mut input).unwrap() {
                decoded_frames += 1;
                retain_or_drop(&mut retained_frames, frame);
            }
        }
        Strategy::SplitSharedStorage => {
            for &frame_len in &scenario.frame_lengths {
                let parser_input = input.split_to(frame_len).freeze();
                let (frame, remaining) = resp_rs::resp3::parse_frame(parser_input).unwrap();
                assert!(remaining.is_empty());
                decoded_frames += 1;
                retain_or_drop(&mut retained_frames, frame);
            }
        }
    }

    assert!(input.is_empty());
    DecodeOutcome {
        decoded_frames,
        retained_frames,
        input,
    }
}

fn retain_or_drop(retained: &mut Option<Vec<Frame>>, frame: Frame) {
    if let Some(frames) = retained {
        frames.push(frame);
    } else {
        black_box(frame);
    }
}

fn decode_first(scenario: &Scenario, strategy: Strategy) -> FirstDecodeOutcome {
    let mut unread = BytesMut::from(scenario.wire.as_slice());
    let frame = match strategy {
        Strategy::HistoricalWholeBufferCopy => {
            let before = unread.len();
            let parser_input = unread.clone().freeze();
            let (frame, remaining) = resp_rs::resp3::parse_frame(parser_input).unwrap();
            let consumed = before - remaining.len();
            assert_eq!(consumed, scenario.frame_lengths[0]);
            unread.advance(consumed);
            frame
        }
        Strategy::ProductionFirstFrameCopy => {
            RespCodec::new().decode(&mut unread).unwrap().unwrap()
        }
        Strategy::SplitSharedStorage => {
            let parser_input = unread.split_to(scenario.frame_lengths[0]).freeze();
            let (frame, remaining) = resp_rs::resp3::parse_frame(parser_input).unwrap();
            assert!(remaining.is_empty());
            frame
        }
    };
    FirstDecodeOutcome { frame, unread }
}

fn decode_fragmented(scenario: &Scenario, chunk_size: usize, retain: bool) -> DecodeOutcome {
    let mut input = BytesMut::new();
    let mut codec = RespCodec::new();
    let mut retained_frames = retain.then(|| Vec::with_capacity(scenario.frame_lengths.len()));
    let mut decoded_frames = 0;

    for chunk in scenario.wire.chunks(chunk_size) {
        input.extend_from_slice(chunk);
        while let Some(frame) = codec.decode(&mut input).unwrap() {
            decoded_frames += 1;
            retain_or_drop(&mut retained_frames, frame);
        }
    }

    assert!(input.is_empty());
    DecodeOutcome {
        decoded_frames,
        retained_frames,
        input,
    }
}

fn validate_equivalence(scenario: &Scenario) {
    let production = decode(scenario, Strategy::ProductionFirstFrameCopy, true);
    let expected = production.retained_frames.as_ref().unwrap();
    assert_eq!(production.decoded_frames, scenario.frame_lengths.len());

    for strategy in [
        Strategy::HistoricalWholeBufferCopy,
        Strategy::SplitSharedStorage,
    ] {
        let alternative = decode(scenario, strategy, true);
        assert_eq!(alternative.decoded_frames, production.decoded_frames);
        assert_eq!(alternative.retained_frames.as_ref().unwrap(), expected);
    }
}

fn allocation_measurement(scenario: &Scenario, strategy: Strategy, retain: bool) -> Value {
    let before = AllocationSnapshot::now();
    let outcome = black_box(decode(scenario, strategy, retain));
    let after_decode = AllocationSnapshot::now();
    assert_eq!(outcome.decoded_frames, scenario.frame_lengths.len());
    assert!(outcome.input.is_empty());
    let retained_frame_count = outcome.retained_frames.as_ref().map_or(0, Vec::len);
    black_box(&outcome);
    drop(outcome);
    let after_drop = AllocationSnapshot::now();

    json!({
        "through_decode": after_decode.delta(before),
        "through_drop": after_drop.delta(before),
        "live_bytes_released_by_drop": after_decode.live_bytes - after_drop.live_bytes,
        "retained_frame_count": retained_frame_count,
    })
}

fn retained_head_measurement(scenario: &Scenario, strategy: Strategy) -> Value {
    let before = AllocationSnapshot::now();
    let outcome = black_box(decode_first(scenario, strategy));
    let after_decode = AllocationSnapshot::now();
    let FirstDecodeOutcome { frame, unread } = outcome;
    assert_eq!(
        unread.len(),
        scenario.wire.len() - scenario.frame_lengths[0]
    );
    drop(unread);
    let after_unread_drop = AllocationSnapshot::now();
    black_box(&frame);
    drop(frame);
    let after_frame_drop = AllocationSnapshot::now();

    json!({
        "through_decode": after_decode.delta(before),
        "after_unread_buffer_drop": after_unread_drop.delta(before),
        "after_retained_frame_drop": after_frame_drop.delta(before),
        "live_bytes_pinned_by_retained_head": after_unread_drop.live_bytes - before.live_bytes,
        "live_bytes_released_by_retained_head_drop": after_unread_drop.live_bytes - after_frame_drop.live_bytes,
    })
}

fn fragmented_allocation_measurement(scenario: &Scenario, chunk_size: usize) -> Value {
    let before = AllocationSnapshot::now();
    let outcome = black_box(decode_fragmented(scenario, chunk_size, true));
    let after_decode = AllocationSnapshot::now();
    assert_eq!(outcome.decoded_frames, scenario.frame_lengths.len());
    assert_eq!(
        outcome.retained_frames.as_ref().unwrap().len(),
        outcome.decoded_frames
    );
    black_box(&outcome);
    drop(outcome);
    let after_drop = AllocationSnapshot::now();

    json!({
        "through_decode": after_decode.delta(before),
        "through_drop": after_drop.delta(before),
        "live_bytes_released_by_drop": after_decode.live_bytes - after_drop.live_bytes,
    })
}

fn timing_samples(
    scenario: &Scenario,
    strategy: Strategy,
    retain: bool,
    samples: usize,
    iterations: usize,
) -> Vec<u128> {
    for _ in 0..3 {
        black_box(decode(scenario, strategy, retain));
    }
    (0..samples)
        .map(|_| {
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(decode(scenario, strategy, retain));
            }
            started.elapsed().as_nanos() / iterations as u128
        })
        .collect()
}

fn timing_samples_first(
    scenario: &Scenario,
    strategy: Strategy,
    samples: usize,
    iterations: usize,
) -> Vec<u128> {
    for _ in 0..3 {
        black_box(decode_first(scenario, strategy));
    }
    (0..samples)
        .map(|_| {
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(decode_first(scenario, strategy));
            }
            started.elapsed().as_nanos() / iterations as u128
        })
        .collect()
}

fn timing_samples_fragmented(
    scenario: &Scenario,
    chunk_size: usize,
    samples: usize,
    iterations: usize,
) -> Vec<u128> {
    for _ in 0..3 {
        black_box(decode_fragmented(scenario, chunk_size, true));
    }
    (0..samples)
        .map(|_| {
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(decode_fragmented(scenario, chunk_size, true));
            }
            started.elapsed().as_nanos() / iterations as u128
        })
        .collect()
}

fn command_output(program: &str, args: &[&str], directory: &Path) -> String {
    Command::new(program)
        .args(args)
        .current_dir(directory)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn dependency_version(lockfile: &str, package: &str) -> String {
    lockfile
        .split("[[package]]")
        .skip(1)
        .find_map(|section| {
            let mut name = None;
            let mut version = None;
            for line in section.lines().map(str::trim) {
                if let Some(value) = line.strip_prefix("name = \"") {
                    name = value.strip_suffix('"');
                } else if let Some(value) = line.strip_prefix("version = \"") {
                    version = value.strip_suffix('"');
                }
            }
            if name == Some(package) {
                version.map(str::to_owned)
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_owned())
}

struct Options {
    samples: usize,
    iterations: usize,
    output: Option<PathBuf>,
    check: bool,
    quiet: bool,
}

fn parse_options() -> Result<Options, String> {
    let mut options = Options {
        samples: 15,
        iterations: 10,
        output: None,
        check: false,
        quiet: false,
    };
    let mut explicit_run = false;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--bench" => {}
            "--evidence" => explicit_run = true,
            "--test" => {
                explicit_run = true;
                options.samples = 1;
                options.iterations = 1;
                options.check = true;
                options.quiet = true;
            }
            "--check" => {
                explicit_run = true;
                options.check = true;
            }
            "--samples" => {
                explicit_run = true;
                options.samples = args
                    .next()
                    .ok_or("--samples requires a positive integer")?
                    .parse()
                    .map_err(|_| "--samples requires a positive integer")?;
            }
            "--iterations" => {
                explicit_run = true;
                options.iterations = args
                    .next()
                    .ok_or("--iterations requires a positive integer")?
                    .parse()
                    .map_err(|_| "--iterations requires a positive integer")?;
            }
            "--output" => {
                explicit_run = true;
                options.output = Some(PathBuf::from(
                    args.next().ok_or("--output requires a path")?,
                ));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if !explicit_run {
        options.samples = 1;
        options.iterations = 1;
        options.check = true;
        options.quiet = true;
    }
    if options.samples == 0 || options.iterations == 0 {
        return Err("--samples and --iterations must be positive".to_owned());
    }
    Ok(options)
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options().map_err(std::io::Error::other)?;
    let scenarios = [simple_scenario(), mixed_scenario()];
    let mut scenario_records = Vec::new();

    for scenario in &scenarios {
        validate_equivalence(scenario);
        let mut strategy_records = Vec::new();
        let mut retained_allocated_bytes = Vec::new();

        for strategy in Strategy::ALL {
            let copied_wire_bytes = strategy.expected_copied_wire_bytes(scenario);
            for retain in [false, true] {
                let allocation = allocation_measurement(scenario, strategy, retain);
                if retain {
                    retained_allocated_bytes.push((
                        strategy,
                        allocation["through_decode"]["allocated_bytes"]
                            .as_u64()
                            .unwrap(),
                    ));
                }
                strategy_records.push(json!({
                    "strategy": strategy.name(),
                    "description": strategy.description(),
                    "retention": if retain { "retain_until_batch_complete" } else { "drop_each_frame" },
                    "expected_copied_wire_bytes": copied_wire_bytes,
                    "allocation": allocation,
                    "raw_duration_ns_per_iteration": timing_samples(
                        scenario,
                        strategy,
                        retain,
                        options.samples,
                        options.iterations,
                    ),
                }));
            }
        }

        if options.check {
            let allocated = |strategy| {
                retained_allocated_bytes
                    .iter()
                    .find_map(|(candidate, bytes)| (*candidate == strategy).then_some(*bytes))
                    .unwrap()
            };
            assert!(
                allocated(Strategy::HistoricalWholeBufferCopy)
                    > allocated(Strategy::ProductionFirstFrameCopy),
                "historical whole-buffer allocation must exceed the production path"
            );
            assert_eq!(
                Strategy::ProductionFirstFrameCopy.expected_copied_wire_bytes(scenario),
                scenario.wire.len()
            );
            assert_eq!(
                Strategy::SplitSharedStorage.expected_copied_wire_bytes(scenario),
                0
            );
        }

        scenario_records.push(json!({
            "name": scenario.name,
            "frame_count": scenario.frame_lengths.len(),
            "wire_bytes": scenario.wire.len(),
            "minimum_frame_bytes": scenario.frame_lengths.iter().min().unwrap(),
            "maximum_frame_bytes": scenario.frame_lengths.iter().max().unwrap(),
            "strategies": strategy_records,
        }));
    }

    let retained_head = retained_head_scenario();
    let production_head = decode_first(&retained_head, Strategy::ProductionFirstFrameCopy);
    let expected_head = production_head.frame.clone();
    drop(production_head);
    for strategy in [
        Strategy::HistoricalWholeBufferCopy,
        Strategy::SplitSharedStorage,
    ] {
        let alternative = decode_first(&retained_head, strategy);
        assert_eq!(alternative.frame, expected_head);
    }
    let retained_head_records = Strategy::ALL
        .into_iter()
        .map(|strategy| {
            let expected_copied_wire_bytes = match strategy {
                Strategy::HistoricalWholeBufferCopy => retained_head.wire.len(),
                Strategy::ProductionFirstFrameCopy => retained_head.frame_lengths[0],
                Strategy::SplitSharedStorage => 0,
            };
            json!({
                "strategy": strategy.name(),
                "description": strategy.description(),
                "expected_copied_wire_bytes": expected_copied_wire_bytes,
                "allocation": retained_head_measurement(&retained_head, strategy),
                "raw_duration_ns_per_iteration": timing_samples_first(
                    &retained_head,
                    strategy,
                    options.samples,
                    options.iterations,
                ),
            })
        })
        .collect::<Vec<_>>();
    if options.check {
        let pinned = |strategy: Strategy| {
            retained_head_records
                .iter()
                .find(|record| record["strategy"] == strategy.name())
                .and_then(|record| {
                    record["allocation"]["live_bytes_pinned_by_retained_head"].as_i64()
                })
                .unwrap()
        };
        assert!(
            pinned(Strategy::HistoricalWholeBufferCopy)
                > pinned(Strategy::ProductionFirstFrameCopy),
            "the historical clone must pin more memory than the bounded first-frame copy"
        );
        assert!(
            pinned(Strategy::SplitSharedStorage) > pinned(Strategy::ProductionFirstFrameCopy),
            "a retained split head must pin the unread receive allocation"
        );
    }

    let fragmented = fragmented_scenario();
    let contiguous = decode(&fragmented, Strategy::ProductionFirstFrameCopy, true);
    let expected_fragmented_frames = contiguous.retained_frames.as_ref().unwrap();
    let fragmented_records = [1usize, 7, 64, 1024]
        .into_iter()
        .map(|chunk_size| {
            let decoded = decode_fragmented(&fragmented, chunk_size, true);
            assert_eq!(
                decoded.retained_frames.as_ref().unwrap(),
                expected_fragmented_frames
            );
            json!({
                "chunk_size": chunk_size,
                "expected_copied_wire_bytes": fragmented.wire.len(),
                "allocation": fragmented_allocation_measurement(&fragmented, chunk_size),
                "raw_duration_ns_per_iteration": timing_samples_fragmented(
                    &fragmented,
                    chunk_size,
                    options.samples,
                    options.iterations,
                ),
            })
        })
        .collect::<Vec<_>>();

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest_dir.join("../..");
    let lockfile = include_str!("../../../Cargo.lock");
    let document = json!({
        "schema_version": 1,
        "kind": "redis-tower-protocol-codec-copy-evidence",
        "generated_unix_seconds": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        "provenance": {
            "source_sha": command_output("git", &["rev-parse", "HEAD"], &workspace),
            "worktree_status": command_output("git", &["status", "--porcelain"], &workspace),
            "rustc": command_output("rustc", &["-vV"], &workspace),
            "cargo": command_output("cargo", &["-vV"], &workspace),
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH,
            "dependencies": {
                "bytes": dependency_version(lockfile, "bytes"),
                "criterion": dependency_version(lockfile, "criterion"),
                "resp-rs": dependency_version(lockfile, "resp-rs"),
                "tokio-util": dependency_version(lockfile, "tokio-util"),
            },
        },
        "measurement_policy": {
            "samples": options.samples,
            "iterations_per_sample": options.iterations,
            "warmup_iterations": 3,
            "allocator": "instrumented std::alloc::System; allocated_bytes is successful initial Layout sizes plus positive realloc growth, live_bytes_delta is the change in requested live Layout bytes, and both exclude allocator metadata and rounding",
            "timing": "single-process raw samples with allocation instrumentation active; directional mechanism evidence, not client throughput",
            "copied_wire_bytes": "deterministic bytes copied solely to create parser input",
        },
        "scenarios": scenario_records,
        "retained_head_lifetime": {
            "name": retained_head.name,
            "frame_count": retained_head.frame_lengths.len(),
            "wire_bytes": retained_head.wire.len(),
            "retained_head_frame_bytes": retained_head.frame_lengths[0],
            "unread_tail_bytes": retained_head.wire.len() - retained_head.frame_lengths[0],
            "strategies": retained_head_records,
        },
        "fragmented_production": {
            "name": fragmented.name,
            "frame_count": fragmented.frame_lengths.len(),
            "wire_bytes": fragmented.wire.len(),
            "retention": "retain_until_batch_complete",
            "chunks": fragmented_records,
        },
    });
    let rendered = serde_json::to_string_pretty(&document)? + "\n";
    if let Some(path) = options.output {
        std::fs::write(path, rendered)?;
    } else if !options.quiet {
        print!("{rendered}");
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("codec allocation evidence failed: {error}");
        std::process::exit(1);
    }
}
