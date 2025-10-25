//! Memory tracking utilities for performance analysis
//!
//! This module provides utilities for tracking memory usage during large payload
//! testing to validate memory efficiency and detect potential leaks.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Global memory tracker for test analysis
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Memory tracking session for individual tests
pub struct MemorySession {
    start_allocated: usize,
    start_peak: usize,
    start_count: usize,
    start_time: Instant,
    measurements: Mutex<Vec<MemoryMeasurement>>,
}

/// Single memory measurement
#[derive(Debug, Clone)]
pub struct MemoryMeasurement {
    pub timestamp: Duration,
    pub allocated_bytes: usize,
    pub allocation_count: usize,
}

/// Memory analysis results
#[derive(Debug, Clone)]
pub struct MemoryAnalysis {
    pub baseline_allocated: usize,
    pub peak_allocated: usize,
    pub total_allocations: usize,
    pub final_allocated: usize,
    pub duration: Duration,
    pub measurements: Vec<MemoryMeasurement>,
}

impl MemorySession {
    /// Start a new memory tracking session
    pub fn new() -> Self {
        let start_allocated = ALLOCATED_BYTES.load(Ordering::SeqCst);
        let start_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);
        let start_count = ALLOCATION_COUNT.load(Ordering::SeqCst);

        Self {
            start_allocated,
            start_peak,
            start_count,
            start_time: Instant::now(),
            measurements: Mutex::new(Vec::new()),
        }
    }

    /// Record current memory state
    pub fn record(&self) {
        let current_allocated = ALLOCATED_BYTES.load(Ordering::SeqCst);
        let current_count = ALLOCATION_COUNT.load(Ordering::SeqCst);
        let timestamp = self.start_time.elapsed();

        let measurement = MemoryMeasurement {
            timestamp,
            allocated_bytes: current_allocated,
            allocation_count: current_count,
        };

        if let Ok(mut measurements) = self.measurements.lock() {
            measurements.push(measurement);
        }
    }

    /// End the session and return analysis
    pub fn finish(self) -> MemoryAnalysis {
        let final_allocated = ALLOCATED_BYTES.load(Ordering::SeqCst);
        let peak_allocated = PEAK_ALLOCATED.load(Ordering::SeqCst);
        let final_count = ALLOCATION_COUNT.load(Ordering::SeqCst);
        let duration = self.start_time.elapsed();

        let measurements = self.measurements.into_inner().unwrap_or_default();

        MemoryAnalysis {
            baseline_allocated: self.start_allocated,
            peak_allocated: peak_allocated.saturating_sub(self.start_peak),
            total_allocations: final_count.saturating_sub(self.start_count),
            final_allocated: final_allocated.saturating_sub(self.start_allocated),
            duration,
            measurements,
        }
    }

    /// Get current memory usage relative to session start
    pub fn current_usage(&self) -> usize {
        let current = ALLOCATED_BYTES.load(Ordering::SeqCst);
        current.saturating_sub(self.start_allocated)
    }

    /// Get peak memory usage relative to session start
    pub fn peak_usage(&self) -> usize {
        let peak = PEAK_ALLOCATED.load(Ordering::SeqCst);
        peak.saturating_sub(self.start_peak)
    }
}

impl MemoryAnalysis {
    /// Calculate memory efficiency ratio (actual vs theoretical minimum)
    pub fn efficiency_ratio(&self, theoretical_minimum: usize) -> f64 {
        if theoretical_minimum == 0 {
            return 1.0;
        }
        self.peak_allocated as f64 / theoretical_minimum as f64
    }

    /// Calculate average memory usage over time
    pub fn average_usage(&self) -> f64 {
        if self.measurements.is_empty() {
            return self.final_allocated as f64;
        }

        let sum: usize = self.measurements.iter().map(|m| m.allocated_bytes).sum();

        sum as f64 / self.measurements.len() as f64
    }

    /// Check if there are potential memory leaks
    pub fn has_potential_leak(&self) -> bool {
        // Consider it a potential leak if final usage is more than 10% of peak
        self.final_allocated > self.peak_allocated / 10
    }

    /// Get memory usage growth rate (bytes per second)
    pub fn growth_rate(&self) -> f64 {
        if self.duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.final_allocated as f64 / self.duration.as_secs_f64()
    }

    /// Get allocation rate (allocations per second)
    pub fn allocation_rate(&self) -> f64 {
        if self.duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.total_allocations as f64 / self.duration.as_secs_f64()
    }

    /// Print detailed analysis report
    pub fn print_report(&self, test_name: &str) {
        println!("=== Memory Analysis Report: {} ===", test_name);
        println!("Duration: {:.2}ms", self.duration.as_secs_f64() * 1000.0);
        println!("Baseline allocated: {} bytes", self.baseline_allocated);
        println!("Peak allocated: {} bytes", self.peak_allocated);
        println!("Final allocated: {} bytes", self.final_allocated);
        println!("Total allocations: {}", self.total_allocations);
        println!("Average usage: {:.1} bytes", self.average_usage());
        println!("Growth rate: {:.1} bytes/sec", self.growth_rate());
        println!("Allocation rate: {:.1} allocs/sec", self.allocation_rate());

        if self.has_potential_leak() {
            println!("⚠️  Potential memory leak detected!");
        } else {
            println!("✅ No memory leaks detected");
        }

        println!("================================");
    }
}

/// Memory tracking utilities
pub struct MemoryTracker;

impl MemoryTracker {
    /// Record an allocation
    pub fn record_allocation(size: usize) {
        ALLOCATED_BYTES.fetch_add(size, Ordering::SeqCst);
        ALLOCATION_COUNT.fetch_add(1, Ordering::SeqCst);

        // Update peak
        let current = ALLOCATED_BYTES.load(Ordering::SeqCst);
        PEAK_ALLOCATED.fetch_max(current, Ordering::SeqCst);
    }

    /// Record a deallocation
    pub fn record_deallocation(size: usize) {
        ALLOCATED_BYTES.fetch_sub(size, Ordering::SeqCst);
    }

    /// Reset all counters (for test isolation)
    pub fn reset() {
        ALLOCATED_BYTES.store(0, Ordering::SeqCst);
        PEAK_ALLOCATED.store(0, Ordering::SeqCst);
        ALLOCATION_COUNT.store(0, Ordering::SeqCst);
    }

    /// Get current allocated bytes
    pub fn current_allocated() -> usize {
        ALLOCATED_BYTES.load(Ordering::SeqCst)
    }

    /// Get peak allocated bytes
    pub fn peak_allocated() -> usize {
        PEAK_ALLOCATED.load(Ordering::SeqCst)
    }

    /// Get total allocation count
    pub fn allocation_count() -> usize {
        ALLOCATION_COUNT.load(Ordering::SeqCst)
    }
}

/// Memory pressure testing utilities
pub struct MemoryPressureTester {
    initial_state: MemoryState,
}

#[derive(Debug, Clone)]
struct MemoryState {
    allocated: usize,
    peak: usize,
    count: usize,
}

impl MemoryPressureTester {
    /// Start memory pressure testing
    pub fn new() -> Self {
        Self {
            initial_state: MemoryState {
                allocated: MemoryTracker::current_allocated(),
                peak: MemoryTracker::peak_allocated(),
                count: MemoryTracker::allocation_count(),
            },
        }
    }

    /// Simulate memory pressure by allocating and deallocating
    pub fn apply_pressure(&self, allocations: usize, size_per_allocation: usize) {
        let mut allocations_made = Vec::new();

        // Allocate
        for _ in 0..allocations {
            let allocation = vec![0u8; size_per_allocation];
            MemoryTracker::record_allocation(size_per_allocation);
            allocations_made.push(allocation);
        }

        // Deallocate
        for allocation in allocations_made {
            MemoryTracker::record_deallocation(allocation.len());
            drop(allocation);
        }
    }

    /// Check if system recovered from pressure
    pub fn check_recovery(&self) -> bool {
        let current = MemoryTracker::current_allocated();
        // Allow for some tolerance (within 10% of initial state)
        let tolerance = self.initial_state.allocated / 10;
        current <= self.initial_state.allocated + tolerance
    }
}

/// Utility for testing memory usage patterns
pub fn measure_memory_usage<F, R>(test_name: &str, operation: F) -> (R, MemoryAnalysis)
where
    F: FnOnce() -> R,
{
    let session = MemorySession::new();

    // Record initial state
    session.record();

    // Execute the operation
    let result = operation();

    // Record final state
    session.record();

    // Get analysis
    let analysis = session.finish();

    // Print report if requested
    if std::env::var("MEMORY_REPORTS").is_ok() {
        analysis.print_report(test_name);
    }

    (result, analysis)
}

/// Macro for easy memory measurement
#[macro_export]
macro_rules! measure_memory {
    ($name:expr, $block:block) => {
        $crate::test_utils::memory_tracking::measure_memory_usage($name, || $block)
    };
}

/// Memory-aware test runner
pub struct MemoryTestRunner {
    max_memory_mb: usize,
    max_allocations: usize,
}

impl MemoryTestRunner {
    /// Create a new memory test runner with limits
    pub fn new(max_memory_mb: usize, max_allocations: usize) -> Self {
        Self {
            max_memory_mb,
            max_allocations,
        }
    }

    /// Run a test with memory limits
    pub fn run_test<F, R>(&self, test_name: &str, test_fn: F) -> Result<(R, MemoryAnalysis), String>
    where
        F: FnOnce() -> R,
    {
        let (result, analysis) = measure_memory_usage(test_name, test_fn);

        // Check memory limits
        let max_bytes = self.max_memory_mb * 1024 * 1024;
        if analysis.peak_allocated > max_bytes {
            return Err(format!(
                "Memory limit exceeded: {} bytes > {} bytes",
                analysis.peak_allocated, max_bytes
            ));
        }

        if analysis.total_allocations > self.max_allocations {
            return Err(format!(
                "Allocation limit exceeded: {} > {}",
                analysis.total_allocations, self.max_allocations
            ));
        }

        Ok((result, analysis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_memory_session_basic() {
        MemoryTracker::reset();

        let session = MemorySession::new();

        // Simulate some allocations
        MemoryTracker::record_allocation(1024);
        session.record();

        MemoryTracker::record_allocation(2048);
        session.record();

        let analysis = session.finish();

        assert!(analysis.peak_allocated >= 3072); // 1024 + 2048
        assert_eq!(analysis.total_allocations, 2);
    }

    #[test]
    fn test_memory_measurement() {
        MemoryTracker::reset();

        let (result, analysis) = measure_memory_usage("test", || {
            MemoryTracker::record_allocation(1000);
            42
        });

        assert_eq!(result, 42);
        assert!(analysis.peak_allocated >= 1000);
    }

    #[test]
    #[ignore = "Memory pressure test - platform specific"]
    fn test_memory_pressure() {
        MemoryTracker::reset();

        let tester = MemoryPressureTester::new();
        tester.apply_pressure(10, 1024);

        // Should recover after pressure test
        thread::sleep(Duration::from_millis(10)); // Allow for cleanup
        assert!(tester.check_recovery());
    }

    #[test]
    fn test_memory_test_runner() {
        MemoryTracker::reset();

        let runner = MemoryTestRunner::new(1, 10); // 1MB limit, 10 allocations

        let result = runner.run_test("small_test", || {
            MemoryTracker::record_allocation(100);
            "success"
        });

        assert!(result.is_ok());
        let (test_result, _analysis) = result.unwrap();
        assert_eq!(test_result, "success");
    }

    #[test]
    fn test_memory_limit_exceeded() {
        MemoryTracker::reset();

        let runner = MemoryTestRunner::new(1, 10); // 1MB limit

        let result = runner.run_test("large_test", || {
            MemoryTracker::record_allocation(2 * 1024 * 1024); // 2MB
            "should_fail"
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Memory limit exceeded"));
    }

    #[test]
    fn test_efficiency_ratio() {
        let analysis = MemoryAnalysis {
            baseline_allocated: 0,
            peak_allocated: 2048,
            total_allocations: 2,
            final_allocated: 1024,
            duration: Duration::from_millis(100),
            measurements: vec![],
        };

        assert_eq!(analysis.efficiency_ratio(1024), 2.0);
        assert_eq!(analysis.efficiency_ratio(2048), 1.0);
        assert_eq!(analysis.efficiency_ratio(0), 1.0);
    }

    #[test]
    fn test_leak_detection() {
        let analysis = MemoryAnalysis {
            baseline_allocated: 0,
            peak_allocated: 1000,
            total_allocations: 1,
            final_allocated: 200, // 20% of peak
            duration: Duration::from_millis(100),
            measurements: vec![],
        };

        assert!(analysis.has_potential_leak());

        let no_leak_analysis = MemoryAnalysis {
            baseline_allocated: 0,
            peak_allocated: 1000,
            total_allocations: 1,
            final_allocated: 50, // 5% of peak
            duration: Duration::from_millis(100),
            measurements: vec![],
        };

        assert!(!no_leak_analysis.has_potential_leak());
    }
}
