//! Command pipelining support.
//!
//! Handles batched command processing for Redis and Memcached protocols.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pipeline configuration.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum number of commands in a pipeline.
    pub max_depth: usize,

    /// Whether to flush immediately on sync commands.
    pub flush_on_sync: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_depth: 64,
            flush_on_sync: true,
        }
    }
}

/// Pipeline state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineState {
    /// Ready to accept commands.
    Ready,
    /// Pipeline is full.
    Full,
    /// Pipeline is flushing.
    Flushing,
}

/// A pipeline of commands waiting to be processed.
#[derive(Debug)]
pub struct Pipeline<C> {
    /// Queued commands.
    commands: VecDeque<C>,

    /// Configuration.
    config: PipelineConfig,

    /// Current state.
    state: PipelineState,

    /// Total commands queued.
    total_queued: AtomicUsize,

    /// Total commands flushed.
    total_flushed: AtomicUsize,
}

impl<C> Pipeline<C> {
    /// Create a new pipeline.
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            commands: VecDeque::with_capacity(config.max_depth),
            config,
            state: PipelineState::Ready,
            total_queued: AtomicUsize::new(0),
            total_flushed: AtomicUsize::new(0),
        }
    }

    /// Get current pipeline depth.
    pub fn depth(&self) -> usize {
        self.commands.len()
    }

    /// Check if pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Check if pipeline is full.
    pub fn is_full(&self) -> bool {
        self.commands.len() >= self.config.max_depth
    }

    /// Get current state.
    pub fn state(&self) -> PipelineState {
        if self.is_full() {
            PipelineState::Full
        } else {
            self.state
        }
    }

    /// Try to enqueue a command.
    ///
    /// Returns `Err(command)` if the pipeline is full.
    pub fn enqueue(&mut self, command: C) -> Result<(), C> {
        if self.is_full() {
            return Err(command);
        }

        self.commands.push_back(command);
        self.total_queued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Dequeue the next command.
    pub fn dequeue(&mut self) -> Option<C> {
        let cmd = self.commands.pop_front();
        if cmd.is_some() {
            self.total_flushed.fetch_add(1, Ordering::Relaxed);
        }
        cmd
    }

    /// Drain all queued commands.
    pub fn drain(&mut self) -> impl Iterator<Item = C> + '_ {
        let count = self.commands.len();
        self.total_flushed.fetch_add(count, Ordering::Relaxed);
        self.commands.drain(..)
    }

    /// Clear the pipeline without processing.
    pub fn clear(&mut self) {
        self.commands.clear();
        self.state = PipelineState::Ready;
    }

    /// Get total commands queued.
    pub fn total_queued(&self) -> usize {
        self.total_queued.load(Ordering::Relaxed)
    }

    /// Get total commands flushed.
    pub fn total_flushed(&self) -> usize {
        self.total_flushed.load(Ordering::Relaxed)
    }
}

impl<C> Default for Pipeline<C> {
    fn default() -> Self {
        Self::new(PipelineConfig::default())
    }
}

/// Response pipeline for matching responses to requests.
#[derive(Debug)]
pub struct ResponsePipeline<R> {
    /// Queued responses.
    responses: VecDeque<R>,

    /// Maximum pending responses.
    max_pending: usize,
}

impl<R> ResponsePipeline<R> {
    /// Create a new response pipeline.
    pub fn new(max_pending: usize) -> Self {
        Self {
            responses: VecDeque::with_capacity(max_pending),
            max_pending,
        }
    }

    /// Get number of pending responses.
    pub fn pending(&self) -> usize {
        self.responses.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.responses.is_empty()
    }

    /// Enqueue a response.
    pub fn enqueue(&mut self, response: R) -> Result<(), R> {
        if self.responses.len() >= self.max_pending {
            return Err(response);
        }
        self.responses.push_back(response);
        Ok(())
    }

    /// Dequeue the next response.
    pub fn dequeue(&mut self) -> Option<R> {
        self.responses.pop_front()
    }

    /// Drain all responses.
    pub fn drain(&mut self) -> impl Iterator<Item = R> + '_ {
        self.responses.drain(..)
    }

    /// Clear all pending responses.
    pub fn clear(&mut self) {
        self.responses.clear();
    }
}

impl<R> Default for ResponsePipeline<R> {
    fn default() -> Self {
        Self::new(64)
    }
}

/// Pipeline metrics.
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    /// Total pipelines processed.
    pub pipelines_total: AtomicUsize,

    /// Total commands in pipelines.
    pub commands_total: AtomicUsize,

    /// Maximum observed pipeline depth.
    pub max_depth_observed: AtomicUsize,

    /// Pipeline depth histogram (1, 2-5, 6-10, 11-20, 21-50, 51+).
    pub depth_histogram: [AtomicUsize; 6],
}

impl PipelineMetrics {
    /// Record a pipeline flush.
    pub fn record_pipeline(&self, depth: usize) {
        self.pipelines_total.fetch_add(1, Ordering::Relaxed);
        self.commands_total.fetch_add(depth, Ordering::Relaxed);

        // Update max depth
        let mut current_max = self.max_depth_observed.load(Ordering::Relaxed);
        while depth > current_max {
            match self.max_depth_observed.compare_exchange_weak(
                current_max,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }

        // Update histogram
        let bucket = match depth {
            1 => 0,
            2..=5 => 1,
            6..=10 => 2,
            11..=20 => 3,
            21..=50 => 4,
            _ => 5,
        };
        self.depth_histogram[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Get average pipeline depth.
    pub fn average_depth(&self) -> f64 {
        let total = self.pipelines_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.commands_total.load(Ordering::Relaxed) as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_basic() {
        let mut pipeline: Pipeline<String> = Pipeline::default();

        assert!(pipeline.is_empty());
        assert_eq!(pipeline.depth(), 0);
        assert_eq!(pipeline.state(), PipelineState::Ready);

        pipeline.enqueue("cmd1".to_string()).unwrap();
        pipeline.enqueue("cmd2".to_string()).unwrap();

        assert_eq!(pipeline.depth(), 2);
        assert!(!pipeline.is_empty());

        assert_eq!(pipeline.dequeue(), Some("cmd1".to_string()));
        assert_eq!(pipeline.dequeue(), Some("cmd2".to_string()));
        assert_eq!(pipeline.dequeue(), None);
    }

    #[test]
    fn test_pipeline_full() {
        let config = PipelineConfig {
            max_depth: 3,
            ..Default::default()
        };
        let mut pipeline: Pipeline<i32> = Pipeline::new(config);

        pipeline.enqueue(1).unwrap();
        pipeline.enqueue(2).unwrap();
        pipeline.enqueue(3).unwrap();

        assert!(pipeline.is_full());
        assert_eq!(pipeline.state(), PipelineState::Full);

        // Can't enqueue when full
        assert!(pipeline.enqueue(4).is_err());
    }

    #[test]
    fn test_pipeline_drain() {
        let mut pipeline: Pipeline<i32> = Pipeline::default();

        for i in 0..5 {
            pipeline.enqueue(i).unwrap();
        }

        let drained: Vec<_> = pipeline.drain().collect();
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);
        assert!(pipeline.is_empty());
    }

    #[test]
    fn test_response_pipeline() {
        let mut pipeline: ResponsePipeline<String> = ResponsePipeline::new(3);

        pipeline.enqueue("r1".to_string()).unwrap();
        pipeline.enqueue("r2".to_string()).unwrap();
        pipeline.enqueue("r3".to_string()).unwrap();

        assert!(pipeline.enqueue("r4".to_string()).is_err());

        assert_eq!(pipeline.dequeue(), Some("r1".to_string()));
        assert_eq!(pipeline.pending(), 2);
    }

    #[test]
    fn test_pipeline_metrics() {
        let metrics = PipelineMetrics::default();

        metrics.record_pipeline(5);
        metrics.record_pipeline(15);
        metrics.record_pipeline(1);

        assert_eq!(metrics.pipelines_total.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.commands_total.load(Ordering::Relaxed), 21);
        assert_eq!(metrics.max_depth_observed.load(Ordering::Relaxed), 15);

        let avg = metrics.average_depth();
        assert!((avg - 7.0).abs() < 0.001);
    }
}
