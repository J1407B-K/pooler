use crate::error::PoolError;
use crate::worker::{Job, Worker};
use crossbeam_deque::Injector;
use event_listener::Event;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMetrics {
    pub workers: usize,
    pub submitted: usize,
    pub completed: usize,
    pub panicked: usize,
    pub pending: usize,
}

#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) submitted: AtomicUsize,
    pub(crate) completed: AtomicUsize,
    pub(crate) panicked: AtomicUsize,
    pub(crate) pending: AtomicUsize,
}

impl Metrics {
    fn snapshot(&self, workers: usize) -> PoolMetrics {
        PoolMetrics {
            workers,
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            panicked: self.panicked.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Default)]
pub struct PoolBuilder {
    worker_count: usize,
}

impl PoolBuilder {
    pub const fn new() -> Self {
        Self { worker_count: 0 }
    }

    pub const fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn build(self) -> Result<WorkerPool, PoolError> {
        WorkerPool::from_worker_count(self.worker_count)
    }
}

pub struct WorkerPool {
    injector: Arc<Injector<Job>>,
    workers: Vec<Worker>,
    work_available: Arc<Event>,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
}

impl WorkerPool {
    pub fn new(size: usize) -> Result<Self, PoolError> {
        Self::builder().worker_count(size).build()
    }

    pub const fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    fn from_worker_count(size: usize) -> Result<Self, PoolError> {
        if size == 0 {
            return Err(PoolError::InvalidSize);
        }

        let injector = Arc::new(Injector::new());
        let work_available = Arc::new(Event::new());
        let mut stealers = Vec::with_capacity(size);
        let locals = (0..size)
            .map(|_| {
                let local = crossbeam_deque::Worker::new_fifo();
                stealers.push(local.stealer());
                local
            })
            .collect::<Vec<_>>();
        let stealers = Arc::new(stealers);
        let shutdown = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(Metrics::default());
        let workers = locals
            .into_iter()
            .map(|local| {
                Worker::spawn(
                    local,
                    Arc::clone(&injector),
                    Arc::clone(&stealers),
                    Arc::clone(&work_available),
                    Arc::clone(&shutdown),
                    Arc::clone(&metrics),
                )
            })
            .collect();
        Ok(Self {
            injector,
            workers,
            work_available,
            shutdown,
            metrics,
        })
    }

    pub fn execute<F>(&self, job: F) -> Result<(), PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(PoolError::Shutdown);
        }
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        self.metrics.pending.fetch_add(1, Ordering::Release);
        self.injector.push(Box::new(job));
        self.work_available.notify(1);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }

    pub fn metrics(&self) -> PoolMetrics {
        self.metrics.snapshot(self.size())
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.work_available.notify(usize::MAX);
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}
