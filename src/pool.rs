use crate::error::{PoolError, TryExecuteError};
use crate::error::{TaskError, TaskPanic};
use crate::task::{FINISHED, QUEUED, RUNNING, TaskHandle};
use crate::terminal::{PoolEvent, TerminalVisualizer};
use crate::worker::{Job, Worker, WorkerContext};
use crossbeam_deque::Injector;
use crossbeam_queue::SegQueue;
use event_listener::{Event, IntoNotification};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc,
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
    queue_capacity: Option<usize>,
    terminal_visualizer: bool,
}

impl PoolBuilder {
    pub const fn new() -> Self {
        Self {
            worker_count: 0,
            queue_capacity: None,
            terminal_visualizer: false,
        }
    }

    pub const fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub const fn queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = Some(queue_capacity);
        self
    }

    pub const fn terminal_visualizer(mut self) -> Self {
        self.terminal_visualizer = true;
        self
    }

    pub fn build(self) -> Result<WorkerPool, PoolError> {
        WorkerPool::from_config(
            self.worker_count,
            self.queue_capacity,
            self.terminal_visualizer,
        )
    }
}

pub struct WorkerPool {
    injector: Arc<Injector<Job>>,
    affine_queues: Arc<Vec<Arc<SegQueue<Job>>>>,
    workers: Vec<Worker>,
    work_available: Arc<Event>,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    queue_capacity: Option<usize>,
    terminal_visualizer: Option<Arc<TerminalVisualizer>>,
    next_task_id: AtomicU64,
}

impl WorkerPool {
    pub fn new(size: usize) -> Result<Self, PoolError> {
        Self::builder().worker_count(size).build()
    }

    pub const fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    fn from_config(
        size: usize,
        queue_capacity: Option<usize>,
        terminal_visualizer: bool,
    ) -> Result<Self, PoolError> {
        if size == 0 {
            return Err(PoolError::InvalidSize);
        }
        if queue_capacity == Some(0) {
            return Err(PoolError::InvalidQueueCapacity);
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
        let affine_queues = Arc::new(
            (0..size)
                .map(|_| Arc::new(SegQueue::new()))
                .collect::<Vec<_>>(),
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(Metrics::default());
        let terminal_visualizer = terminal_visualizer.then(|| TerminalVisualizer::new(size));
        let workers = locals
            .into_iter()
            .enumerate()
            .map(|(worker_id, local)| {
                Worker::spawn(
                    worker_id,
                    local,
                    WorkerContext {
                        injector: Arc::clone(&injector),
                        stealers: Arc::clone(&stealers),
                        affine_queues: Arc::clone(&affine_queues),
                        work_available: Arc::clone(&work_available),
                        shutdown: Arc::clone(&shutdown),
                        metrics: Arc::clone(&metrics),
                        terminal_visualizer: terminal_visualizer.clone(),
                    },
                )
            })
            .collect();
        Ok(Self {
            injector,
            affine_queues,
            workers,
            work_available,
            shutdown,
            metrics,
            queue_capacity,
            terminal_visualizer,
            next_task_id: AtomicU64::new(1),
        })
    }

    pub fn execute<F>(&self, job: F) -> Result<(), PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.try_execute(job).map_err(|error| error.pool_error())
    }

    /// Attempts to submit a job without waiting for capacity.
    ///
    /// When submission fails, the error owns the original job. Call
    /// [`TryExecuteError::into_job`] to keep it for a later retry.
    pub fn try_execute<F>(&self, job: F) -> Result<(), TryExecuteError<F>>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = self.reserve(job)?;
        self.injector.push(self.make_job(job));
        self.work_available.notify(1.additional());
        Ok(())
    }

    /// Reserves capacity for every job before submitting any of them.
    ///
    /// A full queue returns the complete batch unchanged, so this operation
    /// never partially submits a batch.
    pub fn try_execute_batch<I, F>(&self, jobs: I) -> Result<(), TryExecuteError<Vec<F>>>
    where
        I: IntoIterator<Item = F>,
        F: FnOnce() + Send + 'static,
    {
        let jobs = jobs.into_iter().collect::<Vec<_>>();
        if jobs.is_empty() {
            return Ok(());
        }
        if self.shutdown.load(Ordering::Acquire) {
            return Err(TryExecuteError::Shutdown(jobs));
        }
        if !self.acquire_task_slots(jobs.len()) {
            return Err(TryExecuteError::Full(jobs));
        }

        let count = jobs.len();
        for job in jobs {
            self.injector.push(self.make_job(job));
        }
        self.work_available.notify(count.additional());
        Ok(())
    }

    pub fn execute_batch<I, F>(&self, jobs: I) -> Result<(), PoolError>
    where
        I: IntoIterator<Item = F>,
        F: FnOnce() + Send + 'static,
    {
        self.try_execute_batch(jobs)
            .map_err(|error| error.pool_error())
    }

    /// Submits a job to the mailbox selected by `key`.
    ///
    /// The selected worker checks its own mailbox before global work. Other
    /// workers may still steal from it when that worker is busy.
    pub fn try_execute_affine<K, F>(&self, key: K, job: F) -> Result<(), TryExecuteError<F>>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        let job = self.reserve(job)?;
        let worker_id = self.affinity_worker(&key);
        self.affine_queues[worker_id].push(self.make_job(job));
        self.work_available.notify(1.additional());
        Ok(())
    }

    pub fn execute_affine<K, F>(&self, key: K, job: F) -> Result<(), PoolError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        self.try_execute_affine(key, job)
            .map_err(|error| error.pool_error())
    }

    pub fn try_execute_with_handle<F, T>(
        &self,
        task: F,
    ) -> Result<TaskHandle<T>, TryExecuteError<F>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let task = self.reserve(task)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let state = Arc::new(std::sync::atomic::AtomicU8::new(QUEUED));
        let task_state = Arc::clone(&state);
        let job = move || {
            if task_state
                .compare_exchange(QUEUED, RUNNING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                let _ = sender.send(Err(TaskError::Cancelled));
                return;
            }

            match panic::catch_unwind(AssertUnwindSafe(task)) {
                Ok(value) => {
                    task_state.store(FINISHED, Ordering::Release);
                    let _ = sender.send(Ok(value));
                }
                Err(payload) => {
                    task_state.store(FINISHED, Ordering::Release);
                    let _ = sender.send(Err(TaskError::Panicked(TaskPanic::from_payload(
                        payload.as_ref(),
                    ))));
                    panic::resume_unwind(payload);
                }
            }
        };
        self.injector.push(self.make_job(job));
        self.work_available.notify(1.additional());
        Ok(TaskHandle::new(receiver, state))
    }

    pub fn execute_with_handle<F, T>(&self, task: F) -> Result<TaskHandle<T>, PoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_execute_with_handle(task)
            .map_err(|error| error.pool_error())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }

    pub fn metrics(&self) -> PoolMetrics {
        self.metrics.snapshot(self.size())
    }

    fn reserve<F>(&self, job: F) -> Result<F, TryExecuteError<F>> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(TryExecuteError::Shutdown(job));
        }
        if !self.acquire_task_slots(1) {
            return Err(TryExecuteError::Full(job));
        }
        Ok(job)
    }

    fn make_job<F>(&self, task: F) -> Job
    where
        F: FnOnce() + Send + 'static,
    {
        let task_id = if self.terminal_visualizer.is_some() {
            self.next_task_id.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        if let Some(terminal_visualizer) = &self.terminal_visualizer {
            terminal_visualizer.record(PoolEvent::Queued { task_id });
        }
        Job {
            task_id,
            task: Box::new(task),
        }
    }

    fn affinity_worker<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.workers.len()
    }

    fn acquire_task_slots(&self, slots: usize) -> bool {
        let mut pending = self.metrics.pending.load(Ordering::Acquire);

        loop {
            let Some(next_pending) = pending.checked_add(slots) else {
                return false;
            };
            if self
                .queue_capacity
                .is_some_and(|capacity| next_pending > capacity)
            {
                return false;
            }

            match self.metrics.pending.compare_exchange_weak(
                pending,
                next_pending,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => pending = current,
            }
        }
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
        if let Some(terminal_visualizer) = &self.terminal_visualizer {
            terminal_visualizer.finish();
        }
    }
}
