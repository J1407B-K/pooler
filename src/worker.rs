use crate::pool::Metrics;
use crate::terminal::{PoolEvent, TerminalVisualizer};
use crossbeam_deque::{Injector, Steal, Stealer, Worker as LocalWorker};
use crossbeam_queue::SegQueue;
use event_listener::{Event, Listener, listener};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;

pub(crate) struct Job {
    pub(crate) task_id: u64,
    pub(crate) task: Box<dyn FnOnce() + Send + 'static>,
}

pub(crate) struct Worker {
    pub(crate) join: Option<thread::JoinHandle<()>>,
}

pub(crate) struct WorkerContext {
    pub(crate) injector: Arc<Injector<Job>>,
    pub(crate) stealers: Arc<Vec<Stealer<Job>>>,
    pub(crate) affine_queues: Arc<Vec<Arc<SegQueue<Job>>>>,
    pub(crate) work_available: Arc<Event>,
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) terminal_visualizer: Option<Arc<TerminalVisualizer>>,
}

impl Worker {
    pub(crate) fn spawn(worker_id: usize, local: LocalWorker<Job>, context: WorkerContext) -> Self {
        let join = thread::spawn(move || worker_loop(worker_id, local, context));
        Self { join: Some(join) }
    }
}

fn worker_loop(worker_id: usize, local: LocalWorker<Job>, context: WorkerContext) {
    loop {
        if let Some(job) = next_job(worker_id, &local, &context) {
            run_job(
                job,
                worker_id,
                &context.metrics,
                &context.shutdown,
                &context.work_available,
                context.terminal_visualizer.as_deref(),
            );
            continue;
        }

        let job = {
            listener!(context.work_available => listener);

            if let Some(job) = next_job(worker_id, &local, &context) {
                Some(job)
            } else if should_stop(&context.shutdown, &context.metrics) {
                return;
            } else {
                listener.wait();
                None
            }
        };

        if let Some(job) = job {
            run_job(
                job,
                worker_id,
                &context.metrics,
                &context.shutdown,
                &context.work_available,
                context.terminal_visualizer.as_deref(),
            );
        }
    }
}

fn next_job(worker_id: usize, local: &LocalWorker<Job>, context: &WorkerContext) -> Option<Job> {
    local
        .pop()
        .or_else(|| context.affine_queues[worker_id].pop())
        .or_else(|| steal_job(&context.injector, local, &context.stealers))
        .or_else(|| steal_from_affine_queues(worker_id, &context.affine_queues))
}

fn steal_job(
    injector: &Injector<Job>,
    local: &LocalWorker<Job>,
    stealers: &[Stealer<Job>],
) -> Option<Job> {
    loop {
        let result = steal_from_injector(injector, local).or_else(|| steal_from_others(stealers));

        match result {
            Steal::Success(job) => return Some(job),
            Steal::Empty => return None,
            Steal::Retry => continue,
        }
    }
}

fn steal_from_injector(injector: &Injector<Job>, local: &LocalWorker<Job>) -> Steal<Job> {
    injector.steal_batch_and_pop(local)
}

fn steal_from_others(stealers: &[Stealer<Job>]) -> Steal<Job> {
    stealers.iter().map(Stealer::steal).collect()
}

fn steal_from_affine_queues(worker_id: usize, affine_queues: &[Arc<SegQueue<Job>>]) -> Option<Job> {
    affine_queues
        .iter()
        .enumerate()
        .filter(|(owner, _)| *owner != worker_id)
        .find_map(|(_, queue)| queue.pop())
}

fn run_job(
    job: Job,
    worker_id: usize,
    metrics: &Metrics,
    shutdown: &AtomicBool,
    work_available: &Event,
    terminal_visualizer: Option<&TerminalVisualizer>,
) {
    if let Some(terminal_visualizer) = terminal_visualizer {
        terminal_visualizer.record(PoolEvent::Started {
            task_id: job.task_id,
            worker_id,
        });
    }

    let panicked = panic::catch_unwind(AssertUnwindSafe(job.task)).is_err();
    if panicked {
        metrics.panicked.fetch_add(1, Ordering::Relaxed);
    }
    metrics.completed.fetch_add(1, Ordering::Relaxed);
    if let Some(terminal_visualizer) = terminal_visualizer {
        terminal_visualizer.record(if panicked {
            PoolEvent::Panicked {
                task_id: job.task_id,
                worker_id,
            }
        } else {
            PoolEvent::Completed {
                task_id: job.task_id,
                worker_id,
            }
        });
    }
    if metrics.pending.fetch_sub(1, Ordering::AcqRel) == 1 && shutdown.load(Ordering::Acquire) {
        work_available.notify(usize::MAX);
    }
}

fn should_stop(shutdown: &AtomicBool, metrics: &Metrics) -> bool {
    shutdown.load(Ordering::Acquire) && metrics.pending.load(Ordering::Acquire) == 0
}
