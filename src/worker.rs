use crossbeam_deque::{Injector, Steal, Stealer, Worker as LocalWorker};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::Duration;

pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct Worker {
    pub(crate) handle: thread::Thread,
    pub(crate) join: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        local: LocalWorker<Job>,
        injector: Arc<Injector<Job>>,
        stealers: Arc<Vec<Stealer<Job>>>,
        shutdown: Arc<AtomicBool>,
        pending: Arc<AtomicUsize>,
    ) -> Self {
        let join = thread::spawn(move || worker_loop(local, injector, stealers, shutdown, pending));
        let handle = join.thread().clone();
        Self {
            handle,
            join: Some(join),
        }
    }
}

fn worker_loop(
    local: LocalWorker<Job>,
    injector: Arc<Injector<Job>>,
    stealers: Arc<Vec<Stealer<Job>>>,
    shutdown: Arc<AtomicBool>,
    pending: Arc<AtomicUsize>,
) {
    loop {
        match next_job(&local, &injector, &stealers) {
            Some(job) => run_job(job, &pending),
            None if should_stop(&shutdown, &pending) => break,
            None => thread::park_timeout(Duration::from_millis(1)),
        }
    }
}

fn next_job(
    local: &LocalWorker<Job>,
    injector: &Injector<Job>,
    stealers: &[Stealer<Job>],
) -> Option<Job> {
    local
        .pop()
        .or_else(|| steal_from_injector(injector, local))
        .or_else(|| steal_from_others(stealers))
}

fn steal_from_injector(injector: &Injector<Job>, local: &LocalWorker<Job>) -> Option<Job> {
    match injector.steal_batch_and_pop(local) {
        Steal::Success(job) => Some(job),
        Steal::Empty | Steal::Retry => None,
    }
}

fn steal_from_others(stealers: &[Stealer<Job>]) -> Option<Job> {
    stealers.iter().find_map(|s| match s.steal() {
        Steal::Success(job) => Some(job),
        Steal::Empty | Steal::Retry => None,
    })
}

fn run_job(job: Job, pending: &AtomicUsize) {
    let _ = panic::catch_unwind(AssertUnwindSafe(job));
    pending.fetch_sub(1, Ordering::AcqRel);
}

fn should_stop(shutdown: &AtomicBool, pending: &AtomicUsize) -> bool {
    shutdown.load(Ordering::Acquire) && pending.load(Ordering::Acquire) == 0
}
