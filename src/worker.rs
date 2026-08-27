use crossbeam_deque::{Injector, Steal, Stealer, Worker as LocalWorker};
use event_listener::{Event, Listener, listener};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;

pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct Worker {
    pub(crate) join: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        local: LocalWorker<Job>,
        injector: Arc<Injector<Job>>,
        stealers: Arc<Vec<Stealer<Job>>>,
        work_available: Arc<Event>,
        shutdown: Arc<AtomicBool>,
        pending: Arc<AtomicUsize>,
    ) -> Self {
        let join = thread::spawn(move || {
            worker_loop(local, injector, stealers, work_available, shutdown, pending)
        });
        Self { join: Some(join) }
    }
}

fn worker_loop(
    local: LocalWorker<Job>,
    injector: Arc<Injector<Job>>,
    stealers: Arc<Vec<Stealer<Job>>>,
    work_available: Arc<Event>,
    shutdown: Arc<AtomicBool>,
    pending: Arc<AtomicUsize>,
) {
    loop {
        if let Some(job) = next_job(&local, &injector, &stealers) {
            run_job(job, &pending, &shutdown, &work_available);
            continue;
        }

        let job = {
            listener!(work_available => listener);

            if let Some(job) = next_job(&local, &injector, &stealers) {
                Some(job)
            } else if should_stop(&shutdown, &pending) {
                return;
            } else {
                listener.wait();
                None
            }
        };

        if let Some(job) = job {
            run_job(job, &pending, &shutdown, &work_available);
        }
    }
}

fn next_job(
    local: &LocalWorker<Job>,
    injector: &Injector<Job>,
    stealers: &[Stealer<Job>],
) -> Option<Job> {
    local.pop().or_else(|| steal_job(injector, local, stealers))
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

fn run_job(job: Job, pending: &AtomicUsize, shutdown: &AtomicBool, work_available: &Event) {
    let _ = panic::catch_unwind(AssertUnwindSafe(job));
    if pending.fetch_sub(1, Ordering::AcqRel) == 1 && shutdown.load(Ordering::Acquire) {
        work_available.notify(usize::MAX);
    }
}

fn should_stop(shutdown: &AtomicBool, pending: &AtomicUsize) -> bool {
    shutdown.load(Ordering::Acquire) && pending.load(Ordering::Acquire) == 0
}
