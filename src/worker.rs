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
        let join = thread::spawn(move || {
            loop {
                let job = local
                    .pop()
                    .or_else(|| match injector.steal_batch_and_pop(&local) {
                        Steal::Success(job) => Some(job),
                        Steal::Empty | Steal::Retry => None,
                    })
                    .or_else(|| {
                        stealers.iter().find_map(|s| match s.steal() {
                            Steal::Success(job) => Some(job),
                            Steal::Empty | Steal::Retry => None,
                        })
                    });
                if let Some(job) = job {
                    let _ = panic::catch_unwind(AssertUnwindSafe(job));
                    pending.fetch_sub(1, Ordering::AcqRel);
                } else if shutdown.load(Ordering::Acquire) && pending.load(Ordering::Acquire) == 0 {
                    break;
                } else {
                    thread::park_timeout(Duration::from_millis(1));
                }
            }
        });
        let handle = join.thread().clone();
        Self {
            handle,
            join: Some(join),
        }
    }
}
