use crate::error::PoolError;
use crate::worker::{Job, Worker};
use crossbeam_deque::Injector;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub struct WorkerPool {
    injector: Arc<Injector<Job>>,
    workers: Vec<Worker>,
    shutdown: Arc<AtomicBool>,
    pending: Arc<AtomicUsize>,
    next_worker: AtomicUsize,
}

impl WorkerPool {
    pub fn new(size: usize) -> Result<Self, PoolError> {
        if size == 0 {
            return Err(PoolError::InvalidSize);
        }
        let injector = Arc::new(Injector::new());
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
        let pending = Arc::new(AtomicUsize::new(0));
        let workers = locals
            .into_iter()
            .map(|local| {
                Worker::spawn(
                    local,
                    Arc::clone(&injector),
                    Arc::clone(&stealers),
                    Arc::clone(&shutdown),
                    Arc::clone(&pending),
                )
            })
            .collect();
        Ok(Self {
            injector,
            workers,
            shutdown,
            pending,
            next_worker: AtomicUsize::new(0),
        })
    }

    pub fn execute<F>(&self, job: F) -> Result<(), PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(PoolError::Shutdown);
        }
        self.pending.fetch_add(1, Ordering::Release);
        self.injector.push(Box::new(job));
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[index].handle.unpark();
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for worker in &self.workers {
            worker.handle.unpark();
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerPool;
    use crate::PoolError;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    #[test]
    fn rejects_zero_workers() {
        assert!(matches!(WorkerPool::new(0), Err(PoolError::InvalidSize)));
    }

    #[test]
    fn executes_all_jobs_before_drop() {
        let pool = WorkerPool::new(4).unwrap();
        let (tx, rx) = mpsc::channel();
        let total = Arc::new(Mutex::new(0));
        for _ in 0..100 {
            let tx = tx.clone();
            let total = Arc::clone(&total);
            pool.execute(move || {
                *total.lock().unwrap() += 1;
                tx.send(()).unwrap();
            })
            .unwrap();
        }
        drop(tx);
        assert_eq!(rx.iter().count(), 100);
        assert_eq!(*total.lock().unwrap(), 100);
    }

    #[test]
    fn a_panicking_job_does_not_stop_the_pool() {
        let pool = WorkerPool::new(1).unwrap();
        let (tx, rx) = mpsc::channel();
        pool.execute(|| panic!("expected test panic")).unwrap();
        pool.execute(move || tx.send(42).unwrap()).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)), Ok(42));
    }

    #[test]
    fn drop_waits_for_submitted_jobs() {
        let (tx, rx) = mpsc::channel();
        {
            let pool = WorkerPool::new(2).unwrap();
            for value in 0..20 {
                let tx = tx.clone();
                pool.execute(move || tx.send(value).unwrap()).unwrap();
            }
        }
        drop(tx);
        assert_eq!(rx.iter().count(), 20);
    }
}
