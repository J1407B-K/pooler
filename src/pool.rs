use crate::error::PoolError;
use crate::worker::{Job, Worker};
use crossbeam_deque::Injector;
use event_listener::Event;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub struct WorkerPool {
    injector: Arc<Injector<Job>>,
    workers: Vec<Worker>,
    work_available: Arc<Event>,
    shutdown: Arc<AtomicBool>,
    pending: Arc<AtomicUsize>,
}

impl WorkerPool {
    pub fn new(size: usize) -> Result<Self, PoolError> {
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
        let pending = Arc::new(AtomicUsize::new(0));
        let workers = locals
            .into_iter()
            .map(|local| {
                Worker::spawn(
                    local,
                    Arc::clone(&injector),
                    Arc::clone(&stealers),
                    Arc::clone(&work_available),
                    Arc::clone(&shutdown),
                    Arc::clone(&pending),
                )
            })
            .collect();
        Ok(Self {
            injector,
            workers,
            work_available,
            shutdown,
            pending,
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
        self.work_available.notify(1);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
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

#[cfg(test)]
mod tests {
    use super::WorkerPool;
    use crate::PoolError;
    use std::sync::{Arc, Mutex, atomic::Ordering, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

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

    #[test]
    fn executes_new_work_while_another_worker_is_busy() {
        let pool = WorkerPool::new(2).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        pool.execute(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        pool.execute(move || done_tx.send(()).unwrap()).unwrap();
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)), Ok(()));

        release_tx.send(()).unwrap();
    }

    #[test]
    fn drop_completes_after_the_last_running_job() {
        let pool = WorkerPool::new(2).unwrap();
        let shutdown = Arc::clone(&pool.shutdown);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();

        pool.execute(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let drop_thread = thread::spawn(move || {
            drop(pool);
            dropped_tx.send(()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while !shutdown.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "Drop did not begin in time");
            thread::yield_now();
        }

        release_tx.send(()).unwrap();
        dropped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop_thread.join().unwrap();
    }
}
