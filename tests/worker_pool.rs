use pooler::{PoolError, PoolMetrics, WorkerPool};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn rejects_zero_workers() {
    assert!(matches!(WorkerPool::new(0), Err(PoolError::InvalidSize)));
}

#[test]
fn builder_creates_a_pool() {
    let pool = WorkerPool::builder().worker_count(2).build().unwrap();
    assert_eq!(pool.size(), 2);
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
fn metrics_count_completed_and_panicking_jobs() {
    let pool = WorkerPool::new(1).unwrap();
    let (tx, rx) = mpsc::channel();

    pool.execute(|| panic!("expected test panic")).unwrap();
    pool.execute(move || tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while pool.metrics().pending != 0 {
        assert!(Instant::now() < deadline, "jobs did not finish in time");
        thread::yield_now();
    }

    assert_eq!(
        pool.metrics(),
        PoolMetrics {
            workers: 1,
            submitted: 2,
            completed: 2,
            panicked: 1,
            pending: 0,
        }
    );
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
fn drop_waits_for_the_last_running_job() {
    let pool = WorkerPool::new(2).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (drop_started_tx, drop_started_rx) = mpsc::channel();
    let (dropped_tx, dropped_rx) = mpsc::channel();

    pool.execute(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })
    .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let drop_thread = thread::spawn(move || {
        drop_started_tx.send(()).unwrap();
        drop(pool);
        dropped_tx.send(()).unwrap();
    });
    drop_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(dropped_rx.recv_timeout(Duration::from_millis(50)).is_err());

    release_tx.send(()).unwrap();
    dropped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    drop_thread.join().unwrap();
}
