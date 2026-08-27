use pooler::{PoolError, PoolMetrics, TaskError, TryExecuteError, WorkerPool};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
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
fn builder_rejects_a_zero_queue_capacity() {
    let result = WorkerPool::builder()
        .worker_count(1)
        .queue_capacity(0)
        .build();
    assert!(matches!(result, Err(PoolError::InvalidQueueCapacity)));
}

#[test]
fn try_execute_applies_backpressure() {
    let pool = WorkerPool::builder()
        .worker_count(1)
        .queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    pool.try_execute(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })
    .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let (retry_tx, retry_rx) = mpsc::channel();
    let job = match pool.try_execute(move || retry_tx.send(()).unwrap()) {
        Err(TryExecuteError::Full(job)) => job,
        _ => panic!("expected the second job to be rejected as full"),
    };

    release_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while pool.metrics().pending != 0 {
        assert!(
            Instant::now() < deadline,
            "first job did not finish in time"
        );
        thread::yield_now();
    }
    pool.try_execute(job).unwrap();
    retry_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn a_panicking_job_releases_its_queue_slot() {
    let pool = WorkerPool::builder()
        .worker_count(1)
        .queue_capacity(1)
        .build()
        .unwrap();
    pool.try_execute(|| panic!("expected test panic")).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while pool.metrics().pending != 0 {
        assert!(
            Instant::now() < deadline,
            "panicking job did not finish in time"
        );
        thread::yield_now();
    }

    let (tx, rx) = mpsc::channel();
    pool.try_execute(move || tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
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

#[test]
fn a_full_batch_is_returned_without_partial_submission() {
    let pool = WorkerPool::builder()
        .worker_count(1)
        .queue_capacity(2)
        .build()
        .unwrap();
    let jobs = (0..3)
        .map(|_| Box::new(|| {}) as Box<dyn FnOnce() + Send>)
        .collect::<Vec<_>>();

    let jobs = match pool.try_execute_batch(jobs) {
        Err(TryExecuteError::Full(jobs)) => jobs,
        _ => panic!("expected the batch to be rejected as a whole"),
    };

    assert_eq!(jobs.len(), 3);
    assert_eq!(pool.metrics().pending, 0);
}

#[test]
fn batch_and_affine_jobs_execute() {
    let pool = WorkerPool::new(2).unwrap();
    let (tx, rx) = mpsc::channel();
    let batch = (0..8)
        .map(|value| {
            let tx = tx.clone();
            Box::new(move || tx.send(value).unwrap()) as Box<dyn FnOnce() + Send>
        })
        .collect::<Vec<_>>();
    pool.execute_batch(batch).unwrap();

    for value in 8..16 {
        let tx = tx.clone();
        pool.execute_affine("account-42", move || tx.send(value).unwrap())
            .unwrap();
    }
    drop(tx);

    let mut values = rx.iter().collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, (0..16).collect::<Vec<_>>());
}

#[test]
fn task_handle_returns_a_value_and_reports_panics() {
    let pool = WorkerPool::new(1).unwrap();
    assert_eq!(pool.execute_with_handle(|| 6 * 7).unwrap().join(), Ok(42));

    let error = pool
        .execute_with_handle::<_, ()>(|| panic!("expected task failure"))
        .unwrap()
        .join()
        .unwrap_err();
    assert!(matches!(error, TaskError::Panicked(_)));

    let deadline = Instant::now() + Duration::from_secs(1);
    while pool.metrics().pending != 0 {
        assert!(Instant::now() < deadline, "panicking handle did not finish");
        thread::yield_now();
    }
    assert_eq!(pool.metrics().panicked, 1);
}

#[test]
fn task_handle_cancels_only_before_the_job_starts() {
    let pool = WorkerPool::new(1).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    pool.execute(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })
    .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let ran = Arc::new(AtomicBool::new(false));
    let ran_by_job = Arc::clone(&ran);
    let handle = pool
        .execute_with_handle(move || ran_by_job.store(true, Ordering::Release))
        .unwrap();
    assert!(handle.cancel());
    assert!(!handle.cancel());

    release_tx.send(()).unwrap();
    assert_eq!(handle.join(), Err(TaskError::Cancelled));
    assert!(!ran.load(Ordering::Acquire));
}
