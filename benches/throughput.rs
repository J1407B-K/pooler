use pooler::WorkerPool;
use std::hint::black_box;
use std::time::Instant;

const WORKERS: usize = 4;
const TASKS: usize = 100_000;

fn main() {
    let pool = WorkerPool::new(WORKERS).expect("worker count must be positive");
    let started = Instant::now();

    for _ in 0..TASKS {
        pool.execute(|| black_box(())).unwrap();
    }

    while pool.metrics().pending != 0 {
        std::hint::spin_loop();
    }

    let elapsed = started.elapsed();
    let throughput = TASKS as f64 / elapsed.as_secs_f64();
    println!(
        "workers={WORKERS} tasks={TASKS} elapsed={elapsed:?} throughput={throughput:.0} tasks/s"
    );
}
