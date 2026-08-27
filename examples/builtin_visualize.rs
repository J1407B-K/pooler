use pooler::WorkerPool;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = WorkerPool::builder()
        .worker_count(4)
        .queue_capacity(16)
        .terminal_visualizer()
        .build()?;

    for task in 1..=16 {
        let duration = Duration::from_millis(250 + (task % 4) as u64 * 150);
        pool.execute(move || thread::sleep(duration))?;
    }

    drop(pool);
    Ok(())
}
