use pooler::WorkerPool;

fn main() {
    let pool = WorkerPool::new(4).expect("pool size must be positive");
    for i in 1..=8 {
        pool.execute(move || println!("任务 {i} 在线程池中执行"))
            .unwrap();
    }
}
