use crate::error::TaskError;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
    mpsc,
};

pub(crate) const QUEUED: u8 = 0;
pub(crate) const RUNNING: u8 = 1;
pub(crate) const FINISHED: u8 = 2;
pub(crate) const CANCELLED: u8 = 3;

pub struct TaskHandle<T> {
    receiver: mpsc::Receiver<Result<T, TaskError>>,
    state: Arc<AtomicU8>,
}

impl<T> TaskHandle<T> {
    pub(crate) fn new(
        receiver: mpsc::Receiver<Result<T, TaskError>>,
        state: Arc<AtomicU8>,
    ) -> Self {
        Self { receiver, state }
    }

    /// Cancels a task only if no worker has started it yet.
    pub fn cancel(&self) -> bool {
        self.state
            .compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn is_finished(&self) -> bool {
        matches!(self.state.load(Ordering::Acquire), FINISHED | CANCELLED)
    }

    pub fn join(self) -> Result<T, TaskError> {
        self.receiver.recv().unwrap_or(Err(TaskError::Disconnected))
    }
}
