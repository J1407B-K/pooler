use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    InvalidSize,
    InvalidQueueCapacity,
    QueueFull,
    Shutdown,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize => f.write_str("worker pool size must be greater than zero"),
            Self::InvalidQueueCapacity => {
                f.write_str("worker pool queue capacity must be greater than zero")
            }
            Self::QueueFull => f.write_str("worker pool queue is full"),
            Self::Shutdown => f.write_str("worker pool has been shut down"),
        }
    }
}

impl std::error::Error for PoolError {}

/// A job that could not be submitted is returned to its caller.
///
/// This makes backpressure recoverable: keep the returned job and submit it
/// again after capacity becomes available.
pub enum TryExecuteError<F> {
    Full(F),
    Shutdown(F),
}

impl<F> fmt::Debug for TryExecuteError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(match self {
            Self::Full(_) => "Full",
            Self::Shutdown(_) => "Shutdown",
        })
        .finish()
    }
}

impl<F> TryExecuteError<F> {
    pub fn into_job(self) -> F {
        match self {
            Self::Full(job) | Self::Shutdown(job) => job,
        }
    }

    pub const fn pool_error(&self) -> PoolError {
        match self {
            Self::Full(_) => PoolError::QueueFull,
            Self::Shutdown(_) => PoolError::Shutdown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPanic {
    message: String,
}

impl TaskPanic {
    pub(crate) fn from_payload(payload: &(dyn std::any::Any + Send)) -> Self {
        let message = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_owned()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "task panicked with a non-string payload".to_owned()
        };
        Self { message }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TaskPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task panicked: {}", self.message)
    }
}

impl std::error::Error for TaskPanic {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    Cancelled,
    Panicked(TaskPanic),
    Disconnected,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("task was cancelled before it started"),
            Self::Panicked(error) => error.fmt(f),
            Self::Disconnected => f.write_str("worker pool disconnected before task completion"),
        }
    }
}

impl std::error::Error for TaskError {}
