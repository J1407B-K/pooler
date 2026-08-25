use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    InvalidSize,
    Shutdown,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize => f.write_str("worker pool size must be greater than zero"),
            Self::Shutdown => f.write_str("worker pool has been shut down"),
        }
    }
}

impl std::error::Error for PoolError {}
