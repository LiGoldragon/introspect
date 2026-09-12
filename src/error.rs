use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected argument: {got}")]
    UnexpectedArgument { got: String },
    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },
    #[error("unexpected router observation reply: {got}")]
    UnexpectedRouterObservationReply { got: String },
    #[error("actor operation failed: {operation}: {detail}")]
    Actor {
        operation: &'static str,
        detail: String,
    },
    #[error("component trace ingestion failed: {detail}")]
    TraceIngestion { detail: String },
    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),
    #[error("triad runtime frame: {0}")]
    TriadRuntimeFrame(#[from] triad_runtime::FrameError),
    #[error("sema-engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),
    /// A datom read or write fault, carried as the datom text of the codec's own
    /// typed `Error` — layer, path and extent included.
    #[error("datom text: {detail}")]
    DatomText { detail: String },
    /// An rkyv Signal frame failed to form or to restore.
    #[error("signal archive: {detail}")]
    SignalArchive { detail: String },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("argument: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),
    #[error("configuration read failed at {path}: {source}")]
    ConfigurationRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("configuration write failed at {path}: {source}")]
    ConfigurationWrite {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("datom file read failed at {path}: {source}")]
    DatomFileRead {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl From<rkyv::rancor::Error> for Error {
    fn from(error: rkyv::rancor::Error) -> Self {
        Self::SignalArchive {
            detail: error.to_string(),
        }
    }
}
