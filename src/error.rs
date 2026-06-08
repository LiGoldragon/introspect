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
    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),
    #[error("sema-engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),
    #[error("nota decode: {0}")]
    Nota(#[from] nota_next::NotaDecodeError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("argument: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),
    #[error("configuration archive decode failed")]
    ConfigurationArchiveDecode,
    #[error("configuration archive encode failed")]
    ConfigurationArchiveEncode,
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
}
