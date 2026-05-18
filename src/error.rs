use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected argument: {got}")]
    UnexpectedArgument { got: String },
    #[error("missing introspection socket path")]
    IntrospectionSocketMissing,
    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },
    #[error("router observation request was rejected: {reason}")]
    RouterObservationRejected {
        reason: signal_core::RequestRejectionReason,
    },
    #[error("unexpected router observation reply: {got}")]
    UnexpectedRouterObservationReply { got: String },
    #[error("actor operation failed: {operation}: {detail}")]
    Actor {
        operation: &'static str,
        detail: String,
    },
    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_core::FrameError),
    #[error("sema-engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),
    #[error("sema: {0}")]
    Sema(#[from] sema::Error),
    #[error("nota codec: {0}")]
    Nota(#[from] nota_codec::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("nota-config: {0}")]
    NotaConfig(#[from] nota_config::Error),
}
