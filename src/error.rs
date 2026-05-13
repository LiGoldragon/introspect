use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected argument: {got}")]
    UnexpectedArgument { got: String },
    #[error("missing introspection socket path")]
    IntrospectionSocketMissing,
    #[error("actor operation failed: {operation}: {detail}")]
    Actor {
        operation: &'static str,
        detail: String,
    },
    #[error("nota codec: {0}")]
    Nota(#[from] nota_codec::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
