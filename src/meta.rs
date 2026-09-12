//! The privileged meta plane client: `meta-signal-introspect` over the
//! owner-only meta socket.
//!
//! One request is one rkyv `Signal<Query>` inside a length-prefixed body; the
//! reply is one `Signal<Response>` the same way. There is no exchange envelope
//! and no sub-reply nesting — the contract's own frame is the wire.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use meta_signal_introspect::{
    ByteViewable, Query as MetaIntrospectQuery, Response as MetaIntrospectResponse, Restorable,
    Signal as MetaIntrospectSignal, Signalizable,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::cli_argument::DatomCommandText;
use crate::datom_text;
use crate::{Error, Result};

const DEFAULT_META_INTROSPECT_SOCKET: &str = "/tmp/meta-introspect.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaIntrospectEndpoint {
    socket: PathBuf,
}

impl MetaIntrospectEndpoint {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn as_path(&self) -> &Path {
        &self.socket
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaIntrospectClient {
    endpoint: MetaIntrospectEndpoint,
    codec: LengthPrefixedCodec,
}

impl MetaIntrospectClient {
    pub fn new(endpoint: MetaIntrospectEndpoint) -> Self {
        Self {
            endpoint,
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn submit(&self, query: MetaIntrospectQuery) -> Result<MetaIntrospectResponse> {
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        let signal = query.signalize().map_err(Error::from)?;
        self.codec
            .write_body(&mut stream, &FrameBody::new(signal.bytes().to_vec()))?;
        let body = self.codec.read_body(&mut stream)?;
        MetaIntrospectSignal::<MetaIntrospectResponse>::from(body.into_bytes())
            .restore()
            .map_err(Error::from)
    }
}

/// The privileged `meta-introspect` CLI: one datom meta `Query` in, one datom
/// meta `Response` out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaIntrospectCommand {
    command: triad_runtime::ComponentCommand,
    environment: MetaIntrospectCommandEnvironment,
}

impl MetaIntrospectCommand {
    pub fn from_env() -> Self {
        Self {
            command: triad_runtime::ComponentCommand::from_environment(),
            environment: MetaIntrospectCommandEnvironment::from_process(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(
            arguments,
            MetaIntrospectCommandEnvironment::from_process(),
        )
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: MetaIntrospectCommandEnvironment,
    ) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: triad_runtime::ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let text = DatomCommandText::from_command(self.command)?;
        let query: MetaIntrospectQuery = datom_text::actualize(text.as_str())?;
        let response = MetaIntrospectClient::new(self.environment.endpoint()).submit(query)?;
        writeln!(output, "{}", datom_text::textualize(&response))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaIntrospectCommandEnvironment {
    socket: String,
}

impl MetaIntrospectCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(
            std::env::var("INTROSPECT_META_SOCKET")
                .unwrap_or(DEFAULT_META_INTROSPECT_SOCKET.to_string()),
        )
    }

    pub fn endpoint(&self) -> MetaIntrospectEndpoint {
        MetaIntrospectEndpoint::new(&self.socket)
    }
}
