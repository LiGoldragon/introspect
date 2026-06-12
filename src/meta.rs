use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use meta_signal_introspect::{
    Frame as MetaIntrospectFrame, FrameBody as MetaIntrospectFrameBody, MetaIntrospectReply,
    Operation as MetaIntrospectOperation,
};
use nota_next::{NotaEncode, NotaSource};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use triad_runtime::{ComponentCommand, FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::cli_argument::NotaCommandText;
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

    pub fn submit(&self, operation: MetaIntrospectOperation) -> Result<MetaIntrospectReply> {
        let exchange = self.exchange();
        let frame = MetaIntrospectFrame::new(MetaIntrospectFrameBody::Request {
            exchange,
            request: signal_frame::Request::from_payload(operation),
        });
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        self.codec
            .write_body(&mut stream, &RuntimeFrameBody::new(frame.encode()?))?;
        let body = self.codec.read_body(&mut stream)?;
        self.reply_from_frame(MetaIntrospectFrame::decode(body.bytes())?)
    }

    fn exchange(&self) -> ExchangeIdentifier {
        let _endpoint = &self.endpoint;
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    fn reply_from_frame(&self, frame: MetaIntrospectFrame) -> Result<MetaIntrospectReply> {
        match frame.into_body() {
            MetaIntrospectFrameBody::Reply { reply, .. } => self.reply_output(reply),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn reply_output(&self, reply: Reply<MetaIntrospectReply>) -> Result<MetaIntrospectReply> {
        let _endpoint = &self.endpoint;
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                other => Err(Error::UnexpectedSignalFrame {
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                got: reason.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaIntrospectCommand {
    command: ComponentCommand,
    environment: MetaIntrospectCommandEnvironment,
}

impl MetaIntrospectCommand {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
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
            command: ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let operation =
            MetaIntrospectOperationText::from_command(self.command)?.into_operation()?;
        let reply = MetaIntrospectClient::new(self.environment.endpoint()).submit(operation)?;
        writeln!(output, "{}", reply.to_nota())?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaIntrospectOperationText {
    text: NotaCommandText,
}

impl MetaIntrospectOperationText {
    fn from_command(command: ComponentCommand) -> Result<Self> {
        Ok(Self {
            text: NotaCommandText::from_command(command)?,
        })
    }

    fn into_operation(self) -> Result<MetaIntrospectOperation> {
        Ok(NotaSource::new(self.text.as_str()).parse::<MetaIntrospectOperation>()?)
    }
}
