use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

use kameo::actor::ActorRef;
use kameo::error::SendError;
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, Request, SessionEpoch,
    SignalVerb, SubReply,
};
use signal_introspect::{
    IntrospectDaemonConfiguration, IntrospectionFrame, IntrospectionFrameBody as FrameBody,
    IntrospectionReply, IntrospectionRequest,
};

use crate::error::{Error, Result};
use crate::runtime::{
    HandleIntrospectionRequest, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use crate::store::StoreLocation;
use crate::supervision::{SupervisionListener, SupervisionProfile, SupervisionSocketMode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionSocket {
    path: PathBuf,
}

impl IntrospectionSocket {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// CLI convenience — the `introspect` CLI may discover the daemon's
    /// socket via `PERSONA_INTROSPECT_SOCKET` / `PERSONA_SOCKET_PATH`
    /// as a last-resort connection target. **Not for the daemon's
    /// production launch path** — the daemon binds the socket path
    /// supplied by `IntrospectDaemonConfiguration.introspect_socket_path`.
    pub fn from_environment() -> Option<Self> {
        std::env::var_os("PERSONA_INTROSPECT_SOCKET")
            .or_else(|| std::env::var_os("PERSONA_SOCKET_PATH"))
            .map(Self::from_path)
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn client(&self) -> IntrospectionSignalClient {
        IntrospectionSignalClient::new(self.path.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketMode(u32);

impl SocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionDaemon {
    socket: IntrospectionSocket,
    targets: TargetSocketDirectory,
    store: StoreLocation,
    socket_mode: Option<SocketMode>,
    supervision: Option<SupervisionListener>,
}

impl IntrospectionDaemon {
    /// Canonical constructor — every production launch reads typed
    /// `IntrospectDaemonConfiguration` from a signal-encoded archive
    /// and hands the record here.
    pub fn from_configuration(configuration: IntrospectDaemonConfiguration) -> Self {
        let targets = TargetSocketDirectory {
            manager_socket: Some(PathBuf::from(configuration.manager_socket_path.as_str())),
            router_socket: Some(PathBuf::from(configuration.router_socket_path.as_str())),
            terminal_socket: Some(PathBuf::from(configuration.terminal_socket_path.as_str())),
        };
        let supervision = SupervisionListener::new(
            SupervisionProfile::introspect(),
            PathBuf::from(configuration.supervision_socket_path.as_str()),
            SupervisionSocketMode::from_octal(configuration.supervision_socket_mode.into_u32()),
        );
        Self {
            socket: IntrospectionSocket::from_path(configuration.introspect_socket_path.as_str()),
            targets,
            store: StoreLocation::new(configuration.store_path.as_str()),
            socket_mode: Some(SocketMode::from_octal(
                configuration.introspect_socket_mode.into_u32(),
            )),
            supervision: Some(supervision),
        }
    }

    /// In-process constructor for tests that build the daemon directly
    /// without going through a configuration file.
    pub fn from_introspection_socket(socket: IntrospectionSocket) -> Self {
        Self {
            socket,
            targets: TargetSocketDirectory::empty(),
            store: StoreLocation::new("/tmp/introspect.sema"),
            socket_mode: None,
            supervision: None,
        }
    }

    pub fn from_socket(socket: impl Into<PathBuf>) -> Self {
        Self::from_introspection_socket(IntrospectionSocket::from_path(socket))
    }

    pub fn with_targets(mut self, targets: TargetSocketDirectory) -> Self {
        self.targets = targets;
        self
    }

    pub fn with_store(mut self, store: StoreLocation) -> Self {
        self.store = store;
        self
    }

    pub fn with_socket_mode(mut self, socket_mode: SocketMode) -> Self {
        self.socket_mode = Some(socket_mode);
        self
    }

    pub fn socket(&self) -> &Path {
        self.socket.path()
    }

    pub fn run(self) -> Result<()> {
        let supervision = self.supervision.clone();
        let bound = self.bind()?;
        let _supervision = supervision.map(SupervisionListener::spawn).transpose()?;
        eprintln!("introspect-daemon socket={}", bound.socket.display());
        bound.serve_forever()
    }

    pub fn bind(self) -> Result<BoundIntrospectionDaemon> {
        if let Some(parent) = self.socket.path().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(self.socket.path());
        let listener = UnixListener::bind(self.socket.path())?;
        if let Some(mode) = self.socket_mode {
            std::fs::set_permissions(
                self.socket.path(),
                std::fs::Permissions::from_mode(mode.as_octal()),
            )?;
        }
        let runtime = tokio::runtime::Runtime::new()?;
        let root = runtime.block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: self.targets,
            store: self.store,
        }))?;
        Ok(BoundIntrospectionDaemon {
            socket: self.socket.path,
            runtime,
            listener,
            root,
        })
    }

    pub fn serve_one(self) -> Result<IntrospectionReply> {
        self.bind()?.serve_one()
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        root: &ActorRef<IntrospectionRoot>,
        stream: UnixStream,
    ) -> Result<IntrospectionReply> {
        let mut connection = IntrospectionConnection::from_stream(stream);
        let request = connection.read_signal_request()?;
        let reply = match runtime.block_on(async {
            root.ask(HandleIntrospectionRequest {
                request: request.request,
            })
            .await
        }) {
            Ok(reply) => reply,
            Err(SendError::HandlerError(error)) => return Err(error),
            Err(error) => {
                return Err(Error::Actor {
                    operation: "handle introspection request",
                    detail: format!("{error:?}"),
                });
            }
        };
        connection.write_signal_reply(request.exchange, request.verb, reply.clone())?;
        Ok(reply)
    }
}

pub struct BoundIntrospectionDaemon {
    socket: PathBuf,
    runtime: tokio::runtime::Runtime,
    listener: UnixListener,
    root: ActorRef<IntrospectionRoot>,
}

impl BoundIntrospectionDaemon {
    pub fn socket(&self) -> &Path {
        self.socket.as_path()
    }

    pub fn serve_one(self) -> Result<IntrospectionReply> {
        let (stream, _address) = self.listener.accept()?;
        let reply = IntrospectionDaemon::handle_connection(&self.runtime, &self.root, stream)?;
        self.runtime
            .block_on(self.root.stop_gracefully())
            .map_err(|error| Error::Actor {
                operation: "stop introspection root",
                detail: error.to_string(),
            })?;
        self.runtime.block_on(self.root.wait_for_shutdown());
        let _ = std::fs::remove_file(&self.socket);
        Ok(reply)
    }

    pub fn serve_forever(self) -> Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let _ = IntrospectionDaemon::handle_connection(&self.runtime, &self.root, stream)?;
        }
        Ok(())
    }
}

pub struct IntrospectionConnection {
    stream: BufReader<UnixStream>,
    signal: IntrospectionFrameCodec,
}

impl IntrospectionConnection {
    pub fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            signal: IntrospectionFrameCodec::default(),
        }
    }

    pub fn read_signal_request(&mut self) -> Result<ReceivedIntrospectionRequest> {
        self.signal.read_request(&mut self.stream)
    }

    pub fn write_signal_reply(
        &mut self,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        reply: IntrospectionReply,
    ) -> Result<()> {
        let stream = self.stream.get_mut();
        self.signal.write_reply(stream, exchange, verb, reply)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntrospectionFrameCodec {
    maximum_frame_bytes: usize,
}

impl IntrospectionFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<IntrospectionFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("frame length {length} exceeds {}", self.maximum_frame_bytes),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(IntrospectionFrame::decode_length_prefixed(&bytes)?)
    }

    pub fn write_frame(&self, writer: &mut impl Write, frame: &IntrospectionFrame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    pub fn read_request(&self, reader: &mut impl Read) -> Result<ReceivedIntrospectionRequest> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Request { exchange, request } => {
                let checked = request.into_checked().map_err(|(error, _request)| {
                    Error::UnexpectedSignalFrame {
                        got: error.to_string(),
                    }
                })?;
                let (operation, tail) = checked.operations.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedSignalFrame {
                        got: format!(
                            "expected one introspection operation, got {}",
                            tail.len() + 1
                        ),
                    });
                }
                Ok(ReceivedIntrospectionRequest {
                    exchange,
                    verb: operation.verb,
                    request: operation.payload,
                })
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> Result<IntrospectionReply> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => {
                    let (sub_reply, tail) = per_operation.into_head_and_tail();
                    if !tail.is_empty() {
                        return Err(Error::UnexpectedSignalFrame {
                            got: format!(
                                "expected one introspection reply operation, got {}",
                                tail.len() + 1
                            ),
                        });
                    }
                    match sub_reply {
                        SubReply::Ok { payload, .. } => Ok(payload),
                        other => Err(Error::UnexpectedSignalFrame {
                            got: format!("{other:?}"),
                        }),
                    }
                }
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: reason.to_string(),
                }),
            },
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_request(
        &self,
        writer: &mut impl Write,
        request: IntrospectionRequest,
    ) -> Result<()> {
        let frame = IntrospectionFrame::new(FrameBody::Request {
            exchange: synthetic_exchange(),
            request: Request::from_payload(request),
        });
        self.write_frame(writer, &frame)
    }

    pub fn write_reply(
        &self,
        writer: &mut impl Write,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        reply: IntrospectionReply,
    ) -> Result<()> {
        let frame = IntrospectionFrame::new(FrameBody::Reply {
            exchange,
            reply: Reply::completed(NonEmpty::single(SubReply::Ok {
                verb,
                payload: reply,
            })),
        });
        self.write_frame(writer, &frame)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedIntrospectionRequest {
    exchange: ExchangeIdentifier,
    verb: SignalVerb,
    request: IntrospectionRequest,
}

impl Default for IntrospectionFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

fn synthetic_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::new(1),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionSignalClient {
    socket: PathBuf,
    signal: IntrospectionFrameCodec,
}

impl IntrospectionSignalClient {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            signal: IntrospectionFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: IntrospectionRequest) -> Result<IntrospectionReply> {
        let mut stream = UnixStream::connect(&self.socket)?;
        self.signal.write_request(&mut stream, request)?;
        let mut reader = BufReader::new(stream);
        self.signal.read_reply(&mut reader)
    }
}
