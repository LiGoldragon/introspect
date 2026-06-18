//! Introspect's daemon hooks — the only daemon code introspect hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, request gating, peer credentials, lifecycle, and the `ExitReport`
//! entry) is emitted into `src/schema/daemon.rs` by schema-rust-next's daemon
//! emitter under the **component-decoded** working tier. Introspect's ordinary
//! socket speaks the hand-written `signal-introspect` `IntrospectionFrame`
//! contract (not a schema-derived root), so the emitted shell owns listener
//! mechanics while introspect owns the per-connection frame decode/encode and
//! drives the existing `IntrospectionRoot` kameo actor tree.
//!
//! Introspect fills the record-1488 escape hatches through
//! `impl ComponentDaemon for IntrospectionDaemon`: how to load its binary
//! `Configuration`, how to open its kameo engine (`build_runtime`), how one
//! working `IntrospectionFrame` connection becomes a reply, and how the meta
//! owner-only meta socket is served.

use std::path::{Path, PathBuf};

use kameo::actor::ActorRef;
use kameo::error::SendError;
use meta_signal_introspect::{
    Frame as MetaIntrospectFrame, FrameBody as MetaIntrospectFrameBody, MetaIntrospectReply,
    Operation as MetaIntrospectOperation, RequestUnimplemented, UnimplementedReason,
};
use signal_frame::{ExchangeIdentifier, NonEmpty, Reply, Request, SubReply};
use signal_introspect::{
    IntrospectDaemonConfiguration, IntrospectionFrame, IntrospectionFrameBody as FrameBody,
    IntrospectionReply, IntrospectionRequest,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use triad_runtime::{
    AcceptedConnection, FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec,
    RequestConcurrencyLimit, SocketMode,
};

use crate::error::Error;
use crate::runtime::{
    HandleIntrospectionRequest, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use crate::store::StoreLocation;

const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

/// The type-level selector for introspect's emitted daemon. It carries no
/// runtime data — it is the marker the emitted `DaemonCommand<IntrospectionDaemon>`
/// and the generated runtime dispatch on, selecting introspect's
/// `Configuration` / `Engine` / `Error` types through the `ComponentDaemon`
/// associated types.
#[derive(Debug)]
pub struct IntrospectionDaemon;

/// Introspect's daemon error: the frame-transport variants the emitted spine
/// surfaces plus introspect's domain error. The emitted
/// `DaemonError<IntrospectionDaemon>` wraps this under its `Component` arm.
#[derive(Debug, Error)]
pub enum IntrospectionDaemonError {
    #[error("daemon frame error: {0}")]
    Frame(#[from] FrameError),

    #[error("daemon signal frame error: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("introspect engine error: {0}")]
    Engine(#[from] Error),

    #[error("unexpected introspection frame: {got}")]
    UnexpectedFrame { got: String },
}

/// The engine the component-decoded daemon shell owns: the running kameo actor
/// tree. The working tier drives the `IntrospectionRoot`; the meta tier is a
/// typed owner policy socket. `ActorRef` is `Send + Sync + Clone`, and the
/// actor mailbox serialises its own state, so the shared `&Engine` the
/// component-decoded shell hands every connection needs no component-internal
/// lock.
#[derive(Clone)]
pub struct IntrospectionEngine {
    root: ActorRef<IntrospectionRoot>,
}

impl IntrospectionEngine {
    /// Start the kameo actor tree. The body is synchronous — kameo `spawn` and
    /// the sema-store open are sync — so the daemon shell's `build_runtime` hook
    /// (run inside the runtime's `block_on`) constructs it without a nested
    /// `block_on`. Each actor's `on_start` is trivial, so the mailbox is ready
    /// to queue requests the instant the ref exists.
    pub fn start(
        targets: TargetSocketDirectory,
        store: StoreLocation,
    ) -> Result<Self, IntrospectionDaemonError> {
        let root = IntrospectionRoot::spawn_root(IntrospectionRootInput { targets, store })
            .map_err(IntrospectionDaemonError::Engine)?;
        Ok(Self { root })
    }

    /// Drive one decoded introspection request through the root actor, returning
    /// the reply payload.
    async fn answer(
        &self,
        request: IntrospectionRequest,
    ) -> Result<IntrospectionReply, IntrospectionDaemonError> {
        match self.root.ask(HandleIntrospectionRequest { request }).await {
            Ok(reply) => Ok(reply),
            Err(SendError::HandlerError(error)) => Err(IntrospectionDaemonError::Engine(error)),
            Err(error) => Err(IntrospectionDaemonError::Engine(Error::Actor {
                operation: "handle introspection request",
                detail: format!("{error:?}"),
            })),
        }
    }

    fn stop(&self) {
        self.root.kill();
    }
}

/// Introspect's binary startup configuration, wrapping the typed
/// `IntrospectDaemonConfiguration` from `signal-introspect` with the
/// `triad_runtime::BindingSurface` projection the emitted shell drives:
/// the working socket is the introspection-query socket, the meta socket is the
/// owner-only `meta-signal-introspect` socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionDaemonConfiguration {
    configuration: IntrospectDaemonConfiguration,
}

impl IntrospectionDaemonConfiguration {
    pub fn new(configuration: IntrospectDaemonConfiguration) -> Self {
        Self { configuration }
    }

    pub fn into_inner(self) -> IntrospectDaemonConfiguration {
        self.configuration
    }

    pub fn from_signal_file(path: &Path) -> Result<Self, Error> {
        let bytes = std::fs::read(path).map_err(|source| Error::ConfigurationRead {
            path: path.to_path_buf(),
            source,
        })?;
        IntrospectDaemonConfiguration::from_rkyv_bytes(bytes.as_slice())
            .map(Self::new)
            .map_err(|_| Error::ConfigurationArchiveDecode)
    }

    fn targets(&self) -> TargetSocketDirectory {
        TargetSocketDirectory {
            manager_socket: Self::peer_socket(self.configuration.manager_socket_path.as_str()),
            router_socket: Self::peer_socket(self.configuration.router_socket_path.as_str()),
            terminal_socket: Self::peer_socket(self.configuration.terminal_socket_path.as_str()),
        }
    }

    /// An empty wire path means "no peer configured" — the prototype daemon then
    /// reports that peer's observation as unseen rather than failing the query
    /// on an unreachable socket. A non-empty path is the peer's live socket.
    fn peer_socket(path: &str) -> Option<PathBuf> {
        if path.is_empty() {
            None
        } else {
            Some(PathBuf::from(path))
        }
    }

    fn store(&self) -> StoreLocation {
        StoreLocation::new(self.configuration.store_path.as_str())
    }
}

impl triad_runtime::BindingSurface for IntrospectionDaemonConfiguration {
    fn socket_path(&self) -> &Path {
        Path::new(self.configuration.introspect_socket_path.as_str())
    }

    fn socket_mode(&self) -> Option<SocketMode> {
        Some(SocketMode::new(
            *self.configuration.introspect_socket_mode.payload() as u32,
        ))
    }

    fn request_concurrency_limit(&self) -> RequestConcurrencyLimit {
        RequestConcurrencyLimit::new(MAXIMUM_CONCURRENT_REQUESTS)
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(Path::new(
            self.configuration.supervision_socket_path.as_str(),
        ))
    }

    fn meta_socket_mode(&self) -> Option<SocketMode> {
        Some(SocketMode::new(
            *self.configuration.supervision_socket_mode.payload() as u32,
        ))
    }

    fn database_path(&self) -> &Path {
        Path::new(self.configuration.store_path.as_str())
    }
}

impl crate::schema::daemon::ComponentDaemon for IntrospectionDaemon {
    type Configuration = IntrospectionDaemonConfiguration;
    type ConfigurationError = Error;
    type Engine = IntrospectionEngine;
    type Error = IntrospectionDaemonError;

    const PROCESS_NAME: &'static str = "introspect-daemon";

    fn load_configuration(path: &Path) -> Result<Self::Configuration, Self::ConfigurationError> {
        IntrospectionDaemonConfiguration::from_signal_file(path)
    }

    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        IntrospectionEngine::start(configuration.targets(), configuration.store())
    }

    fn stop(engine: &Self::Engine) -> Result<(), Self::Error> {
        engine.stop();
        Ok(())
    }

    /// Serve one working introspection-query connection: decode the
    /// `IntrospectionFrame` request off the accepted stream, drive it through
    /// the `IntrospectionRoot` actor, and write the reply frame back.
    async fn handle_working_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let mut transport = IntrospectionTransport::new(connection);
        let received = transport.read_request().await?;
        let reply = engine.answer(received.request).await?;
        transport.write_reply(received.exchange, reply).await
    }

    /// Serve one owner-only meta connection. The durable meta contract is
    /// `meta-signal-introspect`; runtime reconfiguration is intentionally still
    /// rejected until the component owns a real hot-configuration reducer.
    async fn handle_meta_connection(
        _engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let mut transport = MetaIntrospectTransport::new(connection);
        let received = transport.read_request().await?;
        let reply = MetaIntrospectReply::RequestUnimplemented(RequestUnimplemented {
            operation: received.operation.kind(),
            reason: UnimplementedReason::NotBuiltYet,
        });
        transport.write_reply(received.exchange, reply).await
    }
}

/// The introspection-query wire transport over one accepted working connection:
/// the triad length-prefix envelope wraps the bare `IntrospectionFrame` archive
/// (the envelope owns the 4-byte length frame, so the inner codec speaks
/// `encode`/`decode`, not `encode_length_prefixed`).
struct IntrospectionTransport {
    connection: AcceptedConnection,
}

impl IntrospectionTransport {
    fn new(connection: AcceptedConnection) -> Self {
        Self { connection }
    }

    async fn read_request(
        &mut self,
    ) -> Result<ReceivedIntrospectionRequest, IntrospectionDaemonError> {
        let frame_bytes = LengthPrefixedCodec::default()
            .read_body_async(self.connection.stream_mut())
            .await?
            .into_bytes();
        match IntrospectionFrame::decode(&frame_bytes)?.into_body() {
            FrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(IntrospectionDaemonError::UnexpectedFrame {
                        got: format!("expected one introspection payload, got {}", tail.len() + 1),
                    });
                }
                Ok(ReceivedIntrospectionRequest { exchange, request })
            }
            other => Err(IntrospectionDaemonError::UnexpectedFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    async fn write_reply(
        &mut self,
        exchange: ExchangeIdentifier,
        reply: IntrospectionReply,
    ) -> Result<(), IntrospectionDaemonError> {
        let frame = IntrospectionFrame::new(FrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        LengthPrefixedCodec::default()
            .write_body_async(
                self.connection.stream_mut(),
                &LengthPrefixedFrameBody::new(frame.encode()?),
            )
            .await?;
        self.connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }
}

struct ReceivedIntrospectionRequest {
    exchange: ExchangeIdentifier,
    request: IntrospectionRequest,
}

/// The meta-introspect wire transport over one accepted meta connection.
struct MetaIntrospectTransport {
    connection: AcceptedConnection,
}

impl MetaIntrospectTransport {
    fn new(connection: AcceptedConnection) -> Self {
        Self { connection }
    }

    async fn read_request(
        &mut self,
    ) -> Result<ReceivedMetaIntrospectRequest, IntrospectionDaemonError> {
        let frame_bytes = match LengthPrefixedCodec::default()
            .read_body_async(self.connection.stream_mut())
            .await
        {
            Ok(body) => body.into_bytes(),
            Err(error) => return Err(error.into()),
        };
        match MetaIntrospectFrame::decode(&frame_bytes)?.into_body() {
            MetaIntrospectFrameBody::Request { exchange, request } => {
                let (operation, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(IntrospectionDaemonError::UnexpectedFrame {
                        got: format!(
                            "expected one meta-introspect operation, got {}",
                            tail.len() + 1,
                        ),
                    });
                }
                Ok(ReceivedMetaIntrospectRequest {
                    exchange,
                    operation,
                })
            }
            other => Err(IntrospectionDaemonError::UnexpectedFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    async fn write_reply(
        &mut self,
        exchange: ExchangeIdentifier,
        reply: MetaIntrospectReply,
    ) -> Result<(), IntrospectionDaemonError> {
        let frame = MetaIntrospectFrame::new(MetaIntrospectFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        LengthPrefixedCodec::default()
            .write_body_async(
                self.connection.stream_mut(),
                &LengthPrefixedFrameBody::new(frame.encode()?),
            )
            .await?;
        self.connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }
}

struct ReceivedMetaIntrospectRequest {
    exchange: ExchangeIdentifier,
    operation: MetaIntrospectOperation,
}

/// A blocking client for the introspection-query socket. The `introspect` CLI
/// uses this at the process edge; the daemon itself binds the socket through
/// the emitted shell and never calls the client path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionSignalClient {
    socket: PathBuf,
}

impl IntrospectionSignalClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn submit(&self, request: IntrospectionRequest) -> crate::Result<IntrospectionReply> {
        use signal_frame::{ExchangeLane, LaneSequence, SessionEpoch};
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let mut stream = UnixStream::connect(&self.socket)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::new(1),
        );
        let request_frame = IntrospectionFrame::new(FrameBody::Request {
            exchange,
            request: Request::from_payload(request),
        });
        stream.write_all(&request_frame.encode_length_prefixed()?)?;
        stream.flush()?;

        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = vec![0_u8; length];
        stream.read_exact(&mut bytes)?;
        match IntrospectionFrame::decode(&bytes)?.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedSignalFrame {
                        got: format!("{other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: reason.to_string(),
                }),
            },
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }
}
