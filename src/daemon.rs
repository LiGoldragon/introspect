//! Introspect's daemon hooks — the only daemon code introspect hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, request gating, peer credentials, lifecycle, and the `ExitReport`
//! entry) is emitted into `src/schema/daemon.rs` by schema-rust's daemon
//! emitter under the **component-decoded** working tier. Introspect's ordinary
//! socket speaks the `signal-introspect` ethos root — one rkyv `Signal<Query>`
//! in, one `Signal<Response>` out — so the emitted shell owns listener
//! mechanics while introspect owns the per-connection Signal restore/form and
//! drives the `IntrospectionRoot` kameo actor tree.
//!
//! Introspect fills the record-1488 escape hatches through
//! `impl ComponentDaemon for IntrospectionDaemon`: how to load its binary
//! `Configuration`, how to open its kameo engine (`build_runtime`), how one
//! working Signal connection becomes a reply, and how the owner-only meta
//! socket is served.

use std::path::{Path, PathBuf};

use kameo::actor::ActorRef;
use kameo::error::SendError;
use meta_signal_introspect::{
    ByteViewable as MetaByteViewable, MetaIntrospectOperationKind, Query as MetaIntrospectQuery,
    RequestUnimplemented, Response as MetaIntrospectResponse, Restorable as MetaRestorable,
    Signal as MetaIntrospectSignal, Signalizable as MetaSignalizable, UnimplementedReason,
};
use signal_introspect::{
    ByteViewable, IntrospectDaemonConfiguration, Query, Response, Restorable, Signal, Signalizable,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use triad_runtime::{
    AcceptedConnection, FrameBody, FrameError, LengthPrefixedCodec, RequestConcurrencyLimit,
    SocketMode,
};

use crate::error::Error;
use crate::runtime::{
    HandleIntrospectionQuery, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
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

    #[error("daemon signal archive error: {detail}")]
    SignalArchive { detail: String },

    #[error("introspect engine error: {0}")]
    Engine(#[from] Error),
}

impl From<rkyv::rancor::Error> for IntrospectionDaemonError {
    fn from(error: rkyv::rancor::Error) -> Self {
        Self::SignalArchive {
            detail: error.to_string(),
        }
    }
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

    /// Drive one restored introspection query through the root actor, returning
    /// the response.
    async fn answer(&self, query: Query) -> Result<Response, IntrospectionDaemonError> {
        match self.root.ask(HandleIntrospectionQuery { query }).await {
            Ok(response) => Ok(response),
            Err(SendError::HandlerError(error)) => Err(IntrospectionDaemonError::Engine(error)),
            Err(error) => Err(IntrospectionDaemonError::Engine(Error::Actor {
                operation: "handle introspection query",
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
#[derive(Debug, Clone, PartialEq)]
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

    /// The basic meta operation of every component is daemon configuration: the
    /// typed record the Persona manager encodes is itself the binary startup
    /// message, read here as the rkyv archive of the contract type.
    pub fn from_signal_file(path: &Path) -> Result<Self, Error> {
        let bytes = std::fs::read(path).map_err(|source| Error::ConfigurationRead {
            path: path.to_path_buf(),
            source,
        })?;
        rkyv::from_bytes::<IntrospectDaemonConfiguration, rkyv::rancor::Error>(bytes.as_slice())
            .map(Self::new)
            .map_err(Error::from)
    }

    fn targets(&self) -> TargetSocketDirectory {
        TargetSocketDirectory {
            manager_socket: Self::peer_socket(self.configuration.manager_socket_path.as_str()),
            router_socket: Self::peer_socket(self.configuration.router_socket_path.as_str()),
            terminal_socket: Self::peer_socket(self.configuration.terminal_socket_path.as_str()),
            trace_socket: Self::peer_socket(self.configuration.trace_socket_path.as_str()),
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
            self.configuration.introspect_socket_mode as u32,
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
            self.configuration.supervision_socket_mode as u32,
        ))
    }

    fn database_path(&self) -> &Path {
        Path::new(self.configuration.store_path.as_str())
    }
}

impl crate::daemon_shell::ComponentDaemon for IntrospectionDaemon {
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

    /// Serve one working introspection-query connection: restore the `Query`
    /// Signal off the accepted stream, drive it through the `IntrospectionRoot`
    /// actor, and write the `Response` Signal back.
    async fn handle_working_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let mut transport = SignalTransport::new(connection);
        let query: Query = transport.read().await?;
        let response = engine.answer(query).await?;
        transport.write(response.signalize()?.bytes()).await
    }

    /// Serve one owner-only meta connection. The durable meta contract is
    /// `meta-signal-introspect`; runtime reconfiguration is intentionally still
    /// rejected until the component owns a real hot-configuration reducer.
    async fn handle_meta_connection(
        _engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let mut transport = SignalTransport::new(connection);
        let bytes = transport.read_bytes().await?;
        let query = MetaIntrospectSignal::<MetaIntrospectQuery>::from(bytes).restore()?;
        let response = MetaIntrospectResponse::RequestUnimplemented(RequestUnimplemented {
            meta_introspect_operation_kind: meta_operation_kind(&query),
            unimplemented_reason: UnimplementedReason::NotBuiltYet,
        });
        transport.write(response.signalize()?.bytes()).await
    }
}

/// Which meta operation a restored meta `Query` names. The contract's
/// `MetaIntrospectOperationKind` is the closed set; this is its projection from
/// the query variant.
fn meta_operation_kind(query: &MetaIntrospectQuery) -> MetaIntrospectOperationKind {
    match query {
        MetaIntrospectQuery::Configure(_) => MetaIntrospectOperationKind::Configure,
    }
}

/// One accepted connection carrying rkyv Signal frames: the triad
/// length-prefix envelope wraps the contract's own Signal bytes, so the
/// envelope owns the 4-byte length frame and the contract owns everything
/// inside it.
struct SignalTransport {
    connection: AcceptedConnection,
}

impl SignalTransport {
    fn new(connection: AcceptedConnection) -> Self {
        Self { connection }
    }

    async fn read_bytes(&mut self) -> Result<Vec<u8>, IntrospectionDaemonError> {
        Ok(LengthPrefixedCodec::default()
            .read_body_async(self.connection.stream_mut())
            .await?
            .into_bytes())
    }

    async fn read<Value>(&mut self) -> Result<Value, IntrospectionDaemonError>
    where
        Signal<Value>: Restorable<Value>,
    {
        let bytes = self.read_bytes().await?;
        Ok(Signal::<Value>::from(bytes).restore()?)
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), IntrospectionDaemonError> {
        LengthPrefixedCodec::default()
            .write_body_async(
                self.connection.stream_mut(),
                &FrameBody::new(bytes.to_vec()),
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

    pub fn submit(&self, query: Query) -> crate::Result<Response> {
        use std::os::unix::net::UnixStream;

        let codec = LengthPrefixedCodec::default();
        let mut stream = UnixStream::connect(&self.socket)?;
        let signal = query.signalize().map_err(Error::from)?;
        codec.write_body(&mut stream, &FrameBody::new(signal.bytes().to_vec()))?;
        let body = codec.read_body(&mut stream)?;
        Signal::<Response>::from(body.into_bytes())
            .restore()
            .map_err(Error::from)
    }
}
