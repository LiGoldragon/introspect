use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kameo::actor::{Actor, ActorRef, Spawn, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible, SendError};
use kameo::message::{Context, Message};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_introspect::{
    ComponentReadiness, ComponentSnapshotObservation, EngineSnapshotObservation,
    IntrospectionTarget, PrototypeWitnessObservation, PrototypeWitnessObservationQuery, Query,
    Response,
};
use signal_persona::EngineIdentifier;
use signal_router::{
    EngineIdentifier as RouterEngineIdentifier, Frame as RouterFrame, FrameBody as RouterFrameBody,
    Input as RouterRequest, Output as RouterReply, RouterSummaryQuery,
};
use tokio::task::JoinHandle;
use triad_runtime::trace::TraceSocketListener;

use crate::error::{Error, Result};
use crate::store::{IntrospectionStore, StoreLocation};
use crate::store_message::{
    FlushTargetedSystemEvents, ReadComponentTrace, ReadDeliveryTrace, ReadSystemEvents,
    RecordComponentTraceEvent, RecordDeliveryTraceEvent, RecordObservation,
    RecordTargetedSystemEvent,
};
use crate::store_record::{ObservationSequence, StoredObservation};
use crate::trace_frame::TracedComponentEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSocketDirectory {
    pub manager_socket: Option<PathBuf>,
    pub router_socket: Option<PathBuf>,
    pub terminal_socket: Option<PathBuf>,
    pub trace_socket: Option<PathBuf>,
}

impl TargetSocketDirectory {
    pub fn empty() -> Self {
        Self {
            manager_socket: None,
            router_socket: None,
            terminal_socket: None,
            trace_socket: None,
        }
    }
}

#[derive(Debug)]
pub struct IntrospectionRoot {
    target_directory: ActorRef<TargetDirectory>,
    query_planner: ActorRef<QueryPlanner>,
    manager_client: ActorRef<ManagerClient>,
    router_client: ActorRef<RouterClient>,
    terminal_client: ActorRef<TerminalClient>,
    trace_listener: ActorRef<ComponentTraceListener>,
    store: ActorRef<IntrospectionStore>,
    projection: ActorRef<DatomProjection>,
    handled_queries: u64,
}

impl IntrospectionRoot {
    /// Spawn the introspection actor tree. The body is entirely synchronous —
    /// kameo `spawn` and the sema-store open are sync — so the daemon shell's
    /// `build_runtime` (a sync hook run inside the runtime's `block_on`) can
    /// call it without a nested `block_on`.
    pub fn spawn_root(input: IntrospectionRootInput) -> Result<ActorRef<Self>> {
        let target_directory = TargetDirectory::spawn(TargetDirectory::new(input.targets.clone()));
        let query_planner = QueryPlanner::spawn(QueryPlanner::new());
        let manager_client =
            ManagerClient::spawn(ManagerClient::new(input.targets.manager_socket.clone()));
        let router_client = RouterClient::spawn(RouterClient::new(input.targets.router_socket));
        let terminal_client =
            TerminalClient::spawn(TerminalClient::new(input.targets.terminal_socket));
        let store = IntrospectionStore::spawn(IntrospectionStore::open(&input.store)?);
        let trace_listener = ComponentTraceListener::spawn(ComponentTraceListener::new(
            input.targets.trace_socket,
            store.clone(),
        ));
        let projection = DatomProjection::spawn(DatomProjection::new());
        Ok(Self::spawn(Self {
            target_directory,
            query_planner,
            manager_client,
            router_client,
            terminal_client,
            trace_listener,
            store,
            projection,
            handled_queries: 0,
        }))
    }

    async fn prototype_witness(
        &mut self,
        query: PrototypeWitnessObservationQuery,
    ) -> Result<Response> {
        self.handled_queries = self.handled_queries.saturating_add(1);
        let router_seen = match self
            .router_client
            .ask(QueryRouterSummary {
                engine: query.engine_identifier.clone(),
            })
            .await
        {
            Ok(readiness) => readiness,
            Err(SendError::HandlerError(error)) => return Err(error),
            Err(error) => {
                return Err(Error::Actor {
                    operation: "query router summary",
                    detail: format!("{error:?}"),
                });
            }
        };

        Ok(Response::PrototypeWitnessObservation(
            PrototypeWitnessObservation {
                engine_identifier: query.engine_identifier,
                first_optional_component_readiness: None,
                second_optional_component_readiness: router_seen,
                third_optional_component_readiness: None,
                delivery_trace_observation_status_option: None,
            },
        ))
    }

    async fn answer(&mut self, query: Query) -> Result<Response> {
        match query {
            Query::EngineSnapshotObservation(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                Ok(Response::EngineSnapshotObservation(
                    EngineSnapshotObservation {
                        engine_identifier: query.engine_identifier,
                        observed_components: vec![
                            IntrospectionTarget::EngineManager,
                            IntrospectionTarget::Router,
                            IntrospectionTarget::Terminal,
                        ],
                    },
                ))
            }
            Query::ComponentSnapshotObservation(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                Ok(Response::ComponentSnapshotObservation(
                    ComponentSnapshotObservation {
                        engine_identifier: query.engine_identifier,
                        introspection_target: query.introspection_target,
                        optional_component_readiness: None,
                    },
                ))
            }
            Query::DeliveryTraceObservation(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                let trace = self
                    .ask_store(ReadDeliveryTrace::new(query), "read delivery trace")
                    .await?;
                Ok(Response::DeliveryTraceObservation(trace))
            }
            Query::ComponentTrace(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                let trace = self
                    .ask_store(ReadComponentTrace::new(query), "read component trace")
                    .await?;
                Ok(Response::ComponentTrace(trace))
            }
            Query::RecordSystemEvent(record) => {
                let accepted = self
                    .ask_store(
                        RecordTargetedSystemEvent::new(record.system_event),
                        "record targeted system event",
                    )
                    .await?;
                Ok(Response::SystemEventAccepted(accepted))
            }
            Query::SystemEvents(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                let events = self
                    .ask_store(ReadSystemEvents::new(query), "read targeted system events")
                    .await?;
                Ok(Response::SystemEvents(events))
            }
            Query::FlushSystemEvents(flush) => {
                let flushed = self
                    .ask_store(
                        FlushTargetedSystemEvents::new(flush.boot_identifier),
                        "flush targeted system events",
                    )
                    .await?;
                Ok(Response::SystemEventsFlushed(flushed))
            }
            Query::PrototypeWitnessObservation(query) => self.prototype_witness(query).await,
        }
    }

    /// Ask the store one message, folding kameo's transport failures into the
    /// component's own error so every store operation reads the same way.
    async fn ask_store<StoreMessage, Answer>(
        &self,
        message: StoreMessage,
        operation: &'static str,
    ) -> Result<Answer>
    where
        IntrospectionStore: Message<StoreMessage, Reply = Result<Answer>>,
        StoreMessage: Send + 'static,
        Answer: Send + 'static,
    {
        match self.store.ask(message).await {
            Ok(answer) => Ok(answer),
            Err(SendError::HandlerError(error)) => Err(error),
            Err(error) => Err(Error::Actor {
                operation,
                detail: format!("{error:?}"),
            }),
        }
    }

    async fn record_observation(&self, query: Query, response: Response) -> Result<()> {
        let observation = StoredObservation::new(
            ObservationSequence::new(self.handled_queries),
            query,
            response,
        );
        self.ask_store(
            RecordObservation::new(observation),
            "record introspection observation",
        )
        .await
        .map(|_receipt| ())
    }

    async fn stop_children(&self) {
        let _ = self.target_directory.stop_gracefully().await;
        let _ = self.query_planner.stop_gracefully().await;
        let _ = self.manager_client.stop_gracefully().await;
        let _ = self.router_client.stop_gracefully().await;
        let _ = self.terminal_client.stop_gracefully().await;
        let _ = self.trace_listener.stop_gracefully().await;
        let _ = self.store.stop_gracefully().await;
        let _ = self.projection.stop_gracefully().await;
        self.target_directory.wait_for_shutdown().await;
        self.query_planner.wait_for_shutdown().await;
        self.manager_client.wait_for_shutdown().await;
        self.router_client.wait_for_shutdown().await;
        self.terminal_client.wait_for_shutdown().await;
        self.trace_listener.wait_for_shutdown().await;
        self.store.wait_for_shutdown().await;
        self.projection.wait_for_shutdown().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionRootInput {
    pub targets: TargetSocketDirectory,
    pub store: StoreLocation,
}

impl Actor for IntrospectionRoot {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> std::result::Result<(), Self::Error> {
        self.stop_children().await;
        Ok(())
    }
}

pub struct ExplainPrototypeWitness {
    pub query: PrototypeWitnessObservationQuery,
}

impl Message<ExplainPrototypeWitness> for IntrospectionRoot {
    type Reply = Result<Response>;

    async fn handle(
        &mut self,
        message: ExplainPrototypeWitness,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let query = Query::PrototypeWitnessObservation(message.query);
        let response = self.answer(query.clone()).await?;
        self.record_observation(query, response.clone()).await?;
        Ok(response)
    }
}

pub struct HandleIntrospectionQuery {
    pub query: Query,
}

impl Message<HandleIntrospectionQuery> for IntrospectionRoot {
    type Reply = Result<Response>;

    async fn handle(
        &mut self,
        message: HandleIntrospectionQuery,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let query = message.query;
        let response = self.answer(query.clone()).await?;
        if !matches!(&query, Query::RecordSystemEvent(_)) {
            self.record_observation(query, response.clone()).await?;
        }
        Ok(response)
    }
}

impl Message<RecordDeliveryTraceEvent> for IntrospectionRoot {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        message: RecordDeliveryTraceEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.ask_store(message, "record delivery trace event")
            .await
            .map(|_receipt| ())
    }
}

#[derive(Debug)]
pub struct TargetDirectory {
    sockets: TargetSocketDirectory,
}

impl TargetDirectory {
    pub fn new(sockets: TargetSocketDirectory) -> Self {
        Self { sockets }
    }

    pub fn sockets(&self) -> &TargetSocketDirectory {
        &self.sockets
    }
}

impl Actor for TargetDirectory {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

#[derive(Debug)]
pub struct QueryPlanner {
    planned_queries: u64,
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self { planned_queries: 0 }
    }

    pub fn planned_queries(&self) -> u64 {
        self.planned_queries
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for QueryPlanner {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

#[derive(Debug)]
pub struct ManagerClient {
    socket: Option<PathBuf>,
}

impl ManagerClient {
    pub fn new(socket: Option<PathBuf>) -> Self {
        Self { socket }
    }

    pub fn socket(&self) -> Option<&Path> {
        self.socket.as_deref()
    }
}

impl Actor for ManagerClient {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// The router observation client.
///
/// `signal-router` is a peer contract still on the exchange-envelope wire: its
/// `Frame` is a `signal-frame` bound exchange frame, not an ethos-root Signal.
/// Introspect speaks the peer's contract as the peer publishes it, so this one
/// path keeps the envelope while introspect's own planes carry bare Signal
/// frames.
#[derive(Debug)]
pub struct RouterClient {
    socket: Option<PathBuf>,
}

impl RouterClient {
    pub fn new(socket: Option<PathBuf>) -> Self {
        Self { socket }
    }

    pub fn socket(&self) -> Option<&Path> {
        self.socket.as_deref()
    }

    fn query_summary_over_socket(
        socket: PathBuf,
        engine: EngineIdentifier,
    ) -> Result<Option<ComponentReadiness>> {
        let mut stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let frame = RouterRequest::Summary(RouterSummaryQuery::new(
            RouterEngineIdentifier::new(engine.clone()).into(),
        ))
        .into_frame(Self::router_exchange());
        stream.write_all(&frame.encode_length_prefixed()?)?;
        stream.flush()?;
        let reply = RouterClientFrameCodec::default().read_frame(&mut stream)?;
        Self::router_summary_readiness(engine, reply)
    }

    fn router_exchange() -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    fn router_summary_readiness(
        expected_engine: EngineIdentifier,
        frame: RouterFrame,
    ) -> Result<Option<ComponentReadiness>> {
        match frame.into_body() {
            RouterFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(RouterReply::Summary(summary)) => {
                        if summary.engine.payload().payload().as_str() == expected_engine.as_str() {
                            Ok(Some(ComponentReadiness::Ready))
                        } else {
                            Ok(Some(ComponentReadiness::NotReady))
                        }
                    }
                    SubReply::Ok(RouterReply::Unimplemented(_)) => Ok(None),
                    other => Err(Error::UnexpectedRouterObservationReply {
                        got: format!("{other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedRouterObservationReply {
                    got: reason.to_string(),
                }),
            },
            other => Err(Error::UnexpectedRouterObservationReply {
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Actor for RouterClient {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct QueryRouterSummary {
    pub engine: EngineIdentifier,
}

impl Message<QueryRouterSummary> for RouterClient {
    type Reply = Result<Option<ComponentReadiness>>;

    async fn handle(
        &mut self,
        message: QueryRouterSummary,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(socket) = self.socket.clone() else {
            return Ok(None);
        };
        tokio::task::spawn_blocking(move || Self::query_summary_over_socket(socket, message.engine))
            .await
            .map_err(|error| Error::Actor {
                operation: "join router summary query",
                detail: error.to_string(),
            })?
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RouterClientFrameCodec {
    maximum_frame_bytes: usize,
}

impl RouterClientFrameCodec {
    const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    fn read_frame(&self, reader: &mut impl Read) -> Result<RouterFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("router frame exceeds {} bytes", self.maximum_frame_bytes),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(RouterFrame::decode_length_prefixed(&bytes)?)
    }
}

impl Default for RouterClientFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

/// The component-trace ingestion plane. Owns the bound trace socket and a
/// background drain task that pulls pushed `ComponentTraceEvent` Signal frames
/// off the socket and forwards each to the store.
///
/// Mirrors `RouterClient`'s socket discipline (sync socket IO behind
/// `spawn_blocking`), but for ingestion rather than query: spirit (and, later,
/// router) PUSH events to this socket; introspect PULLs them off into durable
/// state. The socket itself is the actor's data — without the bound listener
/// the actor has no job — so the no-blocking-handler rule is honored by running
/// the continuous `collect_for` accept loop on the blocking pool, never inside
/// a message handler.
#[derive(Debug)]
pub struct ComponentTraceListener {
    socket: Option<PathBuf>,
    store: ActorRef<IntrospectionStore>,
    drain_task: Option<JoinHandle<()>>,
}

impl ComponentTraceListener {
    /// The window each blocking `collect_for` call accepts pushed frames for
    /// before the drain loop yields back to forward what it gathered. Short so
    /// ingested events reach the store promptly; the loop runs continuously.
    const COLLECT_WINDOW: Duration = Duration::from_millis(50);

    pub fn new(socket: Option<PathBuf>, store: ActorRef<IntrospectionStore>) -> Self {
        Self {
            socket,
            store,
            drain_task: None,
        }
    }

    pub fn socket(&self) -> Option<&Path> {
        self.socket.as_deref()
    }

    /// The continuous drain loop: repeatedly accept a window of pushed frames on
    /// the blocking pool, then forward each to the store. `ask` (not `tell`) so
    /// the store's fallible `Result` reply is consumed here rather than panicking
    /// the store actor on the tell-of-fallible-handler trap.
    async fn drain(
        listener: Arc<TraceSocketListener<TracedComponentEvent>>,
        store: ActorRef<IntrospectionStore>,
    ) {
        while store.is_alive() {
            let collector = Arc::clone(&listener);
            let collected =
                tokio::task::spawn_blocking(move || collector.collect_for(Self::COLLECT_WINDOW))
                    .await;
            let events = match collected {
                Ok(Ok(events)) => events,
                Ok(Err(_trace_error)) => Vec::new(),
                Err(_join_error) => break,
            };
            for event in events {
                if store
                    .ask(RecordComponentTraceEvent::new(event.into_event()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

impl Actor for ComponentTraceListener {
    type Args = Self;
    type Error = Error;

    async fn on_start(
        mut state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        if let Some(socket) = state.socket.clone() {
            let listener =
                TraceSocketListener::<TracedComponentEvent>::bind(socket).map_err(|error| {
                    Error::TraceIngestion {
                        detail: error.to_string(),
                    }
                })?;
            let store = state.store.clone();
            state.drain_task = Some(tokio::spawn(Self::drain(Arc::new(listener), store)));
        }
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> std::result::Result<(), Self::Error> {
        if let Some(task) = self.drain_task.take() {
            task.abort();
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TerminalClient {
    socket: Option<PathBuf>,
}

impl TerminalClient {
    pub fn new(socket: Option<PathBuf>) -> Self {
        Self { socket }
    }

    pub fn socket(&self) -> Option<&Path> {
        self.socket.as_deref()
    }
}

impl Actor for TerminalClient {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// The text projection plane: what introspect has rendered as datom.
#[derive(Debug)]
pub struct DatomProjection {
    rendered_outputs: u64,
}

impl DatomProjection {
    pub fn new() -> Self {
        Self {
            rendered_outputs: 0,
        }
    }

    pub fn rendered_outputs(&self) -> u64 {
        self.rendered_outputs
    }
}

impl Default for DatomProjection {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for DatomProjection {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}
