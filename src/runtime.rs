use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use kameo::actor::{Actor, ActorRef, Spawn, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible, SendError};
use kameo::message::{Context, Message};
use signal_core::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload,
    SessionEpoch, SignalVerb, SubReply,
};
use signal_persona_auth::EngineId;
use signal_persona_introspect::{
    ComponentReadiness, ComponentSnapshot, EngineSnapshot, IntrospectionReply,
    IntrospectionRequest, IntrospectionTarget, PrototypeWitness, PrototypeWitnessQuery,
};
use signal_persona_router::{
    RouterFrame, RouterFrameBody, RouterReply, RouterRequest, RouterSummaryQuery,
};

use crate::error::{Error, Result};
use crate::store::{
    IntrospectionStore, ObservationSequence, ReadDeliveryTrace, RecordDeliveryTraceEvent,
    RecordObservation, StoreLocation, StoredObservation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSocketDirectory {
    pub manager_socket: Option<PathBuf>,
    pub router_socket: Option<PathBuf>,
    pub terminal_socket: Option<PathBuf>,
}

impl TargetSocketDirectory {
    pub fn empty() -> Self {
        Self {
            manager_socket: None,
            router_socket: None,
            terminal_socket: None,
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
    store: ActorRef<IntrospectionStore>,
    projection: ActorRef<NotaProjection>,
    handled_queries: u64,
}

impl IntrospectionRoot {
    pub async fn start_root(input: IntrospectionRootInput) -> Result<ActorRef<Self>> {
        let target_directory = TargetDirectory::spawn(TargetDirectory::new(input.targets.clone()));
        let query_planner = QueryPlanner::spawn(QueryPlanner::new());
        let manager_client =
            ManagerClient::spawn(ManagerClient::new(input.targets.manager_socket.clone()));
        let router_client = RouterClient::spawn(RouterClient::new(input.targets.router_socket));
        let terminal_client =
            TerminalClient::spawn(TerminalClient::new(input.targets.terminal_socket));
        let store = IntrospectionStore::spawn(IntrospectionStore::open(&input.store)?);
        let projection = NotaProjection::spawn(NotaProjection::new());
        Ok(Self::spawn(Self {
            target_directory,
            query_planner,
            manager_client,
            router_client,
            terminal_client,
            store,
            projection,
            handled_queries: 0,
        }))
    }

    async fn prototype_witness(
        &mut self,
        query: PrototypeWitnessQuery,
    ) -> Result<IntrospectionReply> {
        self.handled_queries = self.handled_queries.saturating_add(1);
        let router_seen = match self
            .router_client
            .ask(QueryRouterSummary {
                engine: query.engine.clone(),
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

        Ok(IntrospectionReply::PrototypeWitness(PrototypeWitness {
            engine: query.engine,
            manager_seen: None,
            router_seen,
            terminal_seen: None,
            delivery_status: None,
        }))
    }

    async fn handle_request(
        &mut self,
        request: IntrospectionRequest,
    ) -> Result<IntrospectionReply> {
        match request {
            IntrospectionRequest::EngineSnapshot(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                Ok(IntrospectionReply::EngineSnapshot(EngineSnapshot {
                    engine: query.engine,
                    observed_components: vec![
                        IntrospectionTarget::EngineManager,
                        IntrospectionTarget::Router,
                        IntrospectionTarget::Terminal,
                    ],
                }))
            }
            IntrospectionRequest::ComponentSnapshot(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                Ok(IntrospectionReply::ComponentSnapshot(ComponentSnapshot {
                    engine: query.engine,
                    target: query.target,
                    readiness: None,
                }))
            }
            IntrospectionRequest::DeliveryTrace(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                let trace = match self.store.ask(ReadDeliveryTrace::new(query)).await {
                    Ok(trace) => trace,
                    Err(SendError::HandlerError(error)) => return Err(error),
                    Err(error) => {
                        return Err(Error::Actor {
                            operation: "read delivery trace",
                            detail: format!("{error:?}"),
                        });
                    }
                };
                Ok(IntrospectionReply::DeliveryTrace(trace))
            }
            IntrospectionRequest::PrototypeWitness(query) => self.prototype_witness(query).await,
        }
    }

    async fn record_observation(
        &self,
        request: IntrospectionRequest,
        reply: IntrospectionReply,
    ) -> Result<()> {
        let observation = StoredObservation::new(
            ObservationSequence::new(self.handled_queries),
            request,
            reply,
        );
        match self.store.ask(RecordObservation::new(observation)).await {
            Ok(_receipt) => Ok(()),
            Err(SendError::HandlerError(error)) => Err(error),
            Err(error) => Err(Error::Actor {
                operation: "record introspection observation",
                detail: format!("{error:?}"),
            }),
        }
    }

    async fn stop_children(&self) {
        let _ = self.target_directory.stop_gracefully().await;
        let _ = self.query_planner.stop_gracefully().await;
        let _ = self.manager_client.stop_gracefully().await;
        let _ = self.router_client.stop_gracefully().await;
        let _ = self.terminal_client.stop_gracefully().await;
        let _ = self.store.stop_gracefully().await;
        let _ = self.projection.stop_gracefully().await;
        self.target_directory.wait_for_shutdown().await;
        self.query_planner.wait_for_shutdown().await;
        self.manager_client.wait_for_shutdown().await;
        self.router_client.wait_for_shutdown().await;
        self.terminal_client.wait_for_shutdown().await;
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
    pub query: PrototypeWitnessQuery,
}

impl Message<ExplainPrototypeWitness> for IntrospectionRoot {
    type Reply = Result<IntrospectionReply>;

    async fn handle(
        &mut self,
        message: ExplainPrototypeWitness,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let request = IntrospectionRequest::PrototypeWitness(message.query);
        let reply = self.handle_request(request.clone()).await?;
        self.record_observation(request, reply.clone()).await?;
        Ok(reply)
    }
}

pub struct HandleIntrospectionRequest {
    pub request: IntrospectionRequest,
}

impl Message<HandleIntrospectionRequest> for IntrospectionRoot {
    type Reply = Result<IntrospectionReply>;

    async fn handle(
        &mut self,
        message: HandleIntrospectionRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let request = message.request;
        let reply = self.handle_request(request.clone()).await?;
        self.record_observation(request, reply.clone()).await?;
        Ok(reply)
    }
}

impl Message<RecordDeliveryTraceEvent> for IntrospectionRoot {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        message: RecordDeliveryTraceEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self.store.ask(message).await {
            Ok(_receipt) => Ok(()),
            Err(SendError::HandlerError(error)) => Err(error),
            Err(error) => Err(Error::Actor {
                operation: "record delivery trace event",
                detail: format!("{error:?}"),
            }),
        }
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
        engine: EngineId,
    ) -> Result<Option<ComponentReadiness>> {
        let mut stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let request = RouterRequest::Summary(RouterSummaryQuery {
            engine: engine.clone(),
        });
        let frame = RouterFrame::new(RouterFrameBody::Request {
            exchange: router_exchange(),
            request: request.into_request(),
        });
        stream.write_all(&frame.encode_length_prefixed()?)?;
        stream.flush()?;
        let reply = RouterClientFrameCodec::default().read_frame(&mut stream)?;
        router_summary_readiness(engine, reply)
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
    pub engine: EngineId,
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

fn router_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn router_summary_readiness(
    expected_engine: EngineId,
    frame: RouterFrame,
) -> Result<Option<ComponentReadiness>> {
    match frame.into_body() {
        RouterFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted {
                outcome: AcceptedOutcome::Completed,
                per_operation,
            } => match per_operation.into_head() {
                SubReply::Ok {
                    verb: SignalVerb::Match,
                    payload: RouterReply::Summary(summary),
                } => {
                    if summary.engine == expected_engine {
                        Ok(Some(ComponentReadiness::Ready))
                    } else {
                        Ok(Some(ComponentReadiness::NotReady))
                    }
                }
                SubReply::Ok {
                    payload: RouterReply::Unimplemented(_),
                    ..
                } => Ok(None),
                other => Err(Error::UnexpectedRouterObservationReply {
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::RouterObservationRejected { reason }),
            other => Err(Error::UnexpectedRouterObservationReply {
                got: format!("{other:?}"),
            }),
        },
        other => Err(Error::UnexpectedRouterObservationReply {
            got: format!("{other:?}"),
        }),
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

#[derive(Debug)]
pub struct NotaProjection {
    rendered_outputs: u64,
}

impl NotaProjection {
    pub fn new() -> Self {
        Self {
            rendered_outputs: 0,
        }
    }

    pub fn rendered_outputs(&self) -> u64 {
        self.rendered_outputs
    }
}

impl Default for NotaProjection {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for NotaProjection {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}
