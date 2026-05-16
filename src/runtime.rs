use std::path::{Path, PathBuf};

use kameo::actor::{Actor, ActorRef, Spawn, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible, SendError};
use kameo::message::{Context, Message};
use signal_persona_introspect::{
    ComponentSnapshot, DeliveryTrace, EngineSnapshot, IntrospectionReply, IntrospectionRequest,
    IntrospectionTarget, PrototypeWitness, PrototypeWitnessQuery,
};

use crate::error::{Error, Result};
use crate::store::{
    IntrospectionStore, ObservationSequence, RecordObservation, StoreLocation, StoredObservation,
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

    pub fn from_environment() -> Self {
        let mut directory = Self {
            manager_socket: std::env::var_os("PERSONA_MANAGER_SOCKET_PATH").map(PathBuf::from),
            router_socket: None,
            terminal_socket: None,
        };
        let count = std::env::var("PERSONA_PEER_SOCKET_COUNT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        for index in 0..count {
            let Some(component) = std::env::var_os(format!("PERSONA_PEER_{index}_COMPONENT"))
            else {
                continue;
            };
            let Some(socket) = std::env::var_os(format!("PERSONA_PEER_{index}_SOCKET_PATH")) else {
                continue;
            };
            match component.to_string_lossy().as_ref() {
                "router" | "persona-router" => {
                    directory.router_socket = Some(PathBuf::from(socket))
                }
                "terminal" | "persona-terminal" => {
                    directory.terminal_socket = Some(PathBuf::from(socket))
                }
                _ => {}
            }
        }
        directory
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

    fn prototype_witness(&mut self, query: PrototypeWitnessQuery) -> IntrospectionReply {
        self.handled_queries = self.handled_queries.saturating_add(1);
        // Skeleton: the introspect daemon has not yet collected
        // observations from its peers; every field is None per the
        // closed-enum contract. Once peer queries land, these become
        // Some(state) carrying the observed closed-enum variant.
        IntrospectionReply::PrototypeWitness(PrototypeWitness {
            engine: query.engine,
            manager_seen: None,
            router_seen: None,
            terminal_seen: None,
            delivery_status: None,
        })
    }

    fn handle_request(&mut self, request: IntrospectionRequest) -> IntrospectionReply {
        match request {
            IntrospectionRequest::EngineSnapshot(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                IntrospectionReply::EngineSnapshot(EngineSnapshot {
                    engine: query.engine,
                    observed_components: vec![
                        IntrospectionTarget::EngineManager,
                        IntrospectionTarget::Router,
                        IntrospectionTarget::Terminal,
                    ],
                })
            }
            IntrospectionRequest::ComponentSnapshot(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                IntrospectionReply::ComponentSnapshot(ComponentSnapshot {
                    engine: query.engine,
                    target: query.target,
                    readiness: None,
                })
            }
            IntrospectionRequest::DeliveryTrace(query) => {
                self.handled_queries = self.handled_queries.saturating_add(1);
                IntrospectionReply::DeliveryTrace(DeliveryTrace {
                    engine: query.engine,
                    correlation: query.correlation,
                    status: None,
                })
            }
            IntrospectionRequest::PrototypeWitness(query) => self.prototype_witness(query),
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
        let reply = self.handle_request(request.clone());
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
        let reply = self.handle_request(request.clone());
        self.record_observation(request, reply.clone()).await?;
        Ok(reply)
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
