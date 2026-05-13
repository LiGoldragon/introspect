use std::path::{Path, PathBuf};

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_persona_introspect::{
    ComponentReadiness, DeliveryTraceStatus, IntrospectionReply, PrototypeWitness,
    PrototypeWitnessQuery,
};

use crate::error::Result;

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
    projection: ActorRef<NotaProjection>,
    handled_queries: u64,
}

impl IntrospectionRoot {
    pub async fn start_root(input: IntrospectionRootInput) -> ActorRef<Self> {
        let target_directory = TargetDirectory::spawn(TargetDirectory::new(input.targets.clone()));
        let query_planner = QueryPlanner::spawn(QueryPlanner::new());
        let manager_client =
            ManagerClient::spawn(ManagerClient::new(input.targets.manager_socket.clone()));
        let router_client = RouterClient::spawn(RouterClient::new(input.targets.router_socket));
        let terminal_client =
            TerminalClient::spawn(TerminalClient::new(input.targets.terminal_socket));
        let projection = NotaProjection::spawn(NotaProjection::new());
        Self::spawn(Self {
            target_directory,
            query_planner,
            manager_client,
            router_client,
            terminal_client,
            projection,
            handled_queries: 0,
        })
    }

    fn prototype_witness(&mut self, query: PrototypeWitnessQuery) -> IntrospectionReply {
        self.handled_queries = self.handled_queries.saturating_add(1);
        let _ = (
            &self.target_directory,
            &self.query_planner,
            &self.manager_client,
            &self.router_client,
            &self.terminal_client,
            &self.projection,
        );
        IntrospectionReply::PrototypeWitness(PrototypeWitness {
            engine: query.engine,
            manager_seen: ComponentReadiness::Unknown,
            router_seen: ComponentReadiness::Unknown,
            terminal_seen: ComponentReadiness::Unknown,
            delivery_status: DeliveryTraceStatus::Unknown,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionRootInput {
    pub targets: TargetSocketDirectory,
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
        Ok(self.prototype_witness(message.query))
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
