use std::path::{Path, PathBuf};

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, CommitLogEntry, Engine, EngineOpen, EngineRecord, KeyRange, QueryPlan, RecordKey,
    SnapshotId, TableDescriptor, TableName, TableReference,
};
use signal_persona_auth::ComponentName;
use signal_persona_introspect::{
    DeliveryTrace, DeliveryTraceEvent, DeliveryTraceJoinKey, DeliveryTraceQuery,
    IntrospectionReply, IntrospectionRequest,
};

use crate::Result;

const INTROSPECTION_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);
const OBSERVATIONS: TableName = TableName::new("introspection_observations");
const DELIVERY_TRACE_EVENTS: TableName = TableName::new("delivery_trace_events");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    path: PathBuf,
}

impl StoreLocation {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// CLI convenience — the `introspect` CLI (which may run an
    /// in-process root for local-only queries) may discover the
    /// introspection store via `PERSONA_INTROSPECT_STORE` or
    /// `PERSONA_STATE_PATH` as a last-resort fallback. **Not for the
    /// daemon's production launch path** — the daemon opens the store
    /// path supplied by `IntrospectDaemonConfiguration.store_path`.
    pub fn from_environment() -> Self {
        match std::env::var_os("PERSONA_INTROSPECT_STORE") {
            Some(path) => Self::new(path),
            None => match std::env::var_os("PERSONA_STATE_PATH") {
                Some(path) => Self::new(path),
                None => Self::new("/tmp/persona-introspect.redb"),
            },
        }
    }

    pub fn as_path(&self) -> &Path {
        self.path.as_path()
    }
}

pub struct IntrospectionStore {
    engine: Engine,
    observations: TableReference<StoredObservation>,
    delivery_trace_events: TableReference<StoredDeliveryTraceEvent>,
}

impl IntrospectionStore {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(
            store.as_path(),
            INTROSPECTION_SCHEMA_VERSION,
        ))?;
        let observations = engine.register_table(TableDescriptor::new(OBSERVATIONS))?;
        let delivery_trace_events =
            engine.register_table(TableDescriptor::new(DELIVERY_TRACE_EVENTS))?;
        Ok(Self {
            engine,
            observations,
            delivery_trace_events,
        })
    }

    pub fn record_observation(&self, observation: StoredObservation) -> Result<ObservationReceipt> {
        let receipt = self
            .engine
            .assert(Assertion::new(self.observations, observation.clone()))?;
        Ok(ObservationReceipt::new(
            observation.sequence(),
            receipt.snapshot(),
        ))
    }

    pub fn observations(&self) -> Result<Vec<StoredObservation>> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.observations))?
            .records()
            .to_vec())
    }

    pub fn record_delivery_trace_event(
        &self,
        event: DeliveryTraceEvent,
    ) -> Result<ObservationReceipt> {
        let stored_event = StoredDeliveryTraceEvent::new(event.clone());
        let receipt = self
            .engine
            .assert(Assertion::new(self.delivery_trace_events, stored_event))?;
        Ok(ObservationReceipt::new(
            ObservationSequence::new(event.key().hop_index.value() as u64),
            receipt.snapshot(),
        ))
    }

    pub fn delivery_trace(&self, query: DeliveryTraceQuery) -> Result<DeliveryTrace> {
        let mut events = self
            .engine
            .match_records(QueryPlan::key_range(
                self.delivery_trace_events,
                delivery_trace_query_range(&query),
            ))?
            .records()
            .iter()
            .filter(|stored_event| stored_event.event().key().matches_query(&query))
            .map(|stored_event| stored_event.event().clone())
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.key().hop_index);
        Ok(DeliveryTrace {
            engine: query.engine,
            message_identifier: query.message_identifier,
            originator: query.originator,
            events,
        })
    }

    pub fn operation_log(&self) -> Result<Vec<CommitLogEntry>> {
        Ok(self.engine.commit_log()?)
    }
}

impl Actor for IntrospectionStore {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordObservation {
    observation: StoredObservation,
}

impl RecordObservation {
    pub fn new(observation: StoredObservation) -> Self {
        Self { observation }
    }
}

impl Message<RecordObservation> for IntrospectionStore {
    type Reply = Result<ObservationReceipt>;

    async fn handle(
        &mut self,
        message: RecordObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_observation(message.observation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDeliveryTraceEvent {
    event: DeliveryTraceEvent,
}

impl RecordDeliveryTraceEvent {
    pub fn new(event: DeliveryTraceEvent) -> Self {
        Self { event }
    }
}

impl Message<RecordDeliveryTraceEvent> for IntrospectionStore {
    type Reply = Result<ObservationReceipt>;

    async fn handle(
        &mut self,
        message: RecordDeliveryTraceEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_delivery_trace_event(message.event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDeliveryTrace {
    query: DeliveryTraceQuery,
}

impl ReadDeliveryTrace {
    pub fn new(query: DeliveryTraceQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadDeliveryTrace> for IntrospectionStore {
    type Reply = Result<DeliveryTrace>;

    async fn handle(
        &mut self,
        message: ReadDeliveryTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.delivery_trace(message.query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadObservations;

impl Message<ReadObservations> for IntrospectionStore {
    type Reply = Result<Vec<StoredObservation>>;

    async fn handle(
        &mut self,
        _message: ReadObservations,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observations()
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[rkyv(derive(Debug))]
pub struct ObservationSequence(u64);

impl ObservationSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationReceipt {
    sequence: ObservationSequence,
    snapshot: SnapshotId,
}

impl ObservationReceipt {
    pub fn new(sequence: ObservationSequence, snapshot: SnapshotId) -> Self {
        Self { sequence, snapshot }
    }

    pub fn sequence(&self) -> ObservationSequence {
        self.sequence
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredObservation {
    sequence: ObservationSequence,
    request: IntrospectionRequest,
    reply: IntrospectionReply,
}

impl StoredObservation {
    pub fn new(
        sequence: ObservationSequence,
        request: IntrospectionRequest,
        reply: IntrospectionReply,
    ) -> Self {
        Self {
            sequence,
            request,
            reply,
        }
    }

    pub fn sequence(&self) -> ObservationSequence {
        self.sequence
    }

    pub fn request(&self) -> &IntrospectionRequest {
        &self.request
    }

    pub fn reply(&self) -> &IntrospectionReply {
        &self.reply
    }
}

impl EngineRecord for StoredObservation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.sequence.value().to_string())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDeliveryTraceEvent {
    event: DeliveryTraceEvent,
}

impl StoredDeliveryTraceEvent {
    pub fn new(event: DeliveryTraceEvent) -> Self {
        Self { event }
    }

    pub fn event(&self) -> &DeliveryTraceEvent {
        &self.event
    }
}

impl EngineRecord for StoredDeliveryTraceEvent {
    fn record_key(&self) -> RecordKey {
        delivery_trace_event_key(&self.event)
    }
}

fn delivery_trace_event_key(event: &DeliveryTraceEvent) -> RecordKey {
    let key = event.key();
    let join_key = delivery_trace_join_key_prefix(&key.join_key());
    RecordKey::new(format!("{}/{:010}", join_key, key.hop_index.value()))
}

fn delivery_trace_query_range(query: &DeliveryTraceQuery) -> KeyRange {
    let prefix = delivery_trace_join_key_prefix(&query.join_key());
    KeyRange::between(
        RecordKey::new(format!("{prefix}/")),
        RecordKey::new(format!("{prefix}/~")),
    )
}

fn delivery_trace_join_key_prefix(key: &DeliveryTraceJoinKey) -> String {
    format!(
        "{}/{}/{}",
        key.engine.as_str(),
        key.message_identifier.into_u64(),
        component_name(key.originator)
    )
}

fn component_name(component: ComponentName) -> &'static str {
    match component {
        ComponentName::Mind => "Mind",
        ComponentName::Message => "Message",
        ComponentName::Router => "Router",
        ComponentName::Terminal => "Terminal",
        ComponentName::Harness => "Harness",
        ComponentName::System => "System",
        ComponentName::Introspect => "Introspect",
        ComponentName::Orchestrate => "Orchestrate",
    }
}
