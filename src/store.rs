use std::path::{Path, PathBuf};

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, CommitLogEntry, Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, SnapshotId,
    TableDescriptor, TableName, TableReference,
};
use signal_persona_introspect::{IntrospectionReply, IntrospectionRequest};

use crate::Result;

const INTROSPECTION_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const OBSERVATIONS: TableName = TableName::new("introspection_observations");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    path: PathBuf,
}

impl StoreLocation {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

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
}

impl IntrospectionStore {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(
            store.as_path(),
            INTROSPECTION_SCHEMA_VERSION,
        ))?;
        let observations = engine.register_table(TableDescriptor::new(OBSERVATIONS))?;
        Ok(Self {
            engine,
            observations,
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
