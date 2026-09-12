//! The store's actor surface: one message per durable operation.
//!
//! The store is a kameo actor so that its `&mut` operations — coalescer
//! ingestion above all — are serialised by the mailbox rather than by a lock
//! shared between actors.

use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible};
use kameo::message::{Context, Message};
use signal_introspect::{
    BootIdentifier, CoalescingClosure, ComponentTrace, ComponentTraceEvent, ComponentTraceQuery,
    DeliveryTraceObservation, DeliveryTraceObservationEvent, DeliveryTraceObservationQuery,
    SystemEvent, SystemEventAccepted, SystemEvents, SystemEventsFlushed, SystemEventsQuery,
};

use crate::Result;
use crate::store::IntrospectionStore;
use crate::store_record::{ObservationReceipt, StoredObservation};

impl Actor for IntrospectionStore {
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
        self.flush_on_shutdown();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct RecordDeliveryTraceEvent {
    event: DeliveryTraceObservationEvent,
}

impl RecordDeliveryTraceEvent {
    pub fn new(event: DeliveryTraceObservationEvent) -> Self {
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

#[derive(Debug, Clone, PartialEq)]
pub struct ReadDeliveryTrace {
    query: DeliveryTraceObservationQuery,
}

impl ReadDeliveryTrace {
    pub fn new(query: DeliveryTraceObservationQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadDeliveryTrace> for IntrospectionStore {
    type Reply = Result<DeliveryTraceObservation>;

    async fn handle(
        &mut self,
        message: ReadDeliveryTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.delivery_trace(message.query)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordComponentTraceEvent {
    event: ComponentTraceEvent,
}

impl RecordComponentTraceEvent {
    pub fn new(event: ComponentTraceEvent) -> Self {
        Self { event }
    }
}

impl Message<RecordComponentTraceEvent> for IntrospectionStore {
    type Reply = Result<ObservationReceipt>;

    async fn handle(
        &mut self,
        message: RecordComponentTraceEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_component_trace_event(message.event)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadComponentTrace {
    query: ComponentTraceQuery,
}

impl ReadComponentTrace {
    pub fn new(query: ComponentTraceQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadComponentTrace> for IntrospectionStore {
    type Reply = Result<ComponentTrace>;

    async fn handle(
        &mut self,
        message: ReadComponentTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.component_trace(message.query)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordTargetedSystemEvent {
    event: SystemEvent,
}

impl RecordTargetedSystemEvent {
    pub fn new(event: SystemEvent) -> Self {
        Self { event }
    }
}

impl Message<RecordTargetedSystemEvent> for IntrospectionStore {
    type Reply = Result<SystemEventAccepted>;

    async fn handle(
        &mut self,
        message: RecordTargetedSystemEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_system_event(message.event)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadSystemEvents {
    query: SystemEventsQuery,
}

impl ReadSystemEvents {
    pub fn new(query: SystemEventsQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadSystemEvents> for IntrospectionStore {
    type Reply = Result<SystemEvents>;

    async fn handle(
        &mut self,
        message: ReadSystemEvents,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.system_events(message.query)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlushTargetedSystemEvents {
    boot: BootIdentifier,
}

impl FlushTargetedSystemEvents {
    pub fn new(boot: BootIdentifier) -> Self {
        Self { boot }
    }
}

impl Message<FlushTargetedSystemEvents> for IntrospectionStore {
    type Reply = Result<SystemEventsFlushed>;

    async fn handle(
        &mut self,
        message: FlushTargetedSystemEvents,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.flush_system_events(message.boot, CoalescingClosure::ExplicitFlush)
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
