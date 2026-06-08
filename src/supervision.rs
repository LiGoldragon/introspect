//! The owner-only engine-management (supervision) relation, served by the
//! introspect daemon's meta listener tier.
//!
//! The kameo `SupervisionPhase` actor answers the announce / readiness / health
//! / stop relation. The emitted daemon shell owns the meta socket and accept
//! loop; `daemon::IntrospectionDaemon::handle_meta_connection` decodes each
//! `signal-engine-management` frame and drives this actor. The frame codec is
//! retained for the CLI / test client side.

use std::io::{Read, Write};

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_engine_management::{
    ComponentHealth, ComponentHealthReport, ComponentIdentity, ComponentKind, ComponentName,
    ComponentReady, EngineManagementProtocolVersion, Frame as SupervisionFrame, FrameBody,
    Operation as SupervisionRequest, Presence, Query as SupervisionQuery,
    Reply as SupervisionReply, StopAcknowledgement,
};
use signal_frame::{ExchangeIdentifier, Reply, SubReply};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionProfile {
    name: ComponentName,
    kind: ComponentKind,
    health: ComponentHealth,
}

impl SupervisionProfile {
    pub fn introspect() -> Self {
        Self {
            name: ComponentName::new("introspect"),
            kind: ComponentKind::Introspect,
            health: ComponentHealth::Running,
        }
    }
}

#[derive(Debug)]
pub struct SupervisionPhase {
    profile: SupervisionProfile,
    request_count: u64,
}

impl SupervisionPhase {
    fn new(profile: SupervisionProfile) -> Self {
        Self {
            profile,
            request_count: 0,
        }
    }

    /// Spawn the supervision actor synchronously — `on_start` is trivial, so the
    /// mailbox queues requests the instant the ref exists, and the daemon
    /// shell's sync `build_runtime` hook needs no nested `block_on`.
    pub fn spawn_phase(profile: SupervisionProfile) -> ActorRef<Self> {
        Self::spawn(Self::new(profile))
    }

    fn reply(&mut self, request: SupervisionRequest) -> SupervisionReply {
        self.request_count = self.request_count.saturating_add(1);
        match request {
            SupervisionRequest::Announce(Presence { .. }) => {
                SupervisionReply::Identified(ComponentIdentity {
                    name: self.profile.name.clone(),
                    kind: self.profile.kind,
                    engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
                    last_fatal_startup_error: None,
                })
            }
            SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(_)) => {
                SupervisionReply::Ready(ComponentReady {
                    component_started_at: None,
                })
            }
            SupervisionRequest::Query(SupervisionQuery::HealthStatus(_)) => {
                SupervisionReply::HealthReport(ComponentHealthReport {
                    health: self.profile.health,
                })
            }
            SupervisionRequest::Stop(_) => {
                SupervisionReply::StopAcknowledged(StopAcknowledgement {
                    drain_completed_at: None,
                })
            }
        }
    }
}

impl Actor for SupervisionPhase {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        phase: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(phase)
    }
}

#[derive(Debug, kameo::Reply)]
pub struct SupervisionPhaseReply {
    reply: SupervisionReply,
}

impl SupervisionPhaseReply {
    pub fn into_reply(self) -> SupervisionReply {
        self.reply
    }
}

#[derive(Debug)]
pub struct HandleSupervisionRequest {
    request: SupervisionRequest,
}

impl HandleSupervisionRequest {
    pub fn new(request: SupervisionRequest) -> Self {
        Self { request }
    }
}

impl Message<HandleSupervisionRequest> for SupervisionPhase {
    type Reply = SupervisionPhaseReply;

    async fn handle(
        &mut self,
        message: HandleSupervisionRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SupervisionPhaseReply {
            reply: self.reply(message.request),
        }
    }
}

/// The supervision frame codec retained for the CLI / test client side: the
/// daemon's own decode/encode lives inline in
/// `daemon::IntrospectionDaemon::handle_meta_connection`.
#[derive(Clone, Copy)]
pub struct SupervisionFrameCodec {
    maximum_frame_bytes: usize,
}

impl SupervisionFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> std::io::Result<SupervisionReply> {
        let frame = self.read_frame(reader)?;
        match frame.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => {
                    let (sub_reply, tail) = per_operation.into_head_and_tail();
                    if !tail.is_empty() {
                        return Err(Self::invalid_data(format!(
                            "expected one supervision reply operation, got {}",
                            tail.len() + 1
                        )));
                    }
                    match sub_reply {
                        SubReply::Ok(payload) => Ok(payload),
                        other => Err(Self::invalid_data(format!("{other:?}"))),
                    }
                }
                Reply::Rejected { reason } => Err(Self::invalid_data(reason)),
            },
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected supervision frame body: {other:?}"),
            )),
        }
    }

    pub fn write_request(
        &self,
        writer: &mut impl Write,
        exchange: ExchangeIdentifier,
        request: SupervisionRequest,
    ) -> std::io::Result<()> {
        let frame = SupervisionFrame::new(FrameBody::Request {
            exchange,
            request: signal_frame::Request::from_payload(request),
        });
        let bytes = frame.encode_length_prefixed().map_err(Self::invalid_data)?;
        writer.write_all(bytes.as_slice())?;
        writer.flush()
    }

    fn invalid_data(error: impl std::fmt::Display) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
    }

    fn read_frame(&self, reader: &mut impl Read) -> std::io::Result<SupervisionFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("supervision frame length {length} exceeds maximum"),
            ));
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        SupervisionFrame::decode_length_prefixed(bytes.as_slice()).map_err(Self::invalid_data)
    }
}
