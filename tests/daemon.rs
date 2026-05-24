use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use introspect::{
    SupervisionFrameCodec,
    daemon::{IntrospectionDaemon, IntrospectionFrameCodec, IntrospectionSignalClient, SocketMode},
    store::StoreLocation,
};
use nota_codec::{Encoder, NotaEncode};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Operation, Request,
    RequestRejectionReason, SessionEpoch, SignalVerb,
};
use signal_frame::{
    ExchangeIdentifier as FrameExchangeIdentifier, ExchangeLane as FrameExchangeLane,
    LaneSequence as FrameLaneSequence, Request as FrameRequest, SessionEpoch as FrameSessionEpoch,
};
use signal_introspect::{
    ComponentSnapshotQuery, DeliveryTraceQuery, EngineSnapshotQuery, IntrospectDaemonConfiguration,
    IntrospectionFrame, IntrospectionFrameBody as FrameBody, IntrospectionReply,
    IntrospectionRequest, IntrospectionTarget, MessageIdentifier, PrototypeWitnessQuery,
};
use signal_persona::engine_management::{
    Frame as SupervisionFrame, FrameBody as SupervisionFrameBody, Operation as SupervisionRequest,
    Query as SupervisionQuery, Reply as SupervisionReply,
};
use signal_persona::{
    ComponentHealth, ComponentKind, ComponentName, EngineManagementProtocolVersion, Presence,
};
use signal_persona_origin::{ComponentName as AuthComponentName, EngineIdentifier};
use signal_persona_origin::{OwnerIdentity, UnixUserId};

fn serve_one(request: IntrospectionRequest) -> IntrospectionReply {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let bound = IntrospectionDaemon::from_socket(socket.clone())
        .with_socket_mode(SocketMode::from_octal(0o600))
        .with_store(StoreLocation::new(directory.path().join("introspect.redb")))
        .bind()
        .expect("daemon binds");
    assert_eq!(
        std::fs::metadata(bound.socket())
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let server = thread::spawn(move || bound.serve_one().expect("serve one"));
    let reply = IntrospectionSignalClient::new(socket)
        .submit(request)
        .expect("client receives reply");
    let served = server.join().expect("server joins");
    assert_eq!(served, reply);
    reply
}

#[test]
fn daemon_applies_spawn_envelope_socket_mode() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let bound = IntrospectionDaemon::from_socket(socket)
        .with_socket_mode(SocketMode::from_octal(0o600))
        .with_store(StoreLocation::new(directory.path().join("introspect.redb")))
        .bind()
        .expect("daemon binds");

    let mode = std::fs::metadata(bound.socket())
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o600);
}

#[test]
fn introspection_frame_codec_rejects_mismatched_signal_verb() {
    let request = Request::from_operations(NonEmpty::single(Operation::new(
        SignalVerb::Assert,
        IntrospectionRequest::EngineSnapshot(EngineSnapshotQuery {
            engine: EngineIdentifier::new("prototype"),
        }),
    )));
    let frame = IntrospectionFrame::new(FrameBody::Request {
        exchange: test_exchange(),
        request,
    });
    let bytes = frame.encode_length_prefixed().expect("frame encodes");
    let mut input = bytes.as_slice();
    let error = IntrospectionFrameCodec::default()
        .read_request(&mut input)
        .expect_err("mismatched verb is rejected");

    assert!(matches!(
        error,
        introspect::Error::UnexpectedSignalFrame { got }
            if got == RequestRejectionReason::VerbPayloadMismatch { index: 0 }.to_string()
    ));
}

#[test]
fn daemon_serves_prototype_witness_over_signal_socket() {
    let reply = serve_one(IntrospectionRequest::PrototypeWitness(
        PrototypeWitnessQuery {
            engine: EngineIdentifier::new("prototype"),
        },
    ));

    match reply {
        IntrospectionReply::PrototypeWitness(witness) => {
            assert_eq!(witness.engine, EngineIdentifier::new("prototype"));
            // Daemon skeleton has not yet collected peer observations;
            // every field is None per the closed-enum contract.
            assert_eq!(witness.manager_seen, None);
            assert_eq!(witness.router_seen, None);
            assert_eq!(witness.terminal_seen, None);
            assert_eq!(witness.delivery_status, None);
        }
        other => panic!("expected prototype witness, got {other:?}"),
    }
}

#[test]
fn daemon_answers_component_supervision_relation() {
    use signal_persona::{SocketMode as WireSocketMode, WirePath};
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let supervision_socket = directory.path().join("supervision.sock");
    let store_path = directory.path().join("introspect.redb");
    let configuration_path = directory.path().join("introspect-daemon.nota");

    let configuration = IntrospectDaemonConfiguration {
        introspect_socket_path: WirePath::new(socket.display().to_string()),
        introspect_socket_mode: WireSocketMode::new(0o600),
        supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
        supervision_socket_mode: WireSocketMode::new(0o600),
        store_path: WirePath::new(store_path.display().to_string()),
        manager_socket_path: WirePath::new(
            directory.path().join("persona.sock").display().to_string(),
        ),
        router_socket_path: WirePath::new(
            directory.path().join("router.sock").display().to_string(),
        ),
        terminal_socket_path: WirePath::new(
            directory.path().join("terminal.sock").display().to_string(),
        ),
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(1000)),
    };
    let mut encoder = Encoder::new();
    configuration
        .encode(&mut encoder)
        .expect("encode introspect config");
    let mut text = encoder.into_string();
    text.push('\n');
    std::fs::write(&configuration_path, text).expect("write config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_introspect-daemon"))
        .arg(&configuration_path)
        .spawn()
        .expect("introspect-daemon starts");

    wait_for_socket(&supervision_socket);
    let mode = std::fs::metadata(&supervision_socket)
        .expect("supervision socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = UnixStream::connect(&supervision_socket).expect("client connects");
    let codec = SupervisionFrameCodec::new(1024 * 1024);

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Announce(Presence {
            expected_component: ComponentName::new("introspect"),
            expected_kind: ComponentKind::Introspect,
            engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("identity reply"),
        SupervisionReply::Identified(identity)
            if identity.name.as_str() == "introspect"
                && identity.kind == ComponentKind::Introspect
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(ComponentName::new(
            "introspect",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::Ready(_)
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::HealthStatus(ComponentName::new(
            "introspect",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("health reply"),
        SupervisionReply::HealthReport(report)
            if report.health == ComponentHealth::Running
    ));

    stop_child(&mut child);
}

#[test]
fn daemon_serves_scaffold_observation_replies_for_all_request_families() {
    let engine = EngineIdentifier::new("prototype");

    let engine_reply = serve_one(IntrospectionRequest::EngineSnapshot(EngineSnapshotQuery {
        engine: engine.clone(),
    }));
    match engine_reply {
        IntrospectionReply::EngineSnapshot(snapshot) => {
            assert_eq!(snapshot.engine, engine);
            assert!(
                snapshot
                    .observed_components
                    .contains(&IntrospectionTarget::EngineManager)
            );
            assert!(
                snapshot
                    .observed_components
                    .contains(&IntrospectionTarget::Router)
            );
            assert!(
                snapshot
                    .observed_components
                    .contains(&IntrospectionTarget::Terminal)
            );
        }
        other => panic!("expected engine snapshot, got {other:?}"),
    }

    let component_reply = serve_one(IntrospectionRequest::ComponentSnapshot(
        ComponentSnapshotQuery {
            engine: EngineIdentifier::new("prototype"),
            target: IntrospectionTarget::Router,
        },
    ));
    match component_reply {
        IntrospectionReply::ComponentSnapshot(snapshot) => {
            assert_eq!(snapshot.target, IntrospectionTarget::Router);
            // No peer observation yet → readiness is None on the carrier
            // record; the inner ComponentReadiness enum stays closed.
            assert_eq!(snapshot.readiness, None);
        }
        other => panic!("expected component snapshot, got {other:?}"),
    }

    let delivery_reply = serve_one(IntrospectionRequest::DeliveryTrace(DeliveryTraceQuery {
        engine: EngineIdentifier::new("prototype"),
        message_identifier: MessageIdentifier::new(7),
        originator: AuthComponentName::Message,
    }));
    match delivery_reply {
        IntrospectionReply::DeliveryTrace(trace) => {
            assert_eq!(trace.message_identifier, MessageIdentifier::new(7));
            assert_eq!(trace.originator, AuthComponentName::Message);
            // No Tap trace observed yet → the carrier's event vector is
            // empty; each present event carries a closed status.
            assert!(trace.events.is_empty());
        }
        other => panic!("expected delivery trace, got {other:?}"),
    }
}

fn write_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(SupervisionFrameBody::Request {
        exchange: test_frame_exchange(),
        request: FrameRequest::from_payload(request),
    });
    let bytes = frame
        .encode_length_prefixed()
        .expect("supervision request encodes");
    stream
        .write_all(bytes.as_slice())
        .expect("supervision request writes");
    stream.flush().expect("supervision request flushes");
}

fn test_frame_exchange() -> FrameExchangeIdentifier {
    FrameExchangeIdentifier::new(
        FrameSessionEpoch::new(1),
        FrameExchangeLane::Connector,
        FrameLaneSequence::new(1),
    )
}

fn test_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::new(1),
    )
}

fn wait_for_socket(socket: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("socket was not created: {}", socket.display());
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}
