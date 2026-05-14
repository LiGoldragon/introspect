use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use persona_introspect::{
    SupervisionFrameCodec,
    daemon::{IntrospectionDaemon, IntrospectionFrameCodec, IntrospectionSignalClient, SocketMode},
};
use signal_core::{FrameBody, Request, SignalVerb};
use signal_persona::{
    ComponentHealth, ComponentHealthQuery, ComponentHello, ComponentKind, ComponentName,
    ComponentReadinessQuery, SupervisionFrame, SupervisionProtocolVersion, SupervisionReply,
    SupervisionRequest,
};
use signal_persona_auth::EngineId;
use signal_persona_introspect::{
    ComponentReadiness, ComponentSnapshotQuery, CorrelationId, DeliveryTraceQuery,
    DeliveryTraceStatus, EngineSnapshotQuery, Frame as IntrospectionFrame, IntrospectionReply,
    IntrospectionRequest, IntrospectionTarget, PrototypeWitnessQuery,
};

fn serve_one(request: IntrospectionRequest) -> IntrospectionReply {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let bound = IntrospectionDaemon::from_socket(socket.clone())
        .with_socket_mode(SocketMode::from_octal(0o600))
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
    let frame = IntrospectionFrame::new(FrameBody::Request(Request::unchecked_operation(
        SignalVerb::Assert,
        IntrospectionRequest::EngineSnapshot(EngineSnapshotQuery {
            engine: EngineId::new("prototype"),
        }),
    )));
    let bytes = frame.encode_length_prefixed().expect("frame encodes");
    let mut input = bytes.as_slice();
    let error = IntrospectionFrameCodec::default()
        .read_request(&mut input)
        .expect_err("mismatched verb is rejected");

    assert!(error.to_string().contains("signal verb mismatch"));
}

#[test]
fn daemon_serves_prototype_witness_over_signal_socket() {
    let reply = serve_one(IntrospectionRequest::PrototypeWitness(
        PrototypeWitnessQuery {
            engine: EngineId::new("prototype"),
        },
    ));

    match reply {
        IntrospectionReply::PrototypeWitness(witness) => {
            assert_eq!(witness.engine, EngineId::new("prototype"));
            assert_eq!(witness.manager_seen, ComponentReadiness::Unknown);
            assert_eq!(witness.router_seen, ComponentReadiness::Unknown);
            assert_eq!(witness.terminal_seen, ComponentReadiness::Unknown);
            assert_eq!(witness.delivery_status, DeliveryTraceStatus::Unknown);
        }
        other => panic!("expected prototype witness, got {other:?}"),
    }
}

#[test]
fn daemon_answers_component_supervision_relation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let supervision_socket = directory.path().join("supervision.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_persona-introspect-daemon"))
        .arg(&socket)
        .env("PERSONA_SOCKET_MODE", "600")
        .env("PERSONA_SUPERVISION_SOCKET_PATH", &supervision_socket)
        .env("PERSONA_SUPERVISION_SOCKET_MODE", "600")
        .spawn()
        .expect("persona-introspect-daemon starts");

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
        SupervisionRequest::ComponentHello(ComponentHello {
            expected_component: ComponentName::new("persona-introspect"),
            expected_kind: ComponentKind::Introspect,
            supervision_protocol_version: SupervisionProtocolVersion::new(1),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("identity reply"),
        SupervisionReply::ComponentIdentity(identity)
            if identity.name.as_str() == "persona-introspect"
                && identity.kind == ComponentKind::Introspect
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentReadinessQuery(ComponentReadinessQuery {
            component: ComponentName::new("persona-introspect"),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::ComponentReady(_)
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentHealthQuery(ComponentHealthQuery {
            component: ComponentName::new("persona-introspect"),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("health reply"),
        SupervisionReply::ComponentHealthReport(report)
            if report.health == ComponentHealth::Running
    ));

    stop_child(&mut child);
}

#[test]
fn daemon_serves_scaffold_observation_replies_for_all_request_families() {
    let engine = EngineId::new("prototype");

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
            engine: EngineId::new("prototype"),
            target: IntrospectionTarget::Router,
        },
    ));
    match component_reply {
        IntrospectionReply::ComponentSnapshot(snapshot) => {
            assert_eq!(snapshot.target, IntrospectionTarget::Router);
            assert_eq!(snapshot.readiness, ComponentReadiness::Unknown);
        }
        other => panic!("expected component snapshot, got {other:?}"),
    }

    let delivery_reply = serve_one(IntrospectionRequest::DeliveryTrace(DeliveryTraceQuery {
        engine: EngineId::new("prototype"),
        correlation: CorrelationId::new("delivery-aab"),
    }));
    match delivery_reply {
        IntrospectionReply::DeliveryTrace(trace) => {
            assert_eq!(trace.correlation, CorrelationId::new("delivery-aab"));
            assert_eq!(trace.status, DeliveryTraceStatus::Unknown);
        }
        other => panic!("expected delivery trace, got {other:?}"),
    }
}

fn write_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(FrameBody::Request(Request::from_payload(request)));
    let bytes = frame
        .encode_length_prefixed()
        .expect("supervision request encodes");
    stream
        .write_all(bytes.as_slice())
        .expect("supervision request writes");
    stream.flush().expect("supervision request flushes");
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
