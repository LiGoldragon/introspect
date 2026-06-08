//! End-to-end witnesses for the introspect daemon on the schema-emitted
//! component-decoded daemon shell.
//!
//! The hand-written `UnixListener` accept loop is gone; every test drives the
//! real `introspect-daemon` binary (argv = one binary rkyv config file), then
//! talks the working `IntrospectionFrame` contract over the introspection-query
//! socket or the supervision contract over the meta socket.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use introspect::IntrospectionDaemonConfiguration;
use introspect::SupervisionFrameCodec;
use introspect::daemon::{IntrospectionDaemon, IntrospectionSignalClient};
use introspect::schema::daemon::ComponentDaemon;
use signal_engine_management::{
    ComponentHealth, ComponentKind, ComponentName, EngineManagementProtocolVersion,
    Operation as SupervisionRequest, Presence, Query as SupervisionQuery,
    Reply as SupervisionReply,
};
use signal_engine_management::{SocketMode as WireSocketMode, WirePath};
use signal_frame::{
    ExchangeIdentifier as FrameExchangeIdentifier, ExchangeLane as FrameExchangeLane,
    LaneSequence as FrameLaneSequence, SessionEpoch as FrameSessionEpoch,
};
use signal_introspect::{
    ComponentSnapshotQuery, DeliveryTraceQuery, EngineSnapshotQuery, IntrospectDaemonConfiguration,
    IntrospectionReply, IntrospectionRequest, IntrospectionTarget, MessageIdentifier,
    PrototypeWitnessQuery,
};
use signal_persona_origin::{ComponentName as AuthComponentName, EngineIdentifier};
use signal_persona_origin::{OwnerIdentity, UnixUserIdentifier};

/// A running `introspect-daemon` child with both socket paths, torn down on
/// drop.
struct DaemonProcess {
    child: Child,
    introspect_socket: std::path::PathBuf,
    supervision_socket: std::path::PathBuf,
    _directory: tempfile::TempDir,
}

impl DaemonProcess {
    fn spawn() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let introspect_socket = directory.path().join("introspect.sock");
        let supervision_socket = directory.path().join("supervision.sock");
        let configuration_path = directory.path().join("introspect-daemon.rkyv");
        let configuration =
            daemon_configuration(directory.path(), &introspect_socket, &supervision_socket);
        write_configuration(&configuration_path, &configuration);

        let child = Command::new(env!("CARGO_BIN_EXE_introspect-daemon"))
            .arg(&configuration_path)
            .spawn()
            .expect("introspect-daemon starts");

        wait_for_socket(&introspect_socket);
        Self {
            child,
            introspect_socket,
            supervision_socket,
            _directory: directory,
        }
    }

    fn submit(&self, request: IntrospectionRequest) -> IntrospectionReply {
        IntrospectionSignalClient::new(self.introspect_socket.clone())
            .submit(request)
            .expect("client receives reply")
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn daemon_applies_configured_socket_mode() {
    let daemon = DaemonProcess::spawn();
    let mode = std::fs::metadata(&daemon.introspect_socket)
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

#[test]
fn daemon_serves_prototype_witness_over_signal_socket() {
    let daemon = DaemonProcess::spawn();
    let reply = daemon.submit(IntrospectionRequest::PrototypeWitness(
        PrototypeWitnessQuery {
            engine: EngineIdentifier::new("prototype"),
        },
    ));

    match reply {
        IntrospectionReply::PrototypeWitness(witness) => {
            assert_eq!(witness.engine, EngineIdentifier::new("prototype"));
            assert_eq!(witness.manager_seen, None);
            assert_eq!(witness.router_seen, None);
            assert_eq!(witness.terminal_seen, None);
            assert_eq!(witness.delivery_status, None);
        }
        other => panic!("expected prototype witness, got {other:?}"),
    }
}

#[test]
fn daemon_configuration_accepts_binary_file_argument() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let supervision_socket = directory.path().join("supervision.sock");
    let configuration_path = directory.path().join("introspect-daemon.rkyv");
    let configuration = daemon_configuration(directory.path(), &socket, &supervision_socket);

    write_configuration(&configuration_path, &configuration);

    let decoded = IntrospectionDaemon::load_configuration(&configuration_path)
        .expect("read binary introspect config")
        .into_inner();

    assert_eq!(decoded, configuration);
}

#[test]
fn daemon_serves_scaffold_observation_replies_for_all_request_families() {
    let daemon = DaemonProcess::spawn();
    let engine = EngineIdentifier::new("prototype");

    let engine_reply = daemon.submit(IntrospectionRequest::EngineSnapshot(EngineSnapshotQuery {
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

    let component_reply = daemon.submit(IntrospectionRequest::ComponentSnapshot(
        ComponentSnapshotQuery {
            engine: EngineIdentifier::new("prototype"),
            target: IntrospectionTarget::Router,
        },
    ));
    match component_reply {
        IntrospectionReply::ComponentSnapshot(snapshot) => {
            assert_eq!(snapshot.target, IntrospectionTarget::Router);
            assert_eq!(snapshot.readiness, None);
        }
        other => panic!("expected component snapshot, got {other:?}"),
    }

    let delivery_reply = daemon.submit(IntrospectionRequest::DeliveryTrace(DeliveryTraceQuery {
        engine: EngineIdentifier::new("prototype"),
        message_identifier: MessageIdentifier::new(7),
        originator: AuthComponentName::Message,
    }));
    match delivery_reply {
        IntrospectionReply::DeliveryTrace(trace) => {
            assert_eq!(trace.message_identifier, MessageIdentifier::new(7));
            assert_eq!(trace.originator, AuthComponentName::Message);
            assert!(trace.events.is_empty());
        }
        other => panic!("expected delivery trace, got {other:?}"),
    }
}

#[test]
fn daemon_answers_component_supervision_relation() {
    let daemon = DaemonProcess::spawn();
    wait_for_socket(&daemon.supervision_socket);
    let mode = std::fs::metadata(&daemon.supervision_socket)
        .expect("supervision socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = UnixStream::connect(&daemon.supervision_socket).expect("client connects");
    let codec = SupervisionFrameCodec::new(1024 * 1024);

    codec
        .write_request(
            &mut stream,
            test_frame_exchange(),
            SupervisionRequest::Announce(Presence {
                expected_component: ComponentName::new("introspect"),
                expected_kind: ComponentKind::Introspect,
                engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
            }),
        )
        .expect("announce writes");
    assert!(matches!(
        codec.read_reply(&mut stream).expect("identity reply"),
        SupervisionReply::Identified(identity)
            if identity.name.as_str() == "introspect"
                && identity.kind == ComponentKind::Introspect
    ));

    codec
        .write_request(
            &mut stream,
            test_frame_exchange(),
            SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(ComponentName::new(
                "introspect",
            ))),
        )
        .expect("readiness writes");
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::Ready(_)
    ));

    codec
        .write_request(
            &mut stream,
            test_frame_exchange(),
            SupervisionRequest::Query(SupervisionQuery::HealthStatus(ComponentName::new(
                "introspect",
            ))),
        )
        .expect("health writes");
    assert!(matches!(
        codec.read_reply(&mut stream).expect("health reply"),
        SupervisionReply::HealthReport(report)
            if report.health == ComponentHealth::Running
    ));
}

fn write_configuration(path: &Path, configuration: &IntrospectDaemonConfiguration) {
    let bytes = configuration
        .to_rkyv_bytes()
        .expect("introspect config rkyv encodes");
    std::fs::write(path, bytes.as_slice()).expect("write binary introspect config");
    // Witness the round-trip the daemon performs at startup.
    let _ = IntrospectionDaemonConfiguration::new(configuration.clone());
}

fn test_frame_exchange() -> FrameExchangeIdentifier {
    FrameExchangeIdentifier::new(
        FrameSessionEpoch::new(1),
        FrameExchangeLane::Connector,
        FrameLaneSequence::new(1),
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

fn daemon_configuration(
    directory: &Path,
    socket: &Path,
    supervision_socket: &Path,
) -> IntrospectDaemonConfiguration {
    IntrospectDaemonConfiguration {
        introspect_socket_path: WirePath::new(socket.display().to_string()),
        introspect_socket_mode: WireSocketMode::new(0o600),
        supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
        supervision_socket_mode: WireSocketMode::new(0o600),
        store_path: WirePath::new(directory.join("introspect.sema").display().to_string()),
        // The prototype daemon has no live peers in this harness; an empty wire
        // path means "no peer configured", so the witness reports each peer as
        // unseen rather than failing on an unreachable socket.
        manager_socket_path: WirePath::new(String::new()),
        router_socket_path: WirePath::new(String::new()),
        terminal_socket_path: WirePath::new(String::new()),
        owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
    }
}
