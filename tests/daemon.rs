//! End-to-end witnesses for the introspect daemon.
//!
//! Every test drives the real `introspect-daemon` binary (argv = one binary
//! rkyv config file), then talks the `signal-introspect` Signal contract over
//! the introspection-query socket or the `meta-signal-introspect` Signal
//! contract over the owner-only meta socket.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use introspect::IntrospectionDaemonConfiguration;
use introspect::daemon::{IntrospectionDaemon, IntrospectionSignalClient};
use introspect::daemon_shell::ComponentDaemon;
use introspect::datom_text;
use introspect::meta::{MetaIntrospectClient, MetaIntrospectEndpoint};
use meta_signal_introspect::{
    MetaIntrospectOperationKind, Query as MetaIntrospectQuery, Response as MetaIntrospectResponse,
    UnimplementedReason,
};
use signal_introspect::{
    BluetoothPowerEvent, BluetoothPowerObservation, BluetoothSystemEvent, BluetoothTarget,
    BluetoothTopic, BootIdentifier, ComponentSnapshotObservationQuery,
    DeliveryTraceObservationQuery, EngineSnapshotObservationQuery, EventSeverity,
    IntrospectDaemonConfiguration, IntrospectionTarget, JournalSource,
    PrototypeWitnessObservationQuery, Query, RecordSystemEvent, Response, SystemEvent,
    SystemEventsQuery, TargetedSystemEvent,
};
use signal_persona::OwnerIdentity;

const ENGINE: &str = "prototype";

/// A running `introspect-daemon` child with both socket paths, torn down on
/// drop.
struct DaemonProcess {
    child: Child,
    introspect_socket: std::path::PathBuf,
    meta_socket: std::path::PathBuf,
    configuration: IntrospectDaemonConfiguration,
    _directory: tempfile::TempDir,
}

impl DaemonProcess {
    fn spawn() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let introspect_socket = directory.path().join("introspect.sock");
        let meta_socket = directory.path().join("meta-introspect.sock");
        let configuration_path = directory.path().join("introspect-daemon.rkyv");
        let configuration =
            daemon_configuration(directory.path(), &introspect_socket, &meta_socket);
        write_configuration(&configuration_path, &configuration);

        let child = Command::new(env!("CARGO_BIN_EXE_introspect-daemon"))
            .arg(&configuration_path)
            .spawn()
            .expect("introspect-daemon starts");

        wait_for_socket(&introspect_socket);
        wait_for_socket(&meta_socket);
        Self {
            child,
            introspect_socket,
            meta_socket,
            configuration,
            _directory: directory,
        }
    }

    fn submit(&self, query: Query) -> Response {
        IntrospectionSignalClient::new(self.introspect_socket.clone())
            .submit(query)
            .expect("client receives response")
    }

    fn bluetooth_event(identifier: i64, observed_at: i64) -> SystemEvent {
        SystemEvent {
            event_identifier: identifier,
            boot_identifier: BootIdentifier {
                first_integer: 0x1234,
                second_integer: 0x5678,
            },
            event_instant: observed_at,
            targeted_system_event: TargetedSystemEvent::Bluetooth(BluetoothSystemEvent {
                bluetooth_target: BluetoothTarget::Controller,
                bluetooth_topic: BluetoothTopic::Power(BluetoothPowerObservation::Event(
                    BluetoothPowerEvent::ObservedOn,
                )),
            }),
            event_severity: EventSeverity::Warning,
            event_provenance: introspect::contract::trusted_journal(
                JournalSource::SystemdBluetoothService,
            ),
            extractor_revision: 1,
            policy_revision: 2,
            bounded_payload_option: None,
        }
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
    let response = daemon.submit(Query::PrototypeWitnessObservation(
        PrototypeWitnessObservationQuery {
            engine_identifier: ENGINE.to_owned(),
        },
    ));

    match response {
        Response::PrototypeWitnessObservation(witness) => {
            assert_eq!(witness.engine_identifier, ENGINE);
            assert_eq!(witness.first_optional_component_readiness, None);
            assert_eq!(witness.second_optional_component_readiness, None);
            assert_eq!(witness.third_optional_component_readiness, None);
            assert_eq!(witness.delivery_trace_observation_status_option, None);
        }
        other => panic!("expected prototype witness, got {other:?}"),
    }
}

#[test]
fn daemon_configuration_accepts_binary_file_argument() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("introspect.sock");
    let meta_socket = directory.path().join("meta-introspect.sock");
    let configuration_path = directory.path().join("introspect-daemon.rkyv");
    let configuration = daemon_configuration(directory.path(), &socket, &meta_socket);

    write_configuration(&configuration_path, &configuration);

    let decoded = IntrospectionDaemon::load_configuration(&configuration_path)
        .expect("read binary introspect config")
        .into_inner();

    assert_eq!(decoded, configuration);
}

#[test]
fn daemon_serves_scaffold_observation_responses_for_all_query_families() {
    let daemon = DaemonProcess::spawn();

    let engine_response = daemon.submit(Query::EngineSnapshotObservation(
        EngineSnapshotObservationQuery {
            engine_identifier: ENGINE.to_owned(),
        },
    ));
    match engine_response {
        Response::EngineSnapshotObservation(snapshot) => {
            assert_eq!(snapshot.engine_identifier, ENGINE);
            for expected in [
                IntrospectionTarget::EngineManager,
                IntrospectionTarget::Router,
                IntrospectionTarget::Terminal,
            ] {
                assert!(snapshot.observed_components.contains(&expected));
            }
        }
        other => panic!("expected engine snapshot, got {other:?}"),
    }

    let component_response = daemon.submit(Query::ComponentSnapshotObservation(
        ComponentSnapshotObservationQuery {
            engine_identifier: ENGINE.to_owned(),
            introspection_target: IntrospectionTarget::Router,
        },
    ));
    match component_response {
        Response::ComponentSnapshotObservation(snapshot) => {
            assert_eq!(snapshot.introspection_target, IntrospectionTarget::Router);
            assert_eq!(snapshot.optional_component_readiness, None);
        }
        other => panic!("expected component snapshot, got {other:?}"),
    }

    let delivery_response = daemon.submit(Query::DeliveryTraceObservation(
        DeliveryTraceObservationQuery {
            engine_identifier: ENGINE.to_owned(),
            message_slot: 7,
            component_name: "Message".to_owned(),
        },
    ));
    match delivery_response {
        Response::DeliveryTraceObservation(trace) => {
            assert_eq!(trace.message_slot, 7);
            assert_eq!(trace.component_name, "Message");
            assert!(trace.delivery_trace_observation_events.is_empty());
        }
        other => panic!("expected delivery trace, got {other:?}"),
    }
}

#[test]
fn targeted_system_event_socket_ingestion_is_durable_and_typed_queryable() {
    use introspect::contract::CoalescedReading;

    let daemon = DaemonProcess::spawn();
    for (identifier, observed_at) in [(41, 10), (42, 20)] {
        let response = daemon.submit(Query::RecordSystemEvent(RecordSystemEvent {
            system_event: DaemonProcess::bluetooth_event(identifier, observed_at),
        }));
        let Response::SystemEventAccepted(accepted) = response else {
            panic!("expected typed system-event acceptance");
        };
        assert_eq!(accepted.event_identifier, 41);
    }

    let response = daemon.submit(Query::SystemEvents(SystemEventsQuery {
        boot_identifier: BootIdentifier {
            first_integer: 0x1234,
            second_integer: 0x5678,
        },
        system_event_domain_option: None,
    }));
    let Response::SystemEvents(events) = response else {
        panic!("expected typed system-event query response");
    };
    assert_eq!(events.system_event_summaries.len(), 1);
    let summary = &events.system_event_summaries[0];
    assert_eq!(summary.representative().event_identifier, 41);
    assert_eq!(summary.count(), 2);
    assert_eq!(summary.suppressed_count(), 1);
    assert_eq!(summary.first_seen(), 10);
    assert_eq!(summary.last_seen(), 20);
}

#[test]
fn daemon_answers_typed_meta_policy_relation() {
    let daemon = DaemonProcess::spawn();
    let mode = std::fs::metadata(&daemon.meta_socket)
        .expect("meta socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let response = MetaIntrospectClient::new(MetaIntrospectEndpoint::new(&daemon.meta_socket))
        .submit(MetaIntrospectQuery::Configure(daemon.configuration.clone()))
        .expect("meta client receives response");
    assert!(matches!(
        response,
        MetaIntrospectResponse::RequestUnimplemented(unimplemented)
            if unimplemented.meta_introspect_operation_kind
                == MetaIntrospectOperationKind::Configure
                && unimplemented.unimplemented_reason == UnimplementedReason::NotBuiltYet
    ));
}

#[test]
fn introspect_cli_reaches_working_socket_and_prints_typed_witness() {
    let daemon = DaemonProcess::spawn();
    let query = datom_text::textualize(&Query::PrototypeWitnessObservation(
        PrototypeWitnessObservationQuery {
            engine_identifier: ENGINE.to_owned(),
        },
    ));
    let output = Command::new(env!("CARGO_BIN_EXE_introspect"))
        .env("INTROSPECT_SOCKET", &daemon.introspect_socket)
        .arg(&query)
        .output()
        .expect("run introspect cli");

    assert!(
        output.status.success(),
        "introspect cli failed on {query}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("introspect cli stdout is utf8");
    assert!(
        stdout.contains("PrototypeWitnessObservation"),
        "unexpected stdout: {stdout}"
    );
    assert!(stdout.contains(ENGINE), "unexpected stdout: {stdout}");
}

#[test]
fn meta_introspect_cli_reaches_policy_socket_and_prints_typed_rejection() {
    let daemon = DaemonProcess::spawn();
    let query = datom_text::textualize(&MetaIntrospectQuery::Configure(
        daemon.configuration.clone(),
    ));
    let output = Command::new(env!("CARGO_BIN_EXE_meta-introspect"))
        .env("INTROSPECT_META_SOCKET", &daemon.meta_socket)
        .arg(&query)
        .output()
        .expect("run meta-introspect cli");

    assert!(
        output.status.success(),
        "meta-introspect cli failed on {query}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("meta-introspect cli stdout is utf8");
    assert!(
        stdout.contains("RequestUnimplemented"),
        "unexpected stdout: {stdout}"
    );
    assert!(stdout.contains("Configure"), "unexpected stdout: {stdout}");
}

fn write_configuration(path: &Path, configuration: &IntrospectDaemonConfiguration) {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(configuration)
        .expect("introspect config rkyv encodes");
    std::fs::write(path, bytes.as_slice()).expect("write binary introspect config");
    // Witness the round-trip the daemon performs at startup.
    let _ = IntrospectionDaemonConfiguration::new(configuration.clone());
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
    meta_socket: &Path,
) -> IntrospectDaemonConfiguration {
    IntrospectDaemonConfiguration {
        introspect_socket_path: socket.display().to_string(),
        introspect_socket_mode: 0o600,
        supervision_socket_path: meta_socket.display().to_string(),
        supervision_socket_mode: 0o600,
        store_path: directory.join("introspect.sema").display().to_string(),
        // The prototype daemon has no live peers in this harness; an empty wire
        // path means "no peer configured", so the witness reports each peer as
        // unseen rather than failing on an unreachable socket.
        manager_socket_path: String::new(),
        router_socket_path: String::new(),
        terminal_socket_path: String::new(),
        // No trace emitter pushes to this harness; an empty wire path disables
        // component-trace ingestion (mirrors the empty-peer-socket convention).
        trace_socket_path: String::new(),
        owner_identity: OwnerIdentity::UnixUser(1000),
    }
}
