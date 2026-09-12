//! The introspection root answers through its actor tree, and reaches a live
//! router over the router's own contract wire.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::thread;

use introspect::runtime::{
    ExplainPrototypeWitness, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use introspect::store::StoreLocation;
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, SessionEpoch};
use signal_introspect::{ComponentReadiness, PrototypeWitnessObservationQuery, Response};
use signal_router::{
    EngineIdentifier as RouterEngineIdentifier, Frame as RouterFrame, FrameBody as RouterFrameBody,
    Input as RouterRequest, Output as RouterReply, RouterSummary,
};

fn read_router_frame(stream: &mut UnixStream) -> RouterFrame {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).expect("read frame prefix");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.resize(4 + length, 0);
    stream.read_exact(&mut bytes[4..]).expect("read frame body");
    RouterFrame::decode_length_prefixed(&bytes).expect("decode router frame")
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn witness_query() -> PrototypeWitnessObservationQuery {
    PrototypeWitnessObservationQuery {
        engine_identifier: "prototype".to_owned(),
    }
}

#[test]
fn prototype_witness_uses_introspection_root_actor() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let directory = tempfile::tempdir().expect("tempdir");
    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory::empty(),
                store: StoreLocation::new(directory.path().join("introspect.sema")),
            })
        })
        .expect("root starts");
    let response = runtime
        .block_on(async {
            root.ask(ExplainPrototypeWitness {
                query: witness_query(),
            })
            .await
        })
        .expect("actor reply");

    match response {
        Response::PrototypeWitnessObservation(witness) => {
            assert_eq!(witness.engine_identifier, "prototype");
            // Daemon skeleton has not yet collected peer observations;
            // every position is None per the closed contract.
            assert_eq!(witness.delivery_trace_observation_status_option, None);
        }
        other => panic!("expected PrototypeWitnessObservation, got {other:?}"),
    }
}

#[test]
fn prototype_witness_queries_live_router_summary_socket() {
    let directory = tempfile::tempdir().expect("tempdir");
    let router_socket = directory.path().join("router.sock");
    let listener = UnixListener::bind(&router_socket).expect("bind router socket");
    let server = thread::spawn(move || {
        let (mut stream, _address) = listener.accept().expect("router accepts");
        let frame = read_router_frame(&mut stream);
        match frame.into_body() {
            RouterFrameBody::Request { request, .. } => {
                let payload = request.payloads.into_head();
                assert!(matches!(payload, RouterRequest::Summary(_)));
            }
            other => panic!("expected router request frame, got {other:?}"),
        }

        let reply = RouterReply::Summary(RouterSummary {
            engine: RouterEngineIdentifier::new("prototype").into(),
            accepted_messages: 0.into(),
            routed_messages: 0.into(),
            deferred_messages: 0.into(),
            failed_messages: 0.into(),
        })
        .into_reply_frame(exchange());
        stream
            .write_all(
                reply
                    .encode_length_prefixed()
                    .expect("encode router reply")
                    .as_slice(),
            )
            .expect("write router reply");
    });

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory {
                    manager_socket: None,
                    router_socket: Some(router_socket),
                    terminal_socket: None,
                    trace_socket: None,
                },
                store: StoreLocation::new(directory.path().join("introspect.sema")),
            })
        })
        .expect("root starts");
    let response = runtime
        .block_on(async {
            root.ask(ExplainPrototypeWitness {
                query: witness_query(),
            })
            .await
        })
        .expect("actor reply");

    match response {
        Response::PrototypeWitnessObservation(witness) => {
            assert_eq!(witness.engine_identifier, "prototype");
            assert_eq!(witness.first_optional_component_readiness, None);
            assert_eq!(
                witness.second_optional_component_readiness,
                Some(ComponentReadiness::Ready)
            );
            assert_eq!(witness.third_optional_component_readiness, None);
            assert_eq!(witness.delivery_trace_observation_status_option, None);
        }
        other => panic!("expected PrototypeWitnessObservation, got {other:?}"),
    }

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops");
    runtime.block_on(root.wait_for_shutdown());
    server.join().expect("router server joins");
}
