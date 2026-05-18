use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::thread;

use persona_introspect::runtime::{
    ExplainPrototypeWitness, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use persona_introspect::store::StoreLocation;
use signal_core::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, SessionEpoch,
    SignalVerb, SubReply,
};
use signal_persona_auth::EngineId;
use signal_persona_introspect::{ComponentReadiness, IntrospectionReply, PrototypeWitnessQuery};
use signal_persona_router::{
    RouterFrame, RouterFrameBody, RouterReply, RouterRequest, RouterSummary,
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

#[test]
fn prototype_witness_uses_introspection_root_actor() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let directory = tempfile::tempdir().expect("tempdir");
    let root = runtime
        .block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: TargetSocketDirectory::empty(),
            store: StoreLocation::new(directory.path().join("introspect.redb")),
        }))
        .expect("root starts");
    let reply = runtime
        .block_on(async {
            root.ask(ExplainPrototypeWitness {
                query: PrototypeWitnessQuery {
                    engine: EngineId::new("prototype"),
                },
            })
            .await
        })
        .expect("actor reply");

    match reply {
        IntrospectionReply::PrototypeWitness(witness) => {
            assert_eq!(witness.engine, EngineId::new("prototype"));
            // Daemon skeleton has not yet collected peer observations;
            // every field is None per the closed-enum contract.
            assert_eq!(witness.delivery_status, None);
        }
        other => panic!("expected PrototypeWitness reply, got {other:?}"),
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
                let operation = request
                    .into_checked()
                    .expect("request passes structural checks")
                    .operations
                    .into_head();
                assert_eq!(operation.verb, SignalVerb::Match);
                assert!(matches!(operation.payload, RouterRequest::Summary(_)));
            }
            other => panic!("expected router request frame, got {other:?}"),
        }

        let reply = RouterFrame::new(RouterFrameBody::Reply {
            exchange: exchange(),
            reply: Reply::Accepted {
                outcome: AcceptedOutcome::Completed,
                per_operation: NonEmpty::single(SubReply::Ok {
                    verb: SignalVerb::Match,
                    payload: RouterReply::Summary(RouterSummary {
                        engine: EngineId::new("prototype"),
                        accepted_messages: 0,
                        routed_messages: 0,
                        deferred_messages: 0,
                        failed_messages: 0,
                    }),
                }),
            },
        });
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
        .block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: TargetSocketDirectory {
                manager_socket: None,
                router_socket: Some(router_socket),
                terminal_socket: None,
            },
            store: StoreLocation::new(directory.path().join("introspect.redb")),
        }))
        .expect("root starts");
    let reply = runtime
        .block_on(async {
            root.ask(ExplainPrototypeWitness {
                query: PrototypeWitnessQuery {
                    engine: EngineId::new("prototype"),
                },
            })
            .await
        })
        .expect("actor reply");

    match reply {
        IntrospectionReply::PrototypeWitness(witness) => {
            assert_eq!(witness.engine, EngineId::new("prototype"));
            assert_eq!(witness.router_seen, Some(ComponentReadiness::Ready));
            assert_eq!(witness.manager_seen, None);
            assert_eq!(witness.terminal_seen, None);
            assert_eq!(witness.delivery_status, None);
        }
        other => panic!("expected PrototypeWitness reply, got {other:?}"),
    }

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops");
    runtime.block_on(root.wait_for_shutdown());
    server.join().expect("router server joins");
}
