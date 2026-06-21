//! Load-bearing slice test (716 test_plan step 3): the end-to-end
//! tracing -> introspect path. An emitting component PUSHES
//! `ComponentTraceEvent` frames over a Unix trace socket; introspect's
//! `ComponentTraceListener` PULLS them off the socket into its sema store; an
//! introspect-owned `ComponentTrace` query returns them filtered by component
//! and event name. No mentci, no spirit dependency — the wire record is the
//! shared `signal-introspect` contract type both ends import.

use std::time::{Duration, Instant};

use introspect::runtime::{
    HandleIntrospectionRequest, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use introspect::store::StoreLocation;
use signal_introspect::{
    ComponentTraceEvent, ComponentTraceQuery, IntrospectionReply, IntrospectionRequest,
    IntrospectionTarget, TraceEventName, TraceLayer, TraceSequence,
};
use signal_persona::EngineIdentifier;
use triad_runtime::trace::TraceLog;

/// One Signal-layer trace event for the prototype engine at the given sequence.
fn signal_event(engine: &EngineIdentifier, event_name: &str, sequence: u64) -> ComponentTraceEvent {
    ComponentTraceEvent::new(
        engine.clone(),
        IntrospectionTarget::Signal,
        TraceLayer::Signal,
        TraceEventName::new(event_name),
        TraceSequence::new(sequence),
    )
}

/// Block until the trace socket the listener binds in `on_start` exists, so the
/// emitter's first `record` connects to a live listener rather than dropping.
fn await_socket(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "trace listener did not bind its socket within the deadline"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Drive a `ComponentTrace` query through the root actor, returning the events.
async fn query_component_trace(
    root: &kameo::actor::ActorRef<IntrospectionRoot>,
    query: ComponentTraceQuery,
) -> Vec<ComponentTraceEvent> {
    let reply = root
        .ask(HandleIntrospectionRequest {
            request: IntrospectionRequest::ComponentTrace(query),
        })
        .await
        .expect("root actor replies to component-trace query");
    match reply {
        IntrospectionReply::ComponentTrace(trace) => trace.into_events(),
        other => panic!("expected ComponentTrace reply, got {other:?}"),
    }
}

/// Poll the `ComponentTrace` query until the store has drained the expected
/// number of events, or the deadline passes. Ingestion is asynchronous (the
/// listener drains on a background blocking loop), so the query is the
/// observation point that proves the push reached durable state.
async fn await_drained(
    root: &kameo::actor::ActorRef<IntrospectionRoot>,
    query: ComponentTraceQuery,
    expected: usize,
) -> Vec<ComponentTraceEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = query_component_trace(root, query.clone()).await;
        if events.len() >= expected || Instant::now() >= deadline {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn pushed_signal_trace_events_are_ingested_and_queryable_by_component_and_name() {
    let directory = tempfile::tempdir().expect("tempdir");
    let trace_socket = directory.path().join("introspect-trace.sock");
    let store = StoreLocation::new(directory.path().join("introspect.sema"));
    let engine = EngineIdentifier::new("prototype");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory {
                    manager_socket: None,
                    router_socket: None,
                    terminal_socket: None,
                    trace_socket: Some(trace_socket.clone()),
                },
                store,
            })
        })
        .expect("root starts");

    // The listener binds its socket in on_start; wait for it before pushing so
    // the emitter connects to a live listener.
    await_socket(&trace_socket);

    // The emitting component pushes three Signal-layer events over the socket,
    // exactly as spirit's testing-trace sink does, using the shared contract
    // type. Sequence order is the monotonic emission order.
    let emitter = TraceLog::<ComponentTraceEvent>::socket(&trace_socket);
    emitter
        .record_result(signal_event(&engine, "SignalStarted", 0))
        .expect("push SignalStarted");
    emitter
        .record_result(signal_event(&engine, "SignalAdmitted", 1))
        .expect("push SignalAdmitted");
    emitter
        .record_result(signal_event(&engine, "SignalReplied", 2))
        .expect("push SignalReplied");

    // Component-wide query (event_name = None) returns all three in sequence
    // order once the listener has drained them into the store.
    let all_query = ComponentTraceQuery::new(engine.clone(), IntrospectionTarget::Signal, None);
    let all_events = runtime.block_on(await_drained(&root, all_query, 3));
    assert_eq!(all_events.len(), 3, "all three pushed events were ingested");
    let names = all_events
        .iter()
        .map(|event| event.event_name.as_str().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "SignalStarted".to_owned(),
            "SignalAdmitted".to_owned(),
            "SignalReplied".to_owned(),
        ],
        "events return in monotonic sequence order"
    );
    let sequences = all_events
        .iter()
        .map(|event| event.sequence.value())
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![0, 1, 2]);

    // Name-narrowed query returns exactly the one matching event.
    let admitted_query = ComponentTraceQuery::new(
        engine.clone(),
        IntrospectionTarget::Signal,
        Some(TraceEventName::new("SignalAdmitted")),
    );
    let admitted = runtime.block_on(query_component_trace(&root, admitted_query));
    assert_eq!(admitted.len(), 1, "event-name filter narrows to one event");
    assert_eq!(admitted[0].event_name, "SignalAdmitted");
    assert_eq!(admitted[0].sequence.value(), 1);

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops gracefully");
    runtime.block_on(root.wait_for_shutdown());
}
