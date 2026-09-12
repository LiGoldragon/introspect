//! Architectural-truth witness for introspect's actor discipline: every public
//! actor noun is data-bearing.
//!
//! A refactor that collapses an actor noun to a marker ZST breaks this witness.

use introspect::runtime::{
    DatomProjection, IntrospectionRoot, ManagerClient, QueryPlanner, RouterClient, TargetDirectory,
    TerminalClient,
};
use introspect::store::IntrospectionStore;

#[test]
fn public_actor_nouns_carry_data() {
    assert!(std::mem::size_of::<IntrospectionRoot>() > 0);
    assert!(std::mem::size_of::<IntrospectionStore>() > 0);
    assert!(std::mem::size_of::<TargetDirectory>() > 0);
    assert!(std::mem::size_of::<QueryPlanner>() > 0);
    assert!(std::mem::size_of::<ManagerClient>() > 0);
    assert!(std::mem::size_of::<RouterClient>() > 0);
    assert!(std::mem::size_of::<TerminalClient>() > 0);
    assert!(std::mem::size_of::<DatomProjection>() > 0);
}
