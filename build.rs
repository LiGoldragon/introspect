use std::{env, path::PathBuf};

use schema_rust_next::{
    MetaListenerTier, NexusDaemonShape, SocketModeBits, WorkingListenerTier,
    build::{GenerationDriver, GenerationPlan, ModuleEmission},
};

const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;

fn main() {
    SchemaBuild::from_environment().run();
}

struct SchemaBuild {
    crate_root: PathBuf,
}

impl SchemaBuild {
    fn from_environment() -> Self {
        Self {
            crate_root: PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir set")),
        }
    }

    fn run(&self) {
        println!("cargo:rerun-if-changed=schema/daemon.schema");
        println!("cargo:rerun-if-changed=src/schema/daemon.rs");

        let plan = GenerationPlan::new(&self.crate_root, "introspect", "0.1.0").with_module(
            ModuleEmission::daemon_module("daemon", Self::daemon_shape()),
        );
        GenerationDriver::new(plan)
            .generate()
            .expect("generate introspect schema artifacts")
            .write_or_check("INTROSPECT_UPDATE_SCHEMA_ARTIFACTS")
            .expect("checked-in introspect schema artifacts are fresh");
    }

    /// Introspect's working tier is component-decoded: the ordinary socket speaks
    /// the hand-written `signal-introspect` `IntrospectionFrame` contract (not a
    /// schema-derived root), so the emitted daemon owns argv/socket/accept/
    /// lifecycle/exit while the component owns the per-connection
    /// `IntrospectionFrame` decode/encode and drives the existing
    /// `IntrospectionRoot` kameo actor tree. The meta tier is the owner-only
    /// engine-management (supervision) socket.
    fn daemon_shape() -> NexusDaemonShape {
        NexusDaemonShape::new(
            "introspect-daemon",
            WorkingListenerTier::component_decoded(),
        )
        .with_meta_tier(MetaListenerTier::new(SocketModeBits::new(
            OWNER_ONLY_SOCKET_MODE,
        )))
    }
}
