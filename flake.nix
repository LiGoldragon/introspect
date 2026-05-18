{
  description = "persona-introspect - Persona inspection-plane daemon and CLI";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      fenix,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        src = craneLib.cleanCargoSource ./.;
        commonArgs = {
          inherit src;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      in
      {
        packages.default = craneLib.buildPackage (commonArgs // { inherit cargoArtifacts; });
        checks = {
          build = craneLib.cargoBuild (commonArgs // { inherit cargoArtifacts; });
          test = craneLib.cargoTest (commonArgs // { inherit cargoArtifacts; });
          test-actor-runtime-truth = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth";
            }
          );
          test-daemon-socket = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test daemon";
            }
          );
          test-router-client-live-summary = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth prototype_witness_queries_live_router_summary_socket -- --exact";
            }
          );
          test-daemon-applies-spawn-envelope-socket-mode = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test daemon daemon_applies_spawn_envelope_socket_mode -- --exact";
            }
          );
          test-daemon-answers-component-supervision-relation = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test daemon daemon_answers_component_supervision_relation -- --exact";
            }
          );
          test-introspection-store-uses-sema-engine = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test store";
            }
          );
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            }
          );
        };
        devShells.default = pkgs.mkShell {
          name = "persona-introspect";
          packages = [
            pkgs.jujutsu
            pkgs.pkg-config
            toolchain
          ];
        };
      }
    );
}
