//! The component's text plane: datom in, datom out.
//!
//! Every contract type introspect speaks — `signal-introspect`'s `Query` and
//! `Response`, `meta-signal-introspect`'s `Query` and `Response` — is generated
//! from an ethos root and carries `datom_codec::Compositional` and
//! `datom_codec::Datomizable` under the contracts' `datom` feature. A CLI
//! argument is therefore read by walking the expected type (`actualize`), and a
//! reply is written by the exact reverse projection (`textualize`). There is no
//! hand-written projection of a reply enum onto text: the type is the codec.

use datom_codec::{Actualizing, Budget, Compositional, Datom, Datomizable, Path, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};

/// Composition steps one command-line value may spend. A CLI argument is a
/// single typed request; this bound is far above any well-formed request and
/// far below anything that could exhaust the process.
const COMPOSITION_STEPS: i64 = 65_536;

/// Bytes of argument text the protos reader may consume.
const READER_BYTES: usize = 1_048_576;

/// Nesting depth the composition may descend. The deepest contract value is a
/// targeted system event, an order of magnitude shallower than this.
const MAXIMUM_DEPTH: i64 = 256;

/// A fresh budget for one text conversion.
pub fn budget() -> Budget {
    Budget {
        remaining: COMPOSITION_STEPS,
        reader: ReaderBudget {
            remaining: READER_BYTES,
        },
        depth: 0,
        maximum_depth: MAXIMUM_DEPTH,
    }
}

/// Read datom text as the contract type the caller expects.
pub fn actualize<Value: Compositional>(text: &str) -> Result<Value, crate::Error> {
    Potential::<Value>::from(text)
        .actualize(&mut budget())
        .map_err(|error| crate::Error::DatomText {
            detail: textualize(&error),
        })
}

/// Project a contract value into its datom text.
pub fn textualize<Value>(value: &Value) -> String
where
    Value: Datomizable<Output = Datom>,
{
    value.datomize(Path::new()).protosize().textualize()
}
