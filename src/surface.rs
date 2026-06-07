use nota_next::{NotaDecode, NotaEncode, NotaSource};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_introspect::{IntrospectionReply, PrototypeWitnessQuery as SignalPrototypeWitnessQuery};
use signal_persona_origin::EngineIdentifier;

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub struct PrototypeWitness {
    pub engine: EngineIdentifier,
}

impl PrototypeWitness {
    pub fn into_signal(self) -> SignalPrototypeWitnessQuery {
        SignalPrototypeWitnessQuery {
            engine: self.engine,
        }
    }
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum Input {
    PrototypeWitness(PrototypeWitness),
}

impl Input {
    pub fn from_nota(text: &str) -> crate::Result<Self> {
        NotaSource::new(text).parse().map_err(Into::into)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct Output {
    reply: IntrospectionReply,
}

impl Output {
    pub fn from_signal(reply: IntrospectionReply) -> Self {
        Self { reply }
    }

    pub fn to_nota(&self) -> String {
        self.reply.to_nota()
    }
}
