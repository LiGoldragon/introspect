use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_introspect::{
    IntrospectionReply, PrototypeWitness as SignalPrototypeWitness,
    PrototypeWitnessQuery as SignalPrototypeWitnessQuery,
};
use signal_persona_origin::EngineIdentifier;

use crate::error::Result;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct PrototypeWitness {
    pub engine: String,
}

impl PrototypeWitness {
    pub fn into_signal(self) -> SignalPrototypeWitnessQuery {
        SignalPrototypeWitnessQuery {
            engine: EngineIdentifier::new(self.engine),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Input {
    PrototypeWitness(PrototypeWitness),
}

impl Input {
    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let input = Self::decode(&mut decoder)?;
        expect_end(&mut decoder)?;
        Ok(input)
    }
}

pub fn expect_end(decoder: &mut Decoder<'_>) -> nota_codec::Result<()> {
    if let Some(token) = decoder.peek_token()? {
        return Err(nota_codec::Error::UnexpectedToken {
            expected: "end of input",
            got: token,
        });
    }
    Ok(())
}

impl NotaEncode for Input {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::PrototypeWitness(input) => input.encode(encoder),
        }
    }
}

impl NotaDecode for Input {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        let head = decoder.peek_record_head()?;
        match head.as_str() {
            "PrototypeWitness" => Ok(Self::PrototypeWitness(PrototypeWitness::decode(decoder)?)),
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "Input",
                got: other.to_string(),
            }),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Output {
    PrototypeWitness(SignalPrototypeWitness),
    Raw(IntrospectionReply),
}

impl Output {
    pub fn from_signal(reply: IntrospectionReply) -> Self {
        match reply {
            IntrospectionReply::PrototypeWitness(witness) => Self::PrototypeWitness(witness),
            other => Self::Raw(other),
        }
    }

    pub fn to_nota(&self) -> Result<String> {
        let mut encoder = Encoder::new();
        self.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }
}

impl NotaEncode for Output {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::PrototypeWitness(output) => output.encode(encoder),
            Self::Raw(output) => output.encode(encoder),
        }
    }
}
