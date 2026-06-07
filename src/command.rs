use std::ffi::OsString;
use std::io::Write;

use kameo::error::SendError;
use signal_introspect::{IntrospectionRequest, PrototypeWitnessQuery};
use signal_persona_origin::EngineIdentifier;

use crate::daemon::IntrospectionSocket;
use crate::error::{Error, Result};
use crate::runtime::{ExplainPrototypeWitness, IntrospectionRoot, IntrospectionRootInput};
use crate::store::StoreLocation;
use crate::surface::{Input, Output};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectCommandLine {
    arguments: Vec<OsString>,
}

impl IntrospectCommandLine {
    pub fn from_env() -> Self {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments<I, S>(arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn run(&self, output: impl Write) -> Result<()> {
        let input = self.input()?;
        self.run_input(input, output)
    }

    fn input(&self) -> Result<Input> {
        match self.arguments.as_slice() {
            [] => Ok(Input::PrototypeWitness(crate::surface::PrototypeWitness {
                engine: EngineIdentifier::new("prototype"),
            })),
            [text] => Input::from_nota(&text.to_string_lossy()),
            [_, extra, ..] => Err(Error::UnexpectedArgument {
                got: extra.to_string_lossy().to_string(),
            }),
        }
    }

    fn run_input(&self, input: Input, mut output: impl Write) -> Result<()> {
        if let Some(socket) = IntrospectionSocket::from_environment() {
            let request = match input {
                Input::PrototypeWitness(query) => {
                    IntrospectionRequest::PrototypeWitness(query.into_signal())
                }
            };
            let reply = socket.client().submit(request)?;
            writeln!(output, "{}", Output::from_signal(reply).to_nota())?;
            return Ok(());
        }

        let runtime = tokio::runtime::Runtime::new()?;
        let root = runtime.block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: crate::runtime::TargetSocketDirectory::empty(),
            store: StoreLocation::from_environment(),
        }))?;
        let reply = match input {
            Input::PrototypeWitness(query) => runtime.block_on(async {
                root.ask(ExplainPrototypeWitness {
                    query: query.into_signal(),
                })
                .await
            }),
        };
        let reply = match reply {
            Ok(reply) => reply,
            Err(SendError::HandlerError(error)) => return Err(error),
            Err(error) => {
                return Err(Error::Actor {
                    operation: "prototype witness",
                    detail: format!("{error:?}"),
                });
            }
        };
        writeln!(output, "{}", Output::from_signal(reply).to_nota())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrototypeWitnessDefault {
    pub engine: EngineIdentifier,
}

impl PrototypeWitnessDefault {
    pub fn into_query(self) -> PrototypeWitnessQuery {
        PrototypeWitnessQuery {
            engine: self.engine,
        }
    }
}
