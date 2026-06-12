use std::io::Write;
use std::path::PathBuf;

use nota_next::NotaSource;
use signal_introspect::IntrospectionRequest;
use triad_runtime::ComponentCommand;

use crate::cli_argument::NotaCommandText;
use crate::daemon::IntrospectionSignalClient;
use crate::error::Result;
use crate::surface::{Input, Output};

const DEFAULT_INTROSPECT_SOCKET: &str = "/tmp/introspect.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectCommandLine {
    command: ComponentCommand,
    environment: IntrospectCommandEnvironment,
}

impl IntrospectCommandLine {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            environment: IntrospectCommandEnvironment::from_process(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(
            arguments,
            IntrospectCommandEnvironment::from_process(),
        )
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: IntrospectCommandEnvironment,
    ) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let input = IntrospectInputText::from_command(self.command)?.into_input()?;
        let reply = IntrospectionSignalClient::new(self.environment.endpoint())
            .submit(input.into_request())?;
        writeln!(output, "{}", Output::from_signal(reply).to_nota())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectCommandEnvironment {
    socket: String,
}

impl IntrospectCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(
            std::env::var("INTROSPECT_SOCKET").unwrap_or(DEFAULT_INTROSPECT_SOCKET.to_string()),
        )
    }

    pub fn endpoint(&self) -> PathBuf {
        PathBuf::from(&self.socket)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IntrospectInputText {
    text: NotaCommandText,
}

impl IntrospectInputText {
    fn from_command(command: ComponentCommand) -> Result<Self> {
        Ok(Self {
            text: NotaCommandText::from_command(command)?,
        })
    }

    fn into_input(self) -> Result<Input> {
        Ok(NotaSource::new(self.text.as_str()).parse::<Input>()?)
    }
}

impl Input {
    fn into_request(self) -> IntrospectionRequest {
        match self {
            Self::PrototypeWitness(query) => {
                IntrospectionRequest::PrototypeWitness(query.into_signal())
            }
        }
    }
}
