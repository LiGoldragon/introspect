use std::io::Write;
use std::path::PathBuf;

use signal_introspect::Query;
use triad_runtime::ComponentCommand;

use crate::cli_argument::DatomCommandText;
use crate::daemon::IntrospectionSignalClient;
use crate::datom_text;
use crate::error::Result;

const DEFAULT_INTROSPECT_SOCKET: &str = "/tmp/introspect.sock";

/// The ordinary `introspect` CLI: one datom `Query` in, one datom `Response`
/// out, over the introspection-query socket.
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
        let text = DatomCommandText::from_command(self.command)?;
        let query: Query = datom_text::actualize(text.as_str())?;
        let response = IntrospectionSignalClient::new(self.environment.endpoint()).submit(query)?;
        writeln!(output, "{}", datom_text::textualize(&response))?;
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
