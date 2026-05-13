use std::ffi::OsString;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionDaemonCommandLine {
    arguments: Vec<OsString>,
}

impl IntrospectionDaemonCommandLine {
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

    pub fn run(&self) -> Result<()> {
        self.reject_extra_arguments()?;
        eprintln!("persona-introspect-daemon scaffold");
        Ok(())
    }

    fn reject_extra_arguments(&self) -> Result<()> {
        if let Some(argument) = self.arguments.first() {
            return Err(Error::UnexpectedArgument {
                got: argument.to_string_lossy().to_string(),
            });
        }
        Ok(())
    }
}
