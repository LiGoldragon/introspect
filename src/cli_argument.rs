use std::{fs, path::PathBuf};

use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::{Error, Result};

/// The single datom text a component CLI takes: either inline on argv, or the
/// contents of a file named on argv. `triad-runtime` classifies the raw
/// argument; the text it yields is read as datom by `crate::datom_text`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatomCommandText {
    text: String,
}

impl DatomCommandText {
    pub fn from_command(command: ComponentCommand) -> Result<Self> {
        match command.dotos_argument()? {
            ComponentArgument::InlineDotos(argument) => Ok(Self::new(argument.into_string())),
            ComponentArgument::DotosFile(argument) => Self::from_path(argument.into_path()),
            ComponentArgument::SignalFile(argument) => Self::from_path(argument.into_path()),
        }
    }

    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn from_path(path: PathBuf) -> Result<Self> {
        let text = fs::read_to_string(&path).map_err(|source| Error::DatomFileRead {
            path: path.clone(),
            source,
        })?;
        Ok(Self::new(text))
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}
