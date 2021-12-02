use derive_more::*;
use directories::ProjectDirs;
use std::{fmt::Display, path::PathBuf};

#[derive(Debug, Deref, DerefMut, FromStr)]

pub struct AkulaDataDir(pub PathBuf);

impl Default for AkulaDataDir {
    fn default() -> Self {
        Self(
            ProjectDirs::from("", "", "Akula")
                .map(|pd| pd.data_dir().to_path_buf())
                .unwrap_or_else(|| "data".into()),
        )
    }
}

impl Display for AkulaDataDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_os_str().to_str().unwrap())
    }
}
