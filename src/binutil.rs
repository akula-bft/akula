use derive_more::*;
use directories::ProjectDirs;
use std::{fmt::Display, path::PathBuf, str::FromStr};

#[derive(AsRef, Clone, Debug, Deref, DerefMut, From)]
#[as_ref(forward)]
#[from(forward)]
pub struct ExpandedPathBuf(pub PathBuf);

impl FromStr for ExpandedPathBuf {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(PathBuf::from_str(&shellexpand::full(s)?)?))
    }
}

#[derive(Clone, Debug, Deref, DerefMut, FromStr)]

pub struct AkulaDataDir(pub ExpandedPathBuf);

impl AkulaDataDir {
    pub fn chain_data_dir(&self) -> PathBuf {
        self.0.join("chaindata")
    }

    pub fn etl_temp_dir(&self) -> PathBuf {
        self.0.join("etl-temp")
    }

    pub fn sentry_db(&self) -> PathBuf {
        self.0.join("sentrydb")
    }
}

impl Default for AkulaDataDir {
    fn default() -> Self {
        Self(ExpandedPathBuf(
            ProjectDirs::from("", "", "Akula")
                .map(|pd| pd.data_dir().to_path_buf())
                .unwrap_or_else(|| "data".into()),
        ))
    }
}

impl Display for AkulaDataDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_os_str().to_str().unwrap())
    }
}
