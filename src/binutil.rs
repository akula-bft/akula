use crate::{models::ChainSpec, res::chainspec};
use anyhow::format_err;
use derive_more::*;
use directories::ProjectDirs;
use expanded_pathbuf::ExpandedPathBuf;
use std::{
    fmt::Display,
    fs::File,
    path::{Path, PathBuf},
};

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

    pub fn nodekey(&self) -> PathBuf {
        self.0.join("nodekey")
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

impl ChainSpec {
    pub fn load_from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(ron::de::from_reader(File::open(path)?)?)
    }

    pub fn load_builtin(name: impl AsRef<str>) -> anyhow::Result<Self> {
        let name = name.as_ref();
        Ok(match name.to_lowercase().as_str() {
            "mainnet" | "ethereum" => chainspec::MAINNET.clone(),
            "ropsten" => chainspec::ROPSTEN.clone(),
            "rinkeby" => chainspec::RINKEBY.clone(),
            "goerli" => chainspec::GOERLI.clone(),
            "sepolia" => chainspec::SEPOLIA.clone(),
            "bsc" => chainspec::BSC.clone(),
            "bsctest" => chainspec::BSCTEST.clone(),
            _ => return Err(format_err!("Network {name} is unknown")),
        })
    }
}
