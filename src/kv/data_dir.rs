use directories::ProjectDirs;
use std::str::FromStr;

#[derive(Debug)]
pub struct DataDir(pub std::path::PathBuf);

impl Default for DataDir {
    fn default() -> Self {
        if let Some(dirs) = ProjectDirs::from("com", "akula-bft", "akula") {
            DataDir(dirs.data_dir().to_path_buf())
        } else {
            DataDir(std::path::PathBuf::from("data"))
        }
    }
}

impl ToString for DataDir {
    fn to_string(&self) -> String {
        String::from(self.0.as_os_str().to_str().unwrap())
    }
}

impl FromStr for DataDir {
    type Err = anyhow::Error;

    fn from_str(path_str: &str) -> anyhow::Result<Self> {
        Ok(DataDir(std::path::PathBuf::from(path_str)))
    }
}
