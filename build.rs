use anyhow::Result;
use vergen::*;

fn main() -> Result<()> {
    let mut config = Config::default();
    *config.git_mut().commit_timestamp_kind_mut() = TimestampKind::DateOnly;
    *config.git_mut().sha_kind_mut() = ShaKind::Short;
    vergen(config)
}
