use serde::Deserialize;

#[derive(Deserialize)]
#[serde(transparent)]
pub struct ChainId(pub u32);
