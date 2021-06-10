use serde::Deserialize;

#[derive(Deserialize, Clone)]
#[serde(transparent)]
pub struct ChainId(pub u32);
