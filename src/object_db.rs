use crate::traits::KV;

pub struct ObjectDatabase<K: KV> {
    kv: K,
}

// impl<K: KV> ObjectDatabase<K> {
//     pub async fn open(path: &str) -> anyhow::Result<Self> {
//         let kv = MdbxOpts::new().path(path).open()?;

//         Ok(Self { kv })
//     }
// }
