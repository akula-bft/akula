use crate::sentry::devp2p::{types::*, util::*};
use anyhow::{anyhow, bail};
use arrayvec::ArrayString;
use async_stream::{stream, try_stream};
use bytes::Bytes;
use data_encoding::*;
use derive_more::{Deref, Display};
use educe::Educe;
use enr::{Enr, EnrKeyUnambiguous, EnrPublicKey};
use maplit::hashset;
use secp256k1::{PublicKey, SecretKey};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fmt::{Display, Formatter},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use task_group::TaskGroup;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_stream::{Stream, StreamExt};
use tracing::*;

mod backend;
pub use self::backend::Backend;

type Base32Hash = ArrayString<BASE32_HASH_LEN>;

pub type QueryStream<K> = Pin<Box<dyn Stream<Item = anyhow::Result<Enr<K>>> + Send + 'static>>;

pub const BASE32_HASH_LEN: usize = 26;
pub const ROOT_PREFIX: &str = "enrtree-root:v1";
pub const LINK_PREFIX: &str = "enrtree://";
pub const BRANCH_PREFIX: &str = "enrtree-branch:";
pub const ENR_PREFIX: &str = "enr:";

const MAX_SINGLE_RESOLUTION: u64 = 10;
const MAX_RESOLUTION_DURATION: u64 = 1800;

pub struct DnsDiscovery {
    #[allow(unused)]
    tasks: TaskGroup,
    receiver: Receiver<anyhow::Result<NodeRecord>>,
}

impl DnsDiscovery {
    #[must_use]
    pub fn new<B: Backend>(
        discovery: Arc<Resolver<B, SecretKey>>,
        domain: String,
        public_key: Option<PublicKey>,
    ) -> Self {
        let tasks = TaskGroup::default();

        let (tx, receiver) = channel(1);
        tasks.spawn_with_name("DNS discovery pump", async move {
            loop {
                let mut query = discovery.query(domain.clone(), public_key);
                let restart_at =
                    std::time::Instant::now() + Duration::from_secs(MAX_RESOLUTION_DURATION);

                loop {
                    match tokio::time::timeout(
                        Duration::from_secs(MAX_SINGLE_RESOLUTION),
                        query.next(),
                    )
                    .await
                    {
                        Ok(Some(Err(e))) => {
                            if tx.send(Err(e)).await.is_err() {
                                return;
                            }
                            break;
                        }
                        Ok(Some(Ok(v))) => {
                            if let Some(addr) = v.tcp_socket() {
                                if tx
                                    .send(Ok(NodeRecord {
                                        addr,
                                        id: pk2id(&v.public_key()),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_) => {}
                    }

                    if std::time::Instant::now() > restart_at {
                        trace!("Restarting DNS resolution");
                        break;
                    }
                }
            }
        });

        Self { tasks, receiver }
    }
}

impl Stream for DnsDiscovery {
    type Item = anyhow::Result<NodeRecord>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

#[derive(Debug, Error)]
#[error("Invalid Enr: {0}")]
pub struct InvalidEnr(String);

fn debug_bytes(b: &Bytes, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(f, "{}", hex::encode(b))
}

#[derive(Clone, Deref, Educe)]
#[educe(Debug)]
pub struct RootRecord {
    #[deref]
    base: UnsignedRoot,
    #[educe(Debug(method = "debug_bytes"))]
    signature: Bytes,
}

#[derive(Clone, Debug, Display)]
#[display(
    fmt = "{} e={} l={} seq={}",
    ROOT_PREFIX,
    enr_root,
    link_root,
    sequence
)]
pub struct UnsignedRoot {
    enr_root: Base32Hash,
    link_root: Base32Hash,
    sequence: usize,
}

impl RootRecord {
    fn verify<K: EnrKeyUnambiguous>(&self, pk: &K::PublicKey) -> anyhow::Result<()> {
        let mut sig = self.signature.clone();

        // TODO: find way to unify with ed25519 sigs
        sig.truncate(64);
        if !pk.verify_v4(self.base.to_string().as_bytes(), &sig) {
            bail!("Public key does not match");
        }

        Ok(())
    }
}

impl Display for RootRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} sig={}",
            self.base,
            BASE64.encode(self.signature.as_ref())
        )
    }
}

#[derive(Clone, Educe)]
#[educe(Debug)]
pub enum DnsRecord<K: EnrKeyUnambiguous> {
    Root(RootRecord),
    Link {
        public_key: K::PublicKey,
        domain: String,
    },
    Branch {
        children: HashSet<Base32Hash>,
    },
    Enr {
        record: Enr<K>,
    },
}

impl<K: EnrKeyUnambiguous> Display for DnsRecord<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root(root_record) => write!(f, "{}", root_record),
            Self::Link { public_key, domain } => write!(
                f,
                "{}{}@{}",
                LINK_PREFIX,
                BASE32_NOPAD.encode(public_key.encode_uncompressed().as_ref()),
                domain
            ),
            Self::Branch { children } => write!(
                f,
                "{}{}",
                BRANCH_PREFIX,
                children
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Enr { record } => write!(f, "{}", record.to_base64()),
        }
    }
}

impl<K: EnrKeyUnambiguous> FromStr for DnsRecord<K> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        trace!("Parsing record {}", s);
        if let Some(root) = s.strip_prefix(ROOT_PREFIX) {
            let mut e = None;
            let mut l = None;
            let mut seq = None;
            let mut sig = None;
            for entry in root.trim().split_whitespace() {
                if let Some(v) = entry.strip_prefix("e=") {
                    trace!("Extracting ENR root: {:?}", v);
                    e = Some(v.parse()?);
                } else if let Some(v) = entry.strip_prefix("l=") {
                    trace!("Extracting link root: {:?}", v);
                    l = Some(v.parse()?);
                } else if let Some(v) = entry.strip_prefix("seq=") {
                    trace!("Extracting sequence: {:?}", v);
                    seq = Some(v.parse()?);
                } else if let Some(v) = entry.strip_prefix("sig=") {
                    trace!("Extracting signature: {:?}", v);
                    let v = BASE64URL_NOPAD.decode(v.as_bytes())?.into();
                    sig = Some(v);
                } else {
                    bail!("Invalid string: {}", entry);
                }
            }

            let v = RootRecord {
                base: UnsignedRoot {
                    enr_root: e.ok_or_else(|| anyhow!("ENR root absent"))?,
                    link_root: l.ok_or_else(|| anyhow!("Link root absent"))?,
                    sequence: seq.ok_or_else(|| anyhow!("Sequence not found"))?,
                },
                signature: sig.ok_or_else(|| anyhow!("Signature not found"))?,
            };

            trace!("Successfully parsed {:?}", v);

            return Ok(DnsRecord::Root(v));
        }

        if let Some(link) = s.strip_prefix(LINK_PREFIX) {
            let mut it = link.split('@');
            let public_key = K::decode_public(
                &BASE32_NOPAD.decode(
                    it.next()
                        .ok_or_else(|| anyhow!("Public key not found"))?
                        .as_bytes(),
                )?,
            )?;
            let domain = it
                .next()
                .ok_or_else(|| anyhow!("Domain not found"))?
                .to_string();

            return Ok(DnsRecord::Link { public_key, domain });
        }

        if let Some(branch) = s.strip_prefix(BRANCH_PREFIX) {
            let children = branch
                .trim()
                .split(',')
                .filter_map(|h| match h.parse::<Base32Hash>() {
                    Ok(v) => {
                        if v.is_empty() {
                            None
                        } else {
                            Some(Ok(v))
                        }
                    }
                    Err(e) => Some(Err(anyhow::Error::new(e))),
                })
                .collect::<anyhow::Result<_>>()?;

            return Ok(DnsRecord::Branch { children });
        }

        if s.starts_with(ENR_PREFIX) {
            let record = s.parse::<Enr<K>>().map_err(InvalidEnr)?;

            return Ok(DnsRecord::Enr { record });
        }

        bail!("Invalid string: {}", s)
    }
}

fn domain_is_allowed<K: EnrKeyUnambiguous>(
    whitelist: &Option<Arc<HashMap<String, K::PublicKey>>>,
    domain: &str,
    public_key: &K::PublicKey,
) -> bool {
    whitelist.as_ref().map_or(true, |whitelist| {
        whitelist.get(domain).map_or(false, |pk| {
            pk.encode().as_ref() == public_key.encode().as_ref()
        })
    })
}

#[derive(Clone, Debug)]
enum BranchKind<K: EnrPublicKey> {
    Enr,
    Link {
        remote_whitelist: Option<Arc<HashMap<String, K>>>,
    },
}

fn resolve_branch<B: Backend, K: EnrKeyUnambiguous>(
    task_group: Arc<TaskGroup>,
    backend: Arc<B>,
    host: String,
    children: HashSet<Base32Hash>,
    kind: BranchKind<K::PublicKey>,
) -> QueryStream<K> {
    let (tx, mut branches_res) = tokio::sync::mpsc::channel(1);
    for subdomain in &children {
        let fqdn = format!("{}.{}", subdomain, host);
        task_group.spawn_with_name(format!("DNS discovery: {}", fqdn), {
            let subdomain = *subdomain;
            let tx = tx.clone();
            let backend = backend.clone();
            let host = host.clone();
            let kind = kind.clone();
            let fqdn = fqdn.clone();
            let task_group = task_group.clone();
            async move {
                if let Err(e) = {
                    let tx = tx.clone();
                    async move {
                        let record = backend.get_record(fqdn).await?;
                        if let Some(record) = record {
                            trace!("Resolved record {}: {:?}", subdomain, record);
                            let record = record.parse()?;
                            match record {
                                DnsRecord::Branch { children } => {
                                    let mut t =
                                        resolve_branch(task_group, backend, host, children, kind);
                                    while let Some(item) = t.try_next().await? {
                                        let _ = tx.send(Ok(item)).await;
                                    }

                                    return Ok(());
                                }
                                DnsRecord::Link { public_key, domain } => {
                                    if let BranchKind::Link { remote_whitelist } = &kind {
                                        if domain_is_allowed::<K>(
                                            remote_whitelist,
                                            &domain,
                                            &public_key,
                                        ) {
                                            let mut t = resolve_tree(
                                                Some(task_group),
                                                backend,
                                                domain,
                                                Some(public_key),
                                                None,
                                                remote_whitelist.clone(),
                                            );
                                            while let Some(item) = t.try_next().await? {
                                                let _ = tx.send(Ok(item)).await;
                                            }
                                        } else {
                                            trace!(
                                                "Skipping subtree for forbidden domain: {}",
                                                domain
                                            );
                                        }
                                        return Ok(());
                                    } else {
                                        return Err(anyhow!(
                                            "Unexpected link record in ENR tree: {}",
                                            subdomain
                                        ));
                                    }
                                }
                                DnsRecord::Enr { record } => {
                                    if let BranchKind::Enr = &kind {
                                        let _ = tx.send(Ok(record)).await;

                                        return Ok(());
                                    } else {
                                        return Err(anyhow!(
                                            "Unexpected ENR record in link tree: {}",
                                            subdomain
                                        ));
                                    }
                                }
                                DnsRecord::Root { .. } => {
                                    return Err(anyhow!("Unexpected root record: {}", subdomain));
                                }
                            }
                        } else {
                            debug!("Child {} is empty", subdomain);
                        }

                        Ok(())
                    }
                }
                .await
                {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });
    }

    Box::pin(stream! {
        trace!("Resolving branch {:?}", children);
        while let Some(v) = branches_res.recv().await {
            yield v;
        }
        trace!("Branch {:?} resolution complete", children);
    })
}

fn resolve_tree<B: Backend, K: EnrKeyUnambiguous>(
    task_group: Option<Arc<TaskGroup>>,
    backend: Arc<B>,
    host: String,
    public_key: Option<K::PublicKey>,
    seen_sequence: Option<usize>,
    remote_whitelist: Option<Arc<HashMap<String, K::PublicKey>>>,
) -> QueryStream<K> {
    Box::pin(try_stream! {
        let task_group = task_group.unwrap_or_default();
        let record = backend.get_record(host.clone()).await?;
        if let Some(record) = &record {
            let record = DnsRecord::<K>::from_str(record)?;
            if let DnsRecord::Root(record) = &record {
                if let Some(pk) = public_key {
                    record.verify::<K>(&pk)?;
                }

                let UnsignedRoot { enr_root, link_root, sequence } = &record.base;

                if let Some(seen) = seen_sequence {
                    if *sequence <= seen {
                        // We have already seen this record.
                        return;
                    }
                }

                let mut s = resolve_branch(task_group.clone(), backend.clone(), host.clone(), hashset![ *link_root ], BranchKind::Link { remote_whitelist });
                while let Some(record) = s.try_next().await? {
                    yield record;
                }

                let mut s = resolve_branch(task_group.clone(),backend.clone(), host.clone(), hashset![ *enr_root ], BranchKind::Enr);
                while let Some(record) = s.try_next().await? {
                    yield record;
                }
            } else {
                Err(anyhow!("Expected root, got {:?}", record))?
            }
            trace!("Resolution of tree at {} complete", host);
        } else {
            warn!("No records found for tree {}", host);
        }
    })
}

pub struct Resolver<B: Backend, K: EnrKeyUnambiguous> {
    backend: Arc<B>,
    task_group: Option<Arc<TaskGroup>>,
    seen_sequence: Option<usize>,
    remote_whitelist: Option<Arc<HashMap<String, K::PublicKey>>>,
}

impl<B: Backend, K: EnrKeyUnambiguous> Resolver<B, K> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            task_group: None,
            seen_sequence: None,
            remote_whitelist: None,
        }
    }

    pub fn with_task_group(&mut self, task_group: Arc<TaskGroup>) -> &mut Self {
        self.task_group = Some(task_group);
        self
    }

    pub fn with_seen_sequence(&mut self, seen_sequence: usize) -> &mut Self {
        self.seen_sequence = Some(seen_sequence);
        self
    }

    pub fn with_remote_whitelist(
        &mut self,
        remote_whitelist: Arc<HashMap<String, K::PublicKey>>,
    ) -> &mut Self {
        self.remote_whitelist = Some(remote_whitelist);
        self
    }

    pub fn query(&self, host: impl Display, public_key: Option<K::PublicKey>) -> QueryStream<K> {
        resolve_tree(
            self.task_group.clone(),
            self.backend.clone(),
            host.to_string(),
            public_key,
            self.seen_sequence,
            self.remote_whitelist.clone(),
        )
    }

    pub fn query_tree(&self, tree_link: impl AsRef<str>) -> QueryStream<K> {
        match DnsRecord::<K>::from_str(tree_link.as_ref()).and_then(|link| {
            if let DnsRecord::Link { public_key, domain } = link {
                info!("{}/{}", domain, hex::encode(public_key.encode()));
                Ok((public_key, domain))
            } else {
                bail!("Unexpected record type")
            }
        }) {
            Ok((public_key, domain)) => self.query(domain, Some(public_key)),
            Err(e) => Box::pin(tokio_stream::once(Err(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use maplit::hashmap;
    use secp256k1::{PublicKey, SecretKey};
    use std::collections::{HashMap, HashSet};

    fn test_records_to_hashmap(
        domain: &str,
        records: &[(Option<&str>, &str)],
    ) -> HashMap<String, String> {
        records
            .iter()
            .map(|(sub, entry)| {
                (
                    format!(
                        "{}{}",
                        sub.map(|s| format!("{}.", s)).unwrap_or_default(),
                        domain
                    ),
                    entry.to_string(),
                )
            })
            .collect()
    }

    fn test_records_to_hashmap_geth(records: &[(&str, &str)]) -> HashMap<String, String> {
        records
            .iter()
            .map(|(domain, entry)| (domain.to_string(), entry.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn eip_example() {
        const DOMAIN: &str = "mynodes.org";
        const TEST_RECORDS: &[(Option<&str>, &str)] = &[
            (
                None,
                "enrtree-root:v1 e=JWXYDBPXYWG6FX3GMDIBFA6CJ4 l=C7HRFPF3BLGF3YR4DY5KX3SMBE seq=1 sig=o908WmNp7LibOfPsr4btQwatZJ5URBr2ZAuxvK4UWHlsB9sUOTJQaGAlLPVAhM__XJesCHxLISo94z5Z2a463gA"
            ), (
                Some("C7HRFPF3BLGF3YR4DY5KX3SMBE"),
                "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@morenodes.example.org"
            ), (
                Some("JWXYDBPXYWG6FX3GMDIBFA6CJ4"),
                "enrtree-branch:2XS2367YHAXJFGLZHVAWLQD4ZY,H4FHT4B454P6UXFD7JCYQ5PWDY,MHTDO6TMUBRIA2XWG5LUDACK24",
            ), (
                Some("2XS2367YHAXJFGLZHVAWLQD4ZY"),
                "enr:-HW4QOFzoVLaFJnNhbgMoDXPnOvcdVuj7pDpqRvh6BRDO68aVi5ZcjB3vzQRZH2IcLBGHzo8uUN3snqmgTiE56CH3AMBgmlkgnY0iXNlY3AyNTZrMaECC2_24YYkYHEgdzxlSNKQEnHhuNAbNlMlWJxrJxbAFvA"
            ), (
                Some("H4FHT4B454P6UXFD7JCYQ5PWDY"),
                "enr:-HW4QAggRauloj2SDLtIHN1XBkvhFZ1vtf1raYQp9TBW2RD5EEawDzbtSmlXUfnaHcvwOizhVYLtr7e6vw7NAf6mTuoCgmlkgnY0iXNlY3AyNTZrMaECjrXI8TLNXU0f8cthpAMxEshUyQlK-AM0PW2wfrnacNI"
            ), (
                Some("MHTDO6TMUBRIA2XWG5LUDACK24"),
                "enr:-HW4QLAYqmrwllBEnzWWs7I5Ev2IAs7x_dZlbYdRdMUx5EyKHDXp7AV5CkuPGUPdvbv1_Ms1CPfhcGCvSElSosZmyoqAgmlkgnY0iXNlY3AyNTZrMaECriawHKWdDRk2xeZkrOXBQ0dfMFLHY4eENZwdufn1S1o"
            )
        ];

        let data = test_records_to_hashmap(DOMAIN, TEST_RECORDS);

        let mut s = Resolver::<_, SecretKey>::new(Arc::new(data))
            .with_remote_whitelist(Arc::new(hashmap!{
                "morenodes.example.org".to_string() => PublicKey::from_slice(&hex!("049f88229042fef9200246f49f94d9b77c4e954721442714e85850cb6d9e5daf2d880ea0e53cb3ac1a75f9923c2726a4f941f7d326781baa6380754a360de5c2b6")).unwrap()
            }))
            .query(DOMAIN.to_string(), None);
        let mut out = HashSet::new();
        while let Some(record) = s.try_next().await.unwrap() {
            assert!(out.insert(record.to_base64()));
        }
        assert_eq!(
            out,
            hashset![
                "enr:-HW4QOFzoVLaFJnNhbgMoDXPnOvcdVuj7pDpqRvh6BRDO68aVi5ZcjB3vzQRZH2IcLBGHzo8uUN3snqmgTiE56CH3AMBgmlkgnY0iXNlY3AyNTZrMaECC2_24YYkYHEgdzxlSNKQEnHhuNAbNlMlWJxrJxbAFvA",
                "enr:-HW4QAggRauloj2SDLtIHN1XBkvhFZ1vtf1raYQp9TBW2RD5EEawDzbtSmlXUfnaHcvwOizhVYLtr7e6vw7NAf6mTuoCgmlkgnY0iXNlY3AyNTZrMaECjrXI8TLNXU0f8cthpAMxEshUyQlK-AM0PW2wfrnacNI",
                "enr:-HW4QLAYqmrwllBEnzWWs7I5Ev2IAs7x_dZlbYdRdMUx5EyKHDXp7AV5CkuPGUPdvbv1_Ms1CPfhcGCvSElSosZmyoqAgmlkgnY0iXNlY3AyNTZrMaECriawHKWdDRk2xeZkrOXBQ0dfMFLHY4eENZwdufn1S1o",
            ].into_iter().map(ToString::to_string).collect()
        );
    }

    #[tokio::test]
    async fn bad_node() {
        const TEST_RECORDS: &[(&str, &str)] = &[
            ("n",                            "enrtree-root:v1 e=INDMVBZEEQ4ESVYAKGIYU74EAA l=C7HRFPF3BLGF3YR4DY5KX3SMBE seq=3 sig=Vl3AmunLur0JZ3sIyJPSH6A3Vvdp4F40jWQeCmkIhmcgwE4VC5U9wpK8C_uL_CMY29fd6FAhspRvq2z_VysTLAA"),
		    ("C7HRFPF3BLGF3YR4DY5KX3SMBE.n", "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@morenodes.example.org"),
		    ("INDMVBZEEQ4ESVYAKGIYU74EAA.n", "enr:-----"),
        ];

        let data = test_records_to_hashmap_geth(TEST_RECORDS);

        let err = Resolver::<_, SecretKey>::new(Arc::new(data))
            .query_tree("enrtree://AKPYQIUQIL7PSIACI32J7FGZW56E5FKHEFCCOFHILBIMW3M6LWXS2@n")
            .collect::<Result<Vec<_>, _>>()
            .await
            .unwrap_err();
        if !err.chain().any(|e| e.is::<InvalidEnr>()) {
            unreachable!("should have seen the correct error")
        }
    }
}
