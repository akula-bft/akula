use crate::{
    accessors::chain,
    kv::{tables, MdbxWithDirHandle},
    models::{BlockBody, BlockHeader, BlockNumber, H256},
    p2p::types::{BlockId, GetBlockHeadersParams},
};
use mdbx::EnvironmentKind;
use std::fmt::Debug;

pub trait Stash: Send + Sync + Debug {
    fn get_headers(&self, _: GetBlockHeadersParams) -> anyhow::Result<Vec<BlockHeader>>;
    fn get_bodies(&self, _: Vec<H256>) -> anyhow::Result<Vec<BlockBody>>;
}

impl Stash for () {
    fn get_headers(&self, _: GetBlockHeadersParams) -> anyhow::Result<Vec<BlockHeader>> {
        Ok(vec![])
    }
    fn get_bodies(&self, _: Vec<H256>) -> anyhow::Result<Vec<BlockBody>> {
        Ok(vec![])
    }
}

impl<E> Stash for MdbxWithDirHandle<E>
where
    E: EnvironmentKind,
{
    fn get_headers(&self, params: GetBlockHeadersParams) -> anyhow::Result<Vec<BlockHeader>> {
        let txn = self.begin().expect("Failed to begin transaction");

        let limit = std::cmp::min(params.limit, 1024);
        let reverse = params.reverse == 1;

        let mut add_op = if params.skip == 0 {
            1
        } else {
            params.skip as i64 + 1
        };
        if reverse {
            add_op = -add_op;
        }

        let mut headers = Vec::with_capacity(limit as usize);
        let mut number_cursor = txn
            .cursor(tables::HeaderNumber)
            .expect("Failed to open cursor, likely a DB corruption");
        let mut canonical_cursor = txn
            .cursor(tables::CanonicalHeader)
            .expect("Failed to open cursor, likely a DB corruption");
        let mut header_cursor = txn
            .cursor(tables::Header)
            .expect("Failed to open cursor, likely a DB corruption");

        let mut next_number = match params.start {
            BlockId::Hash(hash) => number_cursor.seek_exact(hash)?.map(|(_, k)| k),
            BlockId::Number(number) => Some(number),
        };

        for _ in 0..limit {
            match next_number {
                Some(block_number) => {
                    if let Some(header_key) = canonical_cursor.seek_exact(block_number)? {
                        if let Some((_, header)) = header_cursor.seek_exact(header_key)? {
                            headers.push(header);
                        }
                    }
                    next_number = u64::try_from(block_number.0 as i64 + add_op)
                        .ok()
                        .map(BlockNumber);
                }
                None => break,
            };
        }

        Ok::<_, anyhow::Error>(headers)
    }

    fn get_bodies(&self, hashes: Vec<H256>) -> anyhow::Result<Vec<BlockBody>> {
        let txn = self.begin().expect("Failed to begin transaction");

        Ok(hashes
            .into_iter()
            .filter_map(|hash| {
                txn.get(tables::HeaderNumber, hash)
                    .unwrap_or(None)
                    .map(|number| (hash, number))
            })
            .filter_map(|(hash, number)| {
                chain::block_body::read_without_senders(&txn, hash, number).unwrap_or(None)
            })
            .collect::<Vec<_>>())
    }
}
