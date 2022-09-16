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
        let txn = self.begin()?;

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
        let mut number_cursor = txn.cursor(tables::HeaderNumber)?;
        let mut header_cursor = txn.cursor(tables::Header)?;

        let mut next_number = match params.start {
            BlockId::Hash(hash) => number_cursor.seek_exact(hash)?.map(|(_, k)| k),
            BlockId::Number(number) => Some(number),
        };

        for _ in 0..limit {
            match next_number {
                Some(block_number) => {
                    if let Some((_, header)) = header_cursor.seek_exact(block_number)? {
                        headers.push(header);
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
            .filter_map(|hash| txn.get(tables::HeaderNumber, hash).unwrap_or(None))
            .filter_map(|number| {
                chain::block_body::read_without_senders(&txn, number).unwrap_or(None)
            })
            .collect::<Vec<_>>())
    }
}
