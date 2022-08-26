// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of OpenEthereum.

// OpenEthereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// OpenEthereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with OpenEthereum.  If not, see <http://www.gnu.org/licenses/>.

use client::{BlockId, EngineClient};
use db;
use engines::{
    parlia::{util::recover_creator, ADDRESS_LENGTH, SIGNATURE_LENGTH, VANITY_LENGTH},
    EngineError,
};
use error::Error;
use ethereum_types::{Address, H256};
use kvdb::KeyValueDB;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use types::header::Header;

/// Snapshot for each block.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Snapshot {
    pub epoch: u64,
    pub number: u64,
    pub hash: H256,

    /// a list of all valid validators, sorted by ascending order.
    pub validators: BTreeSet<Address>,
    pub recents: BTreeMap<u64, Address>,
}

impl Snapshot {
    pub fn new(validators: BTreeSet<Address>, number: u64, hash: H256, epoch: u64) -> Self {
        Snapshot {
            validators,
            number,
            hash,
            epoch,
            ..Default::default()
        }
    }

    pub fn load(db_ins: Arc<dyn KeyValueDB>, hash: &H256) -> Option<Snapshot> {
        if let Ok(b) = db_ins.get(db::COL_PARLIA_SNAPSHOT, hash) {
            if b.is_none() {
                return None;
            }
            if let Ok(snap) = serde_json::from_slice(&b.unwrap()) {
                return Some(snap);
            }
            None
        } else {
            None
        }
    }

    pub fn store(&self, db_ins: Arc<dyn KeyValueDB>) {
        let value = serde_json::to_vec(&self).unwrap();
        let mut tx = db_ins.transaction();
        tx.put(db::COL_PARLIA_SNAPSHOT, &self.hash, &value);
        db_ins.write(tx).unwrap();
    }

    pub fn clone(&self) -> Snapshot {
        Snapshot {
            validators: self.validators.clone(),
            recents: self.recents.clone(),
            ..*self
        }
    }

    /// Apply a new header to current snap
    pub fn apply(
        &mut self,
        client: Arc<dyn EngineClient>,
        header: &Header,
        chain_id: &u64,
    ) -> Result<Snapshot, Error> {
        let num = header.number();
        if self.number + 1 != num {
            Err(EngineError::ParliaUnContinuousHeader)?
        }
        let creator = recover_creator(header, chain_id)?;
        let mut snap = self.clone();
        snap.hash = header.hash();
        snap.number = num;
        let limit = (snap.validators.len() / 2 + 1) as u64;
        if num >= limit {
            snap.recents.remove(&(num - limit));
        }
        if !snap.validators.contains(&creator) {
            Err(EngineError::ParliaUnauthorizedValidator)?
        }
        for (_, recent) in snap.recents.iter() {
            if *recent == creator {
                Err(EngineError::ParliaRecentlySigned)?
            }
        }
        snap.recents.insert(num, creator);
        if num > 0 && num % snap.epoch == (snap.validators.len() / 2) as u64 {
            let checkpoint_header =
                find_ancient_header(client, header, (snap.validators.len() / 2) as u64)?;
            let extra = checkpoint_header.extra_data();
            let validator_bytes = &extra[VANITY_LENGTH..(extra.len() - SIGNATURE_LENGTH)];
            let new_validators = parse_validators(validator_bytes)?;
            let old_limit = snap.validators.len() / 2 + 1;
            let new_limit = new_validators.len() / 2 + 1;
            if new_limit < old_limit {
                for i in 0..(old_limit - new_limit) {
                    snap.recents.remove(&(num - ((new_limit + i) as u64)));
                }
            }
            snap.validators = new_validators;
        }
        Ok(snap)
    }

    /// Returns true if the block difficulty should be `inturn`
    pub fn inturn(&self, author: &Address) -> bool {
        let suppose_val = &self.suppose_validator();
        suppose_val == author
    }

    pub fn suppose_validator(&self) -> Address {
        let index = (self.number + 1) % (self.validators.len() as u64);
        let mut ite = self.validators.iter();
        let mut res = ite.next().unwrap();
        if index > 0 {
            for _ in 0..index {
                res = ite.next().unwrap();
            }
        }
        return *res;
    }

    pub fn back_off_time(&self, _: &Address) -> u64 {
        // TODO, so far so good.
        return 0;
    }
}

fn find_ancient_header(
    client: Arc<dyn EngineClient>,
    header: &Header,
    ite: u64,
) -> Result<Header, Error> {
    let cur_header_op = Some(header.clone());
    let mut cur_header = cur_header_op.unwrap();
    for _ in 0..ite {
        let cur_header_op = client.block_header(BlockId::Hash(*cur_header.parent_hash()));
        if cur_header_op.is_none() {
            Err(EngineError::ParliaUnContinuousHeader)?
        }
        cur_header = cur_header_op.unwrap().decode()?;
    }
    Ok(cur_header)
}

pub fn parse_validators(validators_bytes: &[u8]) -> Result<BTreeSet<Address>, Error> {
    if validators_bytes.len() % ADDRESS_LENGTH != 0 {
        Err(EngineError::ParliaInvalidValidatorBytes)?
    }
    let n = validators_bytes.len() / ADDRESS_LENGTH;
    let mut validators = BTreeSet::new();
    for i in 0..n {
        let s: &[u8] = &validators_bytes[(i * ADDRESS_LENGTH)..((i + 1) * ADDRESS_LENGTH)];
        validators.insert(Address::from_slice(s) as Address);
    }
    Ok(validators)
}
