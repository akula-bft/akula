use super::{delta::*, object::*, *};
use crate::{crypto::*, models::*};
use bytes::Bytes;
use evmodin::host::AccessStatus;
use hex_literal::hex;
use std::{collections::*, fmt::Debug};

#[derive(Debug)]
pub struct Snapshot {
    journal_size: usize,
    log_size: usize,
    refund: u64,
}

#[derive(Debug)]
pub struct IntraBlockState<'db, S>
where
    S: State,
{
    db: &'db mut S,

    pub(crate) objects: HashMap<Address, Object>,
    pub(crate) storage: HashMap<Address, Storage>,
    pub(crate) incarnations: HashMap<Address, u64>,

    // pointer stability?
    pub(crate) existing_code: HashMap<H256, Bytes>,
    pub(crate) new_code: HashMap<H256, Bytes>,

    pub(crate) journal: Vec<Delta>,

    // substate
    pub(crate) self_destructs: HashSet<Address>,
    pub(crate) logs: Vec<Log>,
    pub(crate) touched: HashSet<Address>,
    pub(crate) refund: u64,
    // EIP-2929 substate
    pub(crate) accessed_addresses: HashSet<Address>,
    pub(crate) accessed_storage_keys: HashMap<Address, HashSet<U256>>,
}

async fn get_object<'m, 'db, S: State>(
    db: &S,
    objects: &'m mut HashMap<Address, Object>,
    address: Address,
) -> anyhow::Result<Option<&'m mut Object>> {
    Ok(match objects.entry(address) {
        hash_map::Entry::Occupied(entry) => Some(entry.into_mut()),
        hash_map::Entry::Vacant(entry) => {
            let accdata = db.read_account(address).await?;

            if let Some(account) = accdata {
                Some(entry.insert(Object {
                    initial: Some(account),
                    current: Some(account),
                }))
            } else {
                None
            }
        }
    })
}

async fn ensure_object<'m: 'j, 'j, S: State>(
    db: &S,
    objects: &'m mut HashMap<Address, Object>,
    journal: &'j mut Vec<Delta>,
    address: Address,
) -> anyhow::Result<()> {
    if let Some(obj) = get_object(db, objects, address).await? {
        if obj.current.is_none() {
            journal.push(Delta::Update {
                address,
                previous: obj.clone(),
            });
            obj.current = Some(Account::default());
        }
    } else {
        journal.push(Delta::Create { address });
        objects.entry(address).insert(Object {
            current: Some(Account::default()),
            ..Default::default()
        });
    }

    Ok(())
}

async fn get_or_create_object<'m: 'j, 'j, S: State>(
    db: &S,
    objects: &'m mut HashMap<Address, Object>,
    journal: &'j mut Vec<Delta>,
    address: Address,
) -> anyhow::Result<&'m mut Object> {
    ensure_object(db, objects, journal, address).await?;
    Ok(objects.get_mut(&address).unwrap())
}

impl<'storage, 'r, S: State> IntraBlockState<'r, S> {
    pub fn new(db: &'r mut S) -> Self {
        Self {
            db,
            objects: Default::default(),
            storage: Default::default(),
            incarnations: Default::default(),
            existing_code: Default::default(),
            new_code: Default::default(),
            journal: Default::default(),
            self_destructs: Default::default(),
            logs: Default::default(),
            touched: Default::default(),
            refund: Default::default(),
            accessed_addresses: Default::default(),
            accessed_storage_keys: Default::default(),
        }
    }

    pub fn db(&mut self) -> &mut S {
        self.db
    }

    pub async fn exists(&mut self, address: Address) -> anyhow::Result<bool> {
        let obj = get_object(self.db, &mut self.objects, address).await?;

        if let Some(obj) = obj {
            if obj.current.is_some() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    // https://eips.ethereum.org/EIPS/eip-161
    pub async fn is_dead(&mut self, address: Address) -> anyhow::Result<bool> {
        let obj = get_object(self.db, &mut self.objects, address).await?;

        if let Some(obj) = obj {
            if let Some(current) = &obj.current {
                return Ok(current.code_hash == EMPTY_HASH
                    && current.nonce == 0
                    && current.balance == 0);
            }
        }

        Ok(true)
    }

    pub async fn create_contract(&mut self, address: Address) -> anyhow::Result<()> {
        let mut current = Account::default();
        let mut initial = None;

        self.journal.push({
            if let Some(prev) = get_object(self.db, &mut self.objects, address).await? {
                initial = prev.initial;
                if let Some(prev_current) = &prev.current {
                    current.balance = prev_current.balance;
                }
                Delta::Update {
                    address,
                    previous: prev.clone(),
                }
            } else {
                Delta::Create { address }
            }
        });

        *self.incarnations.entry(address).or_default() += 1;
        self.journal.push(Delta::Incarnation { address });

        self.objects.insert(
            address,
            Object {
                current: Some(current),
                initial,
            },
        );

        if let Some(removed) = self.storage.remove(&address) {
            self.journal.push(Delta::StorageWipe {
                address,
                storage: removed,
            });
        } else {
            self.journal.push(Delta::StorageCreate { address });
        }

        Ok(())
    }

    pub async fn destruct(&mut self, address: Address) -> anyhow::Result<()> {
        // Doesn't create a delta since it's called at the end of a transcation,
        // when we don't need snapshots anymore.

        *self.incarnations.entry(address).or_default() += 1;
        self.storage.remove(&address);
        if let Some(obj) = get_object(self.db, &mut self.objects, address).await? {
            obj.current = None;
        }

        Ok(())
    }

    pub fn record_selfdestruct(&mut self, address: Address) {
        if self.self_destructs.insert(address) {
            self.journal.push(Delta::Selfdestruct { address });
        }
    }
    pub async fn destruct_selfdestructs(&mut self) -> anyhow::Result<()> {
        for address in self.self_destructs.iter().copied().collect::<Vec<_>>() {
            self.destruct(address).await?;
        }

        Ok(())
    }
    pub async fn destruct_touched_dead(&mut self) -> anyhow::Result<()> {
        for address in self.touched.iter().copied().collect::<Vec<_>>() {
            if self.is_dead(address).await? {
                self.destruct(address).await?;
            }
        }

        Ok(())
    }

    pub fn number_of_self_destructs(&self) -> usize {
        self.self_destructs.len()
    }

    pub async fn get_balance(&mut self, address: Address) -> anyhow::Result<U256> {
        Ok(get_object(self.db, &mut self.objects, address)
            .await?
            .map(|object| object.current.as_ref().map(|current| current.balance))
            .flatten()
            .unwrap_or(U256::ZERO))
    }
    pub async fn set_balance(
        &mut self,
        address: Address,
        value: impl AsU256,
    ) -> anyhow::Result<()> {
        let obj =
            get_or_create_object(self.db, &mut self.objects, &mut self.journal, address).await?;

        let current = obj.current.as_mut().unwrap();
        self.journal.push(Delta::UpdateBalance {
            address,
            previous: current.balance,
        });
        current.balance = value.as_u256();
        self.touch(address);

        Ok(())
    }
    pub async fn add_to_balance(
        &mut self,
        address: Address,
        addend: impl AsU256,
    ) -> anyhow::Result<()> {
        let obj =
            get_or_create_object(self.db, &mut self.objects, &mut self.journal, address).await?;

        let current = obj.current.as_mut().unwrap();
        self.journal.push(Delta::UpdateBalance {
            address,
            previous: current.balance,
        });
        current.balance += addend.as_u256();
        self.touch(address);

        Ok(())
    }
    pub async fn subtract_from_balance(
        &mut self,
        address: Address,
        subtrahend: U256,
    ) -> anyhow::Result<()> {
        let obj =
            get_or_create_object(self.db, &mut self.objects, &mut self.journal, address).await?;

        let current = obj.current.as_mut().unwrap();
        self.journal.push(Delta::UpdateBalance {
            address,
            previous: current.balance,
        });
        current.balance -= subtrahend;
        self.touch(address);

        Ok(())
    }

    pub fn touch(&mut self, address: Address) {
        let inserted = self.touched.insert(address);

        // See Yellow Paper, Appendix K "Anomalies on the Main Network"
        const RIPEMD_ADDRESS: Address = H160(hex!("0000000000000000000000000000000000000003"));
        if inserted && address != RIPEMD_ADDRESS {
            self.journal.push(Delta::Touch { address });
        }
    }

    pub async fn get_nonce(&mut self, address: Address) -> anyhow::Result<u64> {
        if let Some(object) = get_object(self.db, &mut self.objects, address).await? {
            if let Some(current) = &object.current {
                return Ok(current.nonce);
            }
        }

        Ok(0)
    }
    pub async fn set_nonce(&mut self, address: Address, nonce: u64) -> anyhow::Result<()> {
        let object =
            get_or_create_object(self.db, &mut self.objects, &mut self.journal, address).await?;
        self.journal.push(Delta::Update {
            address,
            previous: object.clone(),
        });

        object.current.as_mut().unwrap().nonce = nonce;

        Ok(())
    }

    pub async fn get_code(&mut self, address: Address) -> anyhow::Result<Option<Bytes>> {
        let obj = get_object(self.db, &mut self.objects, address).await?;

        if let Some(obj) = obj {
            if let Some(current) = &obj.current {
                let code_hash = current.code_hash;
                if code_hash != EMPTY_HASH {
                    if let Some(code) = self.new_code.get(&code_hash) {
                        return Ok(Some(code.clone()));
                    }

                    if let Some(code) = self.existing_code.get(&code_hash) {
                        return Ok(Some(code.clone()));
                    }

                    let code = self.db.read_code(code_hash).await?;
                    self.existing_code.insert(code_hash, code.clone());
                    return Ok(Some(code));
                }
            }
        }

        Ok(None)
    }

    pub async fn get_code_hash(&mut self, address: Address) -> anyhow::Result<H256> {
        if let Some(object) = get_object(self.db, &mut self.objects, address).await? {
            if let Some(current) = &object.current {
                return Ok(current.code_hash);
            }
        }

        Ok(EMPTY_HASH)
    }

    pub async fn set_code(&mut self, address: Address, code: Bytes) -> anyhow::Result<()> {
        let obj =
            get_or_create_object(self.db, &mut self.objects, &mut self.journal, address).await?;
        self.journal.push(Delta::Update {
            address,
            previous: obj.clone(),
        });
        obj.current.as_mut().unwrap().code_hash = keccak256(&code);

        // Don't overwrite already existing code so that views of it
        // that were previously returned by get_code() are still valid.
        self.new_code
            .entry(obj.current.as_mut().unwrap().code_hash)
            .or_insert(code);

        Ok(())
    }

    pub fn access_account(&mut self, address: Address) -> AccessStatus {
        if self.accessed_addresses.insert(address) {
            self.journal.push(Delta::AccountAccess { address });

            AccessStatus::Cold
        } else {
            AccessStatus::Warm
        }
    }

    pub fn access_storage(&mut self, address: Address, key: U256) -> AccessStatus {
        if self
            .accessed_storage_keys
            .entry(address)
            .or_default()
            .insert(key)
        {
            self.journal.push(Delta::StorageAccess { address, key });

            AccessStatus::Cold
        } else {
            AccessStatus::Warm
        }
    }

    async fn get_storage(
        &mut self,
        address: Address,
        key: U256,
        original: bool,
    ) -> anyhow::Result<U256> {
        if let Some(obj) = get_object(self.db, &mut self.objects, address).await? {
            if obj.current.is_some() {
                let storage = self.storage.entry(address).or_default();

                if !original {
                    if let Some(v) = storage.current.get(&key) {
                        return Ok(*v);
                    }
                }

                if let Some(v) = storage.committed.get(&key) {
                    return Ok(v.original);
                }

                if obj.initial.is_none() || self.incarnations.contains_key(&address) {
                    return Ok(U256::ZERO);
                }

                let val = self.db.read_storage(address, key).await?;

                self.storage.entry(address).or_default().committed.insert(
                    key,
                    CommittedValue {
                        initial: val,
                        original: val,
                    },
                );

                return Ok(val);
            }
        }

        Ok(U256::ZERO)
    }

    pub async fn get_current_storage(
        &mut self,
        address: Address,
        key: U256,
    ) -> anyhow::Result<U256> {
        self.get_storage(address, key, false).await
    }

    // https://eips.ethereum.org/EIPS/eip-2200
    pub async fn get_original_storage(
        &mut self,
        address: Address,
        key: U256,
    ) -> anyhow::Result<U256> {
        self.get_storage(address, key, true).await
    }

    pub async fn set_storage(
        &mut self,
        address: Address,
        key: U256,
        value: U256,
    ) -> anyhow::Result<()> {
        let previous = self.get_current_storage(address, key).await?;
        if previous == value {
            return Ok(());
        }
        self.storage
            .entry(address)
            .or_default()
            .current
            .insert(key, value);

        self.journal.push(Delta::StorageChange {
            address,
            key,
            previous,
        });

        Ok(())
    }

    pub async fn write_to_db(self, block_number: BlockNumber) -> anyhow::Result<()> {
        self.db.begin_block(block_number);

        for (address, incarnation) in self.incarnations {
            if incarnation > 0 {
                self.db.erase_storage(address).await?
            }
        }

        for (address, storage) in self.storage {
            if let Some(obj) = self.objects.get(&address) {
                if obj.current.is_some() {
                    for (key, val) in &storage.committed {
                        self.db
                            .update_storage(address, *key, val.initial, val.original)
                            .await?;
                    }
                }
            }
        }

        for (address, obj) in self.objects {
            self.db.update_account(address, obj.initial, obj.current);
        }

        for (code_hash, code) in self.new_code {
            self.db.update_code(code_hash, code).await?
        }

        Ok(())
    }

    pub fn take_snapshot(&self) -> Snapshot {
        Snapshot {
            journal_size: self.journal.len(),
            log_size: self.logs.len(),
            refund: self.refund,
        }
    }
    pub fn revert_to_snapshot(&mut self, snapshot: Snapshot) {
        for _ in 0..self.journal.len() - snapshot.journal_size {
            self.journal.pop().unwrap().revert(self);
        }
        self.logs.truncate(snapshot.log_size);
        self.refund = snapshot.refund;
    }

    pub fn finalize_transaction(&mut self) {
        for storage in self.storage.values_mut() {
            for (key, val) in &storage.current {
                storage.committed.entry(*key).or_default().original = *val;
            }
            storage.current.clear();
        }
    }

    // See Section 6.1 "Substate" of the Yellow Paper
    pub fn clear_journal_and_substate(&mut self) {
        self.journal.clear();

        // and the substate
        self.self_destructs.clear();
        self.logs.clear();
        self.touched.clear();
        self.refund = 0;
        // EIP-2929
        self.accessed_addresses.clear();
        self.accessed_storage_keys.clear();
    }

    pub fn add_log(&mut self, log: Log) {
        self.logs.push(log);
    }

    pub fn logs(&self) -> &[Log] {
        &self.logs
    }

    pub fn add_refund(&mut self, addend: u64) {
        self.refund += addend;
    }

    pub fn subtract_refund(&mut self, subtrahend: u64) {
        self.refund -= subtrahend;
    }

    pub fn get_refund(&self) -> u64 {
        self.refund
    }
}
