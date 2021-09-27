use crate::kv::tables::AccountChange;

use super::*;

#[async_trait]
impl HistoryKind for AccountHistory {
    type Key = Address;
    type Value = EncodedAccount;
    type IndexChunkKey = Address;
    type IndexTable = tables::AccountHistory;
    type ChangeSetTable = tables::AccountChangeSet;
    type EncodedStream<'cs> = impl EncodedStream<'cs, Self::ChangeSetTable>;

    fn index_chunk_key(key: Self::Key) -> Self::IndexChunkKey {
        key
    }
    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: BlockNumber,
        needle: Self::Key,
    ) -> anyhow::Result<Option<Self::Value>>
    where
        C: CursorDupSort<'tx, Self::ChangeSetTable>,
    {
        if let Some((_, v)) = cursor.seek_both_range(block_number, needle).await? {
            let (_, (address, account)) = Self::decode(block_number, v);

            if address == needle {
                return Ok(Some(account));
            }
        }

        Ok(None)
    }

    fn encode(block_number: BlockNumber, s: &ChangeSet<Self>) -> Self::EncodedStream<'_> {
        s.iter().map(move |(address, account)| {
            (
                block_number,
                AccountChange {
                    address: *address,
                    account: account.clone(),
                },
            )
        })
    }

    fn decode(
        block_number: <Self::ChangeSetTable as Table>::Key,
        AccountChange { address, account }: <Self::ChangeSetTable as Table>::Value,
    ) -> (BlockNumber, Change<Self::Key, Self::Value>) {
        (block_number, (address, account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_types::Address;
    use hex_literal::hex;

    #[test]
    fn account_encoding() {
        let mut ch = ChangeSet::<AccountHistory>::default();

        for (i, val) in vec![
            &hex!("f7f6db1eb17c6d582078e0ffdd0c") as &[u8],
            &hex!("b1e9b5c16355eede662031dd621d08faf4ea") as &[u8],
            &hex!("862cf52b74f1cea41ddd8ffa4b3e7c7790") as &[u8],
        ]
        .into_iter()
        .enumerate()
        {
            let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abBc{}", i)
                .parse::<Address>()
                .unwrap();

            ch.insert((address, val.iter().copied().collect()));
        }

        let mut ch2 = AccountChangeSet::new();

        for (k, v) in AccountHistory::encode(1.into(), &ch) {
            let (_, change) = AccountHistory::decode(k, v);

            ch2.insert(change);
        }

        assert_eq!(ch, ch2);
    }
}
