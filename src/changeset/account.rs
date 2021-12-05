use crate::kv::tables::AccountChange;

use super::*;

#[async_trait]
impl HistoryKind for AccountHistory {
    type Key = Address;
    type Value = Option<Account>;
    type IndexTable = tables::AccountHistory;
    type ChangeSetTable = tables::AccountChangeSet;
    type EncodedStream<'cs> = impl EncodedStream<'cs, Self::ChangeSetTable>;

    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: BlockNumber,
        needle: Self::Key,
    ) -> anyhow::Result<Option<Self::Value>>
    where
        C: CursorDupSort<'tx, Self::ChangeSetTable>,
    {
        if let Some(v) = cursor.seek_both_range(block_number, needle).await? {
            let (_, (address, account)) = Self::decode(block_number, v);

            if address == needle {
                return Ok(Some(account));
            }
        }

        Ok(None)
    }

    fn encode(block_number: BlockNumber, s: &ChangeSet<Self>) -> Self::EncodedStream<'_> {
        s.iter()
            .map(move |(&address, &account)| (block_number, AccountChange { address, account }))
    }

    fn decode(
        block_number: <Self::ChangeSetTable as Table>::Key,
        AccountChange { address, account }: <Self::ChangeSetTable as Table>::Value,
    ) -> (BlockNumber, (Self::Key, Self::Value)) {
        (block_number, (address, account))
    }
}
