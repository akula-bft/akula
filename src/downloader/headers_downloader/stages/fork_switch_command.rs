use super::{
    headers::{
        header_slices,
        header_slices::{align_block_num_to_slice_start, HeaderSliceStatus, HeaderSlices},
    },
    SaveStage,
};
use crate::{kv::traits::MutableTransaction, models::BlockNumber};
use std::{
    ops::{DerefMut, Range},
    sync::Arc,
};

pub struct ForkSwitchCommand {
    header_slices: Arc<HeaderSlices>,
    fork_header_slices: Arc<HeaderSlices>,
    canonical_range: Range<BlockNumber>,
    fork_range: Range<BlockNumber>,
    connection_block_num: BlockNumber,
}

impl ForkSwitchCommand {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        fork_header_slices: Arc<HeaderSlices>,
        canonical_range: Range<BlockNumber>,
        fork_range: Range<BlockNumber>,
        connection_block_num: BlockNumber,
    ) -> Self {
        ForkSwitchCommand {
            header_slices,
            fork_header_slices,
            canonical_range,
            fork_range,
            connection_block_num,
        }
    }

    pub fn connection_block_num(&self) -> BlockNumber {
        self.connection_block_num
    }

    fn is_canonical_slice_status(status: HeaderSliceStatus) -> bool {
        (status == HeaderSliceStatus::Verified) || (status == HeaderSliceStatus::Saved)
    }

    pub async fn execute<'tx, 'db: 'tx, RwTx: MutableTransaction<'db>>(
        self,
        tx: &'tx RwTx,
    ) -> anyhow::Result<()> {
        self.switch_to_fork();
        self.update_canonical_chain_headers(tx).await
    }

    fn switch_to_fork(&self) {
        // promote the fork chain
        let mut num = self.fork_range.start;
        while num < self.fork_range.end {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let fork_slice_lock = self
                .fork_header_slices
                .find_by_start_block_num(num)
                .unwrap();

            let mut slice_mut = slice_lock.write();
            let slice = slice_mut.deref_mut();

            let mut fork_slice_mut = fork_slice_lock.write();
            let fork_slice = fork_slice_mut.deref_mut();

            // promote the status
            self.header_slices
                .set_slice_status(slice, fork_slice.status);
            slice.headers = fork_slice.headers.take();
            slice.refetch_attempt = 0;

            num = BlockNumber(num.0 + slice.len() as u64);
        }

        // adjust num to point to the first canonical slice after the fork
        // in case if the last fork slice is partial
        num = align_block_num_to_slice_start(BlockNumber(
            num.0 + (header_slices::HEADER_SLICE_SIZE as u64) - 1,
        ));

        // discard the canonical chain after the fork
        while num < self.canonical_range.end {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let mut slice_mut = slice_lock.write();
            let slice = slice_mut.deref_mut();

            // slices within the canonical range past the fork must be in a canonical status
            assert!(Self::is_canonical_slice_status(slice.status));
            let len = slice.len();
            assert!(len > 0, "a canonical chain slice must have headers");

            // reset the status
            self.header_slices
                .set_slice_status(slice, HeaderSliceStatus::Empty);
            slice.headers = None;
            slice.refetch_attempt = 0;

            num = BlockNumber(num.0 + len as u64);
        }

        // discard fork
        self.fork_header_slices.clear();
    }

    async fn update_canonical_chain_headers<'tx, 'db: 'tx, RwTx: MutableTransaction<'db>>(
        &self,
        tx: &'tx RwTx,
    ) -> anyhow::Result<()> {
        let mut num = self.fork_range.start;
        while num < self.fork_range.end {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let len = slice_lock.read().len();

            let headers = {
                let slice = slice_lock.read();
                if slice.status == HeaderSliceStatus::Saved {
                    // this clone happens mostly on the stack (except extra_data)
                    slice.headers.clone()
                } else {
                    None
                }
            };

            if let Some(headers) = headers {
                for header in headers {
                    if header.number() <= self.connection_block_num {
                        continue;
                    }
                    SaveStage::update_canonical_chain_header(&header, tx).await?;
                }
            }

            num = BlockNumber(num.0 + len as u64);
        }

        Ok(())
    }
}
