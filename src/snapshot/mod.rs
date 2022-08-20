use crate::{
    kv::traits::{TableDecode, TableObject},
    models::*,
};
use anyhow::{bail, format_err};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

pub const DEFAULT_STRIDE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();

pub const INDEX_FILE_NAME: &str = "index";
pub const SEGMENT_FILE_NAME: &str = "segment";
pub const READY_FILE_NAME: &str = "ready";

pub trait SnapshotVersion {
    const ID: &'static str;
    const STRIDE: NonZeroUsize;
}

pub trait SnapshotObject: TableObject {
    const ID: &'static str;
}

#[derive(Debug)]
pub struct V1;

impl SnapshotVersion for V1 {
    const ID: &'static str = "v1";
    const STRIDE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();
}

#[derive(Debug)]
struct Snapshot {
    total_items: usize,
    segment_len: usize,
    segment: BufReader<File>,
    index: BufReader<File>,
}

impl Snapshot {
    fn read(&mut self, idx: usize) -> anyhow::Result<Vec<u8>> {
        {
            let idx_seek_pos = (idx * 8) as u64;
            let idx_seeked_to = self.index.seek(SeekFrom::Start(idx_seek_pos))?;
            if idx_seeked_to != idx_seek_pos {
                bail!("idx seek invalid: {idx_seeked_to} != {idx_seek_pos}");
            }
        }

        let mut seg_seek_pos_buf = [0_u8; 8];
        self.index.read_exact(&mut seg_seek_pos_buf)?;
        let seg_seek_pos = u64::from_be_bytes(seg_seek_pos_buf);

        let entry_size = if idx + 1 < self.total_items {
            let mut seg_seek_end_buf = [0_u8; 8];
            self.index.read_exact(&mut seg_seek_end_buf)?;
            let seg_seek_end = u64::from_be_bytes(seg_seek_end_buf);

            let entry_size = seg_seek_end
                .checked_sub(seg_seek_pos)
                .ok_or_else(|| format_err!("size negative"))? as usize;

            let seg_seeked_to = self.segment.seek(SeekFrom::Start(seg_seek_pos))?;
            if seg_seeked_to != seg_seek_pos {
                bail!("seg seek invalid: {seg_seeked_to} != {seg_seek_pos}");
            }

            entry_size
        } else {
            self.segment_len - seg_seek_pos as usize
        };

        let mut entry = vec![0; entry_size];

        self.segment.read_exact(&mut entry)?;

        Ok(entry)
    }
}

#[derive(Debug)]
pub struct Snapshotter<Version, T>
where
    Version: SnapshotVersion,
    T: SnapshotObject,
{
    base_path: PathBuf,
    snapshots: Vec<Snapshot>,
    _marker: PhantomData<(Version, T)>,
}

impl<Version, T> Snapshotter<Version, T>
where
    Version: SnapshotVersion,
    T: SnapshotObject,
{
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();

        std::fs::create_dir_all(&path)?;

        let mut snapshots = vec![];

        let mut snapshot_idx = 0;
        loop {
            let snapshot_directory = Self::snapshot_directory(snapshot_idx);
            let snapshot_path = path.join(&snapshot_directory);

            if !path
                .join(format!("{}.{}", snapshot_directory, READY_FILE_NAME))
                .try_exists()?
            {
                break;
            }
            let index = OpenOptions::new()
                .read(true)
                .open(snapshot_path.join(INDEX_FILE_NAME))?;

            let segment = OpenOptions::new()
                .read(true)
                .open(snapshot_path.join(SEGMENT_FILE_NAME))?;

            let segment_len = segment.metadata()?.len() as usize;

            snapshots.push(Snapshot {
                total_items: Version::STRIDE.get(),
                segment_len,
                segment: BufReader::new(segment),
                index: BufReader::new(index),
            });
            snapshot_idx += 1;
        }

        Ok(Self {
            base_path: path,
            snapshots,
            _marker: PhantomData,
        })
    }

    fn snapshot_directory(snapshot_idx: usize) -> String {
        format!("{}-{}-{snapshot_idx:08}", Version::ID, T::ID)
    }

    pub fn get(&mut self, block_number: BlockNumber) -> anyhow::Result<Option<T>> {
        let snapshot_idx = block_number.0 as usize / Version::STRIDE.get();
        if let Some(snapshot) = self.snapshots.get_mut(snapshot_idx) {
            let entry_idx = block_number.0 as usize % Version::STRIDE.get();
            return Ok(Some(TableDecode::decode(&snapshot.read(entry_idx)?)?));
        }

        Ok(None)
    }

    pub fn max_block(&self) -> Option<BlockNumber> {
        (self.snapshots.len() * Version::STRIDE.get())
            .checked_sub(1)
            .map(|v| BlockNumber(v as u64))
    }

    pub fn next_max_block(&self) -> BlockNumber {
        BlockNumber((((self.snapshots.len() + 1) * Version::STRIDE.get()) - 1) as u64)
    }

    pub fn snapshot_paths(&self) -> Vec<(String, PathBuf)> {
        (0..self.snapshots.len())
            .map(|snapshot_idx| {
                let snapshot_dir = Self::snapshot_directory(snapshot_idx);
                let snapshot_path = self.base_path.join(&snapshot_dir);
                (snapshot_dir, snapshot_path)
            })
            .collect()
    }

    pub fn snapshot(
        &mut self,
        mut items: impl Iterator<Item = anyhow::Result<(BlockNumber, T)>>,
    ) -> anyhow::Result<()> {
        let mut last_block = self.max_block();

        let next_snapshot_idx = self.snapshots.len();

        let snapshot_directory_name = Self::snapshot_directory(next_snapshot_idx);
        let snapshot_path = self.base_path.join(&snapshot_directory_name);

        let segment_file_path = snapshot_path.join(SEGMENT_FILE_NAME);
        let idx_file_path = snapshot_path.join(INDEX_FILE_NAME);
        let ready_file_path = self
            .base_path
            .join(format!("{}.{}", snapshot_directory_name, READY_FILE_NAME));

        if let Err(e) = std::fs::remove_dir_all(&snapshot_path) {
            if !matches!(e.kind(), ErrorKind::NotFound) {
                return Err(e.into());
            }
        }
        std::fs::create_dir_all(&snapshot_path)?;

        let mut segment = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(segment_file_path)?;

        let mut i = 0;
        let mut total_len = 0_usize;

        let mut index_data = Vec::with_capacity(Version::STRIDE.get() * 8);

        while let Some((block, item)) = items.next().transpose()? {
            if let Some(last_block) = last_block {
                if block != last_block + 1 {
                    return Err(format_err!("block gap between {last_block} and {block}"));
                }
            } else if block != 0 {
                return Err(format_err!("Empty snapshotter, but block (#{block}) != 0"));
            }

            last_block = Some(block);

            let encoded = item.encode();
            index_data.extend_from_slice(&total_len.to_be_bytes());
            segment.write_all(encoded.as_ref())?;

            i += 1;
            total_len += encoded.as_ref().len();

            if i == Version::STRIDE.get() {
                break;
            }
        }

        if i != Version::STRIDE.get() {
            return Err(format_err!("end too early"));
        }

        segment.flush()?;

        // Do this at the very end both for performance and to ensure index only gets created when all is said and done
        let mut index = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(idx_file_path)?;
        index.write_all(&index_data)?;

        index.flush()?;

        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(ready_file_path)?;

        self.snapshots.push(Snapshot {
            total_items: i,
            segment_len: total_len,
            segment: BufReader::new(segment),
            index: BufReader::new(index),
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::traits::TableEncode;
    use derive_more::From;
    use hex_literal::hex;
    use tempfile::tempdir;

    #[test]
    fn snapshot() {
        let tmp_dir = tempdir().unwrap();

        let items = [0x42_u64, 0x0, 0xDEADBEEF, 0xAACC, 0xBAADCAFE];

        struct TestSnapshot;

        impl SnapshotVersion for TestSnapshot {
            const ID: &'static str = "test";
            const STRIDE: NonZeroUsize = NonZeroUsize::new(4).unwrap();
        }

        #[derive(Clone, Copy, Debug, From)]
        pub struct TestNumber(pub U256);

        impl TableEncode for TestNumber {
            type Encoded = <U256 as TableEncode>::Encoded;

            fn encode(self) -> Self::Encoded {
                self.0.encode()
            }
        }

        impl TableDecode for TestNumber {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                U256::decode(b).map(From::from)
            }
        }

        impl SnapshotObject for U256 {
            const ID: &'static str = "testobj";
        }

        for new in [true, false] {
            let mut snapshotter = Snapshotter::<TestSnapshot, U256>::new(&tmp_dir).unwrap();

            if new {
                assert_eq!(snapshotter.max_block(), None);
                assert_eq!(snapshotter.next_max_block(), 3);
                assert_eq!(snapshotter.get(0.into()).unwrap(), None);
                snapshotter
                    .snapshot(
                        items
                            .iter()
                            .enumerate()
                            .map(|(block, item)| Ok((BlockNumber(block as u64), item.as_u256()))),
                    )
                    .unwrap();
            }

            assert_eq!(snapshotter.max_block(), Some(BlockNumber(3)));
            assert_eq!(snapshotter.next_max_block(), 7);

            {
                let snapshot = snapshotter.snapshots.get_mut(0).unwrap();
                let mut segment_buffer = vec![];
                snapshot.segment.seek(SeekFrom::Start(0)).unwrap();
                snapshot.segment.read_to_end(&mut segment_buffer).unwrap();

                assert_eq!(&segment_buffer, &hex!("42DEADBEEFAACC"));

                let mut index_buffer = vec![];
                snapshot.index.seek(SeekFrom::Start(0)).unwrap();
                snapshot.index.read_to_end(&mut index_buffer).unwrap();

                assert_eq!(
                    &index_buffer,
                    &hex!("0000000000000000000000000000000100000000000000010000000000000005")
                );
            }

            for (i, item) in items.iter().enumerate().take(TestSnapshot::STRIDE.get()) {
                assert_eq!(
                    snapshotter.get((i as u64).into()).unwrap(),
                    Some(item.as_u256())
                );
            }

            for i in TestSnapshot::STRIDE.get()..items.len() {
                assert_eq!(snapshotter.get((i as u64).into()).unwrap(), None);
            }
        }
    }
}
