use crate::{
    kv::traits::{TableDecode, TableObject},
    models::*,
};
use anyhow::{bail, format_err};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

pub const DEFAULT_STRIDE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();
pub const VERSION: usize = 1;
pub const CONFIG_FILE: &str = "snapshot_config.ron";

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

    fn index_file_name(idx: usize) -> String {
        format!("segment_{idx:08}.idx")
    }

    fn segment_file_name(idx: usize) -> String {
        format!("segment_{idx:08}.seg")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotConfig {
    pub version: usize,
    pub stride: NonZeroUsize,
}

#[derive(Debug)]
pub struct Snapshotter<T>
where
    T: TableObject,
{
    base_path: PathBuf,
    stride: NonZeroUsize,
    snapshots: Vec<Snapshot>,
    _marker: PhantomData<T>,
}

impl<T> Snapshotter<T>
where
    T: TableObject,
{
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::new_with_stride(path, None)
    }

    fn new_with_stride(
        path: impl AsRef<Path>,
        stride: Option<NonZeroUsize>,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();

        std::fs::create_dir_all(&path)?;

        let config_path = path.join(CONFIG_FILE);

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&config_path)
        {
            Ok(mut new_config_file) => {
                // No config, snapshot starting anew

                let stride = stride.unwrap_or(DEFAULT_STRIDE);
                let config = SnapshotConfig {
                    version: VERSION,
                    stride,
                };

                ron::ser::to_writer(&mut new_config_file, &config)?;
                new_config_file.flush()?;

                Ok(Self {
                    base_path: path,
                    stride,
                    snapshots: vec![],
                    _marker: PhantomData,
                })
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        // Config already exists, let's read it.
                        let mut config_file = OpenOptions::new().read(true).open(&config_path)?;

                        let config = ron::de::from_reader::<_, SnapshotConfig>(&mut config_file)?;

                        let mut snapshots = vec![];

                        let mut snapshot_idx = 0;
                        loop {
                            let index = match OpenOptions::new()
                                .read(true)
                                .open(path.join(Snapshot::index_file_name(snapshot_idx)))
                            {
                                Ok(file) => BufReader::new(file),
                                Err(e) => match e.kind() {
                                    std::io::ErrorKind::NotFound => break,
                                    _ => return Err(e.into()),
                                },
                            };

                            let segment = OpenOptions::new()
                                .read(true)
                                .open(path.join(Snapshot::segment_file_name(snapshot_idx)))?;

                            let segment_len = segment.metadata()?.len() as usize;

                            snapshots.push(Snapshot {
                                total_items: config.stride.get(),
                                segment_len,
                                segment: BufReader::new(segment),
                                index,
                            });
                            snapshot_idx += 1;
                        }

                        Ok(Self {
                            base_path: path,
                            stride: config.stride,
                            snapshots,
                            _marker: PhantomData,
                        })
                    }
                    _ => Err(e.into()),
                }
            }
        }
    }

    pub fn get(&mut self, block_number: BlockNumber) -> anyhow::Result<Option<T>> {
        let snapshot_idx = block_number.0 as usize / self.stride;
        if let Some(snapshot) = self.snapshots.get_mut(snapshot_idx) {
            let entry_idx = block_number.0 as usize % self.stride;
            return Ok(Some(TableDecode::decode(&snapshot.read(entry_idx)?)?));
        }

        Ok(None)
    }

    pub fn max_block(&self) -> Option<BlockNumber> {
        (self.snapshots.len() * self.stride.get())
            .checked_sub(1)
            .map(|v| BlockNumber(v as u64))
    }

    pub fn next_max_block(&self) -> BlockNumber {
        BlockNumber((((self.snapshots.len() + 1) * self.stride.get()) - 1) as u64)
    }

    pub fn stride(&self) -> NonZeroUsize {
        self.stride
    }

    pub fn snapshot(
        &mut self,
        mut items: impl Iterator<Item = anyhow::Result<(BlockNumber, T)>>,
    ) -> anyhow::Result<()> {
        let mut last_block = self.max_block();

        let next_snapshot_idx = self.snapshots.len();

        let segment_file_path = self
            .base_path
            .join(Snapshot::segment_file_name(next_snapshot_idx));
        let idx_file_path = self
            .base_path
            .join(Snapshot::index_file_name(next_snapshot_idx));

        for path in [&segment_file_path, &idx_file_path] {
            match std::fs::remove_file(path) {
                Err(e) if !matches!(e.kind(), ErrorKind::NotFound) => {
                    return Err(e.into());
                }
                _ => {}
            }
        }

        let mut segment = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(segment_file_path)?;

        let mut i = 0;
        let mut total_len = 0_usize;

        let mut index_data = Vec::with_capacity(self.stride.get() * 8);

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

            if i == self.stride.get() {
                break;
            }
        }

        if i != self.stride.get() {
            return Err(format_err!("end too early"));
        }

        segment.flush()?;

        // Do this at the very end both for performance and to ensure index only gets created when all is said and done
        let mut index = tempfile::NamedTempFile::new()?;
        index.write_all(&index_data)?;
        let index = index.persist(idx_file_path)?;

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
    use hex_literal::hex;
    use tempfile::tempdir;

    #[test]
    fn snapshot() {
        let tmp_dir = tempdir().unwrap();

        let stride = 4;
        let items = [0x42_u64, 0x0, 0xDEADBEEF, 0xAACC, 0xBAADCAFE];

        for new in [true, false] {
            let mut snapshotter = Snapshotter::<U256>::new_with_stride(
                &tmp_dir,
                Some(NonZeroUsize::new(stride).unwrap()),
            )
            .unwrap();

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

            for (i, item) in items.iter().enumerate().take(stride) {
                assert_eq!(
                    snapshotter.get((i as u64).into()).unwrap(),
                    Some(item.as_u256())
                );
            }

            for i in stride..items.len() {
                assert_eq!(snapshotter.get((i as u64).into()).unwrap(), None);
            }
        }
    }
}
