use serde::Serialize;
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MetaT<V, T> {
	tags: V,
	keys: V,
	timestamps: T,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BlockT<M, I> {
	meta: M,
	index: Vec<I>,
}

type HeaderMeta = MetaT<u64, u64>;
type BlockHeader = BlockT<HeaderMeta, u64>;
type BlockMeta = MetaT<Vec<String>, Vec<i64>>;
type BlockContent = BlockT<BlockMeta, Vec<u64>>;

fn header_size(size: usize) -> u64 {
	// (struct byte) +
	// (3 * u64 = tags + keys + timestamps) +
	// (max array overhead) +
	// (size * u64)
	return (1 + 3 * 9 + 5 + size * 9).try_into().unwrap();
}

impl BlockHeader {
	fn read_header(mut read: impl Read + Seek) -> anyhow::Result<Self> {
		read.seek(SeekFrom::Start(0))?;
		let (tags, keys, timestamps, index) = rmp_serde::from_read(read)?;
		Ok(Self {
			meta: HeaderMeta {
				tags,
				keys,
				timestamps,
			},
			index,
		})
	}

	fn read_index(&self, mut read: impl Read + Seek, index: usize) -> anyhow::Result<Vec<u64>> {
		read.seek(SeekFrom::Start(self.index[index]))?;
		Ok(rmp_serde::from_read(read)?)
	}

	fn write_header(&self, mut write: impl Write + Seek, limit: u64) -> anyhow::Result<()> {
		write.seek(SeekFrom::Start(0))?;
		let BlockHeader { meta, index } = self;
		(meta.tags, meta.keys, meta.timestamps, index)
			.serialize(&mut rmp_serde::Serializer::new(&mut write))?;
		if write.stream_position()? > limit {
			panic!("header has overwritten data");
		}
		Ok(())
	}
}

impl HeaderMeta {
	fn read_meta(&self, mut read: impl Read + Seek) -> anyhow::Result<BlockMeta> {
		read.seek(SeekFrom::Start(self.tags))?;
		let tags = rmp_serde::from_read(&mut read)?;
		read.seek(SeekFrom::Start(self.keys))?;
		let keys = rmp_serde::from_read(&mut read)?;
		read.seek(SeekFrom::Start(self.timestamps))?;
		let timestamps = rmp_serde::from_read(&mut read)?;
		Ok(BlockMeta {
			tags,
			keys,
			timestamps,
		})
	}
}

impl BlockMeta {
	fn write_meta(&self, mut write: impl Write + Seek, offset: u64) -> anyhow::Result<HeaderMeta> {
		let tags = write.seek(SeekFrom::Start(offset))?;
		self.tags
			.serialize(&mut rmp_serde::Serializer::new(&mut write))?;
		let keys = write.stream_position()?;
		self.keys
			.serialize(&mut rmp_serde::Serializer::new(&mut write))?;
		let timestamps = write.stream_position()?;
		self.timestamps
			.serialize(&mut rmp_serde::Serializer::new(&mut write))?;
		Ok(HeaderMeta {
			tags,
			keys,
			timestamps,
		})
	}
}

impl BlockContent {
	fn write_content(&self, mut write: impl Write + Seek) -> anyhow::Result<(u64, BlockHeader)> {
		let content_offset = header_size(self.index.len());
		let meta = self.meta.write_meta(&mut write, content_offset)?;

		let mut index = vec![];
		for ind in &self.index {
			index.push(write.stream_position()?);
			ind.serialize(&mut rmp_serde::Serializer::new(&mut write))?;
		}

		Ok((content_offset, BlockHeader { meta, index }))
	}

	fn write_all(&self, mut write: impl Write + Seek) -> anyhow::Result<BlockHeader> {
		let (content_offset, header) = self.write_content(&mut write)?;
		header.write_header(&mut write, content_offset)?;
		Ok(header)
	}
}

#[derive(Debug, Default, Clone)]
pub struct Block {
	header: BlockHeader,
	content: BlockContent,
}

impl From<BlockHeader> for Block {
	fn from(header: BlockHeader) -> Self {
		let mut content = BlockContent::default();
		content.index.resize_with(header.index.len(), Vec::default);
		Self { header, content }
	}
}

impl Block {
	pub fn read_meta(&mut self, read: impl Read + Seek) -> anyhow::Result<()> {
		self.content.meta = self.header.meta.read_meta(read)?;
		Ok(())
	}

	pub fn read_index(&mut self, read: impl Read + Seek, index: usize) -> anyhow::Result<()> {
		self.content.index[index] = self.header.read_index(read, index)?;
		Ok(())
	}

	pub fn write(&mut self, output: impl Write + Seek) -> anyhow::Result<()> {
		self.header = self.content.write_all(output)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::io::Cursor;

	use super::*;

	#[test]
	fn basic() -> anyhow::Result<()> {
		let content = BlockContent {
			meta: BlockMeta {
				tags: vec!["tag0".to_string(), "tag1".to_string(), "tag2".to_string()],
				keys: vec!["key0".to_string(), "key1".to_string()],
				timestamps: vec![100, 300],
			},
			index: vec![vec![0], vec![0, 1], vec![1]],
		};

		let mut buf = Cursor::new(vec![0; 128]);
		content.write_all(&mut buf)?;

		let mut read_block = Block::from(BlockHeader::read_header(&mut buf)?);
		read_block.read_meta(&mut buf)?;
		for i in 0..3 {
			read_block.read_index(&mut buf, i)?;
		}

		assert_eq!(content, read_block.content);
		Ok(())
	}

	#[test]
	fn header_size_is_enough() -> anyhow::Result<()> {
		let meta = HeaderMeta {
			tags: std::u64::MAX,
			keys: std::u64::MAX,
			timestamps: std::u64::MAX,
		};
		for size in [0, 1, 16, 1024 * 1024] {
			let header = BlockHeader {
				meta,
				index: vec![std::u64::MAX; size],
			};
			let size_est = header_size(size);
			let buf = Cursor::new(vec![0; size_est as usize]);
			header.write_header(buf, size_est)?;
		}
		Ok(())
	}

	#[test]
	fn data() -> anyhow::Result<()> {
		const BASE: u64 = 1024;
		let content = BlockContent {
			meta: BlockMeta {
				tags: (0..BASE).map(|i| format!("tag{}", i)).collect(),
				keys: (0..BASE * 10).map(|i| format!("key{}", i)).collect(),
				timestamps: (0..BASE as i64 * 10).map(|i| i * 100).collect(),
			},
			index: (0..BASE)
				.map(|i| (0..i).map(|j| j * 10).collect())
				.collect(),
		};

		let mut buf = Cursor::new(vec![0; 128]);
		assert!(content.write_all(&mut buf).is_ok());
		println!("{}", buf.get_ref().len());

		let mut read_block = Block::from(BlockHeader::read_header(&mut buf)?);
		read_block.read_meta(&mut buf)?;
		let ind_size = read_block.header.index.len();
		for i in 0..ind_size {
			read_block.read_index(&mut buf, i)?;
		}

		assert_eq!(content, read_block.content);
		Ok(())
	}
}
