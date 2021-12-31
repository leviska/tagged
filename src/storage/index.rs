use serde::{Deserialize, Serialize};
use std::{
	collections::BTreeMap,
	io::{Read, Seek, SeekFrom, Write},
};

type Offset = u64;

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
struct BlockHeader {
	tags: Offset,
	keys: Offset,
	timestamps: Offset,
	index: Vec<Offset>,
	from: u64,
	to: u64,
}

impl BlockHeader {
	pub fn read_header<T: Read + Seek>(input: &mut T) -> Result<BlockHeader, anyhow::Error> {
		input.seek(SeekFrom::Start(0))?;
		let header = rmp_serde::from_read(input)?;
		return Result::Ok(header);
	}

	pub fn read_meta<T: Read + Seek>(self, mut input: &mut T) -> Result<BlockFile, anyhow::Error> {
		input.seek(SeekFrom::Start(self.tags))?;
		let tags = rmp_serde::from_read(&mut input)?;
		input.seek(SeekFrom::Start(self.keys))?;
		let keys = rmp_serde::from_read(&mut input)?;
		input.seek(SeekFrom::Start(self.timestamps))?;
		let timestamps = rmp_serde::from_read(&mut input)?;
		let indexes = self.index.len();
		let block = BlockFile {
			header: self,
			data: BlockData {
				tags,
				keys,
				timestamps,
				index: vec![vec![]; indexes],
				read: vec![false; indexes],
			},
		};
		return Result::Ok(block);
	}
}

#[derive(Debug, Default, PartialEq)]
pub struct BlockData {
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<u64>,
	index: Vec<Vec<u64>>,
	read: Vec<bool>,
}

impl BlockData {
	pub fn try_get_index(&self, id: usize) -> Option<&Vec<u64>> {
		if self.read[id] {
			return Some(&self.index[id]);
		}
		return None;
	}

	pub fn write<T: Write + Seek>(self, mut output: &mut T) -> Result<BlockFile, anyhow::Error> {
		let mut header = BlockHeader::default();
		let header_size = header_size(self.index.len());

		header.tags = output.seek(SeekFrom::Start(header_size as u64))?;
		self.tags
			.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		header.keys = output.seek(SeekFrom::Current(0))?;
		self.keys
			.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		header.timestamps = output.seek(SeekFrom::Current(0))?;
		self.timestamps
			.serialize(&mut rmp_serde::Serializer::new(&mut output))?;

		for ind in self.index.iter() {
			header.index.push(output.seek(SeekFrom::Current(0))?);
			ind.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		}

		output.seek(SeekFrom::Start(0))?;
		header.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		if header_size < output.seek(SeekFrom::Current(0))? {
			return Err(anyhow::anyhow!("header has overwritten data"));
		}

		return Result::Ok(BlockFile { header, data: self });
	}
}

#[derive(Debug)]
pub struct BlockFile {
	header: BlockHeader,
	data: BlockData,
}

fn header_size(size: usize) -> u64 {
	// (struct byte) +
	// (3 * u64 = tags + keys + timestamps) +
	// (max array overhead) +
	// (size * u64) +
	// (2 * u64 = from + to)
	return (1 + 3 * 9 + 5 + size * 9 + 2 * 9) as u64;
}

impl BlockFile {
	pub fn try_get_index(&self, id: usize) -> Option<&Vec<u64>> {
		return self.data.try_get_index(id);
	}

	pub fn read_index<T: Read + Seek>(
		&mut self,
		input: &mut T,
		id: usize,
	) -> Result<&Vec<u64>, anyhow::Error> {
		if self.data.read[id] {
			return Ok(&self.data.index[id]);
		}

		input.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id] = rmp_serde::from_read(input)?;
		self.data.read[id] = true;

		return Result::Ok(&self.data.index[id]);
	}

	pub fn update_index<T: Write + Seek>(
		&self,
		mut output: &mut T,
		id: usize,
	) -> Result<(), anyhow::Error> {
		output.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id].serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		return Ok(());
	}
}

#[derive(Debug, Default, Clone)]
pub struct ActiveBlock {
	index: BTreeMap<String, Vec<u64>>,
	keys: Vec<String>,
	timestamps: Vec<u64>,
}

impl ActiveBlock {
	pub fn push(&mut self, key: &str, tags: &[String]) {
		let id = self.keys.len() as u64;
		self.keys.push(key.to_string());
		self.timestamps.push(
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis() as u64,
		);
		for tag in tags {
			self.index.entry(tag.to_string()).or_default().push(id);
		}
	}

	pub fn into_block(self) -> BlockData {
		let size = self.index.len();
		let mut tags = Vec::with_capacity(size);
		let mut index = Vec::with_capacity(size);
		for (tag, ind) in self.index {
			tags.push(tag);
			index.push(ind);
		}
		return BlockData {
			tags,
			keys: self.keys,
			timestamps: self.timestamps,
			index,
			read: vec![true; size],
		};
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Cursor;

	#[test]
	fn basic() {
		let mut block = BlockData {
			tags: vec!["tag0".to_string(), "tag1".to_string(), "tag2".to_string()],
			keys: vec!["key0".to_string(), "key1".to_string()],
			timestamps: vec![100, 300],
			index: vec![vec![0], vec![0, 1], vec![1]],
			read: vec![true; 3],
		};
		let mut buf = Cursor::new(vec![0; 128]);
		let block = block.write(&mut buf).unwrap();

		let header = BlockHeader::read_header(&mut buf).unwrap();
		let mut read_block = header.read_meta(&mut buf).unwrap();
		read_block.read_index(&mut buf, 0).unwrap();
		read_block.read_index(&mut buf, 1).unwrap();
		read_block.read_index(&mut buf, 2).unwrap();

		assert_eq!(block.data, read_block.data);
	}

	#[test]
	fn header_size() {
		let mut header = BlockHeader::default();

		let buf = rmp_serde::to_vec(&header).unwrap();
		assert!((buf.len() as u64) <= super::header_size(header.index.len()));

		header.tags = std::u64::MAX;
		header.keys = std::u64::MAX;
		header.timestamps = std::u64::MAX;

		let buf = rmp_serde::to_vec(&header).unwrap();
		assert!((buf.len() as u64) <= super::header_size(header.index.len()));

		header.index.resize(1, std::u64::MAX);
		let buf = rmp_serde::to_vec(&header).unwrap();
		assert!((buf.len() as u64) <= super::header_size(header.index.len()));

		header.index.resize(16, std::u64::MAX);
		let buf = rmp_serde::to_vec(&header).unwrap();
		assert!((buf.len() as u64) <= super::header_size(header.index.len()));

		header.index.resize(1024, std::u64::MAX);
		let buf = rmp_serde::to_vec(&header).unwrap();
		assert!((buf.len() as u64) <= super::header_size(header.index.len()));

		header.index.resize(1024 * 1024, std::u64::MAX);
		let buf = rmp_serde::to_vec(&header).unwrap();
		assert!((buf.len() as u64) <= super::header_size(header.index.len()));
	}

	#[test]
	fn data() {
		let mut block = BlockData::default();
		const BASE: i32 = 1024;
		for i in 0..BASE {
			block.tags.push(format!("tag{}", i));
		}
		for i in 0..BASE * 10 {
			block.keys.push(format!("key{}", i));
		}
		for i in 0..BASE * 10 {
			block.timestamps.push((i * 100) as u64);
		}
		for i in 0..BASE {
			block.index.push(Vec::default());
			for j in (0..i * 10).step_by(10) {
				block.index.last_mut().unwrap().push(j as u64);
			}
		}
		block.read.resize(block.index.len(), true);

		let mut buf = Cursor::new(vec![0; 128]);
		let block = block.write(&mut buf).unwrap();

		println!("{}", buf.get_ref().len());

		let header = BlockHeader::read_header(&mut buf).unwrap();
		let mut read_block = header.read_meta(&mut buf).unwrap();
		let ind_size = read_block.header.index.len();
		for i in 0..ind_size {
			read_block.read_index(&mut buf, i).unwrap();
		}

		assert_eq!(block.data, read_block.data);
	}
}
