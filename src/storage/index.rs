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
			tags,
			keys,
			timestamps,
			index: vec![vec![]; indexes],
			read: vec![false; indexes],
		};
		return Result::Ok(block);
	}
}

#[derive(Debug, Default)]
pub struct BlockFile {
	header: BlockHeader,
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<u64>,
	index: Vec<Vec<u64>>,
	read: Vec<bool>,
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
	pub fn read_index<T: Read + Seek>(
		&mut self,
		input: &mut T,
		id: usize,
	) -> Result<&Vec<u64>, anyhow::Error> {
		if self.read[id] {
			return Ok(&self.index[id]);
		}

		input.seek(SeekFrom::Start(self.header.index[id]))?;
		self.index[id] = rmp_serde::from_read(input)?;
		self.read[id] = true;

		return Result::Ok(&self.index[id]);
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

	pub fn generate_block(self) -> BlockFile {
		let mut block = BlockFile::default();
		block.tags.reserve(self.index.len());
		block.index.reserve(self.index.len());
		block.keys = self.keys;
		block.timestamps = self.timestamps;
		for (tag, ind) in self.index {
			block.tags.push(tag);
			block.index.push(ind);
		}
		return block;
	}
}

#[cfg(test)]
mod tests {
	use std::io::Cursor;

	use super::*;

	fn assert_no_err<T>(r: Result<T, anyhow::Error>) -> T {
		if r.is_err() {
			assert!(false, "expected no error, got: {}", r.err().unwrap())
		}
		return r.unwrap();
	}

	#[test]
	fn basic() {
		let mut block = BlockFile::default();
		block.tags = vec!["tag0".to_string(), "tag1".to_string(), "tag2".to_string()];
		block.keys = vec!["key0".to_string(), "key1".to_string()];
		block.timestamps = vec![100, 300];
		block.index = vec![vec![0], vec![0, 1], vec![1]];

		let mut buf = Cursor::new(vec![0; 128]);
		assert!(block.write(&mut buf).is_ok());

		let mut read_block = BlockFile::default();
		assert_no_err(read_block.read_header(&mut buf));
		assert_no_err(read_block.read_meta(&mut buf));
		assert_no_err(read_block.read_index(&mut buf, 0));
		assert_no_err(read_block.read_index(&mut buf, 1));
		assert_no_err(read_block.read_index(&mut buf, 2));

		assert_eq!(block.tags, read_block.tags);
		assert_eq!(block.keys, read_block.keys);
		assert_eq!(block.timestamps, read_block.timestamps);
		assert_eq!(block.index, read_block.index);
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
		let mut block = BlockFile::default();
		const BASE: i32 = 1024;
		for i in 0..BASE {
			block.tags.push(format!("tag{}", i));
		}
		for i in 0..BASE * 10 {
			block.keys.push(format!("key{}", i));
		}
		for i in 0..BASE * 10 {
			block.timestamps.push((i * 100) as i64);
		}
		for i in 0..BASE {
			block.index.push(Vec::default());
			for j in (0..i * 10).step_by(10) {
				block.index.last_mut().unwrap().push(j as u64);
			}
		}

		let mut buf = Cursor::new(vec![0; 128]);
		assert!(block.write(&mut buf).is_ok());

		println!("{}", buf.get_ref().len());

		let mut read_block = BlockFile::default();
		assert_no_err(read_block.read_header(&mut buf));
		assert_no_err(read_block.read_meta(&mut buf));
		let ind_size = read_block.header.as_ref().unwrap().index.len();
		for i in 0..ind_size {
			assert_no_err(read_block.read_index(&mut buf, i));
		}

		assert_eq!(block.tags, read_block.tags);
		assert_eq!(block.keys, read_block.keys);
		assert_eq!(block.timestamps, read_block.timestamps);
		assert_eq!(block.index, read_block.index);
	}
}
