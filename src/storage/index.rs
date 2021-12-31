use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
struct BlockHeader {
	tags: u64,
	keys: u64,
	timestamps: u64,
	index: Vec<u64>,
}

#[derive(Debug, Default, Clone)]
pub struct Block {
	header: Option<BlockHeader>,
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<i64>,
	index: Vec<Vec<u64>>,
}

fn header_size(size: usize) -> u64 {
	// (struct byte) +
	// (3 * u64 = tags + keys + timestamps) +
	// (max array overhead) +
	// (size * u64)
	return (1 + 3 * 9 + 5 + size * 9) as u64;
}

impl Block {
	pub fn read_header<T: Read + Seek>(&mut self, input: &mut T) -> Result<(), anyhow::Error> {
		input.seek(SeekFrom::Start(0))?;
		let header: BlockHeader = rmp_serde::from_read(input)?;
		self.index.resize(header.index.len(), Vec::default());
		self.header = Some(header);
		return Result::Ok(());
	}

	pub fn read_meta<T: Read + Seek>(&mut self, mut input: &mut T) -> Result<(), anyhow::Error> {
		let header = self
			.header
			.as_ref()
			.ok_or(anyhow::anyhow!("block header hasn't been read"))?;
		input.seek(SeekFrom::Start(header.tags))?;
		self.tags = rmp_serde::from_read(&mut input)?;
		input.seek(SeekFrom::Start(header.keys))?;
		self.keys = rmp_serde::from_read(&mut input)?;
		input.seek(SeekFrom::Start(header.timestamps))?;
		self.timestamps = rmp_serde::from_read(&mut input)?;
		return Result::Ok(());
	}

	pub fn read_index<T: Read + Seek>(
		&mut self,
		input: &mut T,
		index: usize,
	) -> Result<(), anyhow::Error> {
		let header = self
			.header
			.as_ref()
			.ok_or(anyhow::anyhow!("block header hasn't been read"))?;

		input.seek(SeekFrom::Start(header.index[index]))?;
		self.index[index] = rmp_serde::from_read(input)?;

		return Result::Ok(());
	}

	pub fn write<T: Write + Seek>(&mut self, mut output: &mut T) -> Result<(), anyhow::Error> {
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

		self.header = Some(header);

		return Result::Ok(());
	}
}

#[cfg(test)]
mod tests {
	use std::error::Error;
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
		let mut block = Block::default();
		block.tags = vec!["tag0".to_string(), "tag1".to_string(), "tag2".to_string()];
		block.keys = vec!["key0".to_string(), "key1".to_string()];
		block.timestamps = vec![100, 300];
		block.index = vec![vec![0], vec![0, 1], vec![1]];

		let mut buf = Cursor::new(vec![0; 128]);
		assert!(block.write(&mut buf).is_ok());

		let mut read_block = Block::default();
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
		let mut block = Block::default();
		const base: i32 = 1024;
		for i in 0..base {
			block.tags.push(format!("tag{}", i));
		}
		for i in 0..base * 10 {
			block.keys.push(format!("key{}", i));
		}
		for i in 0..base * 10 {
			block.timestamps.push((i * 100) as i64);
		}
		for i in 0..base {
			block.index.push(Vec::default());
			for j in (0..i * 10).step_by(10) {
				block.index.last_mut().unwrap().push(j as u64);
			}
		}

		let mut buf = Cursor::new(vec![0; 128]);
		assert!(block.write(&mut buf).is_ok());

		println!("{}", buf.get_ref().len());

		let mut read_block = Block::default();
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
