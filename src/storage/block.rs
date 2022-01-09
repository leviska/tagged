use super::*;
use serde::{Deserialize, Serialize};
use std::{
	collections::BTreeMap,
	io::{Read, Seek, SeekFrom, Write},
};

pub type Index = u64;
pub type Offset = u64;
pub type Timestamp = u64;

pub trait SearchBlock {
	fn get_tags(&self) -> &[String];
	fn get_keys(&self) -> &[String];
	fn try_get_index(&self, id: usize) -> Option<&[u64]>;
	fn get_index(&mut self, id: usize) -> Result<&[u64], anyhow::Error>;
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, PartialEq)]
struct BlockHeader {
	start: Offset,
	tags: Offset,
	keys: Offset,
	timestamps: Offset,
	index: Vec<Offset>,
	from: Timestamp,
	to: Timestamp,
	size: u64,
}

fn header_size(size: usize) -> Offset {
	// (struct byte) +
	// (4 * u64 = start + tags + keys + timestamps) +
	// (max array overhead) +
	// (size * u64) +
	// (2 * u64 = from + to)
	// (1 * u64 = size)
	return 1 + 4 * 9 + 5 + size as Offset * 9 + 2 * 9 + 9;
}

#[allow(dead_code)]
impl BlockHeader {
	pub fn read_header(
		mut input: impl Read + Seek,
		start: u64,
	) -> Result<BlockHeader, anyhow::Error> {
		input.seek(SeekFrom::Start(start))?;
		let header: BlockHeader = rmp_serde::from_read(input)?;
		debug_assert_eq!(header.start, start);
		return Ok(header);
	}

	pub fn read_meta<T: Write + Read + Seek>(
		self,
		mut file: T,
	) -> Result<BlockFile<T>, anyhow::Error> {
		file.seek(SeekFrom::Start(self.tags))?;
		let tags = rmp_serde::from_read(&mut file)?;
		file.seek(SeekFrom::Start(self.keys))?;
		let keys = rmp_serde::from_read(&mut file)?;
		file.seek(SeekFrom::Start(self.timestamps))?;
		let timestamps = rmp_serde::from_read(&mut file)?;
		let indexes = self.index.len();
		let block = BlockFile {
			file,
			header: self,
			data: BlockData {
				tags,
				keys,
				timestamps,
				index: vec![Default::default(); indexes],
			},
			read: vec![false; indexes],
		};
		return Ok(block);
	}
}

#[derive(Debug, Default, PartialEq)]
pub struct BlockData {
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<Timestamp>,
	index: Vec<Vec<Index>>,
}

#[allow(dead_code)]
impl BlockData {
	pub fn write<T: Read + Write + Seek>(
		self,
		mut file: T,
	) -> Result<BlockFile<T>, (BlockData, anyhow::Error)> {
		let result = self.write_impl(&mut file);
		let indexes = self.index.len();
		return match result {
			Ok(header) => Ok(BlockFile {
				file,
				header,
				data: self,
				read: vec![true; indexes],
			}),
			Err(err) => Err((self, err)),
		};
	}

	fn write_impl(&self, mut output: impl Write + Seek) -> Result<BlockHeader, anyhow::Error> {
		let mut header = BlockHeader::default();
		let header_size = header_size(self.index.len());

		header.start = output.seek(SeekFrom::Current(0))?;
		header.tags = output.seek(SeekFrom::Current(header_size as i64))?;
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

		let end = output.seek(SeekFrom::Current(0))?;
		header.size = end - header.start;
		header.from = self.timestamps.first().cloned().unwrap_or(0);
		header.to = self.timestamps.last().cloned().unwrap_or(0);

		output.seek(SeekFrom::Start(header.start))?;
		header.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		assert!(
			header.start + header_size >= output.seek(SeekFrom::Current(0))?,
			"header has overwritten data"
		);
		output.seek(SeekFrom::Start(header.start + header.size))?;

		return Ok(header);
	}

	pub fn merge(mut self, mut other: BlockData) -> BlockData {
		// you really shouldn't merge empty block in the first place
		// but it can be convinient in the array merge
		if self.keys.is_empty() {
			return other;
		}
		if other.keys.is_empty() {
			return self;
		}

		debug_assert_eq!(
			self.timestamps.iter().max().unwrap(),
			self.timestamps.last().unwrap()
		);
		debug_assert_eq!(
			other.timestamps.iter().min().unwrap(),
			other.timestamps.first().unwrap()
		);

		if self.timestamps.last().unwrap() > other.timestamps.first().unwrap() {
			if other.timestamps.last().unwrap() > self.timestamps.first().unwrap() {
				panic!("blocks intersect");
			}
			return other.merge(self);
		}

		let mut index_map = BTreeMap::<String, Vec<Index>>::default();
		for (arr, tag) in self.index.into_iter().zip(&self.tags) {
			index_map.insert(tag.clone(), arr);
		}

		for (arr, tag) in other.index.into_iter().zip(&other.tags) {
			index_map
				.entry(tag.clone())
				.or_default()
				.extend(arr.into_iter().map(|x| x + self.keys.len() as Index));
		}

		self.tags = Vec::with_capacity(index_map.len());
		self.index = Vec::with_capacity(index_map.len());
		for (k, v) in index_map {
			self.tags.push(k);
			self.index.push(v);
		}

		self.keys.append(&mut other.keys);
		self.timestamps.append(&mut other.timestamps);

		return self;
	}

	pub fn range(&self) -> (Timestamp, Timestamp) {
		(
			*self.timestamps.first().unwrap(),
			*self.timestamps.last().unwrap(),
		)
	}
}

#[derive(Debug)]
pub struct BlockFile<T> {
	file: T,
	header: BlockHeader,
	data: BlockData,
	read: Vec<bool>,
}

#[allow(dead_code)]
impl<T: Read + Write + Seek> BlockFile<T> {
	pub fn read_all(&mut self) -> Result<(), anyhow::Error> {
		for i in 0..self.data.index.len() {
			self.get_index(i)?;
		}
		return Ok(());
	}

	pub fn update_index(&mut self, id: usize) -> Result<(), anyhow::Error> {
		self.file.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id].serialize(&mut rmp_serde::Serializer::new(&mut self.file))?;
		return Ok(());
	}

	pub fn range(&self) -> (Timestamp, Timestamp) {
		return self.data.range();
	}

	pub fn release(self) -> (T, BlockData) {
		return (self.file, self.data);
	}
}

impl<T: Read + Write + Seek> SearchBlock for BlockFile<T> {
	fn get_tags(&self) -> &[String] {
		&self.data.tags
	}

	fn get_keys(&self) -> &[String] {
		&self.data.keys
	}

	fn try_get_index(&self, id: usize) -> Option<&[u64]> {
		if self.read[id] {
			Some(&self.data.index[id])
		} else {
			None
		}
	}

	fn get_index(&mut self, id: usize) -> Result<&[u64], anyhow::Error> {
		if self.read[id] {
			Ok(&self.data.index[id])
		} else {
			self.file.seek(SeekFrom::Start(self.header.index[id]))?;
			self.data.index[id] = rmp_serde::from_read(&mut self.file)?;
			self.read[id] = true;

			Ok(&self.data.index[id])
		}
	}
}

#[derive(Debug)]
pub struct InMemoryBlock {
	data: BlockData,
	size: u64,
}

impl InMemoryBlock {
	pub fn merge(self, other: InMemoryBlock) -> InMemoryBlock {
		let data = self.data.merge(other.data);
		return InMemoryBlock {
			data,
			size: self.size + other.size,
		};
	}

	pub fn write<T: Write + Read + Seek>(
		self,
		file: T,
	) -> Result<BlockFile<T>, (BlockData, anyhow::Error)> {
		return self.data.write(file);
	}

	pub fn size(&self) -> u64 {
		return self.size;
	}

	pub fn range(&self) -> (Timestamp, Timestamp) {
		return self.data.range();
	}
}

impl SearchBlock for InMemoryBlock {
	fn get_tags(&self) -> &[String] {
		&self.data.tags
	}

	fn get_keys(&self) -> &[String] {
		&self.data.keys
	}

	fn try_get_index(&self, id: usize) -> Option<&[u64]> {
		Some(&self.data.index[id])
	}

	fn get_index(&mut self, id: usize) -> Result<&[u64], anyhow::Error> {
		Ok(&self.data.index[id])
	}
}

#[derive(Debug, Default, Clone)]
pub struct ActiveBlock {
	index: BTreeMap<String, Vec<Index>>,
	keys: Vec<String>,
	timestamps: Vec<Timestamp>,
	size: u64,
}

#[allow(dead_code)]
impl ActiveBlock {
	pub fn push(&mut self, key: String, tags: Vec<String>) {
		self.size += tags.len() as u64;

		let id = self.keys.len() as Index;
		self.keys.push(key);
		self.timestamps.push(std::cmp::max(
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis() as Timestamp,
			self.timestamps.last().cloned().unwrap_or(0), // TODO: use ts from previous active block as starting point
		));
		for tag in tags.into_iter() {
			self.index.entry(tag).or_default().push(id);
		}
	}

	pub fn into_block(self) -> InMemoryBlock {
		let mut tags = Vec::with_capacity(self.index.len());
		let mut index = Vec::with_capacity(self.index.len());
		for (k, v) in self.index {
			tags.push(k);
			index.push(v);
		}
		return InMemoryBlock {
			data: BlockData {
				tags,
				keys: self.keys,
				timestamps: self.timestamps,
				index,
			},
			size: self.size,
		};
	}

	pub fn size(&self) -> u64 {
		return self.size;
	}
}

#[cfg(test)]
#[path = "tests/block.rs"]
mod block_test;
