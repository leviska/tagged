use serde::{Deserialize, Serialize};
use std::{
	collections::BTreeMap,
	io::{Read, Seek, SeekFrom, Write},
	sync::Arc,
};

type Offset = u64;
type Timestamp = u64;

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
struct BlockHeader {
	tags: Offset,
	keys: Offset,
	timestamps: Offset,
	index: Vec<Offset>,
	from: Timestamp,
	to: Timestamp,
}

#[allow(dead_code)]
impl BlockHeader {
	pub fn read_header(mut input: impl Read + Seek) -> Result<BlockHeader, anyhow::Error> {
		input.seek(SeekFrom::Start(0))?;
		let header = rmp_serde::from_read(input)?;
		return Ok(header);
	}

	pub fn read_meta(self, mut input: impl Read + Seek) -> Result<BlockFile, anyhow::Error> {
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
				index: vec![Default::default(); indexes],
				read: vec![false; indexes],
			},
		};
		return Ok(block);
	}
}

#[derive(Debug, Default, PartialEq)]
pub struct BlockData {
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<Timestamp>,
	index: Vec<Arc<Vec<u64>>>,
	read: Vec<bool>,
}

#[allow(dead_code)]
impl BlockData {
	pub fn try_get_index(&self, id: usize) -> Option<&Vec<u64>> {
		if self.read[id] {
			return Some(&self.index[id]);
		}
		return None;
	}

	pub fn write(self, mut output: impl Write + Seek) -> Result<BlockFile, anyhow::Error> {
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

		header.from = self.timestamps.first().cloned().unwrap_or(0);
		header.to = self.timestamps.last().cloned().unwrap_or(0);

		output.seek(SeekFrom::Start(0))?;
		header.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		if header_size < output.seek(SeekFrom::Current(0))? {
			return Err(anyhow::anyhow!("header has overwritten data"));
		}

		return Ok(BlockFile { header, data: self });
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

		debug_assert_eq!(self.read.iter().filter(|x| !*x).count(), 0);
		debug_assert_eq!(other.read.iter().filter(|x| !*x).count(), 0);
		debug_assert_eq!(
			self.timestamps.iter().max().unwrap(),
			self.timestamps.last().unwrap()
		);
		debug_assert_eq!(
			other.timestamps.iter().max().unwrap(),
			other.timestamps.last().unwrap()
		);

		if self.timestamps.last().unwrap() > other.timestamps.first().unwrap() {
			return other.merge(self);
		}

		let mut index_map = BTreeMap::<String, Arc<Vec<u64>>>::default();
		for (arr, tag) in self.index.into_iter().zip(&self.tags) {
			index_map.insert(tag.clone(), arr);
		}

		for (arr, tag) in other.index.into_iter().zip(&other.tags) {
			Arc::get_mut(index_map.entry(tag.clone()).or_default())
				.unwrap()
				.extend(
					Arc::try_unwrap(arr)
						.unwrap()
						.into_iter()
						.map(|x| x + self.tags.len() as u64),
				);
		}

		self.tags = Vec::with_capacity(index_map.len());
		self.index = Vec::with_capacity(index_map.len());
		for (k, v) in index_map {
			self.tags.push(k);
			self.index.push(v);
		}

		self.keys.append(&mut other.keys);
		self.timestamps.append(&mut other.timestamps);
		self.read.resize(self.index.len(), true);

		return self;
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

#[allow(dead_code)]
impl BlockFile {
	pub fn try_get_index(&self, id: usize) -> Option<&Vec<u64>> {
		return self.data.try_get_index(id);
	}

	pub fn read_index(
		&mut self,
		mut input: impl Read + Seek,
		id: usize,
	) -> Result<&Vec<u64>, anyhow::Error> {
		if self.data.read[id] {
			return Ok(&self.data.index[id]);
		}

		input.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id] = Arc::new(rmp_serde::from_read(input)?);
		self.data.read[id] = true;

		return Ok(&self.data.index[id]);
	}

	pub fn read_all(&mut self, mut input: impl Read + Seek) -> Result<(), anyhow::Error> {
		for i in 0..self.data.index.len() {
			self.read_index(&mut input, i)?;
		}
		return Ok(());
	}

	pub fn update_index(
		&self,
		mut output: impl Write + Seek,
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

#[allow(dead_code)]
impl ActiveBlock {
	pub fn push(&mut self, key: &str, tags: &[String]) {
		let id = self.keys.len() as u64;
		self.keys.push(key.to_string());
		self.timestamps.push(std::cmp::max(
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis() as u64,
			self.timestamps.last().cloned().unwrap_or(0), // TODO: use ts from previous active block as starting point
		));
		for tag in tags {
			self.index.entry(tag.to_string()).or_default().push(id);
		}
	}

	pub fn into_block(self) -> BlockData {
		let size = self.index.len();
		let mut tags = Vec::with_capacity(self.index.len());
		let mut index = Vec::with_capacity(self.index.len());
		for (k, v) in self.index {
			tags.push(k);
			index.push(Arc::new(v));
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
#[path = "block_test.rs"]
mod block_test;
