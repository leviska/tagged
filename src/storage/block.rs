use serde::{Deserialize, Serialize};
use std::{
	collections::BTreeMap,
	io::{Read, Seek, SeekFrom, Write},
	sync::Arc,
};

pub type Index = u64;
pub type Offset = u64;
pub type Timestamp = u64;

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
	index: Vec<Arc<Vec<Index>>>,
}

#[allow(dead_code)]
impl BlockData {
	pub fn write(self, output: impl Write + Seek) -> Result<BlockFile, (BlockData, anyhow::Error)> {
		let result = self.write_impl(output);
		let indexes = self.index.len();
		return match result {
			Ok(header) => Ok(BlockFile {
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

		let mut index_map = BTreeMap::<String, Arc<Vec<Index>>>::default();
		for (arr, tag) in self.index.into_iter().zip(&self.tags) {
			index_map.insert(tag.clone(), arr);
		}

		for (arr, tag) in other.index.into_iter().zip(&other.tags) {
			Arc::get_mut(index_map.entry(tag.clone()).or_default())
				.expect("someone using index while trying to merge")
				.extend(
					Arc::try_unwrap(arr)
						.expect("someone using index while trying to merge")
						.into_iter()
						.map(|x| x + self.tags.len() as Index),
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

		return self;
	}
}

#[derive(Debug)]
pub struct BlockFile {
	header: BlockHeader,
	data: BlockData,
	read: Vec<bool>,
}

#[allow(dead_code)]
impl BlockFile {
	pub fn try_get_index(&self, id: usize) -> Option<Arc<Vec<Index>>> {
		if self.read[id] {
			Some(Arc::clone(&self.data.index[id]))
		} else {
			None
		}
	}

	pub fn read_index(
		&mut self,
		mut input: impl Read + Seek,
		id: usize,
	) -> Result<Arc<Vec<Index>>, anyhow::Error> {
		if let Some(ind) = self.try_get_index(id) {
			return Ok(ind);
		}

		input.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id] = Arc::new(rmp_serde::from_read(input)?);
		self.read[id] = true;

		return Ok(Arc::clone(&self.data.index[id]));
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

	pub fn write(self, output: impl Write + Seek) -> Result<BlockFile, (BlockData, anyhow::Error)> {
		return self.data.write(output);
	}

	pub fn size(&self) -> u64 {
		return self.size;
	}

	pub fn first_timestamp(&self) -> Timestamp {
		return *self.data.timestamps.first().unwrap();
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
			index.push(Arc::new(v));
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
