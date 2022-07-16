use serde::{Deserialize, Serialize};
use std::{
	collections::BTreeMap,
	fs::File,
	io::{Read, Seek, SeekFrom, Write},
	path::PathBuf,
	sync::Arc,
};

pub type Index = u64;
pub type Offset = u64;
pub type Timestamp = u64;

#[derive(Debug, Copy, Clone)]
pub enum BlockType {
	File,
	InMemory,
}

pub trait SearchBlock {
	fn get_tags(&self) -> &[String];
	fn get_keys(&self) -> &[String];
	fn read_index(&mut self, id: usize) -> Result<(), anyhow::Error>;
	fn try_get_index(&self, id: usize) -> Option<Arc<Vec<u64>>>;
	fn get_type(&self) -> BlockType;
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, PartialEq)]
pub struct BlockHeader {
	start: Offset,
	tags: Offset,
	keys: Offset,
	timestamps: Offset,
	index: Vec<Offset>,
	from: Timestamp,
	to: Timestamp,
	// block size inside file
	size: u64,
}

/// upper bound of size of header on disk
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
		};
		return Ok(block);
	}
}

#[derive(Debug, Default, PartialEq)]
pub struct BlockData {
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<Timestamp>,
	index: Vec<Option<Arc<Vec<Index>>>>,
}

impl BlockData {
	pub fn write<T: Read + Write + Seek>(
		self,
		mut file: T,
	) -> Result<BlockFile<T>, (Self, anyhow::Error)> {
		let result = self.write_impl(&mut file);
		return match result {
			Ok(header) => Ok(BlockFile {
				file,
				header,
				data: self,
			}),
			Err(err) => Err((self, err)),
		};
	}

	fn write_impl(&self, output: impl Write + Seek) -> Result<BlockHeader, anyhow::Error> {
		let mut header = BlockHeader::default();
		let header_size = header_size(self.index.len());

		let mut output = std::io::BufWriter::new(output);

		header.start = output.stream_position()?;
		header.tags = output.seek(SeekFrom::Current(header_size as i64))?;
		self.tags
			.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		header.keys = output.stream_position()?;
		self.keys
			.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		header.timestamps = output.stream_position()?;

		self.timestamps
			.serialize(&mut rmp_serde::Serializer::new(&mut output))?;

		let mut buf = Vec::with_capacity(
			self.index
				.iter()
				.map(|opt| opt.as_ref().map(|vec| vec.len()).unwrap_or(0))
				.max()
				.unwrap_or(0),
		);
		for ind in self.index.iter() {
			header.index.push(output.stream_position()?);
			let ind = ind.as_ref().ok_or(anyhow::anyhow!(
				"all indexes must be loaded to save the block"
			))?;
			buf.resize(0, 0);
			if !ind.is_empty() {
				buf.push(*ind.first().unwrap());
				for i in 1..ind.len() {
					buf.push(ind[i] - ind[i - 1]);
				}
			}
			buf.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		}

		let end = output.stream_position()?;
		header.size = end - header.start;
		header.from = self.timestamps.first().cloned().unwrap_or(0);
		header.to = self.timestamps.last().cloned().unwrap_or(0);

		output.seek(SeekFrom::Start(header.start))?;
		header.serialize(&mut rmp_serde::Serializer::new(&mut output))?;
		assert!(
			header.start + header_size >= output.stream_position()?,
			"header has overwritten data"
		);
		output.seek(SeekFrom::Start(header.start + header.size))?;

		println!("{:?}", header);

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

		debug_assert_eq!(self.timestamps.iter().max(), self.timestamps.last());
		debug_assert_eq!(other.timestamps.iter().min(), other.timestamps.first());

		if self.timestamps.last().unwrap() > other.timestamps.first().unwrap() {
			if other.timestamps.last().unwrap() > self.timestamps.first().unwrap() {
				panic!("blocks intersect");
			}
			return other.merge(self);
		}

		let unwrap_index = |x: Option<Arc<Vec<Index>>>| {
			x.ok_or(anyhow::anyhow!(
				"all indexes must be loaded to merge blocks"
			))
		};
		let index_map: Result<BTreeMap<_, _>, _> = self
			.tags
			.into_iter()
			.zip(self.index.into_iter().map(unwrap_index))
			.map(|(tag, index)| index.map(|index| (tag, index)))
			.collect();
		// right now even if we throw the error out, we'll just panic on the higher level
		// so keep this for time being (it's more like a debug assert)
		let mut index_map = index_map.unwrap();

		other
			.index
			.into_iter()
			.map(unwrap_index)
			.map(|index| {
				index.map(|index| {
					Arc::try_unwrap(index)
						.unwrap()
						.into_iter()
						.map(|x| x + self.keys.len() as Index)
				})
			})
			.zip(other.tags)
			.for_each(|(index, tag)| {
				Arc::get_mut(index_map.entry(tag).or_default())
					.unwrap()
					.extend(index.unwrap());
			});

		self.tags = Vec::with_capacity(index_map.len());
		self.index = Vec::with_capacity(index_map.len());
		for (k, v) in index_map {
			self.tags.push(k);
			self.index.push(Some(v));
		}

		self.keys.append(&mut other.keys);
		self.timestamps.append(&mut other.timestamps);

		return self;
	}

	pub fn range(&self) -> (Timestamp, Timestamp) {
		// we shouldn't have empty blocks at all
		(
			*self.timestamps.first().unwrap(),
			*self.timestamps.last().unwrap(),
		)
	}

	pub fn release(&mut self, ind: usize) {
		self.index[ind] = None;
	}
}

#[derive(Debug)]
pub struct BlockFile<T> {
	file: T,
	header: BlockHeader,
	data: BlockData,
}

#[allow(dead_code)]
impl<T: Read + Write + Seek> BlockFile<T> {
	pub fn read_all(&mut self) -> Result<(), anyhow::Error> {
		for i in 0..self.data.index.len() {
			self.read_index(i)?;
		}
		return Ok(());
	}

	pub fn update_index(&mut self, id: usize) -> Result<(), anyhow::Error> {
		self.file.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id]
			.as_ref()
			.ok_or(anyhow::anyhow!("index must be loaded to update it"))?
			.serialize(&mut rmp_serde::Serializer::new(&mut self.file))?;
		return Ok(());
	}

	pub fn range(&self) -> (Timestamp, Timestamp) {
		return self.data.range();
	}

	pub fn release_all(self) -> (T, BlockHeader, BlockData) {
		return (self.file, self.header, self.data);
	}

	pub fn release(&mut self, ind: usize) {
		self.data.release(ind);
	}
}

impl<T: Read + Write + Seek> SearchBlock for BlockFile<T> {
	fn get_tags(&self) -> &[String] {
		&self.data.tags
	}

	fn get_keys(&self) -> &[String] {
		&self.data.keys
	}

	fn try_get_index(&self, id: usize) -> Option<Arc<Vec<u64>>> {
		return self.data.index[id].as_ref().map(|x| Arc::clone(&x));
	}

	fn read_index(&mut self, id: usize) -> Result<(), anyhow::Error> {
		match self.data.index[id] {
			Some(_) => Ok(()),
			None => {
				self.file.seek(SeekFrom::Start(self.header.index[id]))?;
				let mut ind: Vec<u64> = rmp_serde::from_read(&mut self.file)?;
				for i in 1..ind.len() {
					ind[i] += ind[i - 1];
				}
				self.data.index[id] = Some(Arc::new(ind));
				Ok(())
			}
		}
	}

	fn get_type(&self) -> BlockType {
		BlockType::File
	}
}

enum LazyBlockVariant {
	Header(BlockHeader),
	File(BlockFile<File>),
}

pub struct LazyBlock {
	name: PathBuf,
	data: LazyBlockVariant,
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
	) -> Result<BlockFile<T>, (Self, anyhow::Error)> {
		return self.data.write(file).map_err(|(data, err)| {
			(
				Self {
					data,
					size: self.size,
				},
				err,
			)
		});
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

	fn try_get_index(&self, id: usize) -> Option<Arc<Vec<u64>>> {
		// we want to force check, that in inmemoryblock we always have indexes
		Some(Arc::clone(self.data.index[id].as_ref().unwrap()))
	}

	fn read_index(&mut self, _: usize) -> Result<(), anyhow::Error> {
		Err(anyhow::anyhow!(
			"shouldn't call read_index on in memory block"
		))
	}

	fn get_type(&self) -> BlockType {
		BlockType::InMemory
	}
}

#[derive(Debug, Default, Clone)]
pub struct ActiveBlock {
	index: BTreeMap<String, Vec<Index>>,
	keys: Vec<String>,
	timestamps: Vec<Timestamp>,
	size: u64,
}

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
			index.push(Some(Arc::new(v)));
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
