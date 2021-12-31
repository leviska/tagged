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
				index: vec![vec![]; indexes],
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

		header.from = self.timestamps.iter().min().cloned().unwrap_or(0);
		header.to = self.timestamps.iter().max().cloned().unwrap_or(0);

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
		if self.timestamps.last().unwrap() > other.timestamps.last().unwrap() {
			return other.merge(self);
		}

		#[cfg(debug_assertions)]
		{
			for r in self.read.iter() {
				assert!(r);
			}
			for r in other.read.iter() {
				assert!(r);
			}
		}

		let mut index_map = BTreeMap::<String, Vec<u64>>::default();
		for (i, arr) in self.index.into_iter().enumerate() {
			index_map.insert(self.tags[i].clone(), arr);
		}

		for (i, arr) in other.index.into_iter().enumerate() {
			index_map
				.entry(other.tags[i].clone())
				.or_default()
				.extend(arr.into_iter().map(|x| x + (self.tags.len() as u64)));
		}

		let (tags, index) = map_to_vecs(index_map);
		self.tags = tags;
		self.index = index;

		self.keys.append(&mut other.keys);
		self.timestamps.append(&mut other.timestamps);
		self.read.resize(self.index.len(), true);

		return self;
	}
}

fn map_to_vecs<K, V>(map: BTreeMap<K, V>) -> (Vec<K>, Vec<V>) {
	let mut keys = Vec::with_capacity(map.len());
	let mut values = Vec::with_capacity(map.len());
	for (k, v) in map {
		keys.push(k);
		values.push(v);
	}
	return (keys, values);
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

	pub fn read_index(
		&mut self,
		mut input: impl Read + Seek,
		id: usize,
	) -> Result<&Vec<u64>, anyhow::Error> {
		if self.data.read[id] {
			return Ok(&self.data.index[id]);
		}

		input.seek(SeekFrom::Start(self.header.index[id]))?;
		self.data.index[id] = rmp_serde::from_read(input)?;
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
		let (tags, index) = map_to_vecs(self.index);
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
	use std::time::*;

	macro_rules! vec_str {
		($($x:expr),*) => (vec![$($x.to_string()),*]);
	}

	#[test]
	fn basic() {
		let block = BlockData {
			tags: vec_str!["tag0", "tag1", "tag2"],
			keys: vec_str!["key0", "key1"],
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
		assert_eq!(read_block.header.from, 100);
		assert_eq!(read_block.header.to, 300);
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

	#[test]
	fn empty() {
		let block = BlockData::default();
		let mut buf = Cursor::new(vec![0; 128]);
		let block = block.write(&mut buf).unwrap();

		let header = BlockHeader::read_header(&mut buf).unwrap();
		let read_block = header.read_meta(&mut buf).unwrap();

		assert_eq!(block.data, read_block.data);
	}

	#[test]
	fn active() {
		let mut start = SystemTime::now();
		std::thread::sleep(Duration::from_millis(1));

		let mut active = ActiveBlock::default();
		active.push("key0", &vec_str!["tag0", "tag1"]);
		active.push("key1", &vec_str!["tag1", "tag3"]);
		active.push("key2", &vec_str!["tag0"]);
		active.push("key3", &vec_str!["tag4", "tag0", "tag2"]);
		active.push("key4", &vec_str![]);
		active.push("key5", &vec_str!["tag0", "tag1"]);

		std::thread::sleep(Duration::from_millis(1));
		let end = SystemTime::now();

		let block = active.into_block();
		let expected = BlockData {
			tags: vec_str!["tag0", "tag1", "tag2", "tag3", "tag4"],
			keys: vec_str!["key0", "key1", "key2", "key3", "key4", "key5"],
			timestamps: block.timestamps.clone(),
			index: vec![vec![0, 2, 3, 5], vec![0, 1, 5], vec![3], vec![1], vec![3]],
			read: vec![true; 5],
		};
		assert_eq!(expected, block);

		for t in block.timestamps {
			let cur = SystemTime::UNIX_EPOCH + Duration::from_millis(t);
			assert!(start <= cur, "{:?} <= {:?}", start, cur);
			assert!(cur <= end, "{:?} <= {:?}", cur, end);
			start = cur;
		}
	}

	#[test]
	fn merge() {
		let mut first = ActiveBlock::default();
		first.push("key0", &vec_str!["tag0", "tag1"]);
		first.push("key1", &vec_str!["tag1", "tag3"]);
		first.push("key2", &vec_str!["tag0"]);
		let first = first.into_block();

		let mut second = ActiveBlock::default();
		second.push("key3", &vec_str!["tag4", "tag0", "tag2"]);
		second.push("key4", &vec_str!["tag2"]);
		second.push("key5", &vec_str!["tag0", "tag1"]);
		second.push("key6", &vec_str!["tag5"]);
		let second = second.into_block();

		let block = first.merge(second);

		let expected = BlockData {
			tags: vec_str!["tag0", "tag1", "tag2", "tag3", "tag4", "tag5"],
			keys: vec_str!["key0", "key1", "key2", "key3", "key4", "key5", "key6"],
			timestamps: Vec::default(), // do not test this
			index: vec![
				vec![0, 2, 3, 5],
				vec![0, 1, 5],
				vec![3, 4],
				vec![1],
				vec![3],
				vec![6],
			],
			read: vec![true; 6],
		};
		assert_eq!(expected.tags, block.tags);
		assert_eq!(expected.keys, block.keys);
		assert_eq!(expected.index, block.index);
		assert_eq!(expected.read, block.read);
	}
}
