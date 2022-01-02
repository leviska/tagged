use super::*;
use std::io::Cursor;
use std::time::*;

macro_rules! vec_str {
		($($x:expr),*) => (vec![$($x.to_string()),*]);
	}

macro_rules! vec_arc {
		($($x:expr),*) => (vec![$(Arc::new($x)),*]);
	}

#[test]
fn basic() {
	let block = BlockData {
		tags: vec_str!["tag0", "tag1", "tag2"],
		keys: vec_str!["key0", "key1"],
		timestamps: vec![100, 300],
		index: vec_arc![vec![0], vec![0, 1], vec![1]],
		read: vec![true; 3],
	};
	let mut buf = Cursor::new(vec![0; 128]);
	let block = block.write(&mut buf).unwrap();

	let header = BlockHeader::read_header(&mut buf, 0).unwrap();
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
		block.index.push(Arc::default());
		for j in (0..i * 10).step_by(10) {
			Arc::get_mut(block.index.last_mut().unwrap())
				.unwrap()
				.push(j as u64);
		}
	}
	block.read.resize(block.index.len(), true);

	let mut buf = Cursor::new(vec![0; 128]);
	let block = block.write(&mut buf).unwrap();

	let header = BlockHeader::read_header(&mut buf, 0).unwrap();
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

	let header = BlockHeader::read_header(&mut buf, 0).unwrap();
	let read_block = header.read_meta(&mut buf).unwrap();

	assert_eq!(block.data, read_block.data);
}

#[test]
fn active() {
	let mut start = SystemTime::now();
	std::thread::sleep(Duration::from_millis(1));

	let mut active = ActiveBlock::default();
	active.push("key0".to_string(), vec_str!["tag0", "tag1"]);
	active.push("key1".to_string(), vec_str!["tag1", "tag3"]);
	active.push("key2".to_string(), vec_str!["tag0"]);
	active.push("key3".to_string(), vec_str!["tag4", "tag0", "tag2"]);
	active.push("key4".to_string(), vec_str![]);
	active.push("key5".to_string(), vec_str!["tag0", "tag1"]);

	std::thread::sleep(Duration::from_millis(1));
	let end = SystemTime::now();

	let block = active.into_block();
	let expected = BlockData {
		tags: vec_str!["tag0", "tag1", "tag2", "tag3", "tag4"],
		keys: vec_str!["key0", "key1", "key2", "key3", "key4", "key5"],
		timestamps: block.timestamps.clone(),
		index: vec_arc![vec![0, 2, 3, 5], vec![0, 1, 5], vec![3], vec![1], vec![3]],
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
	first.push("key0".to_string(), vec_str!["tag0", "tag1"]);
	first.push("key1".to_string(), vec_str!["tag1", "tag3"]);
	first.push("key2".to_string(), vec_str!["tag0"]);
	let first = first.into_block();

	let mut second = ActiveBlock::default();
	second.push("key3".to_string(), vec_str!["tag4", "tag0", "tag2"]);
	second.push("key4".to_string(), vec_str!["tag2"]);
	second.push("key5".to_string(), vec_str!["tag0", "tag1"]);
	second.push("key6".to_string(), vec_str!["tag5"]);
	let second = second.into_block();

	let block = first.merge(second);

	let expected = BlockData {
		tags: vec_str!["tag0", "tag1", "tag2", "tag3", "tag4", "tag5"],
		keys: vec_str!["key0", "key1", "key2", "key3", "key4", "key5", "key6"],
		timestamps: Vec::default(), // do not test this
		index: vec_arc![
			vec![0, 2, 3, 5],
			vec![0, 1, 5],
			vec![3, 4],
			vec![1],
			vec![3],
			vec![6]
		],
		read: vec![true; 6],
	};
	assert_eq!(expected.tags, block.tags);
	assert_eq!(expected.keys, block.keys);
	assert_eq!(expected.index, block.index);
	assert_eq!(expected.read, block.read);
}

#[test]
fn merge_order() {
	let mut first = ActiveBlock::default();
	first.push("key0".to_string(), vec_str!["tag0"]);
	let first = first.into_block();

	std::thread::sleep(Duration::from_millis(1));

	let mut second = ActiveBlock::default();
	second.push("key1".to_string(), vec_str!["tag0"]);
	let second = second.into_block();

	let block = second.merge(first);

	assert_eq!(vec_str!["key0", "key1"], block.keys);
}

// we'll probably stick to single file = single block
// but it was easy to implement, so let's test it
#[test]
fn two_blocks_in_one_file() {
	let mut first = ActiveBlock::default();
	first.push("key0".to_string(), vec_str!["tag0", "tag1"]);
	first.push("key1".to_string(), vec_str!["tag1", "tag3"]);
	first.push("key2".to_string(), vec_str!["tag0"]);
	let first = first.into_block();

	let mut second = ActiveBlock::default();
	second.push("key3".to_string(), vec_str!["tag4", "tag0", "tag2"]);
	second.push("key4".to_string(), vec_str!["tag2"]);
	second.push("key5".to_string(), vec_str!["tag0", "tag1"]);
	second.push("key6".to_string(), vec_str!["tag5"]);
	let second = second.into_block();

	let mut buf = Cursor::new(vec![]);
	let first = first.write(&mut buf).unwrap();
	assert_eq!(first.header.start, 0);
	assert_ne!(first.header.size, 0);
	assert_eq!(
		buf.get_ref().len() as u64,
		first.header.start + first.header.size
	);

	let second = second.write(&mut buf).unwrap();
	assert_eq!(second.header.start, first.header.start + first.header.size);
	assert_ne!(second.header.size, 0);
	assert_eq!(
		buf.get_ref().len() as u64,
		second.header.start + second.header.size
	);

	let mut check = |block: &BlockFile| {
		let mut read = BlockHeader::read_header(&mut buf, block.header.start)
			.unwrap()
			.read_meta(&mut buf)
			.unwrap();

		read.read_all(&mut buf).unwrap();

		assert_eq!(block.header, read.header);
		assert_eq!(block.data, read.data);
	};

	check(&first);
	check(&second);
}
