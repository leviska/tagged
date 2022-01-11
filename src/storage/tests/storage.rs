use super::*;
use std::collections::BTreeMap;

macro_rules! vec_str {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
struct Data {
	key: String,
	tags: Vec<String>,
}

impl Data {
	fn new(key: &str, tags: Vec<String>) -> Data {
		Data {
			key: key.to_string(),
			tags,
		}
	}

	fn simple() -> Vec<Data> {
		vec![
			Data::new("key00", vec_str!["tag0", "tag1"]),
			Data::new("key01", vec_str!["tag1", "tag2"]),
			Data::new("key02", vec_str!["tag0", "tag3"]),
			Data::new("key03", vec_str!["tag0"]),
			Data::new("key04", vec_str!["tag4"]),
			Data::new("key05", vec_str!["tag5", "tag2"]),
			Data::new("key06", vec_str!["tag5", "tag1", "tag0", "tag3"]),
			Data::new("key07", vec_str!["tag6", "tag0", "tag3"]),
			Data::new("key08", vec_str!["tag0"]),
			Data::new("key09", vec_str!["tag7"]),
			Data::new("key10", vec_str!["tag1", "tag2"]),
			Data::new("key11", vec_str!["tag8"]),
			Data::new("key12", vec_str!["tag3", "tag2"]),
			Data::new("key13", vec_str!["tag5"]),
			Data::new("key14", vec_str!["tag6", "tag4"]),
		]
	}

	fn from_block(block: &dyn SearchBlock) -> Vec<Data> {
		let mut map: BTreeMap<String, Vec<String>> = BTreeMap::default();
		let tags = block.get_tags();
		let keys = block.get_keys();
		for (j, tag) in tags.iter().enumerate() {
			let tag_keys: Option<Vec<&str>> = block
				.try_get_index(j)
				.map(|v| v.iter().map(|i| -> &str { &keys[(*i) as usize] }).collect());

			if let Some(tag_keys) = tag_keys {
				for key in tag_keys {
					map.entry(key.to_string()).or_default().push(tag.clone());
				}
			}
		}
		return map
			.into_iter()
			.map(|(key, tags)| Data { key, tags })
			.collect();
	}
}

fn read_all(iter: StorageIter) -> (Vec<Data>, String) {
	let mut storage_data = Vec::default();
	let mut debug_output = String::new();

	for (i, block) in iter.enumerate() {
		let block = block.read().unwrap();
		let data = Data::from_block(&*block);
		debug_output += &format!("block={} type={:?}\n", i, block.get_type());
		for d in data.iter() {
			debug_output += &format!("{:?}\n", d);
		}
		debug_output += "\n";
		storage_data.extend(data.into_iter().rev());
	}
	storage_data.reverse();
	return (storage_data, debug_output);
}

fn check_storage(storage: &Storage, data: &[Data]) {
	let (storage_data, debug_output) = read_all(storage.iter());
	if storage_data != data {
		print!("{}", debug_output);
	}
	assert_eq!(storage_data, data);
}

fn init_data_dir() -> std::path::PathBuf {
	let path = std::path::Path::new("./data");
	std::fs::remove_dir_all(path).unwrap();
	path.to_path_buf()
}

#[tokio::test]
async fn basic() {
	env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();

	let config = Config {
		data_dir: init_data_dir(),
		max_active_size: 3,
		max_block_size: 10,
	};
	let (storage, stop) = Storage::new(config).unwrap();
	let mut data = Data::simple();

	tokio::task::yield_now().await;

	for i in 0..data.len() {
		log::debug!("push={}", i);
		storage
			.push(data[i].key.clone(), data[i].tags.clone())
			.unwrap();
		data[i].tags.sort();
		check_storage(&storage, &data[0..i + 1]);

		tokio::task::yield_now().await;
	}

	stop.await.unwrap();
	// check, that this is the only usage of arc
	Arc::try_unwrap(storage).unwrap();
}
