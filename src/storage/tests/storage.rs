use super::*;

macro_rules! vec_str {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

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
			Data::new("key0", vec_str!["tag0", "tag1"]),
			Data::new("key1", vec_str!["tag1", "tag2"]),
			Data::new("key2", vec_str!["tag0", "tag3"]),
			Data::new("key3", vec_str!["tag0"]),
			Data::new("key4", vec_str!["tag4"]),
			Data::new("key5", vec_str!["tag5", "tag2"]),
			Data::new("key6", vec_str!["tag5", "tag1", "tag0", "tag3"]),
			Data::new("key7", vec_str!["tag6", "tag0", "tag3"]),
			Data::new("key8", vec_str!["tag0"]),
			Data::new("key9", vec_str!["tag7"]),
			Data::new("key10", vec_str!["tag1", "tag2"]),
			Data::new("key11", vec_str!["tag8"]),
			Data::new("key12", vec_str!["tag3", "tag2"]),
			Data::new("key13", vec_str!["tag5"]),
			Data::new("key14", vec_str!["tag6", "tag4"]),
		]
	}
}

fn check_storage(storage: &Storage) {
	for (i, block) in storage.iter().enumerate() {
		let block = block.read().unwrap();
		let tags = block.get_tags();
		let keys = block.get_keys();
		println!("\tblock i={}", i);
		for (j, tag) in tags.iter().enumerate() {
			let indexes: Option<Vec<&str>> = block
				.try_get_index(j)
				.map(|v| v.iter().map(|i| -> &str { &keys[(*i) as usize] }).collect());

			//let indexes = block.try_get_index(j);
			println!("\ttag: {:?}; index: {:?}", tag, indexes);
		}
		println!();
	}
}

fn init_data_dir() -> std::path::PathBuf {
	let path = std::path::Path::new("./data");
	std::fs::remove_dir_all(path).unwrap();
	path.to_path_buf()
}

#[tokio::test]
async fn basic() {
	env_logger::builder().is_test(true).try_init().unwrap();

	let config = Config {
		data_dir: init_data_dir(),
		max_active_size: 3,
		max_block_size: 10,
	};
	let storage = Storage::new(config).unwrap();
	let data = Data::simple();

	for _ in 0..10 {
		for (i, obj) in data.iter().enumerate() {
			tokio::task::yield_now().await;
			storage.push(obj.key.clone(), obj.tags.clone()).unwrap();
			println!("iter {}", i);
			check_storage(&storage);
		}
	}
}
