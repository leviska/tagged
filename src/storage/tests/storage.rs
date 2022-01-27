use crate::tests;

use super::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::time::Instant;

macro_rules! vec_str {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

fn random_string(min_len: usize, max_len: usize) -> String {
	thread_rng()
		.sample_iter(&Alphanumeric)
		.take(thread_rng().gen_range(min_len..=max_len))
		.map(char::from)
		.collect()
}

fn new_doc(key: &str, tags: Vec<String>) -> Document {
	Document {
		key: key.to_string(),
		tags,
	}
}

fn simple_data() -> Vec<Document> {
	vec![
		new_doc("key00", vec_str!["tag0", "tag1"]),
		new_doc("key01", vec_str!["tag1", "tag2"]),
		new_doc("key02", vec_str!["tag0", "tag3"]),
		new_doc("key03", vec_str!["tag0"]),
		new_doc("key04", vec_str!["tag4"]),
		new_doc("key05", vec_str!["tag5", "tag2"]),
		new_doc("key06", vec_str!["tag5", "tag1", "tag0", "tag3"]),
		new_doc("key07", vec_str!["tag6", "tag0", "tag3"]),
		new_doc("key08", vec_str!["tag0"]),
		new_doc("key09", vec_str!["tag7"]),
		new_doc("key10", vec_str!["tag1", "tag2"]),
		new_doc("key11", vec_str!["tag8"]),
		new_doc("key12", vec_str!["tag3", "tag2"]),
		new_doc("key13", vec_str!["tag5"]),
		new_doc("key14", vec_str!["tag6", "tag4"]),
	]
}

fn gen_vec<T>(count: usize, f: fn(usize) -> T) -> Vec<T> {
	(0..count).map(f).collect()
}

fn random_data(size: usize) -> Vec<Document> {
	gen_vec(size, |_| Document {
		key: random_string(3, 10),
		tags: gen_vec(thread_rng().gen_range(1..20), |i| {
			format!("tag_name_{}_{}", thread_rng().gen_range(1..20), i)
		}),
	})
}

fn from_block(block: &dyn SearchBlock) -> Vec<Document> {
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
		.map(|(key, tags)| Document { key, tags })
		.collect();
}

fn read_all(iter: StorageIter) -> (Vec<Document>, String) {
	let mut storage_data = Vec::default();
	let mut debug_output = String::new();

	for (i, block) in iter.enumerate() {
		let block = block.read().unwrap();
		let data = from_block(&*block);
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

fn check_storage(storage: &Storage, data: &[Document]) {
	let (storage_data, debug_output) = read_all(storage.iter());
	if storage_data != data {
		print!("{}", debug_output);
	}
	assert_eq!(storage_data, data);
}

#[test]
fn basic() -> Result<(), anyhow::Error> {
	tests::async_basic!(data_dir, {
		let config = Config {
			data_dir: data_dir.to_path_buf(),
			max_active_size: 3,
			max_block_size: 10,
		};
		let (storage, stop) = Storage::new(config, Arc::new(uuid::v1::Context::new(0)))?;
		let mut data = simple_data();

		tokio::task::yield_now().await;

		for i in 0..data.len() {
			log::debug!("push={}", i);
			storage
				.push(data[i].key.clone(), data[i].tags.clone())
				.await?;
			data[i].tags.sort();
			check_storage(&storage, &data[0..i + 1]);

			tokio::task::yield_now().await;
		}

		stop.await?;
		Arc::try_unwrap(storage).map_err(|_| anyhow::anyhow!("someone still using storage"))?;

		Ok(())
	})
}

#[test]
fn bench() -> Result<(), anyhow::Error> {
	tests::async_basic!(data_dir, {
		let config = Config {
			data_dir: data_dir.to_path_buf(),
			max_active_size: 800,
			max_block_size: 100 * 800,
		};
		let (storage, stop) = Storage::new(config, Arc::new(uuid::v1::Context::new(0)))?;
		const BATCH_SIZE: usize = 1000;
		const BATCHES_SIZE: usize = 100;
		let batches = gen_vec(BATCHES_SIZE, |_| random_data(BATCH_SIZE));

		tokio::task::yield_now().await;

		log::debug!("started");
		let start = Instant::now();
		let mut join = Vec::with_capacity(batches.len());
		for batch in batches {
			//tokio::task::yield_now().await;
			let storage = Arc::clone(&storage);
			join.push(tokio::task::spawn(async move {
				storage.push_batch(batch).await
			}));
		}

		let err: Result<Vec<_>, _> = futures::future::join_all(join).await.into_iter().collect();
		err?;

		let took = Instant::now() - start;
		log::debug!(
			"took {:.2?}; {}op/s",
			took,
			BATCH_SIZE * BATCHES_SIZE * 1000000000 / took.as_nanos() as usize
		);

		stop.await?;

		Ok(())
	})
}
