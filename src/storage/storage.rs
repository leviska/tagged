use super::*;
use futures::Future;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::Notify;

#[allow(dead_code)]
const MIN_TIME: Timestamp = 0;
#[allow(dead_code)]
const MAX_TIME: Timestamp = std::u64::MAX;

#[allow(dead_code)]
pub fn range_intersect(a: (u64, u64), b: (u64, u64)) -> bool {
	debug_assert!(a.0 <= a.1);
	debug_assert!(b.0 <= b.1);
	a.0 <= b.1 && a.1 <= b.0
}

#[derive(Debug, Default, Clone)]
pub struct Config {
	data_dir: PathBuf,
	max_active_size: u64,
	max_block_size: u64,
}

struct StorageLockedIter<'a, T> {
	data: RwLockReadGuard<'a, Vec<Arc<RwLock<T>>>>,
	cur: usize,
}

impl<'a, T> StorageLockedIter<'a, T> {
	pub fn new(data: RwLockReadGuard<'a, Vec<Arc<RwLock<T>>>>) -> StorageLockedIter<'a, T> {
		StorageLockedIter {
			cur: data.len(),
			data,
		}
	}
}

impl<'a, T: SearchBlock + 'static> std::iter::Iterator for StorageLockedIter<'a, T> {
	type Item = Arc<RwLock<dyn SearchBlock>>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.cur > 0 {
			self.cur -= 1;
			Some(Arc::<RwLock<T>>::clone(&self.data[self.cur]))
		} else {
			None
		}
	}
}

enum StorageIterType<'a> {
	Active,
	InMemory(StorageLockedIter<'a, InMemoryBlock>),
	File(StorageLockedIter<'a, BlockFile<File>>),
}

pub struct StorageIter<'a> {
	storage: &'a Storage,
	cur: StorageIterType<'a>,
}

impl<'a> StorageIter<'a> {
	pub fn new(storage: &'a Storage) -> StorageIter<'a> {
		StorageIter {
			storage,
			cur: StorageIterType::Active,
		}
	}
}

impl<'a> std::iter::Iterator for StorageIter<'a> {
	type Item = Arc<RwLock<dyn SearchBlock>>;

	fn next(&mut self) -> Option<Self::Item> {
		match &mut self.cur {
			StorageIterType::Active => {
				let active_lock = self.storage.active_block.read().unwrap();
				let val = active_lock.clone();

				// first acquire the lock, then drop the other one
				// so active doesn't become inmemory inbetween
				let inmemory_lock = self.storage.compact_list.read().unwrap();
				std::mem::drop(active_lock);
				self.cur = StorageIterType::InMemory(StorageLockedIter::new(inmemory_lock));

				// make the conversion only here, so we don't take active_lock for too long
				let val = val.into_block();

				return Some(Arc::new(RwLock::new(val)));
			}
			StorageIterType::InMemory(iter) => {
				let v = iter.next();
				if v.is_some() {
					v
				} else {
					// here we unlock after taking the lock too
					let lock = self.storage.block_files.read().unwrap();
					self.cur = StorageIterType::File(StorageLockedIter::new(lock));
					self.next()
				}
			}
			StorageIterType::File(iter) => iter.next(),
		}
	}
}

#[allow(dead_code)]
pub fn read_indicies(
	block: Arc<RwLock<dyn SearchBlock>>,
	ids: &[usize],
) -> Result<impl Iterator<Item = Arc<Vec<u64>>>, anyhow::Error> {
	let mut res = vec![None; ids.len()];
	let mut skipped = Vec::default();
	{
		let block = block.read().unwrap();
		for (i, id) in ids.iter().enumerate() {
			if let Some(index) = block.try_get_index(*id) {
				res[i] = Some(index);
			} else {
				skipped.push(i);
			}
		}
	}

	if !skipped.is_empty() {
		let mut block = block.write().unwrap();
		for i in skipped.iter() {
			block.read_index(ids[*i])?;
		}
		// probably don't care about still being in write lock
		for i in skipped.iter() {
			res[*i] = Some(block.try_get_index(ids[*i]).unwrap());
		}
	}

	return Ok(res.into_iter().map(|x| x.unwrap()));
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Document {
	pub key: String,
	pub tags: Vec<String>,
}

#[derive(Debug)]
pub struct Storage {
	block_files: RwLock<Vec<Arc<RwLock<BlockFile<File>>>>>,
	compact_list: RwLock<Vec<Arc<RwLock<InMemoryBlock>>>>,
	active_block: RwLock<Box<ActiveBlock>>,
	config: Config,
	context: Arc<uuid::v1::Context>,

	bg_notify: Notify,
	stopped: std::sync::atomic::AtomicBool,
}

#[allow(dead_code)]
impl Storage {
	pub fn new(
		config: Config,
		context: Arc<uuid::v1::Context>,
	) -> Result<
		(
			Arc<Storage>,
			impl Future<Output = Result<(), anyhow::Error>>,
		),
		anyhow::Error,
	> {
		let storage = Arc::new(Storage {
			block_files: Default::default(),
			compact_list: Default::default(),
			active_block: Default::default(),
			bg_notify: Default::default(),
			stopped: Default::default(),
			config,
			context,
		});

		let self_copy = Arc::clone(&storage);
		let join = tokio::task::spawn(async move {
			log::info!("save worker started");
			self_copy.save_worker().await;
			log::info!("save worker stopped");
		});

		let self_copy = Arc::clone(&storage);
		let stop = async {
			self_copy.send_stop();
			join.await.map_err(anyhow::Error::msg)
		};

		return Ok((storage, stop));
	}

	pub async fn push(&self, key: String, tags: Vec<String>) -> Result<(), anyhow::Error> {
		self.push_impl(|active| {
			active.push(key, tags);
		})
		.await
	}

	// can overflow active block size up to batch size
	pub async fn push_batch(&self, docs: Vec<Document>) -> Result<(), anyhow::Error> {
		self.push_impl(|active| {
			for doc in docs {
				active.push(doc.key, doc.tags);
			}
		})
		.await
	}

	async fn push_impl(
		&self,
		pusher: impl FnOnce(&mut Box<ActiveBlock>),
	) -> Result<(), anyhow::Error> {
		let mut active = self.acquire_active().await;
		pusher(&mut active);
		if active.size() >= self.config.max_active_size {
			self.bg_notify.notify_one();
		}
		return Ok(());
	}

	async fn acquire_active<'a>(&'a self) -> RwLockWriteGuard<'a, Box<ActiveBlock>> {
		// in some cases we can give no runtime for block saving,
		// so we'll force ourselfes to yield here, if he hit the limit
		loop {
			{
				let active = self.active_block.write().unwrap();
				if active.size() < self.config.max_active_size {
					return active;
				}
			}
			tokio::task::yield_now().await;
		}
	}

	pub fn iter<'a>(&'a self) -> StorageIter<'a> {
		return StorageIter::new(self);
	}

	pub fn send_stop(self: Arc<Self>) {
		self.stopped
			.store(true, std::sync::atomic::Ordering::SeqCst);
		self.bg_notify.notify_waiters();
	}

	async fn save_worker(self: &Arc<Self>) {
		while self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
			self.bg_notify.notified().await;

			let self_copy = Arc::clone(self);
			tokio::task::spawn_blocking(move || {
				self_copy.save_active();
			})
			.await
			.unwrap();
		}
	}

	fn save_active(self: &Arc<Self>) {
		let mut active = self.active_block.write().unwrap();
		if active.size() < self.config.max_active_size {
			log::info!("missed saving active block");
			return;
		}

		log::info!("saving active block");
		let old_active = std::mem::take(active.as_mut());

		let mut compact_list = self.compact_list.write().unwrap();
		std::mem::drop(active);

		let block = old_active.into_block();
		compact_list.push(Arc::new(RwLock::new(block)));

		let new_block = self.compact(compact_list.as_mut());
		// guarantee, that new_block will not be lost while iterating
		// TODO: implement queue (look in todo in write_block)
		let mut block_files = self.block_files.write().unwrap();
		std::mem::drop(compact_list);
		if let Some(new_block) = new_block {
			self.write_block(new_block, block_files.as_mut());
		}
	}

	fn compact(
		&self,
		compact_list: &mut Vec<Arc<RwLock<InMemoryBlock>>>,
	) -> Option<Box<InMemoryBlock>> {
		log::info!("compaction started");
		let start_size = compact_list.len();
		// we take locks here, but we actually have gurantee, that they are free
		// so may be change this to unsafe later
		while let [.., prev, last] = &compact_list[..] {
			let need_merge = {
				let prev = prev.read().unwrap();
				let last = last.read().unwrap();
				prev.size() < last.size() * 4
			};
			if !need_merge {
				break;
			}
			let last = Arc::try_unwrap(compact_list.pop().unwrap())
				.unwrap()
				.into_inner()
				.unwrap();
			let prev = Arc::try_unwrap(compact_list.pop().unwrap())
				.unwrap()
				.into_inner()
				.unwrap();
			let new_block = prev.merge(last);
			compact_list.push(Arc::new(RwLock::new(new_block)));
		}
		log::info!(
			"compaction ended: compacted {} blocks",
			start_size - compact_list.len()
		);
		let need_write = compact_list
			.last()
			.map(|block| block.read().unwrap().size() > self.config.max_block_size)
			.unwrap_or(false);
		if need_write {
			let block = Arc::try_unwrap(compact_list.pop().unwrap())
				.unwrap()
				.into_inner()
				.unwrap();
			return Some(Box::new(block));
		}
		return None;
	}

	fn write_block(
		&self,
		block: Box<InMemoryBlock>,
		block_files: &mut Vec<Arc<RwLock<BlockFile<File>>>>,
	) {
		log::info!("writing block on disk");
		let result = self.try_write(block);
		if result.is_err() {
			// this is bad, because we'll just lose this block until next restart
			// when it will be read from WAL
			// TODO: save block in some queue, return blocks from it in search
			// TODO: and try to rewrite them
			log::error!("can't write block: {}", result.err().unwrap());
			return;
		}
		let block = result.unwrap();
		debug_assert!(
			block_files.is_empty()
				|| block_files.last().unwrap().read().unwrap().range().1 <= block.range().0
		);
		block_files.push(Arc::new(RwLock::new(block)));
		log::info!("writing block on disk: success");
	}

	fn try_write(&self, block: Box<InMemoryBlock>) -> Result<BlockFile<File>, anyhow::Error> {
		let file = File::options()
			.create(true)
			.truncate(true)
			.read(true)
			.write(true)
			.open(self.name_file(block.range().0))?;
		return block.write(file).map_err(|(_, err)| err);
	}

	fn name_file(&self, ts: Timestamp) -> PathBuf {
		let ts = uuid::v1::Timestamp::from_unix(
			self.context.as_ref(),
			ts / 1000,
			(ts % 1000 * 1_000_000) as u32,
		);
		let id = uuid::Uuid::new_v1(ts, &[0, 0, 0, 0, 0, 0])
			.unwrap()
			.to_hyphenated()
			.to_string();
		return self.config.data_dir.join(id).with_extension("index");
	}
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod storage_test;
