use tokio::sync::Notify;

use super::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};

const MIN_TIME: Timestamp = 0;
const MAX_TIME: Timestamp = std::u64::MAX;

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

type BlockVec<T> = Vec<Arc<RwLock<T>>>;

struct StorageLockedIter<'a, T> {
	data: RwLockReadGuard<'a, BlockVec<T>>,
	cur: usize,
}

impl<'a, T> StorageLockedIter<'a, T> {
	pub fn new(data: RwLockReadGuard<'a, BlockVec<T>>) -> StorageLockedIter<'a, T> {
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

				// first aquire the lock, then drop the other one
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

#[derive(Debug)]
pub struct Storage {
	block_files: RwLock<BlockVec<BlockFile<File>>>,
	compact_list: RwLock<BlockVec<InMemoryBlock>>,
	active_block: RwLock<Box<ActiveBlock>>,
	bg_notify: Notify,
	stopped: std::sync::atomic::AtomicBool,
	config: Config,
}

#[allow(dead_code)]
impl Storage {
	pub fn new(config: Config) -> Result<Arc<Storage>, anyhow::Error> {
		std::fs::create_dir_all(&config.data_dir)?;
		let storage = Arc::new(Storage {
			block_files: Default::default(),
			compact_list: Default::default(),
			active_block: Default::default(),
			bg_notify: Default::default(),
			stopped: Default::default(),
			config,
		});

		let self_copy = Arc::clone(&storage);
		tokio::task::spawn(async move {
			println!("storage: save worker started");
			self_copy.save_worker().await;
			println!("storage: save worker ended");
		});

		return Ok(storage);
	}

	pub fn push(self: &Arc<Self>, key: String, tags: Vec<String>) -> Result<(), anyhow::Error> {
		let mut active = self.active_block.write().unwrap();
		active.push(key, tags);
		// TODO: wal.push()?;
		if active.size() >= self.config.max_active_size {
			self.bg_notify.notify_one();
		}
		return Ok(());
	}

	pub fn iter<'a>(&'a self) -> StorageIter<'a> {
		return StorageIter::new(self);
	}

	async fn save_worker(self: &Arc<Self>) {
		loop {
			if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
				break;
			}
			self.bg_notify.notified().await;
			println!("storage: save worker cycle");

			let self_copy = Arc::clone(self);
			tokio::task::spawn_blocking(move || {
				self_copy.save_active();
			})
			.await
			.unwrap_or_else(|err| {
				log::error!("storage: worker join err: {}", err);
			});
		}
	}

	fn save_active(self: &Arc<Self>) {
		let mut active = self.active_block.write().unwrap();
		if active.size() < self.config.max_active_size {
			return;
		}

		let mut new_active = Box::<ActiveBlock>::default();
		std::mem::swap(&mut new_active, &mut active);

		let mut compact_list = self.compact_list.write().unwrap();
		std::mem::drop(active);

		let block = new_active.into_block();
		compact_list.push(Arc::new(RwLock::new(block)));

		let new_block = self.compact(compact_list.as_mut());
		std::mem::drop(compact_list);
		if let Some(new_block) = new_block {
			self.write_block(new_block);
		}
	}

	fn compact(
		&self,
		compact_list: &mut Vec<Arc<RwLock<InMemoryBlock>>>,
	) -> Option<Box<InMemoryBlock>> {
		// we take locks here, but we actually have gurantee, that they are free
		// so may be change this to unsafe later
		while let [.., prev, last] = &compact_list[..] {
			let need_merge = {
				let prev = prev.read().unwrap();
				let last = last.read().unwrap();
				prev.size() < last.size()
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
		let need_write = if let Some(block) = compact_list.last() {
			let block = block.read().unwrap();
			if block.size() > self.config.max_block_size {
				true
			} else {
				false
			}
		} else {
			false
		};
		if need_write {
			let block = Arc::try_unwrap(compact_list.pop().unwrap())
				.unwrap()
				.into_inner()
				.unwrap();
			return Some(Box::new(block));
		}
		return None;
	}

	fn write_block(&self, block: Box<InMemoryBlock>) {
		let (ts, _) = block.range();
		let result = self.try_write(block, ts);
		if result.is_err() {
			// this is bad, because we'll just lose this block until next restart
			// when it will be read from WAL
			// TODO: save block in some queue, return blocks from it in search
			// TODO: and try to rewrite them
			log::error!("can't write block: {}", result.err().unwrap());
			return;
		}
		let block = result.unwrap();
		let mut block_files = self.block_files.write().unwrap();
		debug_assert!(
			block_files.is_empty()
				|| block_files.last().unwrap().read().unwrap().range().1 <= block.range().0
		);
		block_files.push(Arc::new(RwLock::new(block)));
	}

	fn try_write(
		&self,
		block: Box<InMemoryBlock>,
		ts: Timestamp,
	) -> Result<BlockFile<File>, anyhow::Error> {
		let mut file = File::create(self.name_file(ts))?;
		return block.write(file).map_err(|(_, err)| err);
	}

	fn name_file(&self, ts: Timestamp) -> PathBuf {
		return self
			.config
			.data_dir
			.join(Path::new(&format!("{}.index", ts)));
	}
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod storage_test;