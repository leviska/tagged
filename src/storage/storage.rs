use super::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

#[derive(Debug, Default)]
pub struct Storage {
	block_files: RwLock<Vec<RwLock<Box<BlockFile>>>>,
	blocks: RwLock<Vec<RwLock<Box<InMemoryBlock>>>>,
	active_block: RwLock<Box<ActiveBlock>>,
	root_dir: PathBuf,
	max_active_size: u64,
	max_block_size: u64,
}

#[allow(dead_code)]
impl Storage {
	pub fn push(self: Arc<Self>, key: String, tags: Vec<String>) {
		let mut active = self.active_block.write().unwrap();
		active.push(key, tags);
		// TODO: wal.push
		if active.size() < self.max_active_size {
			return;
		}
		let mut new_active = Box::<ActiveBlock>::default();
		std::mem::swap(&mut new_active, &mut active);
		// std::mem::drop(active); // TODO: optimization?

		let self_copy = Arc::clone(&self);
		tokio::task::spawn_blocking(move || {
			self_copy.save_active(new_active);
		});
	}

	fn save_active(self: Arc<Self>, active: Box<ActiveBlock>) {
		let block = active.into_block();
		let mut blocks = self.blocks.write().unwrap();
		blocks.push(RwLock::new(Box::new(block)));

		Arc::clone(&self).compact(blocks.as_mut());
	}

	fn compact(self: Arc<Self>, blocks: &mut Vec<RwLock<Box<InMemoryBlock>>>) {
		// we take locks here, but we actually have gurantee, that they are free
		// so may be change this to unsafe later
		while let [.., prev, last] = &blocks[..] {
			let need_merge = {
				let prev = prev.read().unwrap();
				let last = last.read().unwrap();
				prev.size() < last.size()
			};
			if !need_merge {
				break;
			}
			let last = blocks.pop().unwrap().into_inner().unwrap();
			let prev = blocks.pop().unwrap().into_inner().unwrap();
			let new_block = prev.merge(*last);
			blocks.push(RwLock::new(Box::new(new_block)));
		}
		let need_write = if let Some(block) = blocks.last() {
			let block = block.read().unwrap();
			if block.size() > self.max_block_size {
				true
			} else {
				false
			}
		} else {
			false
		};
		if need_write {
			let block = blocks.pop().unwrap().into_inner().unwrap();
			let self_copy = Arc::clone(&self);
			tokio::task::spawn_blocking(move || {
				self_copy.write_block(block);
			});
		}
	}

	fn write_block(&self, block: Box<InMemoryBlock>) {
		let ts = block.first_timestamp();
		let result = self.try_write(block, ts);
	}

	fn try_write(
		&self,
		block: Box<InMemoryBlock>,
		ts: Timestamp,
	) -> Result<BlockFile, anyhow::Error> {
		let mut file = File::create(self.name_file(ts))?;
		return block.write(&mut file).map_err(|(_, err)| err);
	}

	fn name_file(&self, ts: Timestamp) -> PathBuf {
		return self.root_dir.join(Path::new(&format!("{}.index", ts)));
	}
}
