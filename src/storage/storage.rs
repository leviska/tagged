use super::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Debug, Default)]
pub struct Storage {
	blocks: RwLock<Vec<RwLock<Box<BlockFile>>>>,
	active: RwLock<Box<ActiveBlock>>,
	root_dir: PathBuf,
	threshold: usize,
}

#[allow(dead_code)]
impl Storage {
	pub fn push(self: Arc<Self>, key: String, tags: Vec<String>) {
		let mut active = self.active.write().unwrap();
		active.push(key, tags);
		// TODO: wal.push
		if active.size() < self.threshold {
			return;
		}
		let mut new_active = Box::<ActiveBlock>::default();
		std::mem::swap(&mut new_active, &mut active);
		// std::mem::drop(active); // TODO: optimization?

		let self_copy = Arc::clone(&self);
		tokio::task::spawn_blocking(move || {
			self_copy.write_active(new_active);
		});
	}

	pub fn write_active(&self, active: Box<ActiveBlock>) {
		let ts = active.first_timestamp();
		let block = active.into_block();
		let block_file = self.try_write(block, ts);
		if block_file.is_err() {
			// TODO: save blockdata in `self` and try again some time later
			log::error!("cannot save active block: {}", block_file.err().unwrap());
			return;
		}
		let block_file = block_file.unwrap();
		let mut blocks = self.blocks.write().unwrap();
		blocks.push(RwLock::new(Box::new(block_file)));
	}

	fn try_write(&self, block: BlockData, ts: Timestamp) -> Result<BlockFile, anyhow::Error> {
		let mut file = File::create(self.name_file(ts))?;
		return block.write(&mut file).map_err(|(_, err)| err);
	}

	fn name_file(&self, ts: Timestamp) -> PathBuf {
		return self.root_dir.join(Path::new(&format!("{}.index", ts)));
	}
}
