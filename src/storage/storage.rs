use super::*;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct Storage {
	files: HashMap<u64, RwLock<Box<BlockFile>>>,
	blocks: BTreeMap<u64, u64>,
	active: Box<ActiveBlock>,
}

impl Storage {
	pub fn push(&mut self, key: &str, tags: &[String]) {}
}
