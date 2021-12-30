use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Block {
	tags: Vec<String>,
	keys: Vec<String>,
	timestamps: Vec<i64>,
	index: Vec<Vec<u64>>,
}

#[cfg(test)]
mod tests {
	use rmp_serde::{Deserializer, Serializer};
	use serde::{Deserialize, Serialize};

	#[test]
	fn some() {}
}
