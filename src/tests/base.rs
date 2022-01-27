use std::path::Path;

fn get_data_dir(test_name: &str) -> std::path::PathBuf {
	std::path::Path::new("./test_data").join(test_name)
}

fn init_logger() {
	let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
		.try_init();
}

pub fn run_with_logger(f: impl FnOnce() -> Result<(), anyhow::Error>) -> Result<(), anyhow::Error> {
	init_logger();
	f()
}

fn remove_dir_all<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
	let err = std::fs::remove_dir_all(path);
	match err {
		Err(x) => match x.kind() {
			std::io::ErrorKind::NotFound => Ok(()),
			_ => Err(x),
		},
		Ok(()) => Ok(()),
	}
}

pub fn run_with_dir(
	test_name: &str,
	f: impl FnOnce(&Path) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
	let data_dir = get_data_dir(test_name);

	// TODO: make work with multiple tests run at the same time
	/*ctrlc::set_handler(move || {
		remove_dir_all(data_dir_copy.as_path()).unwrap();
		std::process::exit(1);
	})
	.expect("Error setting Ctrl-C handler");*/

	remove_dir_all(data_dir.as_path()).unwrap();
	std::fs::create_dir_all(data_dir.as_path()).unwrap();

	let res = f(data_dir.as_path());

	remove_dir_all(data_dir.as_path()).unwrap();

	res
}

pub fn run_basic(
	test_name: &str,
	f: impl FnOnce(&Path) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
	run_with_logger(|| run_with_dir(test_name, f))
}

// lambda futures are unstable :(
pub fn run_async(
	f: impl futures::Future<Output = Result<(), anyhow::Error>>,
) -> Result<(), anyhow::Error> {
	tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap()
		.block_on(f)
}

#[allow(dead_code)]
pub fn run_async_threaded(
	f: impl futures::Future<Output = Result<(), anyhow::Error>>,
) -> Result<(), anyhow::Error> {
	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.unwrap()
		.block_on(f)
}

macro_rules! test_name {
	() => {{
		fn f() {}
		fn type_name_of<T>(_: T) -> &'static str {
			std::any::type_name::<T>()
		}
		let name = type_name_of(f);
		&(&name[..name.len() - 3]).replace("::", "/")
	}};
}

macro_rules! async_basic {
	($data_dir: ident, $test_func:expr) => {{
		crate::tests::run_basic(crate::tests::test_name!(), |$data_dir| {
			crate::tests::run_async(async move { $test_func })
		})
	}};
}

#[allow(unused_macros)]
macro_rules! async_threaded_basic {
	($data_dir: ident, $test_func:expr) => {{
		crate::tests::run_basic(crate::tests::test_name!(), |$data_dir| {
			crate::tests::run_async_threaded(async move { $test_func })
		})
	}};
}

pub(crate) use async_basic;
#[allow(unused_imports)]
pub(crate) use async_threaded_basic;
pub(crate) use test_name;
