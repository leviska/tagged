//#![allow(dead_code)]

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::convert::Infallible;

mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	//let addr = ([127, 0, 0, 1], 3000).into();
	//let server = Server::bind(&addr).serve();
	return Result::Ok(());
}
