// This file is part of dirigent.

// Copyright (C) Frederik Gartenmeister.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate as dirigent;
use crate::{
	traits::{Context, ExitStatus, Index, Priority, Program},
	Pid,
};

#[derive(Clone)]
struct Message {
	text: String,
}

impl dirigent::traits::Message for Message {
	type Response = ();
}

#[derive(Clone, Debug)]
struct TestProgram1;
#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram1 {
	async fn start(self: Box<Self>, mut ctx: Box<dyn Context>) -> ExitStatus {
		tracing::info!("Hello, World from Test Programm 1!");

		let msg = Message {
			text: "Hello From Program 1".to_owned(),
		};
		ctx.send(msg.into()).await.unwrap();

		// Will never receive
		ctx.recv().await.unwrap();

		tracing::info!("NEEVER");

		Ok(())
	}
}

#[derive(Clone, Debug)]
struct TestProgram2 {
	name: &'static str,
}

#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram2 {
	async fn start(self: Box<Self>, mut ctx: Box<dyn Context>) -> ExitStatus {
		tracing::info!("Hello, World from Test Programm {}!", self.name);

		let msg = Message {
			text: format!("Hello From Program {}", self.name),
		};
		ctx.send(msg.into()).await.unwrap();

		let envelope = ctx.recv().await.unwrap();

		let msg = envelope.read_ref::<Message>().unwrap();

		tracing::info!("{} received: {}", self.name, msg.text);

		Ok(())
	}

	fn index(&self) -> Box<dyn Index> {
		Box::new(dirigent::traits::FullIndex {})
	}
}

#[test]
fn test_1() {
	use tokio::runtime::Runtime;

	let subscriber = tracing_subscriber::FmtSubscriber::builder()
		.with_max_level(tracing::Level::TRACE)
		.finish();
	tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let mut dirigent = dirigent::Dirigent::<Box<dyn Program>, _>::new(rt.handle().clone());

	dirigent
		.schedule(Box::new(TestProgram1 {}), "Test 1", Priority::High)
		.unwrap();
	dirigent
		.schedule(
			Box::new(TestProgram2 { name: "Test 2" }),
			"Test 2",
			Priority::Custom(2000),
		)
		.unwrap();

	let takt = dirigent.takt();
	rt.spawn(async move {
		let mut takt = takt;
		takt.schedule_and_start(
			Box::new(TestProgram2 { name: "Test 3" }),
			"Test 3",
			Priority::Middle,
		)
		.await
		.unwrap();

		takt.kill(Pid::new(2)).await.unwrap();
	});

	rt.block_on(async move {
		dirigent.begin().await.unwrap();
	});
}

#[test]
fn test_2() {
	use tokio::runtime::Runtime;

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let mut dirigent = dirigent::Dirigent::<TestProgram2, _>::new(rt);

	dirigent
		.schedule(TestProgram2 { name: "Test 2" }, "Test 2", Priority::High)
		.unwrap();
	let takt = dirigent.takt();

	let rt = Runtime::new().unwrap();
	rt.spawn(async move {
		let mut takt = takt;
		takt.schedule_and_start(TestProgram2 { name: "Test 1" }, "Test 1", Priority::High)
			.await
			.unwrap();
	});

	rt.block_on(async move {
		dirigent.begin().await.expect("Failed launching dirigent.");
	});
}
