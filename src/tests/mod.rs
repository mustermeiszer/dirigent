// This file is part of dirigent.

// Copyright (C) Frederik Gartenmeister.
// SPDX-License-Identifier: Apache-2.0

use tokio::runtime::Handle;

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
	envelope::Envelope,
	traits::{Context, ExitStatus, Index, Process, Program},
};

#[derive(Clone)]
struct Message {
	text: &'static str,
}

impl dirigent::traits::Message for Message {
	type Response = ();
}

#[derive(Clone)]
struct TestProgram1;
#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram1 {
	async fn start(self: Box<Self>, mut ctx: Box<dyn Context>) -> ExitStatus {
		println!("Hello, World from Test Programm 1!");

		let msg = Message {
			text: "Hello From Program 1",
		};
		ctx.send(msg.into()).await.unwrap();

		// Will never receive
		ctx.recv().await.unwrap();

		println!("NEEVER");

		Ok(())
	}
}

#[derive(Clone)]
struct TestProgram2;
#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram2 {
	async fn start(self: Box<Self>, mut ctx: Box<dyn Context>) -> ExitStatus {
		println!("Hello, World from Test Programm 2!");

		let msg = Message {
			text: "Hello From Program 2",
		};
		ctx.send(msg.into()).await.unwrap();

		let envelope = ctx.recv().await.unwrap();

		let msg = envelope.read_ref::<Message>().unwrap();

		println!("{}", msg.text);

		Ok(())
	}

	fn index(&self) -> Box<dyn Index> {
		Box::new(dirigent::traits::FullIndex {})
	}
}

#[test]
fn test_1() {
	use tokio::runtime::Runtime;

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let mut dirigent = dirigent::Dirigent::<Box<dyn Program>, _>::new(rt.handle().clone());

	dirigent.schedule(Box::new(TestProgram1 {})).unwrap();
	dirigent.schedule(Box::new(TestProgram2 {})).unwrap();
	let takt = dirigent.takt();

	rt.block_on(async move {
		dirigent.begin().await;
	});
}
