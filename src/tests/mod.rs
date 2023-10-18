// This file is part of dirigent.

// Copyright (C) Frederik Gartenmeister.
// SPDX-License-Identifier: Apache-2.0

use std::{task::Poll, time::Duration};

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
	traits::{Context, ExitStatus, Index, Program},
	Pid,
};

#[derive(Clone)]
struct Message {
	from: String,
	text: String,
}

impl dirigent::traits::Message for Message {}

#[derive(Clone, Debug)]
struct TestProgram1;
#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram1 {
	async fn start(self: Box<Self>, mut ctx: Box<dyn Context>) -> ExitStatus {
		tracing::info!("Hello, World from Test Programm 1!");

		let msg = Message {
			from: "Programm 1".to_string(),
			text: "Hello From Program 1".to_string(),
		};
		if let Err(e) = ctx.send(msg.into()).await {
			tracing::error!("Program 1: Could not send, {:?}", e)
		};

		loop {
			match ctx.try_recv().await {
				Ok(envelope) => {
					if let Some(envelope) = envelope {
						envelope.try_read_ref::<Message, _, _>(|msg| {
							tracing::info!("Program 1 received: {}", msg.text)
						});
					}
				}
				Err(_e) => {
					//tracing::error!("Program 1: Could not recv, {:?}", e)
				}
			}

			//futures_timer::Delay::new(Duration::from_secs(3)).await;
		}

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
			from: self.name.to_string(),
			text: format!("Hello From Program {}", self.name),
		};

		if let Err(e) = ctx.send(msg.into()).await {
			tracing::error!("{}: Could not send, {:?}", self.name, e)
		};

		loop {
			match ctx.recv().await {
				Ok(envelope) => {
					//if let Some(envelope) = envelope {
					envelope.try_read_ref::<Message, _, _>(|msg| {
						tracing::info!("{} received: {}", self.name, msg.text)
					});
					//}
				}
				Err(_e) => {
					//tracing::error!("{}: Could not recv, {:?}", self.name, e)
				}
			}

			//futures_timer::Delay::new(Duration::from_secs(3)).await;
		}

		Ok(())
	}

	fn index(&self) -> Box<dyn Index> {
		Box::new(NotFromSelf {
			name: self.name.to_string(),
		})
	}
}

struct NotFromSelf {
	name: String,
}

impl dirigent::traits::Index for NotFromSelf {
	fn indexed(&self, t: &Envelope) -> bool {
		t.try_read_ref::<Message, _, _>(|m| m.from != self.name)
			.unwrap_or(false)
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
		.schedule(Box::new(TestProgram2 { name: "FOO" }), "FOO")
		.unwrap();
	dirigent
		.schedule(Box::new(TestProgram2 { name: "BAR" }), "BAR")
		.unwrap();

	let takt = dirigent.takt();
	rt.spawn(async move {
		let mut takt = takt;

		futures_timer::Delay::new(Duration::from_secs(1)).await;
		takt.kill(Pid::new(2)).await;
		futures_timer::Delay::new(Duration::from_secs(5)).await;
		takt.stop(Pid::new(1)).await.unwrap();
	});

	rt.block_on(async move {
		dirigent.begin().await.unwrap();
	});

	tracing::info!("Finished main...");
}

#[test]
fn test_2() {
	use tokio::runtime::Runtime;

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let mut dirigent = dirigent::Dirigent::<TestProgram2, _>::new(rt);

	dirigent
		.schedule(TestProgram2 { name: "Test 2" }, "Test 2")
		.unwrap();
	let takt = dirigent.takt();

	let rt = Runtime::new().unwrap();
	rt.spawn(async move {
		let mut takt = takt;
		takt.schedule_and_start(TestProgram2 { name: "Test 1" }, "Test 1")
			.await
			.unwrap();
	});

	rt.block_on(async move {
		dirigent.begin().await.expect("Failed launching dirigent.");
	});
}
