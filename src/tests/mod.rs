// This file is part of dirigent.

// Copyright (C) Frederik Gartenmeister.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use tokio::select;
use tracing::{debug, info};

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
	traits::{Context, ExitStatus, Index, IndexRegistry, InstanceError, Program},
	Pid,
};

#[derive(Clone)]
struct Message {
	from: String,
	text: String,
}

impl dirigent::traits::Message for Message {}

#[derive(Clone, Debug)]
struct TestProgram {
	name: &'static str,
}

#[async_trait::async_trait]
impl Program for TestProgram {
	async fn start(
		self: Box<Self>,
		ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus {
		tracing::info!("Hello, World from Test Programm {}!", self.name);

		tracing::info!("Programm {}, registering index...", self.name);

		registry
			.register(Arc::new(NotFromSelf::new(self.name.to_string())))
			.await;

		let name = self.name.to_string();
		let name_clone = name.clone();

		let sender = ctx.sender();
		ctx.spawn_sub(Box::pin(async move {
			loop {
				debug!("Sending message from {}", name_clone);
				let msg = Message {
					from: name_clone.clone(),
					text: format!("Hello From Program {}", name_clone.clone()),
				};

				futures_timer::Delay::new(Duration::from_millis(500)).await;
				let res = sender.send(Envelope::from(msg)).await;
				debug!("{:?}", res);
			}
		}));

		loop {
			match ctx.recv().await {
				Ok(envelope) => {
					envelope.try_read_ref::<Message, _, _>(|msg| {
						tracing::info!("{} received: {}", name.clone(), msg.text)
					});
				}
				Err(_e) => {}
			}
		}
	}
}

struct NotFromSelf {
	name: String,
}

impl NotFromSelf {
	fn new(name: String) -> Self {
		NotFromSelf { name }
	}
}

impl Index for NotFromSelf {
	fn indexed(&self, t: &Envelope) -> bool {
		t.try_read_ref::<Message, _, _>(|m| m.from != self.name)
			.unwrap_or(false)
	}
}

#[tracing_test::traced_test]
#[test]
fn it_works() {
	use tokio::runtime::Runtime;

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let (dirigent, takt) = dirigent::Dirigent::<Box<dyn Program>, _>::new(rt.handle().clone());

	let mut takt_clone = takt.clone();
	rt.spawn(async move {
		futures_timer::Delay::new(Duration::from_secs(1)).await;
		let pid_1 = takt_clone
			.run(Box::new(TestProgram { name: "FOO" }), "FOO")
			.await
			.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		let _pid_2 = takt_clone
			.run(Box::new(TestProgram { name: "BAR" }), "BAR")
			.await
			.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		takt_clone.preempt(pid_1).await.unwrap();
		takt_clone.kill(Pid::new(3)).await.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		takt_clone.unpreempt(pid_1).await.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		takt_clone.force_shutdown().await.unwrap();
	});

	let mut takt_clone = takt.clone();
	rt.spawn(async move {
		futures_timer::Delay::new(Duration::from_secs(10)).await;
		takt_clone.force_shutdown().await.unwrap();
	});

	Runtime::new().unwrap().block_on(async move {
		dirigent.begin().await.unwrap();
	});

	tracing::info!("Finished main...");
}

//#[tracing_test::traced_test]
#[test]
fn killing_all_when_dirigent_dropped() {
	use tokio::runtime::Runtime;

	let _ = tracing_subscriber::fmt::try_init();

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let (dirigent, takt) = dirigent::Dirigent::<Box<dyn Program>, _>::new(rt.handle().clone());

	let mut takt_clone = takt.clone();
	rt.spawn(async move {
		futures_timer::Delay::new(Duration::from_secs(1)).await;
		let pid_1 = takt_clone
			.run(Box::new(TestProgram { name: "FOO" }), "FOO")
			.await
			.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		let _pid_2 = takt_clone
			.run(Box::new(TestProgram { name: "BAR" }), "BAR")
			.await
			.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		takt_clone.preempt(pid_1).await.unwrap();
		takt_clone.kill(Pid::new(3)).await.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		takt_clone.unpreempt(pid_1).await.unwrap();
	});

	// This should trigger the end of the control loop
	// drop(takt);

	Runtime::new().unwrap().block_on(async move {
		let timer = futures_timer::Delay::new(Duration::from_secs(8)).fuse();
		let _ = select! {
			() = timer => {
				info!("Timer run out. Canceling Dirigent...");
				Ok(())
			},
			res = dirigent.begin().fuse() => {res}
		};
	});

	tracing::info!("Finished main...");
}
