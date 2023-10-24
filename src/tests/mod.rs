// This file is part of dirigent.

// Copyright (C) Frederik Gartenmeister.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

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
impl dirigent::traits::Program for TestProgram {
	async fn start(
		self: Box<Self>,
		mut ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus {
		tracing::info!("Hello, World from Test Programm {}!", self.name);

		tracing::info!("Programm {}, registering index...", self.name);

		registry
			.register(Arc::new(NotFromSelf::new(self.name.to_string())))
			.await;

		let msg = Message {
			from: self.name.to_string(),
			text: format!("Hello From Program {}", self.name),
		};

		ctx.send(msg.clone().into()).await.unwrap();
		ctx.send(msg.into()).await.unwrap();

		ctx.spawn_sub(Box::pin(async move {
			loop {
				futures_timer::Delay::new(Duration::from_millis(500)).await
			}
		}));

		loop {
			match ctx.recv().await {
				Ok(envelope) => {
					envelope.try_read_ref::<Message, _, _>(|msg| {
						tracing::info!("{} received: {}", self.name, msg.text)
					});
				}
				Err(_e) => return Err(InstanceError::Killed),
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

#[test]
fn it_works() {
	use tokio::runtime::Runtime;

	let subscriber = tracing_subscriber::FmtSubscriber::builder()
		.with_max_level(tracing::Level::TRACE)
		.finish();
	tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

	// Create the runtime
	let rt = Runtime::new().unwrap();
	let (dirigent, takt) =
		dirigent::Dirigent::<Box<dyn Program>, _, 1024, 1>::new(rt.handle().clone());

	let mut takt_clone = takt.clone();
	rt.spawn(async move {
		futures_timer::Delay::new(Duration::from_secs(1)).await;
		takt_clone
			.run(Box::new(TestProgram { name: "FOO" }), "FOO")
			.await
			.unwrap();
		futures_timer::Delay::new(Duration::from_secs(2)).await;
		takt_clone
			.run(Box::new(TestProgram { name: "BAR" }), "BAR")
			.await
			.unwrap();
	});

	let mut takt_clone = takt.clone();
	rt.spawn(async move {
		futures_timer::Delay::new(Duration::from_secs(5)).await;
		takt_clone.force_shutdown().await.unwrap();
	});

	rt.block_on(async move {
		dirigent.begin().await.unwrap();
	});

	tracing::info!("Finished main...");
}
