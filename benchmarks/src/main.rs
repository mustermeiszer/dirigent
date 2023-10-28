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

use std::time::Duration;

use dirigent::traits::Program;
use tokio::runtime::Handle;

use crate::instrums::{consumer_all::ConsumerAll, consumer_slow::ConsumerSlow, spammer::Spammer};

mod instrums;

const SEND_MESSAGES: usize = 100_000;
const AMOUNT_CONSUMER: usize = 100;
const DELAY: u64 = 1;

#[tokio::main]
async fn main() {
	let _ = tracing_subscriber::fmt::try_init();

	let (dirigent, takt) = dirigent::Dirigent::<Box<dyn Program>, _>::new(Handle::current());

	let handle = Handle::current();
	handle.spawn(async move {
		dirigent.begin().await.unwrap();
	});

	let (send, recv) = dirigent::channel::mpsc::channel::<(&'static str, Duration)>();

	for _ in 0..AMOUNT_CONSUMER {
		takt.run(
			Box::new(ConsumerAll::<SEND_MESSAGES>::new(send.clone())),
			"ConsumeAll",
		)
		.await
		.unwrap();
	}

	for _ in 0..AMOUNT_CONSUMER {
		takt.run(
			Box::new(ConsumerSlow::<SEND_MESSAGES>::new(
				Duration::from_millis(DELAY),
				send.clone(),
			)),
			"ConsumeSlow",
		)
		.await
		.unwrap();
	}

	takt.run(
		Box::new(Spammer::new(SEND_MESSAGES, send.clone())),
		"Spammer",
	)
	.await
	.unwrap();

	drop(send);

	let mut times = Vec::new();
	loop {
		match recv.recv().await {
			Ok(time) => times.push(time),
			Err(_) => {
				takt.force_shutdown().await.unwrap();
				break;
			}
		}
	}

	println!("Benches for {} send messages: {:?}", SEND_MESSAGES, times);
}
