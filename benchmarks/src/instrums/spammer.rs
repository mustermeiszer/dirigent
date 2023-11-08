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

use std::{
	fmt::{Debug, Formatter},
	time::{Duration, Instant},
};

use dirigent::{
	channel::mpsc,
	traits::{Context, ExitStatus, IndexRegistry, Program},
};

use crate::instrums::Message;

pub struct Spammer {
	amount: usize,
	ret: mpsc::Sender<(&'static str, Duration)>,
}

impl Debug for Spammer {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Spammer")
			.field("amount", &self.amount)
			.finish()
	}
}

impl Spammer {
	pub fn new(amount: usize, ret: mpsc::Sender<(&'static str, Duration)>) -> Self {
		Spammer { amount, ret }
	}
}

#[async_trait::async_trait]
impl Program for Spammer {
	async fn start(
		self: Box<Spammer>,
		ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus {
		drop(registry);

		let start = Instant::now();
		for _ in 0..self.amount {
			ctx.send(Message::new().into()).await.unwrap();
		}
		let elapsed = start.elapsed();

		self.ret.send(("Spammer", elapsed)).await.unwrap();

		Ok(())
	}
}
