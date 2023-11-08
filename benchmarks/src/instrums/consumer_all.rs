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
	sync::Arc,
	time::{Duration, Instant},
};

use dirigent::{
	channel::mpsc,
	index::FullIndex,
	traits::{Context, ExitStatus, IndexRegistry, Program},
};

use crate::instrums::Message;

#[derive(Debug)]
pub struct ConsumerAll<const MSGS: usize> {
	ret: mpsc::Sender<(&'static str, Duration)>,
}

impl<const MSGS: usize> ConsumerAll<MSGS> {
	pub fn new(ret: mpsc::Sender<(&'static str, Duration)>) -> Self {
		ConsumerAll { ret }
	}
}

#[async_trait::async_trait]
impl<const MSGS: usize> Program for ConsumerAll<MSGS> {
	async fn start(
		self: Box<ConsumerAll<MSGS>>,
		ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus {
		registry.register(Arc::new(FullIndex::new())).await;

		let start = Instant::now();
		let mut count = 0;
		loop {
			ctx.recv().await.unwrap().read_ref::<Message>().unwrap();
			count += 1;

			if count == MSGS {
				let elapsed = start.elapsed();
				self.ret.send(("All", elapsed)).await.unwrap();
				break;
			}
		}

		Ok(())
	}
}
