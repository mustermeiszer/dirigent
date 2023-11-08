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

use std::sync::Arc;

use tracing::error;

use crate::{channel, envelope::Envelope, traits, traits::Index};

pub struct FullIndex;
impl FullIndex {
	pub fn new() -> Self {
		FullIndex {}
	}
}

#[async_trait::async_trait]
impl Index for FullIndex {
	async fn indexed(&self, _t: &Envelope) -> bool {
		true
	}
}

pub struct IndexRegistry(channel::oneshot::Sender<Arc<dyn Index>>);
impl IndexRegistry {
	pub fn new(sender: channel::oneshot::Sender<Arc<dyn Index>>) -> Self {
		IndexRegistry(sender)
	}
}

#[async_trait::async_trait]
impl traits::IndexRegistry for IndexRegistry {
	async fn register(self: Box<Self>, index: Arc<dyn Index>) {
		if let Err(_) = self.0.send(index).await {
			error!("IndexRegistry was unable to set Index. Receiver has been dropped.");
		}
	}
}
