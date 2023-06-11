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

use futures::future::Future;

use crate::traits::{ExitStatus, Spawner};

impl Spawner for tokio::runtime::Handle {
	type Handle = Self;

	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		self.spawn_blocking(|| future);
	}

	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		self.spawn(future);
	}

	fn handle(&self) -> Self::Handle {
		self.clone()
	}
}

impl Spawner for tokio::runtime::Runtime {
	type Handle = tokio::runtime::Handle;

	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		self.spawn_blocking(|| future);
	}

	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		self.spawn(future);
	}

	fn handle(&self) -> Self::Handle {
		self.handle().clone()
	}
}
