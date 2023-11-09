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
use tracing::{debug, warn};

use crate::{
	process::{Pid, SubPidAllocation},
	scheduler::{ScheduleExt, Scheduler},
	traits,
	traits::{ExitStatus, Spawner},
};

pub struct SubSpawner<Spawner> {
	parent_pid: Pid,
	parent_name: &'static str,
	scheduler: Scheduler,
	spawner: Spawner,
	pid_allocation: SubPidAllocation,
}

impl<Spawner: traits::Spawner> SubSpawner<Spawner> {
	pub fn new(
		parent_pid: Pid,
		parent_name: &'static str,
		scheduler: Scheduler,
		spawner: Spawner,
	) -> Self {
		SubSpawner {
			parent_pid,
			parent_name,
			scheduler,
			spawner,
			pid_allocation: SubPidAllocation::new(parent_pid),
		}
	}

	fn spawn_blocking_named(
		&self,
		future: impl Future<Output = ExitStatus> + Send + 'static,
		name: &'static str,
		pid: Pid,
	) {
		let future = future.schedule(self.scheduler.reference(pid));

		let name = name.clone();
		let parent_pid = self.parent_pid;
		let parent_name = self.parent_name;

		let future = async move {
			let res = future.await;

			match res {
				Err(e) => {
					warn!(
						"Subprocess \"{}\" [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
						name, pid, parent_name, parent_pid, e
					)
				}
				Ok(inner_res) => {
					if let Err(e) = inner_res {
						warn!(
							"Subprocess \"{}\" [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
							name, pid, parent_name, parent_pid, e
						)
					} else {
						debug!(
							"Subprocess \"{}\" [{:?}, Parent: [{},{:?}]] finished.",
							name, pid, parent_name, parent_pid,
						)
					}
				}
			}

			Ok(())
		};

		self.spawner.spawn_blocking(future)
	}

	fn spawn_named(
		&self,
		future: impl Future<Output = ExitStatus> + Send + 'static,
		name: &'static str,
		pid: Pid,
	) {
		let future = future.schedule(self.scheduler.reference(pid));

		let name = name.clone();
		let parent_pid = self.parent_pid;
		let parent_name = self.parent_name;

		let future = async move {
			let res = future.await;

			match res {
				Err(e) => {
					warn!(
						"Subprocess \"{}\" [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
						name, pid, parent_name, parent_pid, e
					)
				}
				Ok(inner_res) => {
					if let Err(e) = inner_res {
						warn!(
							"Subprocess \"{}\" [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
							name, pid, parent_name, parent_pid, e
						)
					} else {
						debug!(
							"Subprocess \"{}\" [{:?}, Parent: [{},{:?}]] finished.",
							name, pid, parent_name, parent_pid,
						)
					}
				}
			}

			Ok(())
		};

		self.spawner.spawn(future)
	}
}

impl<S: Spawner> Spawner for SubSpawner<S> {
	type Handle = SubSpawner<S::Handle>;

	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		self.spawn_blocking_named(future, "_sub_blocking", self.pid_allocation.pid())
	}

	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		self.spawn_named(future, "_sub", self.pid_allocation.pid())
	}

	fn spawn_blocking_named(
		&self,
		name: &'static str,
		future: impl Future<Output = ExitStatus> + Send + 'static,
	) {
		self.spawn_blocking_named(future, name, self.pid_allocation.pid())
	}

	fn spawn_named(
		&self,
		name: &'static str,
		future: impl Future<Output = ExitStatus> + Send + 'static,
	) {
		self.spawn_named(future, name, self.pid_allocation.pid())
	}

	fn handle(&self) -> Self::Handle {
		SubSpawner {
			parent_name: self.parent_name,
			parent_pid: self.parent_pid,
			spawner: self.spawner.handle(),
			scheduler: self.scheduler.clone(),
			pid_allocation: self.pid_allocation.clone(),
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(not(target_arch = "wasm32"))]
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

/// A wrapper type that allows to implement external traits for
/// all generic types that implement `trait Spawner`.
pub struct Wrapper<T: traits::SubSpawner>(T);

impl<T: traits::SubSpawner> Wrapper<T> {
	pub fn new(t: T) -> Self {
		Wrapper(t)
	}
}

#[cfg(feature = "libp2p")]
impl<T: traits::SubSpawner> libp2p_swarm::Executor for Wrapper<T> {
	fn exec(
		&self,
		future: core::pin::Pin<
			Box<(dyn futures::Future<Output = ()> + std::marker::Send + 'static)>,
		>,
	) {
		self.0.spawn_sub(Box::pin(async move {
			future.await;

			Ok(())
		}))
	}
}
