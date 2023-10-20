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

#![allow(dead_code)]

use core::ops::AddAssign;
use std::{arch::aarch64::vcvtpd_u64_f64, fmt::Debug, time::Duration};

use futures::{future::BoxFuture, select, Future, FutureExt};
use tracing::{error, info, trace, warn};

use crate::{
	channel::{oneshot::channel, RecvError, SendError},
	envelope::Envelope,
	process::{BusSignal, PidAllocation, Process, ProcessPool, Spawnable},
	traits::{
		ExitStatus, Index, Program, ScheduleExt, Scheduler, SchedulerRef, Spawner, TimeoutExt,
	},
};

pub mod channel;
pub mod envelope;
mod process;
pub mod spawner;
#[cfg(test)]
mod tests;
pub mod traits;

pub const DEFAULT_SHUTDOWN_TIME_SECS: u64 = 10;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Pid(usize);
impl Pid {
	pub fn new(pid: usize) -> Self {
		Pid(pid)
	}

	pub fn id(&self) -> usize {
		self.0
	}
}

#[derive(Debug)]
enum Command<P> {
	Schedule {
		program: *const P,
		name: &'static str,
		return_pid: channel::mpsc::Sender<Pid>,
	},
	Start(Pid),
	Stop(Pid),
	Unpreempt(Pid),
	Preempt(Pid),
	FetchRunning(channel::mpsc::Sender<Vec<Pid>>),
	Kill(Pid),
	Shutdown,
	ForceShutdown,
}

// NOTE: The safety results from the fact, that the dirigent
//       only ever receives the raw point from the public
//       interface of the `struct Takt`, which takes care
//       of leaking an owned valued of `trait Program`.
unsafe impl<P: Send> Send for Command<P> {}

struct Dirigent<P: Program, Spawner> {
	spawner: Spawner,
	takt_to_dirigent_recv: channel::mpsc::Receiver<Command<P>>,
}

impl<P: Program, S: Spawner> Dirigent<P, S> {
	pub fn new(spawner: S) -> (Self, Takt<P>) {
		let (takt_to_dirigent_send, takt_to_dirigent_recv) = channel::mpsc::channel();
		(
			Dirigent {
				spawner,
				takt_to_dirigent_recv,
			},
			Takt {
				sender: takt_to_dirigent_send,
			},
		)
	}

	pub async fn begin(self) -> ExitStatus {
		let Dirigent {
			spawner,
			takt_to_dirigent_recv,
		} = self;
		let mut pid_allocation = process::PidAllocation::new();
		let mut process_pool = ProcessPool::new();
		let mut scheduled_processes = Vec::<Spawnable<S::Handle, P>>::new();

		loop {
			// Serve commands first
			select! {
				cmd_res = takt_to_dirigent_recv.recv().fuse() => {
					if let Ok(cmd) = cmd_res {
						match cmd {
							Command::Schedule {
								program,
								return_pid,
								name,
							} => {
								// TODO: Verify safety assumptions...

								// SAFETY:
								//  - it is impossible to create an instance of `enum Command` outside
								//    of this repository
								//  - the `struct Takt` takes an owner value of `P: Program`
								//      - the owned type is boxed and leaked then
								//  - `trait Program` is only implemented for `Box<dyn Program + 'a>`
								//    and not not for any other smart pointer
								//  - We NEVER clone a command
								//  - Channels ALWAYS assure a message is only delivered once over a
								//    channel
								//
								//  Hence, it is safe for us to regard the pointer as valid and
								// dereference it here
								let program = unsafe { *Box::from_raw(program as *mut P) };

								let pid = pid_allocation.pid();
								let spawnable = Spawnable::new(pid, name, spawner.handle(), program);

								trace!(
									"Scheduled program {} [with {:?}]",
									name,
									pid,
								);

								scheduled_processes.push(spawnable);
								drop_err(return_pid.send(pid).await);
							}
							Command::Start(pid) => {
								if let Some(index) =
									scheduled_processes.iter().position(|spawnable| spawnable.pid == pid)
								{
									let scheduled = scheduled_processes.swap_remove(index);
									process_pool.add(scheduled);
								} else {
									warn!(
										"Could not start program with {:?}. Reason: Not scheduled.",
										pid
									)
								}
							}
							_ => {},
							}
				} else {
					warn!("All Takt instances dropped. Dirigent can no longer receive commands.");
				}
			}
			_ = process_pool.recv_all().fuse() => {}
			}
		}

		Ok(())
	}
}

#[derive(Clone)]
pub struct Takt<P: Program> {
	sender: channel::mpsc::Sender<Command<P>>,
}

unsafe impl<P: Program> Send for Takt<P> {}
unsafe impl<P: Program> Sync for Takt<P> {}

impl<P: Program> Takt<P> {
	async fn schedule(&mut self, program: P, name: &'static str) -> Result<Pid, ()> {
		let (send, recv) = channel::mpsc::channel::<Pid>();
		let cmd = Command::Schedule {
			program: Box::into_raw(Box::new(program)),
			return_pid: send,
			name,
		};
		// TODO: Handle
		self.sender.send(cmd).await.unwrap();

		// TODO: Handle
		let pid = recv.recv().await.unwrap();

		Ok(pid)
	}

	async fn schedule_and_start(&mut self, program: P, name: &'static str) -> Result<Pid, ()> {
		let pid = self.schedule(program, name).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	async fn start(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Start(pid)).await.unwrap();

		Ok(())
	}

	async fn stop(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Stop(pid)).await.unwrap();

		Ok(())
	}

	async fn preempt(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Preempt(pid)).await.unwrap();

		Ok(())
	}

	async fn kill(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Kill(pid)).await.unwrap();

		Ok(())
	}

	async fn unpreempt(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Unpreempt(pid)).await.unwrap();

		Ok(())
	}

	async fn shutdown(&mut self) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Shutdown).await.unwrap();

		Ok(())
	}

	async fn force_shutdown(&mut self) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::ForceShutdown).await.unwrap();

		Ok(())
	}

	async fn end(self) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Shutdown).await.unwrap();

		Ok(())
	}
}

pub struct SubSpawner<Spawner> {
	parent_pid: Pid,
	parent_name: &'static str,
	scheduler: Scheduler,
	spawner: Spawner,
	pid_allocation: PidAllocation,
}

impl<Spawner: traits::Spawner> SubSpawner<Spawner> {
	fn new(
		parent_pid: Pid,
		parent_name: &'static str,
		scheduler: Scheduler,
		spawner: Spawner,
		pid_allocation: PidAllocation,
	) -> Self {
		SubSpawner {
			parent_pid,
			parent_name,
			scheduler,
			spawner,
			pid_allocation,
		}
	}

	fn spawn_blocking_named(
		&self,
		future: impl Future<Output = ExitStatus> + Send + 'static,
		name: &'static str,
		pid: Pid,
	) {
		let future = future.schedule(self.scheduler.reference());

		let name = name.clone();
		let parent_pid = self.parent_pid;
		let parent_name = self.parent_name;

		let future = async move {
			let res = future.await;

			match res {
				Err(e) => {
					warn!(
						"Subprocess {} [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
						name, pid, parent_name, parent_pid, e
					)
				}
				Ok(inner_res) => {
					if let Err(e) = inner_res {
						warn!(
							"Subprocess {} [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
							name, pid, parent_name, parent_pid, e
						)
					} else {
						trace!(
							"Subprocess {} [{:?}, Parent: [{},{:?}]] finished.",
							name,
							pid,
							parent_name,
							parent_pid,
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
		let future = future.schedule(self.scheduler.reference());

		let name = name.clone();
		let parent_pid = self.parent_pid;
		let parent_name = self.parent_name;

		let future = async move {
			let res = future.await;

			match res {
				Err(e) => {
					warn!(
						"Subprocess {} [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
						name, pid, parent_name, parent_pid, e
					)
				}
				Ok(inner_res) => {
					if let Err(e) = inner_res {
						warn!(
							"Subprocess {} [{:?}, Parent: [{},{:?}]] exited with error: {:?}",
							name, pid, parent_name, parent_pid, e
						)
					} else {
						trace!(
							"Subprocess {} [{:?}, Parent: [{},{:?}]] finished.",
							name,
							pid,
							parent_name,
							parent_pid,
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

fn drop_err<T, E: Debug>(res: Result<T, E>) {
	match res {
		Ok(_) => {}
		Err(e) => warn!("Received error: {:?}", e),
	}
}
