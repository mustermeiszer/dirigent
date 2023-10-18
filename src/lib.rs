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
use std::{arch::aarch64::vcvtpd_u64_f64, time::Duration};

use futures::{future::BoxFuture, select, Future, FutureExt};
use tracing::{error, info, trace, warn};

use crate::{
	channel::{oneshot::channel, RecvError, SendError},
	envelope::Envelope,
	process::BusSignal,
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

enum Activity<'a, P> {
	Ready(P),
	Playing(process::ProcessRef<'a>),
}
struct Instrum<'a, P> {
	level: Activity<'a, P>,
	pid: Pid,
	name: &'static str,
}

impl<'a, P> Instrum<'a, P> {
	fn new() -> Self {
		todo!()
	}

	fn play(&mut self) -> Result<(), ()> {
		todo!()
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
		let pid_allocaton = process::PidAllocation::new();
		let (programs_to_bus_send, programs_to_bus_recv) = channel::mpsc::channel::<Envelope>();
		let (bus_to_processes_send, bus_to_processes_recv) =
			channel::spmc::channel::<process::BusSignal>();

		let mut active_processes = Vec::<Instrum<P>>::new();
		let mut scheduled_processes = Vec::<Instrum<P>>::new();

		loop {
			// Serve commands first
			select! {
				cmd_res = takt_to_dirigent_recv.recv().fuse() => {
					if let Ok(cmd) = cmd_res {
						/*
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

								let instrum = Instrum {
									program,
									pid: pid_allocation.pid(),
									name,
								};

								trace!(
									"Scheduled program {} [with {:?}]",
									instrum.name,
									instrum.pid,
								);

								scheduled.push(instrum);
								return_pid.send(pid_allocation.last()).await.unwrap();
							}
							Command::FetchRunning(return_pids) => {
								let pids = running.iter().map(|active| active.pid).collect::<Vec<_>>();
								if let Err(_) = return_pids.try_send(pids).await {
									warn!("Receiver dropped. Could not send pids to requester.",)
								}
							}
							Command::Start(pid) => {
								if let Some(index) =
									scheduled.iter().position(|instrum| instrum.pid == pid)
								{
									let active = scheduled.swap_remove(index).play(&spawner);
									running.push(active);
								} else {
									warn!(
										"Could not start program with {:?}. Reason: Not scheduled.",
										pid
									)
								}
							}
							Command::Stop(pid) => {
								if let Some(index) =
									running.iter().position(|instrum| instrum.pid == pid)
								{
									running.swap_remove(index).shutdown();
								} else {
									warn!(
										"Could not kill program with {:?}. Reason: Not running.",
										pid
									)
								}
							}
							Command::Preempt(pid) => {
								if let Some(index) =
									running.iter().position(|instrum| instrum.pid == pid)
								{
									running
										.get_mut(index)
										.expect("Index is correct. qed.")
										.preempt();
								} else {
									warn!(
										"Could not preempt program with {:?}. Reason: Not running.",
										pid
									)
								}
							}
							Command::Unpreempt(pid) => {
								if let Some(index) =
									running.iter().position(|instrum| instrum.pid == pid)
								{
									running
										.get_mut(index)
										.expect("Index is correct. qed.")
										.schedule();
								} else {
									warn!(
										"Could not preempt program with {:?}. Reason: Not running.",
										pid
									)
								}
							}
							Command::Kill(pid) => {
								if let Some(index) =
									running.iter().position(|instrum| instrum.pid == pid)
								{
									running.swap_remove(index).kill();
								} else {
									warn!(
										"Could not kill program with {:?}. Reason: Not running.",
										pid
									)
								}
							}
							Command::ForceShutdown => {
								info!("Force shutting down dirigent.");

								running.iter_mut().for_each(|mut active| active.kill());
								break;
							}
							Command::Shutdown => {
								info!("Starting shutdown of dirigent...");

								running.iter_mut().for_each(|mut active| active.shutdown());

								// Allowing the rest of the system to shutdown
								futures_timer::Delay::new(Duration::from_secs(
									DEFAULT_SHUTDOWN_TIME_SECS,
								))
								.await;

								info!("Stopping dirigent!");
								break;
						}
					}
					*/
				} else {
					warn!("All Takt instances dropped. Dirigent can no longer receive commands.");
				}
			}
			bus_res	= programs_to_bus_recv.recv().fuse() => {
				if let Ok(envelope) = bus_res {
					if let Err(err) = bus_to_processes_send
						.send(process::BusSignal::Message(envelope.clone()))
						.await
					{
						match err {
							SendError::Closed(_) => {
								warn!("Sending to processes failed. Reason: Closed. Dropping message: {:?}", envelope);
								error!("Channel to processes is closed. Fatal error. Shutting down.");
								break;
							}
							SendError::Full(_) => {
								warn!(
									"Sending to processes failed. Reason: Full. Dropping message: {:?}",
									envelope
								);
							}
						}
					}
				} else {
					// NOTE: Unreachable state as we always keep one sender here in this stack
					unreachable!("One sender to bus is alwaus alive. qed.");
				}
			}
			}
		}

		Ok(())
	}
}

#[derive(Clone)]
struct Takt<P: Program> {
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

struct SubSpawner<Spawner> {
	parent_pid: Pid,
	parent_name: &'static str,
	scheduler_ref: SchedulerRef,
	spawner: Spawner,
}

impl<Spawner: traits::Spawner> SubSpawner<Spawner> {
	fn spawn_blocking(
		&self,
		future: impl Future<Output = ExitStatus> + Send + 'static,
		name: &'static str,
		pid: Pid,
	) {
		let future = future.schedule(self.scheduler_ref.clone());

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

	fn spawn(
		&self,
		future: impl Future<Output = ExitStatus> + Send + 'static,
		name: &'static str,
		pid: Pid,
	) {
		let future = future.schedule(self.scheduler_ref.clone());

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
