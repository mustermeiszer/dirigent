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

use futures::{future::BoxFuture, Future};
use tracing::{trace, warn};

use crate::{
	channel::{oneshot::channel, RecvError, SendError},
	envelope::Envelope,
	traits::{
		ExitStatus, Index, Priority, PriorityExt, Program, ScheduleExt, Scheduler, SchedulerRef,
		Spawner,
	},
};

pub mod channel;
pub mod envelope;
pub mod spawner;
#[cfg(test)]
mod tests;
pub mod traits;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Pid(usize);

impl Pid {
	pub fn new(pid: usize) -> Self {
		Pid(pid)
	}
}

#[derive(Debug)]
enum Command<P> {
	Schedule {
		program: *const P,
		name: &'static str,
		priority: Priority,
		return_pid: channel::mpsc::Sender<Pid>,
	},
	Start(Pid),
	Preempt(Pid),
	FetchRunning(channel::mpsc::Sender<Vec<Pid>>),
	Kill(Pid),
	KillAll,
	Shutdown,
	ForceShutdown,
}

// NOTE: The safety results from the fact, that the dirigent
//       only ever receives the raw point from the public
//       interface of the `struct Takt`, which takes care
//       of leaking an owned valued of `trait Program`.
unsafe impl<P: Send> Send for Command<P> {}

struct Instrum<P> {
	pid: Pid,
	name: &'static str,
	priority: Priority,
	program: P,
}

impl<P: Program> Instrum<P> {
	fn play<S: Spawner>(self, spawner: &S) -> ActiveInstrum {
		let Instrum {
			pid,
			name,
			program,
			priority,
		} = self;

		let (sender_of_dirigent, recv_of_program) = channel::mpsc::channel();
		let (sender_of_program, recv_of_dirigent) = channel::mpsc::channel();
		let index = program.index();
		let scheduler = Scheduler::new();

		let context = ContextImpl {
			sub_pid_allocation: PidAllocation::new(),
			spawner: SubSpawner {
				parent_name: name.clone(),
				parent_pid: pid,
				priority,
				scheduler_ref: scheduler.reference(),
				spawner: spawner.handle(),
			},
			recv: recv_of_program,
			sender: sender_of_program,
		};

		let process = Box::new(program).start(Box::new(context));
		// Add a prioritize wrapper around the future, so we can prioritize it.
		// Add a scheduler wrapper around the future, so we can control it.
		let process = process.prioritize(priority).schedule(scheduler.reference());

		let process = async move {
			let res = process.await;

			match res {
				Err(e) => {
					warn!("Process {} [{:?}], exited with error: {:?}", name, pid, e)
				}
				Ok(inner_res) => {
					if let Err(e) = inner_res {
						warn!("Process {} [{:?}]exited with error: {:?}", name, pid, e)
					} else {
						trace!("Process {} [{:?}] finished.", name, pid,)
					}
				}
			}

			Ok(())
		};

		let active = ActiveInstrum {
			pid,
			name,
			scheduler,
			to_instance: sender_of_dirigent,
			index,
			from_instance: recv_of_dirigent,
		};

		trace!(
			"Starting program '{} [with {:?}, {:?}]'.",
			name,
			pid,
			priority
		);

		spawner.spawn(process);

		active
	}
}

struct ActiveInstrum {
	pid: Pid,
	name: &'static str,
	scheduler: Scheduler,
	index: Box<dyn Index>,
	to_instance: channel::mpsc::Sender<Envelope>,
	from_instance: channel::mpsc::Receiver<Envelope>,
}

impl ActiveInstrum {
	async fn produced(&self) -> Option<Envelope> {
		// TODO: Handle error or make the method non result
		self.from_instance.try_recv().await.ok().flatten()
	}

	async fn try_consume(&self, envelope: Envelope) {
		if self.index.indexed(&envelope) {
			// TODO: Fix error or make not result
			// TODO: Remove the program if not alive

			if self.scheduler.alive() {
				let message_id = envelope.message_id();

				if let Err(e) = self.to_instance.try_send(envelope.clone()).await {
					match e {
						SendError::Closed(_) => {
							warn!(
								"Program: {} ({:?}) failed to be send to. Reason: Channel is closed. Message ID: {:?}",
								self.name, self.pid, message_id
							)
						}
						SendError::Full(_) => {
							warn!(
								"Program: {} ({:?}) failed to be send to. Reason: Channel is full. Message ID: {:?}",
								self.name, self.pid, message_id
							)
						}
					}
				}
			}
		}
	}

	async fn send(
		&self,
		msg: impl Into<Envelope> + Send,
	) -> Result<(), channel::SendError<Envelope>> {
		let env: Envelope = msg.into();

		if self.scheduler.alive() {
			self.to_instance.send(env).await
		} else {
			// TODO: Remove the program if not alive
			Ok(())
		}
	}

	// TODO: Research race-conditions
	fn schedule(&mut self) {
		trace!(
			"Scheduling program {} [{:?}] for running.",
			self.name,
			self.pid,
		);
		self.scheduler.schedule();
	}

	// TODO: Research race-conditions
	fn preempt(&mut self) {
		trace!("Preempting program {} [{:?}].", self.name, self.pid,);
		self.scheduler.preempt();
	}

	// TODO: Research race-conditions
	fn kill(&mut self) {
		trace!("Killing program {} [{:?}].", self.name, self.pid,);
		self.scheduler.kill();
	}
}

struct Dirigent<P: Program, Spawner> {
	spawner: Spawner,
	scheduled: Vec<Instrum<P>>,
	running: Vec<ActiveInstrum>,

	takt_sender: channel::mpsc::Sender<Command<P>>,
	receiver: channel::mpsc::Receiver<Command<P>>,
	pid_allocation: PidAllocation,
}

struct PidAllocation(usize);
impl PidAllocation {
	pub fn new() -> PidAllocation {
		PidAllocation(0)
	}

	pub fn last(&self) -> Pid {
		Pid(self.0)
	}

	pub fn pid(&mut self) -> Pid {
		self.0.add_assign(1);
		Pid(self.0)
	}
}

impl<P: Program, S: Spawner> Dirigent<P, S> {
	pub fn new(spawner: S) -> Self {
		let (takt_sender, receiver) = channel::mpsc::channel();
		Dirigent {
			spawner,
			scheduled: Vec::new(),
			running: Vec::new(),
			takt_sender,
			receiver,
			pid_allocation: PidAllocation::new(),
		}
	}

	pub fn schedule(
		&mut self,
		program: P,
		name: &'static str,
		priority: Priority,
	) -> Result<(), ()> {
		let instrum = Instrum {
			pid: self.pid_allocation.pid(),
			name,
			priority,
			program,
		};

		trace!(
			"Scheduled program {} [with {:?}, {:?}]",
			instrum.name,
			instrum.pid,
			instrum.priority
		);

		self.scheduled.push(instrum);

		Ok(())
	}

	pub fn takt(&self) -> Takt<P> {
		Takt {
			sender: self.takt_sender.clone(),
		}
	}

	pub async fn begin(self) -> ExitStatus {
		let Dirigent {
			spawner,
			receiver,
			mut running,
			takt_sender: _takt_sender,
			scheduled,
			mut pid_allocation,
		} = self;

		for instrum in scheduled {
			let active = instrum.play(&spawner);
			running.push(active);
		}

		let mut scheduled = Vec::new();

		loop {
			// Serve commands first
			if let Ok(cmd) = receiver.try_recv().await {
				if let Some(cmd) = cmd {
					match cmd {
						Command::Schedule {
							program,
							return_pid,
							name,
							priority,
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
								priority,
							};

							trace!(
								"Scheduled program {} [with {:?}, {:?}]",
								instrum.name,
								instrum.pid,
								instrum.priority
							);

							scheduled.push(instrum);
							return_pid.send(pid_allocation.last()).await.unwrap();
						}
						Command::FetchRunning(_) => {}
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
						Command::Preempt(_) => {}
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
						Command::KillAll => {}
						Command::ForceShutdown => {}
						Command::Shutdown => break,
					}
				}
			} else {
				// Something is off now. We stop working
				panic!("Dirigent can no longer receive commands.");
			}

			// Serve processes
			//
			// Collect messages first
			let mut envelopes = Vec::with_capacity(running.len());
			for active in &mut running {
				if let Some(envelope) = active.produced().await {
					envelopes.push(envelope)
				}
			}

			// Distribute messages
			for envelope in envelopes {
				for active in &mut running {
					let cloned_envelope = envelope.clone();
					active.try_consume(cloned_envelope).await
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
	async fn schedule(
		&mut self,
		program: P,
		name: &'static str,
		priority: Priority,
	) -> Result<Pid, ()> {
		let (send, recv) = channel::mpsc::channel::<Pid>();
		let cmd = Command::Schedule {
			program: Box::into_raw(Box::new(program)),
			return_pid: send,
			name,
			priority,
		};
		// TODO: Handle
		self.sender.send(cmd).await.unwrap();

		// TODO: Handle
		let pid = recv.recv().await.unwrap();

		Ok(pid)
	}

	async fn schedule_and_start(
		&mut self,
		program: P,
		name: &'static str,
		priority: Priority,
	) -> Result<Pid, ()> {
		let pid = self.schedule(program, name, priority).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	async fn start(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Start(pid)).await.unwrap();

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

	/*
	async fn command(&mut self, cmd: Command<P>) -> Result<(), ()> {
		self.sender.send(Command::C(pid)).await
	}
	 */

	async fn end(self) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Shutdown).await.unwrap();

		Ok(())
	}
}

struct SubSpawner<Spawner> {
	parent_pid: Pid,
	parent_name: &'static str,
	priority: Priority,
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
		let future = future
			.prioritize(self.priority)
			.schedule(self.scheduler_ref.clone());

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
		let future = future
			.prioritize(self.priority)
			.schedule(self.scheduler_ref.clone());

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

pub struct ContextImpl<Spawner> {
	spawner: SubSpawner<Spawner>,
	sub_pid_allocation: PidAllocation,
	recv: channel::mpsc::Receiver<Envelope>,
	sender: channel::mpsc::Sender<Envelope>,
}

#[async_trait::async_trait]
impl<S> traits::Context for ContextImpl<S>
where
	S: Spawner,
{
	async fn try_recv(&mut self) -> Result<Option<Envelope>, RecvError> {
		self.recv.try_recv().await
	}

	async fn recv(&mut self) -> Result<Envelope, RecvError> {
		self.recv.recv().await
	}

	async fn send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		self.sender.send(envelope).await
	}

	async fn try_send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		self.sender.try_send(envelope).await
	}

	fn sender(&self) -> channel::mpsc::Sender<Envelope> {
		self.sender.clone()
	}

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>, name: Option<&'static str>) {
		// TODO: Possible to concat with parent name?
		let name = name.unwrap_or("_sub");
		self.spawner.spawn(sub, name, self.sub_pid_allocation.pid())
	}

	fn spawn_sub_blocking(
		&mut self,
		sub: BoxFuture<'static, ExitStatus>,
		name: Option<&'static str>,
	) {
		// TODO: Possible to concat with parent name?
		let name = name.unwrap_or("_sub_blocking");
		self.spawner
			.spawn_blocking(sub, name, self.sub_pid_allocation.pid())
	}
}
