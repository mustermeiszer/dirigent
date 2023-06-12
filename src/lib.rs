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

// TODO: Remove std dependency
use std::{
	ops::AddAssign,
	pin::Pin,
	sync::{Arc, Mutex},
	task::{Context, Poll},
};

use futures::{future::BoxFuture, Future};

use crate::{
	envelope::Envelope,
	traits::{ExitStatus, Index, Program, Spawner},
};

pub mod channel;
pub mod envelope;
pub mod spawner;
#[cfg(test)]
mod tests;
pub mod traits;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Pid(usize);

enum Command<P> {
	Schedule {
		program: *const P,
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

unsafe impl<P: Send> Send for Command<P> {}

impl<P> Clone for Command<P> {
	fn clone(&self) -> Self {
		match self {
			Command::Schedule {
				program,
				return_pid,
			} => Command::Schedule {
				program: program.clone(),
				return_pid: return_pid.clone(),
			},
			Command::Start(pid) => Command::Start(*pid),
			Command::Preempt(pid) => Command::Start(*pid),
			Command::FetchRunning(sender) => Command::FetchRunning(sender.clone()),
			Command::Kill(pid) => Command::Kill(*pid),
			Command::KillAll => Command::KillAll,
			Command::Shutdown => Command::Shutdown,
			Command::ForceShutdown => Command::ForceShutdown,
		}
	}
}

struct Instrum<P> {
	pid: Pid,
	program: P,
}

impl<P: Program> Instrum<P> {
	fn play<S: Spawner>(self, spawner: &S) -> ActiveInstrum {
		let Instrum { pid, program } = self;

		let (sender_of_dirigent, recv_of_program) = channel::mpsc::channel();
		let (sender_of_program, recv_of_dirigent) = channel::mpsc::channel();
		let index = program.index();

		let context = ContextImpl {
			spawner: SubSpawner {
				spawner: spawner.handle(),
			},
			recv: recv_of_program,
			sender: sender_of_program,
		};

		let process = Box::new(program).start(Box::new(context));

		let mut active = ActiveInstrum {
			pid,
			state: State::Init(sync_wrapper::SyncWrapper::new(process)),
			to_instance: sender_of_dirigent,
			index,
			from_instance: recv_of_dirigent,
		};

		active.start(spawner);
		active
	}
}

struct ActiveInstrum {
	pid: Pid,
	state: State,
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
			self.to_instance
				.try_send(envelope)
				.await
				.expect("Sending to instance failed. Panic...")
		}
	}

	fn initialized(&self) -> bool {
		match &self.state {
			State::Uninit | State::Running(_) | State::Preempted(_) | State::Exited => false,
			State::Init(_) => true,
		}
	}

	fn alive(&self) -> bool {
		match &self.state {
			State::Uninit | State::Init(_) | State::Exited => false,
			State::Running(_) | State::Preempted(_) => true,
		}
	}

	fn start(&mut self, spawner: &impl Spawner) {
		if self.initialized() {
			let mut state = State::Uninit;
			core::mem::swap(&mut self.state, &mut state);

			if let State::Init(state) = state {
				let instance_state = Arc::new(Mutex::new(InstanceState::Running));

				let spawned_instance = SpawnedInstance {
					inner: state.into_inner(),
					instance_state: instance_state.clone(),
				};
				self.state = State::Running(RefSpawnedInstance(instance_state));
				spawner.spawn(spawned_instance)
			} else {
				unreachable!("Control flow prohibits this state.")
			}
		} else {
			// TODO: Error here
		}
	}

	async fn send(&self, msg: impl Into<Envelope> + Send) -> Result<(), ()> {
		let env: Envelope = msg.into();

		// TODO: Might wanna remove the program if not alive?
		if self.alive() {
			self.to_instance.send(env).await
		} else {
			Ok(())
		}
	}

	// TODO: Research race-conditions
	async fn preempt(&mut self) -> Result<(), ()> {
		let spawned = match &self.state {
			State::Uninit | State::Init(_) | State::Preempted(_) | State::Exited => return Err(()),
			State::Running(spawned) => {
				let mut l = spawned.0.lock().map_err(|_| ())?;
				*l = InstanceState::Preempted;

				spawned
			}
		};

		self.state = State::Preempted(spawned.clone());

		Ok(())
	}

	// TODO: Research race-conditions
	async fn kill(&mut self) -> Result<(), ()> {
		match &self.state {
			State::Uninit | State::Init(_) | State::Exited => return Err(()),
			State::Running(spawned) | State::Preempted(spawned) => {
				let mut l = spawned.0.lock().map_err(|_| ())?;
				*l = InstanceState::Killed;
			}
		}
		self.state = State::Exited;

		Ok(())
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

	pub fn schedule(&mut self, program: P) -> Result<(), ()> {
		self.scheduled.push(Instrum {
			pid: self.pid_allocation.pid(),
			program,
		});
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
			let active_instrum = instrum.play(&spawner);
			running.push(active_instrum);
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

							scheduled.push(Instrum {
								program,
								pid: pid_allocation.pid(),
							});

							return_pid.send(pid_allocation.last()).await.unwrap();
						}
						Command::FetchRunning(_) => {}
						Command::Start(pid) => {
							if let Some(index) =
								scheduled.iter().position(|instrum| instrum.pid == pid)
							{
								running.push(scheduled.swap_remove(index).play(&spawner));
							} else {
								// TODO: Trace warning here
							}
						}
						Command::Preempt(_) => {}
						Command::Kill(_) => {}
						Command::KillAll => {}
						Command::ForceShutdown => {}
						Command::Shutdown => break,
					}
				}
			} else {
				// Something is off now. We stop working
				//panic!("Dirigent can no longer receive commands.
				// Panic...")
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
	async fn schedule(&mut self, program: P) -> Result<Pid, ()> {
		let (send, recv) = channel::mpsc::channel::<Pid>();
		let cmd = Command::Schedule {
			program: Box::into_raw(Box::new(program)),
			return_pid: send,
		};
		self.sender.send(cmd).await?;

		recv.recv().await
	}

	async fn schedule_and_start(&mut self, program: P) -> Result<Pid, ()> {
		let pid = self.schedule(program).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	async fn start(&mut self, pid: Pid) -> Result<(), ()> {
		self.sender.send(Command::Start(pid)).await
	}

	async fn preempt(&mut self, pid: Pid) -> Result<(), ()> {
		self.sender.send(Command::Preempt(pid)).await
	}

	async fn kill(&mut self, pid: Pid) -> Result<(), ()> {
		self.sender.send(Command::Kill(pid)).await
	}

	/*
	async fn command(&mut self, cmd: Command<P>) -> Result<(), ()> {
		self.sender.send(Command::C(pid)).await
	}
	 */

	async fn end(self) -> Result<(), ()> {
		self.sender.send(Command::Shutdown).await
	}
}

pub struct SubSpawner<Spawner> {
	spawner: Spawner,
}

pub struct ContextImpl<Spawner> {
	spawner: SubSpawner<Spawner>,
	recv: channel::mpsc::Receiver<Envelope>,
	sender: channel::mpsc::Sender<Envelope>,
}

#[async_trait::async_trait]
impl<S> traits::Context for ContextImpl<S>
where
	S: Spawner,
{
	async fn try_recv(&mut self) -> Result<Option<Envelope>, ()> {
		self.recv.try_recv().await
	}

	async fn recv(&mut self) -> Result<Envelope, ()> {
		self.recv.recv().await
	}

	async fn send(&mut self, envelope: Envelope) -> Result<(), ()> {
		self.sender.try_send(envelope).await
	}

	fn sender(&self) -> channel::mpsc::Sender<Envelope> {
		self.sender.clone()
	}

	fn spawn_sub(&mut self, _sub: BoxFuture<'static, ExitStatus>) {
		todo!()
	}

	fn spawn_sub_blocking(&mut self, _sub: BoxFuture<'static, ExitStatus>) {
		todo!()
	}
}

pub enum State {
	Uninit,
	Init(sync_wrapper::SyncWrapper<BoxFuture<'static, ExitStatus>>),
	Running(RefSpawnedInstance),
	Preempted(RefSpawnedInstance),
	Exited,
}

#[derive(Clone)]
pub struct RefSpawnedInstance(Arc<Mutex<InstanceState>>);

#[pin_project::pin_project]
struct SpawnedInstance {
	#[pin]
	inner: BoxFuture<'static, ExitStatus>,
	instance_state: Arc<Mutex<InstanceState>>,
}

#[derive(Clone, Copy)]
pub enum InstanceState {
	Preempted,
	Running,
	Killed,
}

impl Future for SpawnedInstance {
	type Output = ExitStatus;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();
		if let Some(state) = this.instance_state.lock().ok() {
			match *state {
				InstanceState::Running => this.inner.poll(cx),
				InstanceState::Preempted => Poll::Pending,
				InstanceState::Killed => Poll::Ready(Err(())),
			}
		} else {
			Poll::Ready(Err(()))
		}
	}
}
