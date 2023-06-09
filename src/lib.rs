#![feature(box_into_inner)]
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
use core::{any::TypeId, marker::PhantomData};
use std::{
	ops::AddAssign,
	pin::Pin,
	sync::{Arc, Mutex},
	task::{Context, Poll},
};

use futures::{future::BoxFuture, Future};

use crate::{
	envelope::Envelope,
	traits::{ExitStatus, Index as _, Index, Process as _, Process, Program, Spawner},
};

pub mod channel;
pub mod envelope;
pub mod spawner;
#[cfg(test)]
mod tests;
pub mod traits;

#[derive(Copy, Clone)]
pub struct Pid(usize);

enum Command<P> {
	Schedule {
		program: *const P,
		return_pid: channel::Sender<Pid>,
	},
	Start(Pid),
	Preempt(Pid),
	FetchRunning(channel::Sender<Vec<Pid>>),
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

struct ActiveInstrum<I> {
	pid: Pid,
	instance: I,
	index: Box<dyn Index>,
	inbound: channel::Receiver<Envelope>,
}

impl<I: Process> ActiveInstrum<I> {
	async fn produced(&self) -> Option<Envelope> {
		// TODO: Handle error or make the method non result
		self.inbound.recv().await.ok()
	}

	async fn try_consume(&self, envelope: Envelope) {
		if self.index.indexed(&envelope) {
			// TODO: Fix error or make not result
			self.instance
				.send(envelope)
				.await
				.expect("Sending to instance failed. Panic...")
		}
	}
}

struct Dirigent<P: Program, Spawner> {
	spawner: Spawner,
	scheduled: Vec<Instrum<P>>,
	running: Vec<ActiveInstrum<Instance>>,

	takt_sender: channel::Sender<Command<P>>,
	receiver: channel::Receiver<Command<P>>,
	pid_allocation: PidAllocation,
}

struct PidAllocation(usize);
impl PidAllocation {
	pub fn new() -> PidAllocation {
		PidAllocation(0)
	}

	pub fn pid(&mut self) -> Pid {
		self.0.add_assign(1);
		Pid(self.0)
	}
}

impl<P: Program, S: Spawner> Dirigent<P, S> {
	pub fn new(spawner: S) -> Self {
		let (takt_sender, receiver) = channel::channel();
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

	pub async fn begin(mut self) -> ExitStatus {
		let Dirigent {
			spawner,
			receiver,
			mut running,
			takt_sender,
			mut scheduled,
			mut pid_allocation,
		} = self;

		for mut instrum in scheduled {
			let (sender_of_dirigent, recv_of_program) = channel::channel();
			let (sender_of_program, recv_of_dirigent) = channel::channel();

			let mut active_instrum = ActiveInstrum {
				pid: instrum.pid,
				instance: Instance {
					state: State::Uninit,
					inbound: sender_of_dirigent,
				},
				index: instrum.program.index(),
				inbound: recv_of_dirigent,
			};

			let context = ContextImpl {
				spawner: SubSpawner {
					spawner: spawner.clone(),
				},
				recv: recv_of_program,
				sender: sender_of_program,
			};

			let process = Box::new(instrum.program).start(Box::new(context));

			active_instrum.instance.init(process);
			active_instrum.instance.start(spawner.clone());

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
							let program =
								unsafe { Box::into_inner(Box::from_raw(program as *mut P)) };

							scheduled.push(Instrum {
								program,
								pid: pid_allocation.pid(),
							})
						}
						Command::FetchRunning(_) => {}
						Command::Start(pid) => {}
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
				let fut = active.produced();
				if let Some(envelope) = fut.await {
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
	sender: channel::Sender<Command<P>>,
}

impl<P: Program> Takt<P> {
	async fn schedule(&mut self, program: P) -> Result<Pid, ()> {
		let (send, mut recv) = channel::channel::<Pid>();
		self.sender
			.send(Command::Schedule {
				program: Box::into_raw(Box::new(program)),
				return_pid: send,
			})
			.await?;

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
	recv: channel::Receiver<Envelope>,
	sender: channel::Sender<Envelope>,
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
		let res = self.sender.try_send(envelope).await;
		res
	}

	fn sender(&self) -> channel::Sender<Envelope> {
		self.sender.clone()
	}

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		todo!()
	}

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>) {
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

pub struct Instance {
	state: State,
	inbound: channel::Sender<Envelope>,
}

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

#[derive(Clone)]
pub struct RefSpawnedInstance(Arc<Mutex<InstanceState>>);

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

impl Instance {
	fn running(&self) -> bool {
		match &self.state {
			State::Uninit | State::Init(_) | State::Preempted(_) | State::Exited => false,
			State::Running(_) => true,
		}
	}

	fn alive(&self) -> bool {
		match &self.state {
			State::Uninit | State::Init(_) | State::Exited => false,
			State::Running(_) | State::Preempted(_) => true,
		}
	}
}

#[async_trait::async_trait]
impl Process for Instance {
	fn init(&mut self, state: impl Future<Output = ExitStatus> + Send + 'static) -> Result<(), ()> {
		match self.state {
			State::Uninit => {
				let boxed: Pin<Box<dyn Future<Output = ExitStatus> + Send + 'static>> =
					Box::pin(state);
				self.state = State::Init(sync_wrapper::SyncWrapper::new(boxed));
				Ok(())
			}
			State::Init(_) | State::Running(_) | State::Preempted(_) | State::Exited => Err(()),
		}
	}

	fn initialized(&self) -> bool {
		match &self.state {
			State::Uninit | State::Running(_) | State::Preempted(_) | State::Exited => false,
			State::Init(_) => true,
		}
	}

	fn start(&mut self, spawner: impl Spawner) {
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
			self.inbound.send(env).await
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
