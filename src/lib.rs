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
#![feature(async_fn_in_trait)]
#![feature(box_into_inner)]

use core::{any::TypeId, marker::PhantomData};

use futures::Future;

use crate::{
	envelope::Envelope,
	traits::{Index as _, Process as _, Process, Program, Spawner},
};

pub mod channel;
pub mod envelope;
pub mod traits;

#[derive(Copy, Clone)]
pub struct Pid(u64);

type Map<K, V> = std::collections::HashMap<K, V>;

enum Command<P> {
	Schedule {
		program: P,
		return_pid: channel::Sender<Pid>,
	},
	Start(Pid),
	Preempt(Pid),
	FetchRunning(channel::Sender<Vec<Pid>>),
	Kill(Pid),
	KillAll,
}

struct Instrum<P> {
	pid: Pid,
	programm: P,
}

struct ActiveInstrum<P: Program, I: traits::Process<P>> {
	pid: Pid,
	instance: I,
	inbound: channel::Receiver<Envelope>,
	_phantom: PhantomData<P>,
}

impl<P: Program, I: traits::Process<P>> ActiveInstrum<P, I> {
	async fn produced(&mut self) -> Option<Envelope> {
		// TODO: Handle error or make the method non result
		self.inbound
			.try_recv()
			.await
			.expect("Failed receiving from Process. Panic...")
	}

	async fn try_consume(&mut self, envelope: Envelope) {
		if <P as Program>::Consumes::indexed(&envelope) {
			// TOD0: Fix error or make not result
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
	running: Vec<ActiveInstrum<P, Instance<P>>>,

	takt_sender: channel::Sender<Command<P>>,
	receiver: channel::Receiver<Command<P>>,
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
		}
	}

	pub fn begin(mut self) -> Takt<P> {
		let spawner = self.spawner.clone();
		let sender = self.takt_sender.clone();

		let fut = async move {
			loop {
				// Serve commands first
				if let Ok(cmd) = self.receiver.try_recv().await {
					if let Some(cmd) = cmd {
						match cmd {
							Command::Schedule { .. } => {}
							Command::FetchRunning(_) => {}
							Command::Start(_) => {}
							Command::Preempt(_) => {}
							Command::Kill(_) => {}
							Command::KillAll => {}
						}
					}
				} else {
					// Something is off now. We stop working
					panic!("Dirigent can no longer receive commands. Panic...")
				}

				// Serve processes
				//
				// Collect messages first
				let mut envelopes = Vec::with_capacity(self.running.len());
				for active in &mut self.running {
					if let Some(envelope) = active.produced().await {
						envelopes.push(envelope)
					}
				}

				// Distribute messages
				for envelope in envelopes {
					for active in &mut self.running {
						active.try_consume(envelope.clone()).await
					}
				}
			}
		};

		spawner.spawn(Box::new(fut));

		Takt {
			sender,
			_phantom: Default::default(),
		}
	}
}

struct Takt<P> {
	sender: channel::Sender<Command<P>>,
	_phantom: PhantomData<P>,
}

impl<P: Program> Takt<P> {
	async fn schedule(&mut self, program: P) -> Result<Pid, ()> {
		let (send, mut recv) = channel::channel::<Pid>();
		self.sender
			.send(Command::Schedule {
				program,
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
		todo!()
	}

	async fn kill(&mut self, pid: Pid) -> Result<(), ()> {
		todo!()
	}

	async fn command(&mut self, cmd: Command<P>) -> Result<(), ()> {
		todo!()
	}

	async fn end(self) -> Result<(), ()> {
		todo!()
	}

	async fn infinity(self) {
		loop {}
	}
}

pub struct SubSpawner<Spawner> {
	spawner: Spawner,
}

pub struct ContextImpl<Spawner> {
	spawner: SubSpawner<Spawner>,
	recv: channel::Receiver<Envelope>,
	inbound: channel::Sender<Envelope>,
	sender: channel::Sender<Envelope>,
}

impl<P, S> traits::Context<P> for ContextImpl<S>
where
	P: traits::Program,
	S: Spawner,
{
	type Process = Instance<P>;

	fn create_process(&mut self) -> Instance<P> {
		Instance {
			inbound: self.inbound.clone(),
			state: State::Uninit,
			_phantom: Default::default(),
		}
	}

	async fn try_recv(&mut self) -> Result<Option<Envelope>, ()> {
		self.recv.try_recv().await
	}

	async fn recv(&mut self) -> Result<Envelope, ()> {
		self.recv.recv().await
	}

	async fn send(&mut self, envelope: impl Into<Envelope>) -> Result<(), ()> {
		let envelope = envelope.into();
		self.sender.send(envelope).await
	}

	fn sender(&self) -> channel::Sender<Envelope> {
		self.sender.clone()
	}

	async fn spawn_sub(&mut self, sub: impl Future<Output = traits::ExitStatus>) {
		todo!()
	}

	async fn spawn_sub_blocking(&mut self, sub: impl Future<Output = traits::ExitStatus>) {
		todo!()
	}
}

pub enum State {
	Uninit,
	Init(Box<dyn Future<Output = traits::ExitStatus>>),
}

pub struct Instance<P> {
	state: State,
	inbound: channel::Sender<Envelope>,
	_phantom: PhantomData<P>,
}

impl<P: traits::Program> traits::Process<P> for Instance<P> {
	fn init(&mut self, state: Box<dyn Future<Output = traits::ExitStatus>>) {
		self.state = State::Init(state);
	}

	fn initialized(&self) -> bool {
		match &self.state {
			State::Uninit => false,
			State::Init(_) => true,
		}
	}

	async fn send(&mut self, msg: impl Into<Envelope>) -> Result<(), ()> {
		let env: Envelope = msg.into();

		if self.initialized() && P::Consumes::indexed(&env) {
			self.inbound.send(env).await
		} else {
			Ok(())
		}
	}

	async fn preempt(&mut self) -> Result<(), ()> {
		todo!()
	}

	async fn kill(&mut self) {
		todo!()
	}
}
