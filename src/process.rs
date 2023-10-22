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

use std::sync::{
	atomic::{AtomicUsize, Ordering},
	Arc,
};

use futures::future::BoxFuture;
use tracing::{error, trace, warn};

use crate::{
	channel,
	channel::{mpmc, mpsc, oneshot, RecvError},
	envelope::Envelope,
	index,
	scheduler::{ScheduleExt, Scheduler},
	spawner::SubSpawner,
	traits,
	traits::{ExitStatus, Index, InstanceError, Program, Spawner},
};

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

#[derive(Clone)]
pub struct PidAllocation(Arc<AtomicUsize>);
impl PidAllocation {
	pub fn new() -> PidAllocation {
		PidAllocation(Arc::new(AtomicUsize::new(1)))
	}

	pub fn pid(&self) -> Pid {
		Pid(self.0.fetch_add(1, Ordering::SeqCst))
	}
}

/// Commands
pub enum ProcessSignal {
	Stop,
	Kill,
	Preempt,
	UnPreempt,
}

pub enum BusSignal {
	Process(Pid, ProcessSignal),
	All(ProcessSignal),
	Message(Envelope),
	Messages(Arc<Vec<Envelope>>),
}

const MAX_RECEIVED: usize = 100;

pub type SPS<S> = SubSpawner<
	<<<<S as Spawner>::Handle as Spawner>::Handle as Spawner>::Handle as Spawner>::Handle,
>;
pub struct Spawnable<S, P> {
	/// The process ID of this process.
	/// Unique for every process run by an dirigent instance
	pub pid: Pid,

	/// The name of the program this process is controlling
	pub name: &'static str,

	spawner: S,

	program: P,

	program_to_bus_send: mpmc::Sender<Envelope>,
}

impl<P: Program, S: Spawner> Spawnable<S, P> {
	pub fn new(
		pid: Pid,
		name: &'static str,
		spawner: S,
		program: P,
		program_to_bus_send: mpmc::Sender<Envelope>,
	) -> Self
	where
		P: Program,
	{
		Spawnable {
			pid,
			name,
			spawner,
			program,
			program_to_bus_send,
		}
	}

	pub fn spawn(self) -> Process<SubSpawner<<<S as Spawner>::Handle as Spawner>::Handle>> {
		let scheduler = Scheduler::new(self.pid);
		let sub_spawner = SubSpawner::new(
			self.pid,
			self.name,
			scheduler.clone(),
			self.spawner.handle(),
			PidAllocation::new(),
		);

		let (context, process_to_program_send) = Context::new(
			self.pid,
			self.name,
			sub_spawner.handle(),
			self.program_to_bus_send,
		);
		let scheduler_ref = scheduler.reference();
		let process = Process::new(
			self.pid,
			self.name,
			sub_spawner.handle(),
			scheduler,
			process_to_program_send,
		);
		let process_ref = process.clone();

		let (index_registry_send, index_registry_recv) = oneshot::channel();
		sub_spawner.spawn(async move {
			if let Ok(index) = index_registry_recv.recv().await {
				process_ref.set_index(index);
				Ok(())
			} else {
				error!(
					"Pid: {:?}. Index registry oneshot failed receiving",
					self.pid
				);
				Err(InstanceError::Unexpected)
			}
		});

		let program = Box::new(self.program)
			.start(
				Box::new(context),
				Box::new(index::IndexRegistry::new(index_registry_send)),
			)
			.schedule(scheduler_ref);

		self.spawner.spawn(async move {
			let res = program.await;

			match res {
				Err(e) => {
					warn!(
						"Process {} [{:?}], exited with error: {:?}",
						self.name, self.pid, e
					)
				}
				Ok(inner_res) => {
					if let Err(e) = inner_res {
						warn!(
							"Process {} [{:?}]exited with error: {:?}",
							self.name, self.pid, e
						)
					} else {
						trace!("Process {} [{:?}] finished.", self.name, self.pid)
					}
				}
			}

			Ok(())
		});

		process
	}
}

pub struct Process<S>(Arc<InnerProcess<S>>);

impl<S> Clone for Process<S> {
	fn clone(&self) -> Self {
		Process(self.0.clone())
	}
}

impl<S: Spawner> Process<S> {
	fn inner_mut(&self) -> &mut InnerProcess<S> {
		let raw = self.0.as_ref() as *const InnerProcess<S> as *mut InnerProcess<S>;

		unsafe { &mut *raw }
	}

	fn set_index(&self, index: Arc<dyn Index>) {
		self.inner_mut().index = Some(index);
	}

	pub fn consume<C>(&self, envelope: Envelope, call_back: C)
	where
		C: FnOnce(&ExitStatus) + Send + 'static,
	{
		if let Some(ref index) = self.0.index {
			let process_ref = self.clone();
			let index = index.clone();

			self.0.spawner.spawn(async move {
				let res = if index.indexed(&envelope) {
					process_ref
						.0
						.process_to_program_send
						.send(envelope.clone())
						.await
						.map_err(|err| {
							error!(
								"Failed sending to program {} [{:?}]. Enveloped missed: {:?}",
								process_ref.0.name, process_ref.0.pid, envelope
							);

							InstanceError::Internal(Box::new(err))
						})
				} else {
					Ok(())
				};

				call_back(&res);

				res
			});
		}
	}

	pub fn consume_all<C>(&self, envelopes: Arc<Vec<Envelope>>, call_back: C)
	where
		C: Fn(&ExitStatus) + Send + 'static,
	{
		if let Some(ref index) = self.0.index {
			let process_ref = self.clone();
			let index = index.clone();

			self.0.spawner.spawn(async move {
				for envelope in envelopes.as_ref() {
					let res = if index.indexed(&envelope) {
						process_ref
							.0
							.process_to_program_send
							.send(envelope.clone())
							.await
							.map_err(|err| {
								error!(
									"Failed sending to program {} [{:?}]. Enveloped missed: {:?}",
									process_ref.0.name, process_ref.0.pid, envelope
								);

								InstanceError::Internal(Box::new(err))
							})
					} else {
						Ok(())
					};

					call_back(&res);
				}

				Ok(())
			});
		}
	}
}

/// A process is spawned by dirigent and provides all means to control a given
/// program. It provides means to
/// * communicate with the program
/// * receive messages from the bus
struct InnerProcess<S> {
	/// The process ID of this process.
	/// Unique for every process run by an dirigent instance
	pid: Pid,

	/// The name of the program this process is controlling
	name: &'static str,

	/// Signals from the process for the program
	process_to_program_send: mpsc::Sender<Envelope>,

	/// The index of the program.
	/// * filters messages received from bus
	/// * indexed messages are forwarded to the program
	index: Option<Arc<dyn Index>>,

	/// The scheduler for all state-machines spawned from this process
	scheduler: Scheduler,

	/// Spawner
	spawner: S,
}

unsafe impl<S: Send> Send for Process<S> {}
unsafe impl<S: Sync> Sync for Process<S> {}

impl<S: Spawner> Process<S> {
	pub fn new(
		pid: Pid,
		name: &'static str,
		spawner: S,
		scheduler: Scheduler,
		process_to_program_send: mpsc::Sender<Envelope>,
	) -> Self {
		Process(Arc::new(InnerProcess {
			pid,
			name,
			spawner,
			scheduler,
			process_to_program_send,
			index: None,
		}))
	}
}

pub struct Context<Spawner> {
	pid: Pid,
	name: &'static str,
	spawner: Spawner,
	from_process: mpsc::Receiver<Envelope>,
	to_bus: mpmc::Sender<Envelope>,
}

impl<Handle: Spawner> Context<Handle> {
	pub fn new(
		pid: Pid,
		name: &'static str,
		spawner: Handle,
		program_to_bus_send: mpmc::Sender<Envelope>,
	) -> (Self, mpsc::Sender<Envelope>) {
		let (process_to_program_send, process_to_program_recv) = mpsc::channel();

		let ctx = Context {
			pid,
			name,
			spawner,
			from_process: process_to_program_recv,
			to_bus: program_to_bus_send,
		};

		(ctx, process_to_program_send)
	}
}

#[async_trait::async_trait]
impl<S> traits::Context for Context<S>
where
	S: Spawner,
{
	fn try_recv(&self) -> Result<Option<Envelope>, RecvError> {
		self.from_process.try_recv()
	}

	async fn recv(&mut self) -> Result<Envelope, RecvError> {
		self.from_process.recv().await
	}

	async fn send(&mut self, envelope: Envelope) -> Result<(), channel::SendError<Envelope>> {
		self.to_bus.send(envelope).await
	}

	fn try_send(&self, envelope: Envelope) -> Result<(), channel::SendError<Envelope>> {
		self.to_bus.try_send(envelope)
	}

	fn sender(&self) -> mpmc::Sender<Envelope> {
		self.to_bus.clone()
	}

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		self.spawner.spawn(sub)
	}

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		self.spawner.spawn_blocking(sub)
	}
}
