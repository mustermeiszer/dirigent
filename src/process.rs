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

use std::{
	fmt::{Debug, Formatter},
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	time::Duration,
};

use futures::future::BoxFuture;
use tracing::{debug, error, trace};

use crate::{
	channel,
	channel::{mpsc, oneshot, RecvError},
	envelope::Envelope,
	index,
	scheduler::Scheduler,
	spawner::SubSpawner,
	traits,
	traits::{ExecuteOnDrop, ExitStatus, Index, InstanceError, Program, Spawner},
	updatable::Updatable,
};

const DEFAULT_PROCESS_SHUTDOWN_TIME: u64 = 5;

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
		Pid(self.0.fetch_add(1, Ordering::Relaxed))
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SubPid {
	parent: Pid,
	sub: Pid,
}

#[derive(Clone)]
pub struct SubPidAllocation {
	parent_pid: Pid,
	sub_pid: Arc<AtomicUsize>,
}

impl SubPidAllocation {
	pub fn new(parent_pid: Pid) -> SubPidAllocation {
		SubPidAllocation {
			parent_pid,
			sub_pid: Arc::new(AtomicUsize::new(1)),
		}
	}

	pub fn pid(&self) -> SubPid {
		SubPid {
			parent: self.parent_pid,
			sub: Pid::new(self.sub_pid.fetch_add(1, Ordering::Relaxed)),
		}
	}
}

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

	program_to_bus_send: mpsc::Sender<Envelope>,
}

impl<P: Program, S: Spawner> Spawnable<S, P> {
	pub fn new(
		pid: Pid,
		name: &'static str,
		spawner: S,
		program: P,
		program_to_bus_send: mpsc::Sender<Envelope>,
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
		);

		let (context, process_to_program_send) = Context::new(
			self.pid,
			self.name,
			sub_spawner.handle(),
			self.program_to_bus_send,
		);
		let process = Process::new(
			self.pid,
			self.name,
			sub_spawner.handle(),
			scheduler,
			process_to_program_send,
		);
		let process_ref = process.clone();

		let (index_registry_send, index_registry_recv) = oneshot::channel();
		sub_spawner.spawn_named("IndexRegistration", async move {
			if let Ok(index) = index_registry_recv.recv().await {
				process_ref.set_index(index);
				debug!("[{}, ({:?})] set index.", self.name, self.pid)
			} else {
				debug!(
					"[{}, ({:?})] has no index. IndexRegistry dropped",
					self.name, self.pid
				)
			}

			Ok(())
		});

		sub_spawner.spawn_named(self.name, async move {
			Box::new(self.program)
				.start(
					Box::new(context),
					Box::new(index::IndexRegistry::new(index_registry_send)),
				)
				.await
		});

		process
	}
}

pub struct Process<S>(Arc<InnerProcess<S>>);

unsafe impl<S: Send> Send for Process<S> {}
unsafe impl<S: Sync> Sync for Process<S> {}

impl<S> Clone for Process<S> {
	fn clone(&self) -> Self {
		Process(self.0.clone())
	}
}

impl<S> Debug for Process<S> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Process")
			.field("pid", &self.0.pid)
			.field("name", &self.0.name)
			.finish()
	}
}

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
			alive: AtomicBool::new(true),
			process_to_program_send,
			index: None,
		}))
	}

	// NOTE ON SAFETY: This is safe as the method is a private interface and is ONLY
	//                 called once during `IndexRegistration`
	fn set_index(&self, index: Arc<dyn Index>) {
		let raw = self.0.as_ref() as *const InnerProcess<S> as *mut InnerProcess<S>;

		let inner_mut = unsafe { &mut *raw };
		inner_mut.index = Some(index);
	}

	pub fn name(&self) -> &'static str {
		self.0.name
	}

	pub fn pid(&self) -> Pid {
		self.0.pid
	}

	pub fn alive(&self) -> bool {
		self.0.alive.load(Ordering::Relaxed)
	}

	pub fn kill(&self) {
		debug!("Killing {} [{:?}]", self.0.name, self.0.pid);
		self.0.alive.store(false, Ordering::Relaxed);
		self.0.scheduler.kill()
	}

	pub fn stop(&self) {
		debug!("Stopping {} [{:?}]", self.0.name, self.0.pid);
		let clone = self.clone();
		clone.0.alive.store(false, Ordering::Relaxed);

		self.0.spawner.spawn_named("ProcessStopping", async move {
			debug!(
				"Stopping: Process is killed in {}.",
				DEFAULT_PROCESS_SHUTDOWN_TIME
			);
			futures_timer::Delay::new(Duration::from_secs(DEFAULT_PROCESS_SHUTDOWN_TIME)).await;

			clone.0.scheduler.kill();
			Ok(())
		})
	}

	pub fn preempt(&self) {
		debug!("Preempting {} [{:?}]", self.0.name, self.0.pid);
		self.0.alive.store(false, Ordering::Relaxed);
		self.0.scheduler.preempt()
	}

	pub fn run(&self) {
		debug!("Running {} [{:?}]", self.0.name, self.0.pid);
		self.0.alive.store(true, Ordering::Relaxed);
		self.0.scheduler.run()
	}

	pub fn consume(&self, envelopes: Arc<Vec<Envelope>>) {
		debug!(
			"Process \"{} [{:?}]\" consuming {:?}",
			self.name(),
			self.pid(),
			envelopes
		);

		if let Some(ref index) = self.0.index {
			let process_ref = self.clone();
			let index = index.clone();

			self.0
				.spawner
				.spawn_named("EnvelopeConsumption", async move {
					for envelope in envelopes.iter() {
						if index.indexed(&envelope) {
							process_ref
								.0
								.process_to_program_send
								.send(envelope.clone())
								.await
								.map_err(|err| {
									error!(
										"Failed sending to program {} [{:?}]. Program can no longer receive messages. Killing process...",
										process_ref.0.name, process_ref.0.pid
									);

									// NOTE: The async `send()` method is never failing on
									//       `SendError::Full`. Hence, we can assume the channel is
									// closed       and the process needs to be killed
									process_ref.kill();

									InstanceError::Internal(Box::new(err))
								})?
						}
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

	alive: AtomicBool,

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

impl<S> Drop for InnerProcess<S> {
	fn drop(&mut self) {
		trace!(
			"Dropping process \"{} [{:?}]\". Killing all subprocesses...",
			self.name,
			self.pid
		);

		self.scheduler.kill()
	}
}

impl<S: Spawner> ExecuteOnDrop for Updatable<Vec<Process<S>>> {
	fn execute(&mut self) {
		let current = self.current();
		trace!("Bus shutting down. Killing: {:?}", current);
		current.iter().for_each(|p| p.kill());
	}
}

#[allow(dead_code)]
pub struct Context<S> {
	pid: Pid,
	name: &'static str,
	spawner: S,
	from_process: mpsc::Receiver<Envelope>,
	to_bus: mpsc::Sender<Envelope>,
}

impl<S: Spawner> Context<S> {
	pub fn new(
		pid: Pid,
		name: &'static str,
		spawner: S,
		program_to_bus_send: mpsc::Sender<Envelope>,
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

	async fn recv(&self) -> Result<Envelope, RecvError> {
		self.from_process.recv().await
	}

	async fn send(&self, envelope: Envelope) -> Result<(), channel::SendError<Envelope>> {
		self.to_bus.send(envelope).await
	}

	fn try_send(&self, envelope: Envelope) -> Result<(), channel::SendError<Envelope>> {
		self.to_bus.try_send(envelope)
	}

	fn sender(&self) -> mpsc::Sender<Envelope> {
		self.to_bus.clone()
	}

	fn spawn_sub(&self, sub: BoxFuture<'static, ExitStatus>) {
		self.spawner.spawn(sub)
	}

	fn spawn_sub_blocking(&self, sub: BoxFuture<'static, ExitStatus>) {
		self.spawner.spawn_blocking(sub)
	}

	fn sub_spawner(&self) -> Box<dyn traits::SubSpawner> {
		Box::new(self.spawner.handle())
	}
}
