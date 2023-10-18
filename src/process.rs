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

use std::{ops::AddAssign, sync::Arc};

use futures::{future::BoxFuture, select, FutureExt};
use tracing::{error, trace, warn};

use crate::{
	channel,
	channel::{mpsc, spmc, spsc, RecvError},
	envelope::Envelope,
	traits,
	traits::{
		ExitStatus, Index, IndexRegistry, InstanceError, Program, ScheduleExt, Scheduler, Spawner,
	},
	Pid, SubSpawner,
};

pub struct PidAllocation(usize);
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

/// Commands
pub enum ProcessSignal {
	Stop,
	Kill,
	Preempt,
	UnPreempt,
}

pub enum ProgramSignal {
	SetIndex(Box<dyn Index>),
}

pub enum BusSignal {
	Process(Pid, ProcessSignal),
	All(ProcessSignal),
	Message(Envelope),
	Messages(Arc<Vec<Envelope>>),
}

pub struct ProcessRef<'a> {
	pid: Pid,
	bus_to_process_send: &'a spmc::Sender<BusSignal>,
}

/// A process is spawned by dirigent and provides all means to control a given
/// program. It provides means to
/// * communicate with the program
/// * receive messages from the bus
pub struct Process<S> {
	/// The process ID of this process.
	/// Unique for every process run by an dirigent instance
	pid: Pid,

	/// The name of the program this process is controlling
	name: &'static str,

	/// Signals from the bus for the process
	bus_to_process_recv: spmc::Receiver<BusSignal>,

	/// Signals from the process for the program
	process_to_program_send: spsc::Sender<Envelope>,

	/// Signal from the program for the process
	/// Only used once for receiving the index.
	program_to_process_recv: spsc::Receiver<ProgramSignal>,

	/// The index of the program.
	/// * filters messages received from bus
	/// * indexed messages are forwarded to the program
	index: Option<Box<dyn Index>>,

	/// The scheduler for all state-machines spawned from this process
	scheduler: Scheduler,

	/// Spawner
	spawner: S,

	// NOTE: This field is only set intermediary, when a process is generated but not yet
	//       spawned
	_program_state_machine: Option<BoxFuture<'static, ExitStatus>>,
}

impl<S: Spawner> Process<S> {
	pub fn new<'a, P>(
		pid: Pid,
		name: &'static str,
		spawner: S,
		program: P,
		bus_to_process_send: &'a spmc::Sender<BusSignal>,
		bus_to_process_recv: spmc::Receiver<BusSignal>,
		program_to_bus_send: mpsc::Sender<Envelope>,
	) -> Result<(Self, ProcessRef<'a>), ()>
	where
		P: Program,
	{
		let (context, mut scheduler, process_to_program_send) =
			Context::new(pid, name, spawner.handle(), program_to_bus_send);
		let (program_to_process_send, program_to_process_recv) = spsc::channel();
		let process = Box::new(program)
			.start(
				Box::new(context),
				IndexRegistry::new(program_to_process_send),
			)
			.schedule(scheduler.reference());

		let _program_state_machine: Option<BoxFuture<ExitStatus>> = Some(Box::pin(async move {
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
		}));

		Ok((
			Process {
				pid,
				name,
				bus_to_process_recv,
				process_to_program_send,
				program_to_process_recv,
				index: None,
				scheduler,
				spawner,
				_program_state_machine,
			},
			ProcessRef {
				pid,
				bus_to_process_send,
			},
		))
	}

	pub async fn spawn(mut self) -> ExitStatus {
		// Spawning the program first
		self.spawner.spawn(
			self._program_state_machine
				.expect("Program is set during new(). qed."),
		);

		loop {
			select! {
				index_res = self.program_to_process_recv.recv().fuse() => {
					if let Ok(sig) = index_res {
						match sig {
							ProgramSignal::SetIndex(index) => self.index = Some(index),
						}
					} else {
						error!("Program not able to communicate with process. Killing program.");
						return Err(InstanceError::Unexpected);

					}
				},
				bus_res = self.bus_to_process_recv.recv().fuse() => {
					if let Ok(sig) = bus_res {
						match sig {
							BusSignal::Message(env) => {
								if let Some(index) = &self.index {
									// Process messages
								}
							}
							BusSignal::Messages(envs) => {
								if let Some(index) = &self.index {
									// Process messages
								}
							}
							BusSignal::Process(pid, sig) if pid == self.pid => {
								// Process signal
								match sig {
									ProcessSignal::Stop => {}
									ProcessSignal::Kill => {}
									ProcessSignal::Preempt => {}
									ProcessSignal::UnPreempt => {}
								}
							}
							BusSignal::Process(_, _) => {},
							BusSignal::All(sig) => {
							// Process signal
								match sig {
									ProcessSignal::Stop => {}
									ProcessSignal::Kill => {}
									ProcessSignal::Preempt => {}
									ProcessSignal::UnPreempt => {}
								}
							}
						}
					} else {
						error!("Program not able to communicate with bus. Killing program.");
						return Err(InstanceError::Unexpected);
					}
				}
			};
		}
	}
}

pub struct Context<Spawner> {
	spawner: SubSpawner<Spawner>,
	sub_pid_allocation: PidAllocation,
	from_process: spsc::Receiver<Envelope>,
	to_bus: mpsc::Sender<Envelope>,
}

impl<Handle: Spawner> Context<Handle> {
	pub fn new(
		pid: Pid,
		name: &'static str,
		spawner: Handle,
		program_to_bus_send: mpsc::Sender<Envelope>,
	) -> (Self, Scheduler, spsc::Sender<Envelope>) {
		let mut scheduler = Scheduler::new(pid);
		let (process_to_program_send, process_to_program_recv) = spsc::channel();

		let ctx = Context {
			sub_pid_allocation: PidAllocation::new(),
			spawner: SubSpawner {
				parent_name: name.clone(),
				parent_pid: pid,
				scheduler_ref: scheduler.reference(),
				spawner: spawner,
			},
			from_process: process_to_program_recv,
			to_bus: program_to_bus_send,
		};

		(ctx, scheduler, process_to_program_send)
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

	fn sender(&self) -> mpsc::Sender<Envelope> {
		self.to_bus.clone()
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
