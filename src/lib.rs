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

use std::{
	fmt::Debug,
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
};

use tracing::{error, info, warn};

use crate::{
	channel::{mpmc, mpsc, oneshot::channel},
	envelope::Envelope,
	process::{Pid, Process, Spawnable},
	traits::{ExitStatus, Program, Spawner},
	updatable::{Updatable, Updater},
};

pub mod channel;
pub mod envelope;
pub mod index;
mod process;
pub mod scheduler;
pub mod shutdown;
pub mod spawner;
#[cfg(test)]
mod tests;
pub mod traits;
pub mod updatable;

pub const DEFAULT_SHUTDOWN_TIME_SECS: u64 = 10;

#[derive(Debug)]
struct RawWrapper<P>(*const P);

impl<P: Program> RawWrapper<P> {
	fn new(p: P) -> Self {
		RawWrapper(Box::into_raw(Box::new(p)))
	}

	fn recover(self) -> P {
		unsafe { *Box::from_raw(self.0 as *mut P) }
	}
}

unsafe impl<P> Send for RawWrapper<P> {}
unsafe impl<P> Sync for RawWrapper<P> {}

#[derive(Debug)]
enum Command<P> {
	Schedule {
		program: RawWrapper<P>,
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

struct Dirigent<
	P: Program,
	Spawner,
	const BUS_SIZE: usize = 1024,
	const MAX_MSG_BATCH_SIZE: usize = 128,
> {
	spawner: Spawner,
	takt_to_dirigent_recv: mpsc::Receiver<Command<P>>,
}

impl<P: Program, S: Spawner, const BUS_SIZE: usize, const MAX_MSG_BATCH_SIZE: usize>
	Dirigent<P, S, BUS_SIZE, MAX_MSG_BATCH_SIZE>
{
	pub fn new(spawner: S) -> (Self, Takt<P>) {
		let (takt_to_dirigent_send, takt_to_dirigent_recv) = mpsc::channel();
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
		let (program_to_bus_send, program_to_bus_recv) =
			mpmc::channel_sized::<Envelope, BUS_SIZE>();
		let shutdown = Arc::new(AtomicBool::new(false));
		let (processes, updater) = Updatable::new(Vec::<Process<process::SPS<S>>>::new());

		// NOTE: Spawning the control structure. The bus itself lives in this
		// state-machine
		spawner.spawn(Self::serve(
			spawner.handle(),
			takt_to_dirigent_recv,
			updater,
			program_to_bus_send.clone(),
			shutdown.clone(),
		));

		loop {
			// TODO: make shutdown a feature that fires once it is shutdown
			if !shutdown.load(Ordering::SeqCst) {
				match program_to_bus_recv.recv().await {
					Ok(envelope) => {
						let mut batch = Vec::with_capacity(MAX_MSG_BATCH_SIZE);
						batch.push(envelope);

						for _ in 1..MAX_MSG_BATCH_SIZE {
							if let Ok(maybe_envelope) = program_to_bus_recv.try_recv() {
								if let Some(envelope) = maybe_envelope {
									batch.push(envelope);
								} else {
									break;
								}
							} else {
								error!("Bus can no longer receive ")
							}
						}

						let batch = Arc::new(batch);
						let active = processes.current().clone();
						active
							.iter()
							.for_each(|process| process.consume(batch.clone()))
					}
					Err(_) => {
						unreachable!("This stack holds a reference to the sender. Qed.")
					}
				}
			} else {
				info!("Dirigent is shutting down. Shutting down bus.");
				break;
			}
		}

		Ok(())
	}

	async fn serve(
		spawner: S::Handle,
		takt_to_dirigent_recv: mpsc::Receiver<Command<P>>,
		mut updater: Updater<Vec<Process<process::SPS<S>>>>,
		program_to_bus_send: mpmc::Sender<Envelope>,
		_shutdown_sig: Arc<AtomicBool>,
	) -> ExitStatus {
		let pid_allocation = process::PidAllocation::new();
		let mut current_processes = Arc::new(Vec::<Process<process::SPS<S>>>::new());
		let mut next_processes = Vec::<Process<process::SPS<S>>>::new();
		let mut scheduled_processes =
			Vec::<Spawnable<<<S as Spawner>::Handle as Spawner>::Handle, P>>::new();

		loop {
			match takt_to_dirigent_recv.recv().await {
				Ok(cmd) => {
					let update_proccesses = match cmd {
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
							let program = program.recover();

							let pid = pid_allocation.pid();
							let spawnable = Spawnable::new(
								pid,
								name,
								spawner.handle(),
								program,
								program_to_bus_send.clone(),
							);

							info!("Scheduled program {} [with {:?}]", name, pid,);

							scheduled_processes.push(spawnable);
							if let Err(e) = return_pid.send(pid).await {
								warn!("Received error: {:?}", e);
							}

							false
						}
						Command::Start(pid) => {
							if let Some(index) = scheduled_processes
								.iter()
								.position(|spawnable| spawnable.pid == pid)
							{
								let process = scheduled_processes.swap_remove(index).spawn();
								info!(
									"Spawned program {} [with {:?}]",
									process.name(),
									process.pid()
								);
								next_processes.push(process);
							} else {
								warn!(
									"Could not start program with {:?}. Reason: Not scheduled.",
									pid
								)
							}

							true
						}
						_ => false,
					};

					if update_proccesses {
						let updated_processes = next_processes
							.iter()
							.chain(current_processes.iter())
							.map(|process| process.clone())
							.collect();
						updater.update(updated_processes);
						current_processes = updater.current();
						next_processes = Vec::new();
					}
				}
				Err(_) => {
					info!("All Takt instances dropped. Dirigent can no longer receive commands.");
					break;
				}
			}
		}

		Ok(())
	}
}

pub struct Takt<P: Program> {
	sender: mpsc::Sender<Command<P>>,
}

impl<P: Program> Clone for Takt<P> {
	fn clone(&self) -> Self {
		Takt {
			sender: self.sender.clone(),
		}
	}
}

unsafe impl<P: Program> Send for Takt<P> {}
unsafe impl<P: Program> Sync for Takt<P> {}

impl<P: Program> Takt<P> {
	pub fn downgrade(&self) -> MinorTakt<P> {
		MinorTakt {
			sender: self.sender.clone(),
		}
	}

	pub async fn schedule(&mut self, program: P, name: &'static str) -> Result<Pid, ()> {
		let (send, recv) = channel::mpsc::channel::<Pid>();
		let cmd = Command::Schedule {
			program: RawWrapper::new(program),
			return_pid: send,
			name,
		};
		// TODO: Handle
		self.sender.send(cmd).await.unwrap();

		// TODO: Handle
		let pid = recv.recv().await.unwrap();

		Ok(pid)
	}

	pub async fn run(&mut self, program: P, name: &'static str) -> Result<Pid, ()> {
		let pid = self.schedule(program, name).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	pub async fn start(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Start(pid)).await.unwrap();

		Ok(())
	}

	pub async fn stop(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Stop(pid)).await.unwrap();

		Ok(())
	}

	pub async fn preempt(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Preempt(pid)).await.unwrap();

		Ok(())
	}

	pub async fn kill(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Kill(pid)).await.unwrap();

		Ok(())
	}

	pub async fn unpreempt(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Unpreempt(pid)).await.unwrap();

		Ok(())
	}

	pub async fn shutdown(&mut self) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Shutdown).await.unwrap();

		Ok(())
	}

	pub async fn force_shutdown(&mut self) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::ForceShutdown).await.unwrap();

		Ok(())
	}
}

pub struct MinorTakt<P: Program> {
	sender: mpsc::Sender<Command<P>>,
}

impl<P: Program> MinorTakt<P> {
	pub async fn run(&mut self, program: P, name: &'static str) -> Result<Pid, ()> {
		let pid = self.schedule(program, name).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	async fn schedule(&mut self, program: P, name: &'static str) -> Result<Pid, ()> {
		let (send, recv) = channel::mpsc::channel::<Pid>();
		let cmd = Command::Schedule {
			program: RawWrapper::new(program),
			return_pid: send,
			name,
		};
		// TODO: Handle
		self.sender.send(cmd).await.unwrap();

		// TODO: Handle
		let pid = recv.recv().await.unwrap();

		Ok(pid)
	}

	async fn start(&mut self, pid: Pid) -> Result<(), ()> {
		// TODO: Handle
		self.sender.send(Command::Start(pid)).await.unwrap();

		Ok(())
	}
}
