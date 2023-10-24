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

use std::{fmt::Debug, sync::Arc, time::Duration};

use futures::{select_biased, FutureExt};
use tracing::{error, info, trace, warn};

use crate::{
	channel::{mpsc, oneshot, oneshot::channel},
	envelope::Envelope,
	process::{Pid, Process, Spawnable},
	shutdown::Shutdown,
	traits::{ExitStatus, InstanceError, Program, Spawner},
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

/// The default time dirigent allows processes to further make process
/// before killing them.
const DEFAULT_SHUTDOWN_TIME: u64 = 5;

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
pub enum Error {
	AlreadyShutdown,
}

#[derive(Debug)]
enum Command<P> {
	Schedule {
		program: RawWrapper<P>,
		name: &'static str,
		return_pid: oneshot::Sender<Pid>,
	},
	Start(Pid),
	Stop(Pid),
	Unpreempt(Pid),
	Preempt(Pid),
	FetchRunning(oneshot::Sender<Vec<Pid>>),
	Kill(Pid),
	Shutdown,
	ForceShutdown,
}

// NOTE: The safety results from the fact, that the dirigent
//       only ever receives the raw point from the public
//       interface of the `struct Takt`, which takes care
//       of leaking an owned valued of `trait Program`.
unsafe impl<P: Send> Send for Command<P> {}

pub struct Dirigent<
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
			mpsc::channel_sized::<Envelope, BUS_SIZE>();
		let (processes, updater) = Updatable::new(Vec::<Process<process::SPS<S>>>::new());
		let (mut shutdown, handle) = Shutdown::new();

		// NOTE: Spawning the control structure. The bus itself lives in this
		//       state-machine.
		spawner.spawn(Self::serve(
			spawner.handle(),
			takt_to_dirigent_recv,
			updater,
			program_to_bus_send.clone(),
			handle,
		));

		loop {
			select_biased! {
				() = shutdown => {
					info!("Dirigent is shutting down. Shutting down bus.");
					break;
				},
				res = program_to_bus_recv.recv().fuse() => {
					match res {
						Ok(envelope) => {
							// NOTE: We optimistically allocate enough memory. Choosing a huge batch
							//       size might leave a lot of unused memory blocked
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
									error!("Bus broken. This must be an OS error as this stack holds a reference to the sender.")
								}
							}

							trace!("Bus received batch of: {}", batch.len());
							let batch = Arc::new(batch);
							let active = processes.current().clone();
							active
								.iter()
								.for_each(|process| process.consume(batch.clone()))
						}
						Err(_) => {
							error!("Bus broken. This must be an OS error as this stack holds a reference to the sender.")
						}
					}
				}
			}
		}

		Ok(())
	}

	async fn serve(
		spawner: S::Handle,
		takt_to_dirigent_recv: mpsc::Receiver<Command<P>>,
		mut updater: Updater<Vec<Process<process::SPS<S>>>>,
		program_to_bus_send: mpsc::Sender<Envelope>,
		shutdown_handle: shutdown::Handle,
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
						Command::ForceShutdown => {
							info!("ForceShutdown: Dirigent ending control event loop.");
							shutdown_handle.shutdown();

							current_processes.iter().for_each(|process| process.kill());

							break;
						}
						Command::Shutdown => {
							info!(
								"Shutdown: Dirigent ending control event loop in {} second.",
								DEFAULT_SHUTDOWN_TIME
							);

							futures_timer::Delay::new(Duration::from_secs(DEFAULT_SHUTDOWN_TIME))
								.await;
							current_processes.iter().for_each(|process| process.kill());

							break;
						}
						Command::Stop(pid) => Self::on_process(
							&current_processes,
							pid,
							|p| {
								p.stop();
								true
							},
							false,
						),
						Command::Kill(pid) => Self::on_process(
							&current_processes,
							pid,
							|p| {
								p.kill();
								true
							},
							false,
						),
						Command::Preempt(pid) => Self::on_process(
							&current_processes,
							pid,
							|p| {
								p.preempt();
								false
							},
							false,
						),
						Command::Unpreempt(pid) => Self::on_process(
							&current_processes,
							pid,
							|p| {
								p.run();
								false
							},
							false,
						),
						Command::FetchRunning(sender) => {
							let running: Vec<Pid> =
								current_processes.iter().map(|p| p.pid()).collect();
							spawner.spawn(async move {
								sender.send(running).await.map_err(|e| {
									trace!("Could not deliver pids. Receiver dropped.");

									InstanceError::Internal(Box::new(e))
								})
							});

							false
						}
					};

					trace!(
						"Current processes (amount: {}): {:?}",
						current_processes.len(),
						current_processes
					);
					if update_proccesses {
						trace!(
							"Lined up processes (amount: {}): {:?}",
							next_processes.len(),
							next_processes
						);
						let updated_processes = next_processes
							.iter()
							.chain(current_processes.iter())
							.filter(|process| process.alive())
							.map(Clone::clone)
							.collect();
						updater.update(updated_processes);
						current_processes = updater.current();
						next_processes = Vec::new();

						trace!(
							"Updated processes (amount: {}): {:?}",
							current_processes.len(),
							current_processes
						);
					}
				}
				Err(_) => {
					info!("Dirigent can no longer receive commands. All Takt instances dropped...");
					break;
				}
			}
		}

		Ok(())
	}

	fn on_process<F, R>(processes: &Vec<Process<process::SPS<S>>>, pid: Pid, f: F, default: R) -> R
	where
		F: FnOnce(&Process<process::SPS<S>>) -> R,
	{
		if let Some(index) = processes.iter().position(|process| process.pid() == pid) {
			f(processes.get(index).expect("Index is existing. qed."))
		} else {
			warn!(
				"Could not execute action on process {:?}. Reason: Not existing.",
				pid
			);

			default
		}
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

	pub async fn schedule(&mut self, program: P, name: &'static str) -> Result<Pid, Error> {
		let (send, recv) = oneshot::channel::<Pid>();
		let cmd = Command::Schedule {
			program: RawWrapper::new(program),
			return_pid: send,
			name,
		};
		self.sender
			.send(cmd)
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		let pid = recv.recv().await.map_err(|_| Error::AlreadyShutdown)?;

		Ok(pid)
	}

	pub async fn run(&mut self, program: P, name: &'static str) -> Result<Pid, Error> {
		let pid = self.schedule(program, name).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	pub async fn start(&mut self, pid: Pid) -> Result<(), Error> {
		self.sender
			.send(Command::Start(pid))
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}

	pub async fn stop(&mut self, pid: Pid) -> Result<(), Error> {
		self.sender
			.send(Command::Stop(pid))
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}

	pub async fn preempt(&mut self, pid: Pid) -> Result<(), Error> {
		self.sender
			.send(Command::Preempt(pid))
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}

	pub async fn kill(&mut self, pid: Pid) -> Result<(), Error> {
		self.sender
			.send(Command::Kill(pid))
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}

	pub async fn unpreempt(&mut self, pid: Pid) -> Result<(), Error> {
		self.sender
			.send(Command::Unpreempt(pid))
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}

	pub async fn shutdown(&mut self) -> Result<(), Error> {
		self.sender
			.send(Command::Shutdown)
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}

	pub async fn force_shutdown(&mut self) -> Result<(), Error> {
		self.sender
			.send(Command::ForceShutdown)
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}
}

pub struct MinorTakt<P: Program> {
	sender: mpsc::Sender<Command<P>>,
}

impl<P: Program> MinorTakt<P> {
	pub async fn run(&mut self, program: P, name: &'static str) -> Result<Pid, Error> {
		let pid = self.schedule(program, name).await?;
		self.start(pid).await?;
		Ok(pid)
	}

	async fn schedule(&mut self, program: P, name: &'static str) -> Result<Pid, Error> {
		let (send, recv) = oneshot::channel::<Pid>();
		let cmd = Command::Schedule {
			program: RawWrapper::new(program),
			return_pid: send,
			name,
		};
		self.sender
			.send(cmd)
			.await
			.map_err(|_| Error::AlreadyShutdown)?;
		let pid = recv.recv().await.map_err(|_| Error::AlreadyShutdown)?;

		Ok(pid)
	}

	async fn start(&mut self, pid: Pid) -> Result<(), Error> {
		self.sender
			.send(Command::Start(pid))
			.await
			.map_err(|_| Error::AlreadyShutdown)?;

		Ok(())
	}
}
