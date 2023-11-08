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
	future::Future,
	pin::Pin,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc, Mutex,
	},
	task::Poll,
};

use futures::task::AtomicWaker;
use tracing::{trace, warn};

use crate::{process::Pid, traits::InstanceError};

#[derive(Clone, Copy)]
pub enum ScheduledState {
	Preempted,
	Running,
	Killed,
	Finished,
}

const PREEMPTED: usize = 0;
const RUNNING: usize = 0b0001;
const KILLED: usize = 0b0010;
const FINISHED: usize = 0b0100;

#[derive(Clone)]
pub struct Scheduler {
	pid: Pid,
	states: Arc<Mutex<Vec<Arc<Inner>>>>,
}

#[derive(Debug)]
struct Inner {
	pid: Pid,
	waker: AtomicWaker,
	state: AtomicUsize,
	finished: AtomicBool,
}

impl Scheduler {
	pub fn new(pid: Pid) -> Scheduler {
		Scheduler {
			states: Arc::new(Mutex::new(Vec::new())),
			pid,
		}
	}

	pub fn reference(&self, pid: Pid) -> ScheduledRef {
		let inner = Arc::new(Inner {
			pid,
			waker: AtomicWaker::new(),
			state: AtomicUsize::new(RUNNING),
			finished: AtomicBool::new(false),
		});

		self.on_states(|states| states.push(inner.clone()));

		ScheduledRef {
			inner,
			pid: self.pid.clone(),
		}
	}

	fn on_states(&self, f: impl FnOnce(&mut Vec<Arc<Inner>>)) {
		match self.states.lock() {
			Ok(mut guard) => {
				trace!(
					"Subprocesses of {:?} before schedule: (number: {}): {:?}",
					self.pid,
					guard.len(),
					guard
				);

				f(guard.as_mut());

				trace!(
					"Subprocesses of {:?} after schedule: (number: {}): {:?}",
					self.pid,
					guard.len(),
					guard
				);
			}
			Err(_) => panic!("Scheduler lock poisoned. Unrecoverable error."),
		}
	}

	pub fn kill(&self) {
		self.on_states(|states| {
			states.retain(|instance| {
				let mut finished = instance.finished.load(Ordering::Relaxed);

				if !finished {
					trace!(
						"Waking instance {:?} of {:?} for killing",
						instance.pid,
						self.pid
					);
				}

				match instance
					.state
					.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |state| match state {
						RUNNING => Some(KILLED),
						PREEMPTED => Some(KILLED),
						KILLED => {
							warn!("Re-killing a killed process. Process stays killed.");
							None
						}
						FINISHED => {
							warn!("Re-killing a finished process. Process stays finished.");
							Some(KILLED)
						}
						_ => unreachable!("All values are covered. qed."),
					}) {
					Ok(_prev) => {
						finished = true;
						// Closing the future
						instance.waker.wake()
					}
					Err(_prev) => {
						finished = true;
						instance.waker.wake()
					}
				}

				!finished
			});
		})
	}

	pub fn preempt(&self) {
		self.on_states(|states| {
			states.retain(|instance| {
				let mut finished = instance.finished.load(Ordering::Relaxed);

				if !finished {
					trace!(
						"Waking instance {:?} of {:?} for preemption",
						instance.pid,
						self.pid
					);
				}

				match instance
					.state
					.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |state| match state {
						RUNNING => Some(PREEMPTED),
						PREEMPTED => None,
						KILLED => {
							warn!("Preempting a killed process. Process stays killed.");
							None
						}
						FINISHED => {
							warn!("Preempting a finished process. Process stays finished.");
							None
						}
						_ => unreachable!("All values are covered. qed."),
					}) {
					Ok(_prev) => {}
					Err(prev) => match prev {
						KILLED | FINISHED => {
							finished = true;
							// Closing the future
							instance.waker.wake()
						}
						_ => {}
					},
				}

				!finished
			});
		})
	}

	pub fn run(&self) {
		self.on_states(|states| {
			states.retain(|instance| {
				let mut finished = instance.finished.load(Ordering::Relaxed);

				if !finished {
					trace!(
						"Waking instance {:?} of {:?} for running",
						instance.pid,
						self.pid
					);
				}

				match instance
					.state
					.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |state| match state {
						RUNNING => None,
						PREEMPTED => Some(RUNNING),
						KILLED => {
							warn!("Re-running a killed process. Process stays killed.");
							None
						}
						FINISHED => {
							warn!("Re-running a finished process. Process stays finished.");
							None
						}
						_ => unreachable!("All values are covered. qed."),
					}) {
					Ok(_) => instance.waker.wake(),
					Err(prev) => {
						match prev {
							KILLED | FINISHED => {
								finished = true;
								// Closing the future
								instance.waker.wake()
							}
							_ => {}
						}
					}
				}

				!finished
			});
		})
	}
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ScheduledRef {
	pid: Pid,
	inner: Arc<Inner>,
}

impl ScheduledRef {
	fn state(&self) -> ScheduledState {
		match self.inner.state.load(Ordering::SeqCst) {
			RUNNING => ScheduledState::Running,
			PREEMPTED => ScheduledState::Preempted,
			KILLED => ScheduledState::Killed,
			FINISHED => ScheduledState::Finished,
			_ => unreachable!("All constant values are covered. qed."),
		}
	}

	fn register_waker(&mut self, waker: &futures::task::Waker) {
		trace!("Registering waker {:?} for {:?}.", waker, self.inner.pid);
		self.inner.waker.register(waker);
	}

	fn set_finished(&self) {
		trace!("Setting {:?}: Finished.", &self);
		self.inner.finished.store(true, Ordering::Relaxed);
	}
}

/// A future that wraps another future with a `Delay` allowing for time-limited
/// futures.
#[pin_project::pin_project]
pub struct Scheduled<F: Future> {
	#[pin]
	future: F,
	scheduler: ScheduledRef,
}

/// Extends `Future` to allow time-limited futures.
pub trait ScheduleExt: Future {
	/// Adds a timeout of `duration` to the given `Future`.
	/// Returns a new `Future`.
	fn schedule(self, scheduler: ScheduledRef) -> Scheduled<Self>
	where
		Self: Sized,
	{
		Scheduled {
			future: self,
			scheduler,
		}
	}
}

impl<F> ScheduleExt for F where F: Future {}

impl<F> Future for Scheduled<F>
where
	F: Future,
{
	type Output = Result<F::Output, InstanceError>;

	fn poll(self: Pin<&mut Self>, ctx: &mut futures::task::Context) -> Poll<Self::Output> {
		let this = self.project();

		this.scheduler.register_waker(ctx.waker());

		match this.scheduler.state() {
			ScheduledState::Killed => {
				trace!("Polling.. State for {:?}: Killing.", this.scheduler);
				this.scheduler.set_finished();
				Poll::Ready(Err(InstanceError::Killed))
			}
			ScheduledState::Running => {
				trace!("Polling.. State for {:?}: Running.", this.scheduler);

				if let Poll::Ready(output) = this.future.poll(ctx) {
					this.scheduler.set_finished();
					Poll::Ready(Ok(output))
				} else {
					Poll::Pending
				}
			}
			ScheduledState::Preempted => {
				trace!("Polling.. State for {:?}: Preempted.", this.scheduler);
				Poll::Pending
			}
			ScheduledState::Finished => {
				trace!("Polling.. State for {:?}: Finished.", this.scheduler);
				this.scheduler.set_finished();
				Poll::Ready(Err(InstanceError::AlreadyFinished))
			}
		}
	}
}
