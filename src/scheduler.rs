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
	time::Duration,
};

use futures::task::AtomicWaker;
use tracing::{error, trace};

use crate::{process::Pid, traits::InstanceError};

#[derive(Clone, Copy)]
pub enum InstanceState {
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

struct Inner {
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

	pub fn reference(&self) -> SchedulerRef {
		let inner = Arc::new(Inner {
			waker: AtomicWaker::new(),
			state: AtomicUsize::new(RUNNING),
			finished: AtomicBool::new(false),
		});

		self.on_states(|states| states.push(inner.clone()));

		SchedulerRef {
			inner,
			pid: self.pid.clone(),
		}
	}

	fn on_states(&self, f: impl FnOnce(&mut Vec<Arc<Inner>>)) {
		match self.states.lock() {
			Ok(mut guard) => f(guard.as_mut()),
			Err(_) => panic!("Scheduler lock poisoned. Unrecoverable error."),
		}
	}

	pub fn waking(&mut self) {
		trace!("Scheduler: Waking all state-machines of {:?}...", self.pid);

		self.on_states(|states| {
			for instance in states {
				instance.waker.wake()
			}
		})
	}

	pub fn kill(&mut self) {
		let mut count = 0usize;
		self.on_states(|states| {
			states.retain(|instance| {
				count += 1;
				let not_finished = !instance.finished.load(Ordering::Relaxed);

				if not_finished {
					trace!(
						"Scheduler: Waking instance {} of {:?} for killing",
						count,
						self.pid
					);

					instance.state.store(KILLED, Ordering::Relaxed);
					instance.finished.store(true, Ordering::Relaxed);
					instance.waker.wake()
				}

				not_finished
			});
		})
	}

	pub fn preempt(&mut self) {
		let mut count = 0usize;
		self.on_states(|states| {
			states.retain(|instance| {
				count += 1;

				let not_dead = !instance.finished.load(Ordering::SeqCst);
				if not_dead {
					instance.state.store(PREEMPTED, Ordering::Relaxed);
					tracing::debug!("Waking {:?} for Preemption", self.pid);
					instance.waker.wake()
				}

				not_dead
			});
		})
	}

	pub fn schedule(&mut self) {
		let mut count = 0usize;
		self.on_states(|states| {
			states.retain(|instance| {
				count += 1;

				let not_dead = !instance.finished.load(Ordering::SeqCst);
				if not_dead {
					instance.state.store(RUNNING, Ordering::Relaxed);
					tracing::debug!("Waking {:?} for Running", self.pid);
					instance.waker.wake()
				}

				not_dead
			});
		})
	}
}

pub struct SchedulerRef {
	inner: Arc<Inner>,
	pid: Pid,
}

impl SchedulerRef {
	fn state(&self) -> InstanceState {
		match self.inner.state.load(Ordering::SeqCst) {
			RUNNING => InstanceState::Running,
			PREEMPTED => InstanceState::Preempted,
			KILLED => InstanceState::Killed,
			_ => unreachable!("All constant values are covered. qed."),
		}
	}

	fn register_waker(&mut self, waker: &futures::task::Waker) {
		trace!(
			"Scheduler: Registering waker {:?} for {:?}.",
			waker,
			self.pid
		);
		self.inner.waker.register(waker);
	}

	fn set_finished(&self) {
		self.inner.finished.store(true, Ordering::Relaxed);
	}
}

/// A future that wraps another future with a `Delay` allowing for time-limited
/// futures.
#[pin_project::pin_project]
pub struct Scheduled<F: Future> {
	#[pin]
	future: F,
	scheduler: SchedulerRef,
}

/// Extends `Future` to allow time-limited futures.
pub trait ScheduleExt: Future {
	/// Adds a timeout of `duration` to the given `Future`.
	/// Returns a new `Future`.
	fn schedule(self, scheduler: SchedulerRef) -> Scheduled<Self>
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
		let mut this = self.project();

		this.scheduler.register_waker(ctx.waker());

		match this.scheduler.state() {
			InstanceState::Killed => {
				trace!("Scheduler poll: Killing {:?} .", this.scheduler.pid);
				this.scheduler.set_finished();
				Poll::Ready(Err(InstanceError::Killed))
			}
			InstanceState::Running => {
				trace!("Scheduler poll: Running{:?} .", this.scheduler.pid);

				if let Poll::Ready(output) = this.future.poll(ctx) {
					this.scheduler.set_finished();
					Poll::Ready(Ok(output))
				} else {
					Poll::Pending
				}
			}
			InstanceState::Preempted => {
				trace!("Scheduler poll: Preempted {:?} .", this.scheduler.pid);
				Poll::Pending
			}
			InstanceState::Finished => {
				error!(
					"Scheduler poll: Polling already finished future of {:?}. This is a bug...",
					this.scheduler.pid
				);
				this.scheduler.set_finished();
				Poll::Ready(Err(InstanceError::Unexpected))
			}
		}
	}
}
