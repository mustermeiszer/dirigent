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

use crate::{process::Pid, traits::InstanceError};

#[derive(Clone, Copy)]
pub enum InstanceState {
	Preempted,
	Running,
	Shutdown,
	Killed,
}

const PREEMPTED: usize = 0;
const RUNNING: usize = 0b0001;
const SHUTDOWN: usize = 0b0010;
const KILLED: usize = 0b0100;

#[derive(Clone)]
pub struct Scheduler {
	pid: Pid,
	states: Arc<Mutex<Vec<Arc<Inner>>>>,
}

struct Inner {
	waker: AtomicWaker,
	state: AtomicUsize,
	dead: AtomicBool,
}

impl Scheduler {
	fn on_states(&self, f: impl FnOnce(&mut Vec<Arc<Inner>>)) {
		match self.states.lock() {
			Ok(mut guard) => f(guard.as_mut()),
			Err(_) => panic!("Scheduler lock poisoned. Unrecoverable error."),
		}
	}

	pub fn waking(&mut self) {
		tracing::debug!("Trying waking for all of {:?}...", self.pid);

		self.on_states(|states| {
			for instance in states {
				instance.waker.wake()
			}
		})
	}

	pub fn cleaning(&mut self) {
		self.on_states(|states| states.retain(|inner| !inner.dead.load(Ordering::Relaxed)))
	}

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
			dead: AtomicBool::new(false),
		});

		self.on_states(|states| states.push(inner.clone()));

		SchedulerRef {
			inner,
			pid: self.pid.clone(),
		}
	}

	pub fn kill(&mut self) {
		let mut count = 0usize;
		self.on_states(|states| {
			states.retain(|instance| {
				count += 1;
				let not_dead = !instance.dead.load(Ordering::SeqCst);
				if not_dead {
					instance.state.store(KILLED, Ordering::Relaxed);
					instance.dead.store(true, Ordering::Relaxed);
					tracing::debug!("Waking instance {} of {:?} for killing", count, self.pid);
					instance.waker.wake()
				}

				not_dead
			});
		})
	}

	pub fn preempt(&mut self) {
		let mut count = 0usize;
		self.on_states(|states| {
			states.retain(|instance| {
				count += 1;

				let not_dead = !instance.dead.load(Ordering::SeqCst);
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

				let not_dead = !instance.dead.load(Ordering::SeqCst);
				if not_dead {
					instance.state.store(RUNNING, Ordering::Relaxed);
					tracing::debug!("Waking {:?} for Running", self.pid);
					instance.waker.wake()
				}

				not_dead
			});
		})
	}

	pub fn shutdown(&mut self) {
		let mut count = 0usize;
		self.on_states(|states| {
			states.retain(|instance| {
				count += 1;

				let not_dead = !instance.dead.load(Ordering::SeqCst);
				if not_dead {
					instance.state.store(SHUTDOWN, Ordering::Relaxed);
					instance.dead.store(true, Ordering::Relaxed);
					tracing::debug!("Waking {:?} for Shutdown", self.pid);
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
			SHUTDOWN => InstanceState::Shutdown,
			KILLED => InstanceState::Killed,
			_ => unreachable!("All constant values are covered. qed."),
		}
	}

	fn register_waker(&mut self, waker: &futures::task::Waker) {
		tracing::trace!("Registering waker {:?} for {:?}.", waker, self.pid);
		self.inner.waker.register(waker);
	}
}

/// A future that wraps another future with a `Delay` allowing for time-limited
/// futures.
#[pin_project::pin_project]
pub struct Scheduled<F: Future> {
	#[pin]
	future: F,
	scheduler: SchedulerRef,
	#[pin]
	shutdown_timer: Option<futures_timer::Delay>,
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
			shutdown_timer: None,
		}
	}
}

impl<F> ScheduleExt for F where F: Future {}

/// TODO: Research race conditions here!!!
impl<F> Future for Scheduled<F>
where
	F: Future,
{
	type Output = Result<F::Output, InstanceError>;

	fn poll(self: Pin<&mut Self>, ctx: &mut futures::task::Context) -> Poll<Self::Output> {
		let mut this = self.project();

		this.scheduler.register_waker(ctx.waker());

		match this.scheduler.state() {
			InstanceState::Shutdown => {
				tracing::debug!("Shutdown. Timer: {:?}", this.shutdown_timer);

				if this.shutdown_timer.is_none() {
					tracing::debug!("Schedule poll {:?}: Shutdown.", this.scheduler.pid);
					let mut delay = futures_timer::Delay::new(Duration::from_secs(2));

					unsafe {
						if Pin::new_unchecked(&mut delay).poll(ctx).is_ready() {
							return Poll::Ready(Err(InstanceError::Shutdown));
						}
					}

					*this.shutdown_timer = Some(delay);

					tracing::debug!("Shutdown. Timer: {:?}", this.shutdown_timer);
				} else {
					if this
						.shutdown_timer
						.get_mut()
						.as_mut()
						.map(|timer| {
							tracing::debug!("Shutdown. Pooling timer for {:?}", this.scheduler.pid);
							unsafe { Pin::new_unchecked(timer) }.poll(ctx).is_ready()
						})
						.unwrap_or(false)
					{
						return Poll::Ready(Err(InstanceError::Shutdown));
					}

					if let Poll::Ready(output) = this.future.poll(ctx) {
						return Poll::Ready(Ok(output));
					}
				}
			}
			InstanceState::Killed => {
				tracing::debug!("Schedule poll {:?}: Killing.", this.scheduler.pid);
				return Poll::Ready(Err(InstanceError::Killed));
			}
			InstanceState::Running => {
				tracing::debug!("Schedule poll {:?}: Running.", this.scheduler.pid);

				if let Poll::Ready(output) = this.future.poll(ctx) {
					return Poll::Ready(Ok(output));
				}
			}
			InstanceState::Preempted => {
				tracing::debug!("Schedule poll {:?}: Preeempted.", this.scheduler.pid);
			}
		}

		Poll::Pending
	}
}

/// A future that wraps another future with a `Delay` allowing for time-limited
/// futures.
#[pin_project::pin_project]
pub struct Timeout<F: Future> {
	#[pin]
	future: F,
	#[pin]
	delay: futures_timer::Delay,
}

/// Extends `Future` to allow time-limited futures.
pub trait TimeoutExt: Future {
	/// Adds a timeout of `duration` to the given `Future`.
	/// Returns a new `Future`.
	fn timeout(self, duration: Duration) -> Timeout<Self>
	where
		Self: Sized,
	{
		Timeout {
			future: self,
			delay: futures_timer::Delay::new(duration),
		}
	}
}

impl<F> TimeoutExt for F where F: Future {}

impl<F> Future for Timeout<F>
where
	F: Future,
{
	type Output = Result<F::Output, InstanceError>;

	fn poll(self: Pin<&mut Self>, ctx: &mut futures::task::Context) -> Poll<Self::Output> {
		let this = self.project();

		if this.delay.poll(ctx).is_ready() {
			return Poll::Ready(Err(InstanceError::Timeouted));
		}

		if let Poll::Ready(output) = this.future.poll(ctx) {
			return Poll::Ready(Ok(output));
		}

		Poll::Pending
	}
}
