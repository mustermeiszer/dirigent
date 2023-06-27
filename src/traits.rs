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

use core::future::Future;
use std::{error::Error, fmt::Debug, pin::Pin, sync::Arc, task::Poll, time::Duration};

use futures::future::BoxFuture;

use crate::{
	channel,
	channel::{RecvError, SendError},
	envelope::Envelope,
};

pub type ExitStatus = Result<(), InstanceError>;

#[derive(Debug)]
pub enum InstanceError {
	Internal(Box<dyn Error + 'static + Send>),
	Killed,
	Timouted,
}

pub trait Message: Clone + Send + Sync + 'static {
	type Response: Send + Sync + 'static;
}

pub trait Index: Send + Sync + 'static {
	fn indexed(&self, t: &Envelope) -> bool;
}

impl Index for Box<dyn Index> {
	fn indexed(&self, t: &Envelope) -> bool {
		(**self).indexed(t)
	}
}

pub struct FullIndex;
impl Index for FullIndex {
	fn indexed(&self, _t: &Envelope) -> bool {
		true
	}
}

pub struct EmptyIndex;
impl Index for EmptyIndex {
	fn indexed(&self, _t: &Envelope) -> bool {
		false
	}
}

pub trait LookUp {
	type Output;

	fn look_up(t: &Envelope) -> Result<Self::Output, ()>;
}

#[async_trait::async_trait]
pub trait Program: 'static + Send + Sync + Debug {
	async fn start(self: Box<Self>, ctx: Box<dyn Context>) -> ExitStatus;

	fn index(&self) -> Box<dyn Index> {
		Box::new(EmptyIndex {})
	}
}

#[async_trait::async_trait]
impl<'a> Program for Box<dyn Program + 'a> {
	async fn start(self: Box<Self>, ctx: Box<dyn Context>) -> ExitStatus {
		Program::start(*self, ctx).await
	}

	fn index(&self) -> Box<dyn Index> {
		(**self).index()
	}
}

#[async_trait::async_trait]
pub trait Context: Send + 'static {
	async fn try_recv(&mut self) -> Result<Option<Envelope>, RecvError>;

	async fn recv(&mut self) -> Result<Envelope, RecvError>;

	async fn send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	async fn try_send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn sender(&self) -> channel::mpsc::Sender<Envelope>;

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>);

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>);
}

#[async_trait::async_trait]
impl Context for Box<dyn Context> {
	async fn try_recv(&mut self) -> Result<Option<Envelope>, RecvError> {
		(**self).try_recv().await
	}

	async fn recv(&mut self) -> Result<Envelope, RecvError> {
		(**self).recv().await
	}

	async fn send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		(**self).send(envelope).await
	}

	async fn try_send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		(**self).try_send(envelope).await
	}

	fn sender(&self) -> channel::mpsc::Sender<Envelope> {
		(**self).sender()
	}

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		(**self).spawn_sub(sub)
	}

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		(**self).spawn_sub_blocking(sub)
	}
}

pub trait Spawner: Send + Sync + 'static {
	type Handle: Spawner;

	/// Spawn the given blocking future.
	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
	/// Spawn the given non-blocking future.
	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
	/// Get a handle to this spawner, allowing to spawn stuff again.
	fn handle(&self) -> Self::Handle;
}

#[derive(Clone, Copy)]
pub enum InstanceState {
	Preempted,
	Running,
	Killed,
}

pub struct Scheduler(Arc<InstanceState>);

impl Scheduler {
	pub fn new() -> Scheduler {
		Scheduler(Arc::new(InstanceState::Running))
	}

	pub fn reference(&self) -> SchedulerRef {
		SchedulerRef(self.0.clone())
	}

	pub fn kill(&mut self) {
		let unsafe_ref = self.0.as_ref();

		unsafe {
			let unsafe_ref = &mut *(unsafe_ref as *const InstanceState as *mut InstanceState);
			*unsafe_ref = InstanceState::Killed;
		}
	}

	pub fn preempt(&mut self) {
		let unsafe_ref = self.0.as_ref();

		unsafe {
			let unsafe_ref = &mut *(unsafe_ref as *const InstanceState as *mut InstanceState);
			*unsafe_ref = InstanceState::Preempted;
		}
	}

	pub fn schedule(&mut self) {
		let unsafe_ref = self.0.as_ref();

		unsafe {
			let unsafe_ref = &mut *(unsafe_ref as *const InstanceState as *mut InstanceState);
			*unsafe_ref = InstanceState::Running;
		}
	}

	pub fn alive(&self) -> bool {
		match *self.0 {
			InstanceState::Running => true,
			InstanceState::Preempted => true,
			InstanceState::Killed => false,
		}
	}
}

#[derive(Clone)]
pub struct SchedulerRef(Arc<InstanceState>);

impl SchedulerRef {
	fn is_killed(&self) -> bool {
		match *self.0 {
			InstanceState::Running => false,
			InstanceState::Preempted => false,
			InstanceState::Killed => true,
		}
	}

	fn is_preempted(&self) -> bool {
		match *self.0 {
			InstanceState::Running => false,
			InstanceState::Preempted => true,
			InstanceState::Killed => false,
		}
	}

	fn is_scheduled(&self) -> bool {
		match *self.0 {
			InstanceState::Running => true,
			InstanceState::Preempted => false,
			InstanceState::Killed => false,
		}
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
		let this = self.project();

		if this.scheduler.is_killed() {
			return Poll::Ready(Err(InstanceError::Killed));
		}

		if this.scheduler.is_preempted() {
			ctx.waker().wake_by_ref();
			return Poll::Pending;
		}

		if let Poll::Ready(output) = this.future.poll(ctx) {
			return Poll::Ready(Ok(output));
		}

		ctx.waker().wake_by_ref();
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
			return Poll::Ready(Err(InstanceError::Timouted));
		}

		if let Poll::Ready(output) = this.future.poll(ctx) {
			return Poll::Ready(Ok(output));
		}

		ctx.waker().wake_by_ref();
		Poll::Pending
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Priority {
	/// Future will evaluate 1/4 of the time being waken up
	Low,
	/// Future will evaluate 1/2 of the time being waken up
	Middle,
	/// Future will evaluate everytime when being waken up
	High,
	/// Costume Value. This value will be the modulo that
	/// the counter for wake ups of the future of the task
	/// will be computed against
	///
	/// E.g.
	/// * Priority::Costume(7) -> future will evolve every 7th wake-up
	Custom(usize),
}

impl Priority {
	fn modulo(&self) -> usize {
		match self {
			Priority::Low => 4,
			Priority::Middle => 2,
			Priority::High => 1,
			Priority::Custom(modulo) => *modulo,
		}
	}
}

/// A future that wraps another future with a `Delay` allowing for time-limited
/// futures.
#[pin_project::pin_project]
pub struct Prioritized<F: Future> {
	#[pin]
	future: F,
	priority: Priority,
	wake_ups: usize,
}

/// Extends `Future` to allow time-limited futures.
pub trait PriorityExt: Future {
	/// Adds a timeout of `duration` to the given `Future`.
	/// Returns a new `Future`.
	fn prioritize(self, priority: Priority) -> Prioritized<Self>
	where
		Self: Sized,
	{
		Prioritized {
			future: self,
			priority,
			wake_ups: 0,
		}
	}
}

impl<F> PriorityExt for F where F: Future {}

impl<F> Future for Prioritized<F>
where
	F: Future,
{
	type Output = F::Output;

	fn poll(self: Pin<&mut Self>, ctx: &mut futures::task::Context) -> Poll<Self::Output> {
		let this = self.project();

		// increment
		*this.wake_ups = this.wake_ups.wrapping_add(1);

		// If there is no remainder we poll the inner future
		if *this.wake_ups % this.priority.modulo() == 0 {
			if let Poll::Ready(output) = this.future.poll(ctx) {
				Poll::Ready(output)
			} else {
				ctx.waker().wake_by_ref();
				Poll::Pending
			}
		} else {
			ctx.waker().wake_by_ref();
			Poll::Pending
		}
	}
}
