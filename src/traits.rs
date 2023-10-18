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
	error::Error,
	fmt::Debug,
	pin::Pin,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	task::Poll,
	time::Duration,
};

use futures::future::{BoxFuture, Future};

use crate::{
	channel,
	channel::{RecvError, SendError},
	envelope::Envelope,
	process::ProgramSignal,
	Pid,
};

pub type ExitStatus = Result<(), InstanceError>;

pub const DEFAULT_SHUTDOWN_TIME_SECS: u64 = 5;

#[derive(Debug)]
pub enum InstanceError {
	Internal(Box<dyn Error + 'static + Send>),
	Shutdown,
	Killed,
	Timeouted,
	Unexpected,
}

/// Almost a marker trait to ensure messages have the same
/// interface.
pub trait Message: Send + Sync + 'static {
	/// A method that is called each time a Message
	/// is read.
	///
	/// * SHOULD be lightweight.
	/// * DEFAULT implementation is doing nothing.
	fn read(&self) {}
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

pub struct IndexRegistry(channel::spsc::Sender<ProgramSignal>);

impl IndexRegistry {
	pub fn new(sender: channel::spsc::Sender<ProgramSignal>) -> Self {
		IndexRegistry(sender)
	}

	pub async fn register(self, index: Box<dyn Index>) {
		self.0.send(ProgramSignal::SetIndex(index)).await;
	}
}

#[async_trait::async_trait]
pub trait Program: 'static + Send + Sync + Debug {
	async fn start(self: Box<Self>, ctx: Box<dyn Context>, registry: IndexRegistry) -> ExitStatus;
}

#[async_trait::async_trait]
impl<'a> Program for Box<dyn Program + 'a> {
	async fn start(self: Box<Self>, ctx: Box<dyn Context>, registry: IndexRegistry) -> ExitStatus {
		Program::start(*self, ctx, registry).await
	}
}

#[async_trait::async_trait]
pub trait Context: Send + 'static {
	fn try_recv(&self) -> Result<Option<Envelope>, RecvError>;

	async fn recv(&mut self) -> Result<Envelope, RecvError>;

	async fn send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn try_send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn sender(&self) -> channel::mpsc::Sender<Envelope>;

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>, name: Option<&'static str>);

	fn spawn_sub_blocking(
		&mut self,
		sub: BoxFuture<'static, ExitStatus>,
		name: Option<&'static str>,
	);
}

#[async_trait::async_trait]
impl Context for Box<dyn Context> {
	fn try_recv(&self) -> Result<Option<Envelope>, RecvError> {
		(**self).try_recv()
	}

	async fn recv(&mut self) -> Result<Envelope, RecvError> {
		(**self).recv().await
	}

	async fn send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		(**self).send(envelope).await
	}

	fn try_send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		(**self).try_send(envelope)
	}

	fn sender(&self) -> channel::mpsc::Sender<Envelope> {
		(**self).sender()
	}

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>, name: Option<&'static str>) {
		(**self).spawn_sub(sub, name)
	}

	fn spawn_sub_blocking(
		&mut self,
		sub: BoxFuture<'static, ExitStatus>,
		name: Option<&'static str>,
	) {
		(**self).spawn_sub_blocking(sub, name)
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
	Shutdown,
	Killed,
}

const PREEMPTED: usize = 0;
const RUNNING: usize = 0b0001;
const SHUTDOWN: usize = 0b0010;
const KILLED: usize = 0b0100;

pub struct Scheduler {
	pid: Pid,
	states: Vec<Arc<Inner>>,
}

struct Inner {
	waker: atomic_waker::AtomicWaker,
	state: AtomicUsize,
	dead: AtomicBool,
}

impl Scheduler {
	pub fn waking(&mut self) {
		tracing::debug!("Trying waking for all of {:?}...", self.pid);

		for instance in &mut self.states {
			instance.waker.wake()
		}
	}

	pub fn cleaning(&mut self) {
		self.states
			.retain(|inner| !inner.dead.load(Ordering::Relaxed))
	}

	pub fn new(pid: Pid) -> Scheduler {
		Scheduler {
			states: Vec::new(),
			pid,
		}
	}

	pub fn reference(&mut self) -> SchedulerRef {
		let inner = Arc::new(Inner {
			waker: atomic_waker::AtomicWaker::new(),
			state: AtomicUsize::new(RUNNING),
			dead: AtomicBool::new(false),
		});
		self.states.push(inner.clone());

		SchedulerRef {
			inner,
			pid: self.pid.clone(),
		}
	}

	pub fn kill(&mut self) {
		let mut count = 0usize;
		self.states.retain(|instance| {
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
	}

	pub fn preempt(&mut self) {
		let mut count = 0usize;
		self.states.retain(|instance| {
			count += 1;

			let not_dead = !instance.dead.load(Ordering::SeqCst);
			if not_dead {
				instance.state.store(PREEMPTED, Ordering::Relaxed);
				tracing::debug!("Waking {:?} for Preemption", self.pid);
				instance.waker.wake()
			}

			not_dead
		});
	}

	pub fn schedule(&mut self) {
		let mut count = 0usize;
		self.states.retain(|instance| {
			count += 1;

			let not_dead = !instance.dead.load(Ordering::SeqCst);
			if not_dead {
				instance.state.store(RUNNING, Ordering::Relaxed);
				tracing::debug!("Waking {:?} for Running", self.pid);
				instance.waker.wake()
			}

			not_dead
		});
	}

	pub fn shutdown(&mut self) {
		let mut count = 0usize;
		self.states.retain(|instance| {
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
	}
}

#[derive(Clone)]
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
			_ => unreachable!(),
		}
	}

	fn register_waker(&mut self, waker: &futures::task::Waker) -> Result<(), InstanceError> {
		tracing::trace!("Registering waker {:?} for {:?}.", waker, self.pid);
		self.inner.waker.register(waker);
		Ok(())
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
					let mut delay =
						futures_timer::Delay::new(Duration::from_secs(DEFAULT_SHUTDOWN_TIME_SECS));

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
						.map(|mut timer| {
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

mod atomic_waker {
	use core::{
		cell::UnsafeCell,
		fmt,
		sync::atomic::{
			AtomicUsize,
			Ordering::{AcqRel, Acquire, Release},
		},
		task::Waker,
	};

	/// A synchronization primitive for task wakeup.
	///
	/// Sometimes the task interested in a given event will change over time.
	/// An `AtomicWaker` can coordinate concurrent notifications with the
	/// consumer potentially "updating" the underlying task to wake up. This is
	/// useful in scenarios where a computation completes in another thread and
	/// wants to notify the consumer, but the consumer is in the process of
	/// being migrated to a new logical task.
	///
	/// Consumers should call `register` before checking the result of a
	/// computation and producers should call `wake` after producing the
	/// computation (this differs from the usual `thread::park` pattern). It is
	/// also permitted for `wake` to be called **before** `register`. This
	/// results in a no-op.
	///
	/// A single `AtomicWaker` may be reused for any number of calls to
	/// `register` or `wake`.
	///
	/// `AtomicWaker` does not provide any memory ordering guarantees, as such
	/// the user should use caution and use other synchronization primitives to
	/// guard the result of the underlying computation.
	pub struct AtomicWaker {
		state: AtomicUsize,
		waker: UnsafeCell<Option<Waker>>,
	}

	/// Idle state
	const WAITING: usize = 0;

	/// A new waker value is being registered with the `AtomicWaker` cell.
	const REGISTERING: usize = 0b01;

	/// The waker currently registered with the `AtomicWaker` cell is being
	/// woken.
	const WAKING: usize = 0b10;

	impl AtomicWaker {
		/// Create an `AtomicWaker`.
		pub fn new() -> AtomicWaker {
			// Make sure that task is Sync
			trait AssertSync: Sync {}
			impl AssertSync for Waker {}

			AtomicWaker {
				state: AtomicUsize::new(WAITING),
				waker: UnsafeCell::new(None),
			}
		}

		/// Registers the waker to be notified on calls to `wake`.
		///
		/// The new task will take place of any previous tasks that were
		/// registered by previous calls to `register`. Any calls to `wake` that
		/// happen after a call to `register` (as defined by the memory ordering
		/// rules), will notify the `register` caller's task and deregister the
		/// waker from future notifications. Because of this, callers should
		/// ensure `register` gets invoked with a new `Waker` **each** time they
		/// require a wakeup.
		///
		/// It is safe to call `register` with multiple other threads
		/// concurrently calling `wake`. This will result in the `register`
		/// caller's current task being notified once.
		///
		/// This function is safe to call concurrently, but this is generally a
		/// bad idea. Concurrent calls to `register` will attempt to register
		/// different tasks to be notified. One of the callers will win and have
		/// its task set, but there is no guarantee as to which caller will
		/// succeed.
		///
		/// # Examples
		///
		/// Here is how `register` is used when implementing a flag.
		///
		/// ```
		/// use std::future::Future;
		/// use std::task::{Context, Poll};
		/// use std::sync::atomic::AtomicBool;
		/// use std::sync::atomic::Ordering::SeqCst;
		/// use std::pin::Pin;
		///
		/// use futures::task::AtomicWaker;
		///
		/// struct Flag {
		///     waker: AtomicWaker,
		///     set: AtomicBool,
		/// }
		///
		/// impl Future for Flag {
		///     type Output = ();
		///
		///     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		///         // Register **before** checking `set` to avoid a race condition
		///         // that would result in lost notifications.
		///         self.waker.register(cx.waker());
		///
		///         if self.set.load(SeqCst) {
		///             Poll::Ready(())
		///         } else {
		///             Poll::Pending
		///         }
		///     }
		/// }
		/// ```
		pub fn register(&self, waker: &Waker) {
			match self.state.compare_and_swap(WAITING, REGISTERING, Acquire) {
				WAITING => {
					unsafe {
						// Locked acquired, update the waker cell
						*self.waker.get() = Some(waker.clone());

						// Release the lock. If the state transitioned to include
						// the `WAKING` bit, this means that a wake has been
						// called concurrently, so we have to remove the waker and
						// wake it.`
						//
						// Start by assuming that the state is `REGISTERING` as this
						// is what we jut set it to.
						let res =
							self.state
								.compare_exchange(REGISTERING, WAITING, AcqRel, Acquire);

						match res {
							Ok(_) => {}
							Err(actual) => {
								// This branch can only be reached if a
								// concurrent thread called `wake`. In this
								// case, `actual` **must** be `REGISTERING |
								// `WAKING`.
								debug_assert_eq!(actual, REGISTERING | WAKING);

								// Take the waker to wake once the atomic operation has
								// completed.
								let waker = (*self.waker.get()).take().unwrap();

								// Just swap, because no one could change state while state ==
								// `REGISTERING` | `WAKING`.
								self.state.swap(WAITING, AcqRel);

								// The atomic swap was complete, now
								// wake the task and return.
								waker.wake();
							}
						}
					}
				}
				WAKING => {
					// Currently in the process of waking the task, i.e.,
					// `wake` is currently being called on the old task handle.
					// So, we call wake on the new waker
					tracing::trace!("Registering while waking. Waking with new waker.");
					waker.wake_by_ref();
				}
				state => {
					// In this case, a concurrent thread is holding the
					// "registering" lock. This probably indicates a bug in the
					// caller's code as racing to call `register` doesn't make much
					// sense.
					//
					// We just want to maintain memory safety. It is ok to drop the
					// call to `register`.
					debug_assert!(state == REGISTERING || state == REGISTERING | WAKING);
					tracing::error!("Registering lock hold by two threads. This is a bug.")
				}
			}
		}

		/// Calls `wake` on the last `Waker` passed to `register`.
		///
		/// If `register` has not been called yet, then this does nothing.
		pub fn wake(&self) {
			if let Some(waker) = self.take() {
				waker.wake();
			}
		}

		/// Returns the last `Waker` passed to `register`, so that the user can
		/// wake it.
		///
		///
		/// Sometimes, just waking the AtomicWaker is not fine grained enough.
		/// This allows the user to take the waker and then wake it separately,
		/// rather than performing both steps in one atomic action.
		///
		/// If a waker has not been registered, this returns `None`.
		pub fn take(&self) -> Option<Waker> {
			// AcqRel ordering is used in order to acquire the value of the `task`
			// cell as well as to establish a `release` ordering with whatever
			// memory the `AtomicWaker` is associated with.
			match self.state.fetch_or(WAKING, AcqRel) {
				WAITING => {
					// The waking lock has been acquired.
					let waker = unsafe { (*self.waker.get()).take() };

					// Release the lock
					self.state.fetch_and(!WAKING, Release);

					waker
				}
				state => {
					// There is a concurrent thread currently updating the
					// associated task.
					//
					// Nothing more to do as the `WAKING` bit has been set. It
					// doesn't matter if there are concurrent registering threads or
					// not.
					//
					debug_assert!(
						state == REGISTERING || state == REGISTERING | WAKING || state == WAKING
					);
					None
				}
			}
		}
	}

	impl Default for AtomicWaker {
		fn default() -> Self {
			AtomicWaker::new()
		}
	}

	impl fmt::Debug for AtomicWaker {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(f, "AtomicWaker")
		}
	}

	unsafe impl Send for AtomicWaker {}
	unsafe impl Sync for AtomicWaker {}
}
