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

use std::{error::Error, fmt::Debug, ops::Deref, sync::Arc};

use futures::future::{BoxFuture, Future};

use crate::{
	channel,
	channel::{RecvError, SendError},
	envelope::Envelope,
	spawner,
};

pub type ExitStatus = Result<(), InstanceError>;

#[derive(Debug)]
pub enum InstanceError {
	Internal(Box<dyn Error + 'static + Send>),
	Shutdown,
	Killed,
	AlreadyFinished,
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

#[async_trait::async_trait]
pub trait Index: Send + Sync + 'static {
	async fn indexed(&self, t: &Envelope) -> bool;
}

#[async_trait::async_trait]
impl Index for Box<dyn Index> {
	async fn indexed(&self, t: &Envelope) -> bool {
		(**self).indexed(t).await
	}
}

#[async_trait::async_trait]
pub trait IndexRegistry: Send + 'static {
	async fn register(self: Box<Self>, index: Arc<dyn Index>);
}

#[async_trait::async_trait]
pub trait Program: 'static + Send + Sync + Debug {
	async fn start(
		self: Box<Self>,
		ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus;
}

#[async_trait::async_trait]
impl<'a> Program for Box<dyn Program + 'a> {
	async fn start(
		self: Box<Self>,
		ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus {
		Program::start(*self, ctx, registry).await
	}
}

#[async_trait::async_trait]
pub trait Context: Send + Sync + 'static {
	fn try_recv(&self) -> Result<Option<Envelope>, RecvError>;

	async fn recv(&self) -> Result<Envelope, RecvError>;

	async fn send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn try_send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn sender(&self) -> channel::mpsc::Sender<Envelope>;

	fn spawn_sub(&self, sub: BoxFuture<'static, ExitStatus>);

	fn spawn_sub_blocking(&self, sub: BoxFuture<'static, ExitStatus>);

	fn sub_spawner(&self) -> spawner::SubSpawner;
}

#[async_trait::async_trait]
impl Context for Box<dyn Context> {
	fn try_recv(&self) -> Result<Option<Envelope>, RecvError> {
		(**self).try_recv()
	}

	async fn recv(&self) -> Result<Envelope, RecvError> {
		(**self).recv().await
	}

	async fn send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		(**self).send(envelope).await
	}

	fn try_send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
		(**self).try_send(envelope)
	}

	fn sender(&self) -> channel::mpsc::Sender<Envelope> {
		(**self).sender()
	}

	fn spawn_sub(&self, sub: BoxFuture<'static, ExitStatus>) {
		(**self).spawn_sub(sub)
	}

	fn spawn_sub_blocking(&self, sub: BoxFuture<'static, ExitStatus>) {
		(**self).spawn_sub_blocking(sub)
	}

	fn sub_spawner(&self) -> spawner::SubSpawner {
		(**self).sub_spawner()
	}
}

pub trait Spawner: Send + Sync + 'static {
	type Handle: Spawner + SubSpawner;

	/// Spawn the given blocking future.
	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
	/// Spawn the given non-blocking future.
	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static);

	/// Spawn the given blocking future.
	fn spawn_blocking_named(
		&self,
		_name: &'static str,
		future: impl Future<Output = ExitStatus> + Send + 'static,
	) {
		self.spawn_blocking(future)
	}
	/// Spawn the given non-blocking future.
	fn spawn_named(
		&self,
		_name: &'static str,
		future: impl Future<Output = ExitStatus> + Send + 'static,
	) {
		self.spawn(future)
	}

	/// Get a handle to this spawner, allowing to spawn stuff again.
	fn handle(&self) -> Self::Handle;
}

impl<T: Spawner> Spawner for Box<T> {
	type Handle = T::Handle;

	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		(**self).spawn_blocking(future)
	}

	fn spawn_blocking_named(
		&self,
		name: &'static str,
		future: impl Future<Output = ExitStatus> + Send + 'static,
	) {
		(**self).spawn_blocking_named(name, future)
	}

	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static) {
		(**self).spawn(future)
	}

	fn spawn_named(
		&self,
		name: &'static str,
		future: impl Future<Output = ExitStatus> + Send + 'static,
	) {
		(**self).spawn_named(name, future)
	}

	fn handle(&self) -> Self::Handle {
		(**self).handle()
	}
}

/// Internal trait to get rid of Handle while still being able to use
/// a trait Object.
pub trait SubSpawner: Send + 'static {
	/// Spawn the given blocking future.
	fn spawn_sub_blocking(&self, future: BoxFuture<'static, ExitStatus>);
	/// Spawn the given non-blocking future.
	fn spawn_sub(&self, future: BoxFuture<'static, ExitStatus>);

	/// Spawn the given blocking future.
	fn spawn_sub_blocking_named(
		&self,
		_name: &'static str,
		future: BoxFuture<'static, ExitStatus>,
	) {
		self.spawn_sub_blocking(future)
	}
	/// Spawn the given non-blocking future.
	fn spawn_sub_named(&self, _name: &'static str, future: BoxFuture<'static, ExitStatus>) {
		self.spawn_sub(future)
	}
}

impl<S: Spawner> SubSpawner for S {
	fn spawn_sub_blocking(&self, future: BoxFuture<'static, ExitStatus>) {
		<S as Spawner>::spawn_blocking(self, future)
	}

	fn spawn_sub(&self, future: BoxFuture<'static, ExitStatus>) {
		<S as Spawner>::spawn(self, future)
	}

	fn spawn_sub_blocking_named(&self, name: &'static str, future: BoxFuture<'static, ExitStatus>) {
		<S as Spawner>::spawn_blocking_named(self, name, future)
	}

	fn spawn_sub_named(&self, name: &'static str, future: BoxFuture<'static, ExitStatus>) {
		<S as Spawner>::spawn_named(self, name, future)
	}
}

pub trait ExecuteOnDrop: 'static {
	fn execute(&mut self);
}

impl<F> ExecuteOnDrop for F
where
	F: FnOnce() + 'static + Clone,
{
	fn execute(&mut self) {
		(self.clone())()
	}
}

pub struct OnDrop<T: ExecuteOnDrop>(T);

impl<T: ExecuteOnDrop> OnDrop<T> {
	pub fn new(t: T) -> Self {
		OnDrop(t)
	}
}

impl<T: ExecuteOnDrop> std::ops::Drop for OnDrop<T> {
	fn drop(&mut self) {
		<T as ExecuteOnDrop>::execute(&mut self.0)
	}
}

impl<T: ExecuteOnDrop> Deref for OnDrop<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(test)]
mod tests {
	use crate::traits::OnDrop;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;

	#[test]
	fn on_drop_with_fn() {
		let atomic = Arc::new(AtomicUsize::new(0));
		let clone = atomic.clone();
		OnDrop::new(move || {
			clone.fetch_add(1, Ordering::SeqCst);
		});

		assert_eq!(atomic.load(Ordering::SeqCst), 1);
	}
}
