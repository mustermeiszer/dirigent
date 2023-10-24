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

use std::{error::Error, fmt::Debug, sync::Arc};

use futures::future::{BoxFuture, Future};

use crate::{
	channel,
	channel::{RecvError, SendError},
	envelope::Envelope,
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

pub trait Index: Send + Sync + 'static {
	fn indexed(&self, t: &Envelope) -> bool;
}

impl Index for Box<dyn Index> {
	fn indexed(&self, t: &Envelope) -> bool {
		(**self).indexed(t)
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
pub trait Context: Send + 'static {
	fn try_recv(&self) -> Result<Option<Envelope>, RecvError>;

	async fn recv(&mut self) -> Result<Envelope, RecvError>;

	async fn send(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn try_send(&self, envelope: Envelope) -> Result<(), SendError<Envelope>>;

	fn sender(&self) -> channel::mpsc::Sender<Envelope>;

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>);

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>);
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
