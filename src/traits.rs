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

use core::{any::Any, future::Future};
use std::pin::Pin;

use futures::future::BoxFuture;

use crate::{channel, envelope, envelope::Envelope};

pub type ExitStatus = Result<(), ()>;

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
	fn indexed(&self, t: &Envelope) -> bool {
		true
	}
}

pub struct EmptyIndex;
impl Index for EmptyIndex {
	fn indexed(&self, t: &Envelope) -> bool {
		false
	}
}

pub trait LookUp {
	type Output;

	fn look_up(t: &Envelope) -> Result<Self::Output, ()>;
}

#[async_trait::async_trait]
pub trait Program: 'static + Send + Sync {
	async fn start(self: Box<Self>, ctx: Box<dyn Context>) -> ExitStatus;

	fn index(&self) -> Box<dyn Index> {
		Box::new(EmptyIndex {})
	}
}

#[async_trait::async_trait]
impl Program for Box<dyn Program> {
	async fn start(self: Box<Self>, ctx: Box<dyn Context>) -> ExitStatus {
		Program::start(*self, ctx).await
	}

	fn index(&self) -> Box<dyn Index> {
		(**self).index()
	}
}

#[async_trait::async_trait]
pub trait Context: Send + 'static {
	async fn try_recv(&mut self) -> Result<Option<Envelope>, ()>;

	async fn recv(&mut self) -> Result<Envelope, ()>;

	async fn send(&mut self, envelope: Envelope) -> Result<(), ()>;

	fn sender(&self) -> channel::Sender<Envelope>;

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>);

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>);
}

#[async_trait::async_trait]
impl Context for Box<dyn Context> {
	async fn try_recv(&mut self) -> Result<Option<Envelope>, ()> {
		(**self).try_recv().await
	}

	async fn recv(&mut self) -> Result<Envelope, ()> {
		(**self).recv().await
	}

	async fn send(&mut self, envelope: Envelope) -> Result<(), ()> {
		(**self).send(envelope).await
	}

	fn sender(&self) -> channel::Sender<Envelope> {
		(**self).sender()
	}

	fn spawn_sub(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		(**self).spawn_sub(sub)
	}

	fn spawn_sub_blocking(&mut self, sub: BoxFuture<'static, ExitStatus>) {
		(**self).spawn_sub_blocking(sub)
	}
}

#[async_trait::async_trait]
pub trait Process: Send + 'static {
	fn init(&mut self, state: impl Future<Output = ExitStatus> + Send + 'static) -> Result<(), ()>;

	fn initialized(&self) -> bool;

	fn start(&mut self, spawner: impl Spawner);

	async fn send(&self, msg: impl Into<Envelope> + Send) -> Result<(), ()>;

	async fn preempt(&mut self) -> Result<(), ()>;

	async fn kill(&mut self) -> Result<(), ()>;
}

pub trait Spawner: Clone + Send + Sync + 'static {
	/// Spawn the given blocking future.
	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
	/// Spawn the given non-blocking future.
	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
}
