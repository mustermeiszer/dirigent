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

use crate::{channel, envelope, envelope::Envelope};

pub trait Message: Clone + Send + 'static {
	type Response;
}

pub trait Index {
	fn indexed(t: &Envelope) -> bool;
}

pub trait LookUp {
	type Output;

	fn look_up(t: &Envelope) -> Result<Self::Output, ()>;
}

pub trait Program: 'static + Sized + Send {
	type Process: Process<Self>;
	type Consumes: Index;

	async fn start(self, ctx: impl Context<Self>) -> Self::Process;

	fn preemption() -> bool {
		true
	}
}

pub trait Context<P: Program> {
	type Process: Process<P>;

	fn create_process(&mut self) -> Self::Process;

	async fn try_recv(&mut self) -> Result<Option<Envelope>, ()>;

	async fn recv(&mut self) -> Result<Envelope, ()>;

	async fn send(&mut self, envelope: impl Into<Envelope>) -> Result<(), ()>;

	fn sender(&self) -> channel::Sender<Envelope>;

	async fn spawn_sub(&mut self, sub: impl Future<Output = ExitStatus>);

	async fn spawn_sub_blocking(&mut self, sub: impl Future<Output = ExitStatus>);
}

pub enum ExitStatus {
	Error,
	Finished,
	Interrupted,
}

pub trait Process<P: Program>: Send + 'static {
	fn init(&mut self, state: Box<dyn Future<Output = ExitStatus> + Send>) -> Result<(), ()>;

	fn initialized(&self) -> bool;

	fn start(&mut self, spawner: impl Spawner);

	async fn send(&self, msg: impl Into<Envelope>) -> Result<(), ()>;

	async fn preempt(&mut self) -> Result<(), ()>;

	async fn kill(&mut self) -> Result<(), ()>;
}

pub trait Spawner: Clone + Send + Sync {
	/// Spawn the given blocking future.
	fn spawn_blocking(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
	/// Spawn the given non-blocking future.
	fn spawn(&self, future: impl Future<Output = ExitStatus> + Send + 'static);
}
