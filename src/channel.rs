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

use std::marker::PhantomData;

const DEFAULT_CHANNEL_SIZE: usize = 512;

pub fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
	channel_sized::<T, DEFAULT_CHANNEL_SIZE>()
}

pub fn channel_sized<T: Send, const SIZE: usize>() -> (Sender<T>, Receiver<T>) {
	let (inner_sender, inner_recv) = flume::bounded::<T>(SIZE);

	(
		Sender {
			inner: inner_sender,
		},
		Receiver { inner: inner_recv },
	)
}

#[derive(Clone)]
pub struct Sender<T: Send> {
	inner: flume::Sender<T>,
}

impl<T: Send> Sender<T> {
	pub async fn send(&self, t: impl Into<T> + Send) -> Result<(), ()> {
		self.inner.send_async(t.into()).await.map_err(|_| ())
	}

	pub async fn try_send(&self, t: impl Into<T> + Send) -> Result<(), ()> {
		self.inner.try_send(t.into()).map_err(|_| ())
	}
}

pub struct Receiver<T: Send> {
	inner: flume::Receiver<T>,
}

impl<T: Send> Receiver<T> {
	pub async fn recv(&self) -> Result<T, ()> {
		self.inner.recv_async().await.map_err(|_| ())
	}

	pub async fn try_recv(&self) -> Result<Option<T>, ()> {
		self.inner.try_recv().map_err(|_| ()).map(|t| Some(t))
	}
}
