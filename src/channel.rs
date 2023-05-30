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

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
	let (inner_sender, inner_recv) = std::sync::mpsc::channel::<T>();

	(
		Sender {
			inner: inner_sender,
		},
		Receiver { inner: inner_recv },
	)
}

pub struct Sender<T> {
	inner: std::sync::mpsc::Sender<T>,
}

impl<T> Clone for Sender<T> {
	fn clone(&self) -> Self {
		Sender {
			inner: self.inner.clone(),
		}
	}
}

impl<T> Sender<T> {
	pub async fn send(&self, t: impl Into<T>) -> Result<(), ()> {
		self.inner.send(t.into()).map_err(|_| ())
	}
}

pub struct Receiver<T> {
	inner: std::sync::mpsc::Receiver<T>,
}

impl<T> Receiver<T> {
	pub async fn recv(&self) -> Result<T, ()> {
		self.inner.recv().map_err(|_| ())
	}

	pub async fn try_recv(&self) -> Result<Option<T>, ()> {
		match self.inner.try_recv() {
			Ok(t) => Ok(Some(t)),
			Err(e) => match e {
				std::sync::mpsc::TryRecvError::Empty => Ok(None),
				std::sync::mpsc::TryRecvError::Disconnected => Err(()),
			},
		}
	}
}
