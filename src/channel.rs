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

const DEFAULT_CHANNEL_SIZE: usize = 2048;

pub fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
	channel_sized::<T, DEFAULT_CHANNEL_SIZE>()
}

pub fn channel_sized<T: Send, const SIZE: usize>() -> (Sender<T>, Receiver<T>) {
	/*
	let (inner_sender, inner_recv) = futures::channel::mpsc::channel::<T>(SIZE);

	(
		Sender {
			inner: inner_sender,
		},
		Receiver { inner: inner_recv },
	)

	 */
	todo!()
}

pub struct Sender<T: Send> {
	//inner: futures::channel::mpsc::Sender<T>,
	//inner: std::sync::mpsc::Sender<T>,
	_phantom: PhantomData<T>,
}

impl<T: Send> Clone for Sender<T> {
	fn clone(&self) -> Self {
		todo!()
	}
}

impl<T: Send> Sender<T> {
	pub async fn send(&self, t: impl Into<T>) -> Result<(), ()> {
		todo!()
	}
}

pub struct Receiver<T> {
	inner: futures::channel::mpsc::Receiver<T>,
}

impl<T> Receiver<T> {
	pub async fn recv(&self) -> Result<T, ()> {
		todo!()
	}

	pub async fn try_recv(&self) -> Result<Option<T>, ()> {
		todo!()
	}
}
