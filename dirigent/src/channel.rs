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

const DEFAULT_CHANNEL_SIZE: usize = 512;

#[derive(thiserror::Error, Debug)]
pub enum SendError<T> {
	#[error("Sending failed. Channel is closed")]
	Closed(T),
	#[error("Sending failed. Channel is full")]
	Full(T),
}

#[derive(Debug)]
pub enum RecvError {
	Closed,
}

pub mod mpsc {
	use super::DEFAULT_CHANNEL_SIZE;
	use crate::channel::{RecvError, SendError};

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

	#[derive(Debug)]
	pub struct Sender<T: Send> {
		inner: flume::Sender<T>,
	}

	// NOTE: Custom impl to prevent enforcing
	//       Clone on T.
	impl<T: Send> Clone for Sender<T> {
		fn clone(&self) -> Self {
			Sender {
				inner: self.inner.clone(),
			}
		}
	}

	impl<T: Send> Sender<T> {
		pub async fn send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner
				.send_async(t.into())
				.await
				.map_err(|e| SendError::Closed(e.0))
		}

		pub fn try_send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner.try_send(t.into()).map_err(|e| match e {
				flume::TrySendError::Full(t) => SendError::Full(t),
				flume::TrySendError::Disconnected(t) => SendError::Closed(t),
			})
		}
	}

	#[derive(Debug)]
	pub struct Receiver<T: Send> {
		inner: flume::Receiver<T>,
	}

	impl<T: Send> Receiver<T> {
		pub async fn recv(&self) -> Result<T, RecvError> {
			self.inner.recv_async().await.map_err(|e| match e {
				flume::RecvError::Disconnected => RecvError::Closed,
			})
		}

		pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
			match self.inner.try_recv() {
				Ok(t) => Ok(Some(t)),
				Err(e) => match e {
					flume::TryRecvError::Disconnected => Err(RecvError::Closed),
					flume::TryRecvError::Empty => Ok(None),
				},
			}
		}
	}
}

pub mod mpmc {
	use std::fmt::{Debug, Formatter};

	use crossfire::mpmc::SharedFutureBoth;

	use super::DEFAULT_CHANNEL_SIZE;
	use crate::channel::{RecvError, SendError};

	pub fn channel<T: Send + Unpin>() -> (Sender<T>, Receiver<T>) {
		channel_sized::<T, DEFAULT_CHANNEL_SIZE>()
	}

	pub fn channel_sized<T: Send + Unpin, const SIZE: usize>() -> (Sender<T>, Receiver<T>) {
		let (inner_sender, inner_recv) = crossfire::mpmc::bounded_future_both::<T>(SIZE);

		(
			Sender {
				inner: inner_sender,
			},
			Receiver { inner: inner_recv },
		)
	}

	pub struct Sender<T: Send> {
		inner: crossfire::mpmc::TxFuture<T, SharedFutureBoth>,
	}

	impl<T: Send> Debug for Sender<T> {
		fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
			f.debug_struct("mpmc::Sender").finish()
		}
	}

	// NOTE: Custom impl to prevent enforcing
	//       Clone on T.
	impl<T: Send> Clone for Sender<T> {
		fn clone(&self) -> Self {
			Sender {
				inner: self.inner.clone(),
			}
		}
	}

	impl<T: Send + Unpin> Sender<T> {
		pub async fn send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner.send(t.into()).await.map_err(|e| match e {
				crossfire::mpmc::SendError(t) => SendError::Closed(t),
			})
		}

		pub fn try_send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner.try_send(t.into()).map_err(|e| match e {
				crossfire::mpmc::TrySendError::Full(t) => SendError::Full(t),
				crossfire::mpmc::TrySendError::Disconnected(t) => SendError::Closed(t),
			})
		}
	}

	pub struct Receiver<T: Send> {
		inner: crossfire::mpmc::RxFuture<T, SharedFutureBoth>,
	}

	impl<T: Send> Debug for Receiver<T> {
		fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
			f.debug_struct("mpmc::Receiver").finish()
		}
	}

	// NOTE: Custom impl to prevent enforcing
	//       Clone on T.
	impl<T: Send> Clone for Receiver<T> {
		fn clone(&self) -> Self {
			Receiver {
				inner: self.inner.clone(),
			}
		}
	}

	impl<T: Send> Receiver<T> {
		pub async fn recv(&self) -> Result<T, RecvError> {
			self.inner.recv().await.map_err(|e| match e {
				crossfire::mpmc::RecvError => RecvError::Closed,
			})
		}

		pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
			match self.inner.try_recv() {
				Ok(t) => Ok(Some(t)),
				Err(e) => match e {
					crossfire::mpmc::TryRecvError::Disconnected => Err(RecvError::Closed),
					crossfire::mpmc::TryRecvError::Empty => Ok(None),
				},
			}
		}
	}
}

pub mod spmc {
	use std::fmt::{Debug, Formatter};

	use crossfire::mpmc::SharedFutureBoth;

	use super::DEFAULT_CHANNEL_SIZE;
	use crate::channel::{RecvError, SendError};

	pub fn channel<T: Send + Unpin>() -> (Sender<T>, Receiver<T>) {
		channel_sized::<T, DEFAULT_CHANNEL_SIZE>()
	}

	pub fn channel_sized<T: Send + Unpin, const SIZE: usize>() -> (Sender<T>, Receiver<T>) {
		let (inner_sender, inner_recv) = crossfire::mpmc::bounded_future_both::<T>(SIZE);

		(
			Sender {
				inner: inner_sender,
			},
			Receiver { inner: inner_recv },
		)
	}

	pub struct Sender<T: Send> {
		inner: crossfire::mpmc::TxFuture<T, SharedFutureBoth>,
	}

	impl<T: Send> Debug for Sender<T> {
		fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
			f.debug_struct("spmc::Sender").finish()
		}
	}

	impl<T: Send + Unpin> Sender<T> {
		pub async fn send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner.send(t.into()).await.map_err(|e| match e {
				crossfire::mpmc::SendError(t) => SendError::Closed(t),
			})
		}

		pub fn try_send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner.try_send(t.into()).map_err(|e| match e {
				crossfire::mpmc::TrySendError::Full(t) => SendError::Full(t),
				crossfire::mpmc::TrySendError::Disconnected(t) => SendError::Closed(t),
			})
		}
	}

	pub struct Receiver<T: Send> {
		inner: crossfire::mpmc::RxFuture<T, SharedFutureBoth>,
	}

	impl<T: Send> Debug for Receiver<T> {
		fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
			f.debug_struct("spmc::Receiver").finish()
		}
	}

	// NOTE: Custom impl to prevent enforcing
	//       Clone on T.
	impl<T: Send> Clone for Receiver<T> {
		fn clone(&self) -> Self {
			Receiver {
				inner: self.inner.clone(),
			}
		}
	}

	impl<T: Send> Receiver<T> {
		pub async fn recv(&self) -> Result<T, RecvError> {
			self.inner.recv().await.map_err(|e| match e {
				crossfire::mpmc::RecvError => RecvError::Closed,
			})
		}

		pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
			match self.inner.try_recv() {
				Ok(t) => Ok(Some(t)),
				Err(e) => match e {
					crossfire::mpmc::TryRecvError::Disconnected => Err(RecvError::Closed),
					crossfire::mpmc::TryRecvError::Empty => Ok(None),
				},
			}
		}
	}
}

pub mod spsc {
	use super::DEFAULT_CHANNEL_SIZE;
	use crate::channel::{RecvError, SendError};

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

	#[derive(Debug)]
	pub struct Sender<T: Send> {
		inner: flume::Sender<T>,
	}

	impl<T: Send> Sender<T> {
		pub async fn send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner
				.send_async(t.into())
				.await
				.map_err(|e| SendError::Closed(e.0))
		}

		pub fn try_send(&self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner.try_send(t.into()).map_err(|e| match e {
				flume::TrySendError::Full(t) => SendError::Full(t),
				flume::TrySendError::Disconnected(t) => SendError::Closed(t),
			})
		}
	}

	#[derive(Debug)]
	pub struct Receiver<T: Send> {
		inner: flume::Receiver<T>,
	}

	impl<T: Send> Receiver<T> {
		pub async fn recv(&self) -> Result<T, RecvError> {
			self.inner.recv_async().await.map_err(|e| match e {
				flume::RecvError::Disconnected => RecvError::Closed,
			})
		}

		pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
			match self.inner.try_recv() {
				Ok(t) => Ok(Some(t)),
				Err(e) => match e {
					flume::TryRecvError::Disconnected => Err(RecvError::Closed),
					flume::TryRecvError::Empty => Ok(None),
				},
			}
		}
	}
}

pub mod oneshot {
	use crate::channel::{RecvError, SendError};

	#[derive(Debug)]
	pub enum TryRecvOk<T, Recv> {
		Value(T),
		Recv(Recv),
	}

	pub fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
		channel_sized::<T, 1>()
	}

	fn channel_sized<T: Send, const SIZE: usize>() -> (Sender<T>, Receiver<T>) {
		let (inner_sender, inner_recv) = flume::bounded::<T>(SIZE);

		(
			Sender {
				inner: inner_sender,
			},
			Receiver { inner: inner_recv },
		)
	}

	#[derive(Debug)]
	pub struct Sender<T: Send> {
		inner: flume::Sender<T>,
	}

	impl<T: Send> Sender<T> {
		pub async fn send(self, t: impl Into<T> + Send) -> Result<(), SendError<T>> {
			self.inner
				.send_async(t.into())
				.await
				.map_err(|e| SendError::Closed(e.0))
		}

		pub fn try_send(self, t: impl Into<T> + Send) -> Result<(), SendError<(Self, T)>> {
			self.inner.try_send(t.into()).map_err(|e| match e {
				flume::TrySendError::Full(t) => SendError::Full((self, t)),
				flume::TrySendError::Disconnected(t) => SendError::Closed((self, t)),
			})
		}
	}

	#[derive(Debug)]
	pub struct Receiver<T: Send> {
		inner: flume::Receiver<T>,
	}

	impl<T: Send> Receiver<T> {
		pub async fn recv(self) -> Result<T, RecvError> {
			self.inner.recv_async().await.map_err(|e| match e {
				flume::RecvError::Disconnected => RecvError::Closed,
			})
		}

		pub fn try_recv(self) -> Result<TryRecvOk<T, Self>, RecvError> {
			match self.inner.try_recv() {
				Ok(t) => Ok(TryRecvOk::Value(t)),
				Err(e) => match e {
					flume::TryRecvError::Disconnected => Err(RecvError::Closed),
					flume::TryRecvError::Empty => Ok(TryRecvOk::Recv(self)),
				},
			}
		}
	}
}
