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

use core::{any::Any, cmp::Ordering};
use std::{any::TypeId, sync::Mutex};

// TODO: This makes this crate wasm incompatible
type Arc<T> = std::sync::Arc<T>;

use crate::{channel, traits, traits::Message};

pub enum ResponseError {
	TooManyResponses,
	ResponseReceiverDropped,
}

#[derive(Clone)]
struct Responder<M: Message> {
	sender: channel::Sender<M::Response>,
	answers: Arc<Mutex<u32>>,
	min_answers: u32,
	max_answers: u32,
}

unsafe impl<M: Message + Send> Send for Responder<M> {}

impl<M: Message> Responder<M> {
	pub async fn respond(&self, with: M::Response) -> Result<(), ResponseError> {
		// TODO: Handle unwrap()
		let mut locked_counter = self.answers.lock().unwrap();
		match self.max_answers.cmp(&(*locked_counter + 1)) {
			Ordering::Greater => Err(ResponseError::TooManyResponses),
			Ordering::Equal | Ordering::Less => {
				self.sender
					.send(with)
					.await
					.map_err(|_| ResponseError::ResponseReceiverDropped)?;
				*locked_counter += 1;
				Ok(())
			}
		}
	}
}

pub struct Letter<M: Message> {
	message: M,
	responder: Option<Responder<M>>,
}

unsafe impl<M: Message + Send> Send for Letter<M> {}

impl<M: Message> From<M> for Letter<M> {
	fn from(message: M) -> Self {
		Letter {
			message,
			responder: None,
		}
	}
}

impl<M: Message> Letter<M> {
	pub fn new(message: M) -> Self {
		Letter {
			message,
			responder: None,
		}
	}

	pub fn expect_response(&mut self) -> channel::Receiver<M::Response> {
		self.expect_responses(1, 1)
	}

	pub fn expect_responses(&mut self, min: u32, max: u32) -> channel::Receiver<M::Response> {
		let (sender, receiver) = channel::channel::<M::Response>();

		self.responder = Some(Responder {
			sender,
			answers: Arc::new(Mutex::new(0)),
			min_answers: min,
			max_answers: max,
		});

		receiver
	}

	pub fn seal(self) -> Envelope {
		Envelope {
			inner: Arc::new(Box::new(self)),
			read: Arc::new(Mutex::new(false)),
		}
	}

	pub fn message(&self) -> &M {
		&self.message
	}
}

pub enum EnvelopeError {
	Response(ResponseError),
	WrongResponse,
	NoResponseExpected,
	MessageNotExpected,
}

impl From<ResponseError> for EnvelopeError {
	fn from(value: ResponseError) -> Self {
		EnvelopeError::Response(value)
	}
}

#[derive(Clone)]
pub struct Envelope {
	inner: Arc<Box<dyn Any + Send + Sync>>,
	read: Arc<Mutex<bool>>,
}

unsafe impl Send for Envelope {}

impl<M: Message> From<M> for Envelope {
	fn from(message: M) -> Self {
		Envelope {
			inner: Arc::new(Box::new(Letter::new(message))),
			read: Arc::new(Mutex::new(false)),
		}
	}
}

impl Envelope {
	pub fn message_id(&self) -> TypeId {
		self.inner.type_id()
	}

	pub fn read_ref<M: Message>(&self) -> Result<&M, EnvelopeError> {
		self.read_as_letter_ref::<M>()
			.map(|msg_ref| &msg_ref.message)
	}

	pub fn read<M: Message>(&self) -> Result<M, EnvelopeError> {
		self.read_as_letter_ref::<M>()
			.map(|msg_ref| msg_ref.message.clone())
	}

	fn read_as_letter_ref<M: Message>(&self) -> Result<&Letter<M>, EnvelopeError> {
		self.inner
			.downcast_ref::<Letter<M>>()
			.map(|output| {
				// TODO: Handle unwrap
				let mut l = self.read.lock().map_err(|_| ()).unwrap();
				*l = true;

				output
			})
			.ok_or(EnvelopeError::MessageNotExpected)
	}

	pub async fn answer<M: Message>(&mut self, msg: M::Response) -> Result<(), EnvelopeError> {
		let letter = self.read_as_letter_ref::<M>()?;

		if let Some(response) = &letter.responder {
			response.respond(msg).await.map_err(|e| e.into())
		} else {
			Err(EnvelopeError::NoResponseExpected)
		}
	}
}
