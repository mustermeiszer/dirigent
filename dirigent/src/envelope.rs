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

use core::any::Any;
use std::any::TypeId;

type Arc<T> = std::sync::Arc<T>;

use crate::traits::Message;

#[derive(Debug)]
pub enum EnvelopeError {
	CouldNotCastToMessage,
}

#[derive(Clone, Debug)]
pub struct Envelope {
	inner: Arc<dyn Any + Send + Sync>,
}

unsafe impl Send for Envelope {}

impl<M: Message> From<M> for Envelope {
	fn from(message: M) -> Self {
		Envelope {
			inner: Arc::new(message),
		}
	}
}

impl Envelope {
	pub fn new<M: Message>(inner: M) -> Self {
		Envelope {
			inner: Arc::new(inner),
		}
	}

	pub fn message_id(&self) -> TypeId {
		self.inner.type_id()
	}

	pub fn try_read_ref<M: Message, F, T>(&self, f: F) -> Option<T>
	where
		F: FnOnce(&M) -> T,
	{
		self.inner_read_as_ref::<M>().map(|msg| f(msg)).ok()
	}

	pub fn try_read<M: Message + Clone, F, T>(&self, f: F) -> Option<T>
	where
		F: FnOnce(M) -> T,
	{
		self.inner_read_as_ref::<M>().map(|msg| f(msg.clone())).ok()
	}

	pub fn read_ref<M: Message>(&self) -> Result<&M, EnvelopeError> {
		self.inner_read_as_ref::<M>()
	}

	pub fn read<M: Message + Clone>(&self) -> Result<M, EnvelopeError> {
		self.inner_read_as_ref::<M>().map(|msg| msg.clone())
	}

	fn inner_read_as_ref<M: Message>(&self) -> Result<&M, EnvelopeError> {
		self.inner
			.downcast_ref::<M>()
			.map(|output| {
				<M as Message>::read(&output);

				output
			})
			.ok_or(EnvelopeError::CouldNotCastToMessage)
	}
}
