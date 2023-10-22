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

use std::sync::Arc;

struct Inner<T> {
	safe: Arc<T>,
	raw: *const T,
}

impl<T> Inner<T> {
	fn new(t: T) -> Arc<Arc<Self>> {
		let arc = Arc::new(t);

		Arc::new(Arc::new(Inner {
			safe: arc.clone(),
			raw: Arc::into_raw(arc),
		}))
	}
}

impl<T> Drop for Inner<T> {
	fn drop(&mut self) {
		unsafe { drop(Arc::from_raw(self.raw)) }
	}
}

pub struct Updater<T> {
	safe: Arc<Arc<Inner<T>>>,
	raw: *const Arc<Inner<T>>,
}

impl<T> Updater<T> {
	// TODO: Check safety
	pub fn update(&mut self, t: T) {
		let inner = Inner::new(t);

		let _drop = Updater {
			raw: self.raw,
			safe: self.safe.clone(),
		};

		// NOTE ON SAFETY: This operation is safe
		//                 as this structure always contains a valid
		//                 'Arc' to the data structure.
		let mutable_raw = unsafe {
			let ptr = &mut *(self.raw as *mut Arc<Inner<T>>);
			ptr
		};

		let inner_ref = &(*inner);
		*mutable_raw = inner_ref.clone();

		self.safe = inner;
	}

	pub fn current(&self) -> Arc<T> {
		self.safe.safe.clone()
	}
}

impl<T> Drop for Updater<T> {
	fn drop(&mut self) {
		unsafe { drop(Arc::from_raw(self.raw)) }
	}
}

unsafe impl<P> Send for Updater<P> {}
unsafe impl<P> Sync for Updater<P> {}

pub struct Updatable<T>(Arc<Arc<Inner<T>>>);

impl<T> Clone for Updatable<T> {
	fn clone(&self) -> Self {
		Updatable(self.0.clone())
	}
}

impl<T> Updatable<T> {
	pub fn new(t: T) -> (Self, Updater<T>) {
		let arc = Arc::new(t);

		let inner = Arc::new(Arc::new(Inner {
			safe: arc.clone(),
			raw: Arc::into_raw(arc),
		}));

		let updatable = Updatable(inner.clone());
		let updater = Updater {
			safe: inner.clone(),
			raw: Arc::into_raw(inner),
		};

		(updatable, updater)
	}

	pub fn current(&self) -> Arc<T> {
		self.0.safe.clone()
	}
}

unsafe impl<P> Send for Updatable<P> {}
unsafe impl<P> Sync for Updatable<P> {}

#[cfg(test)]
mod test {
	#[test]
	fn dropping_works() {
		todo!()
	}
}
