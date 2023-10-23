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

use std::sync::{
	atomic::{AtomicBool, Ordering},
	Arc,
};

struct Inner<T> {
	safe: Arc<T>,
	raw: *const T,
}

impl<T> Inner<T> {
	fn new(t: T) -> Self {
		let arc = Arc::new(t);

		Inner {
			safe: arc.clone(),
			raw: Arc::into_raw(arc),
		}
	}
}

impl<T> Drop for Inner<T> {
	fn drop(&mut self) {
		unsafe { drop(Arc::from_raw(self.raw)) }
	}
}

pub struct Updater<T> {
	raw: *const Inner<T>,
	dropped: Arc<AtomicBool>,
}

impl<T> Updater<T> {
	pub fn update(&mut self, t: T) {
		let inner = Inner::new(t);

		// NOTE ON SAFETY: This operation is safe
		let mutable_raw = unsafe {
			let ptr = &mut *(self.raw as *mut Inner<T>);
			ptr
		};

		*mutable_raw = inner;
	}

	pub fn current(&self) -> Arc<T> {
		let reference = unsafe { &*self.raw };

		reference.safe.clone()
	}
}

impl<T> Drop for Updater<T> {
	fn drop(&mut self) {
		if self.dropped.load(Ordering::SeqCst) {
			unsafe { drop(Arc::from_raw(self.raw)) }
		} else {
			self.dropped.store(true, Ordering::SeqCst)
		}
	}
}

unsafe impl<P> Send for Updater<P> {}
unsafe impl<P> Sync for Updater<P> {}

pub struct Updatable<T> {
	raw: *const Inner<T>,
	dropped: Arc<AtomicBool>,
}

impl<T> Updatable<T> {
	pub fn new(t: T) -> (Self, Updater<T>) {
		let drop = Arc::new(AtomicBool::new(false));
		let raw = Arc::into_raw(Arc::new(Inner::new(t)));
		let updatable = Updatable {
			raw,
			dropped: drop.clone(),
		};
		let updater = Updater { raw, dropped: drop };

		(updatable, updater)
	}

	pub fn current(&self) -> Arc<T> {
		let reference = unsafe { &*self.raw };

		reference.safe.clone()
	}
}

unsafe impl<P> Send for Updatable<P> {}
unsafe impl<P> Sync for Updatable<P> {}

impl<T> Drop for Updatable<T> {
	fn drop(&mut self) {
		if self.dropped.load(Ordering::SeqCst) {
			unsafe { drop(Arc::from_raw(self.raw)) }
		} else {
			self.dropped.store(true, Ordering::SeqCst)
		}
	}
}

#[cfg(test)]
mod test {
	use std::thread;

	use crate::updatable::Updatable;

	#[test]
	fn it_works() {
		let first = vec![1u8, 2u8, 3u8];
		let second = vec![4u8, 5u8, 6u8];

		let (updatable, mut updater) = Updatable::new(first.clone());

		let first_updatable_ref = updatable.current();
		let first_updater_ref = updater.current();
		assert_eq!(first_updatable_ref.as_ref(), &first);
		assert_eq!(first_updater_ref.as_ref(), &first);

		updater.update(second.clone());

		let second_updatable_ref = updatable.current();
		let second_updater_ref = updater.current();

		assert_eq!(first_updatable_ref.as_ref(), &first);
		assert_eq!(first_updater_ref.as_ref(), &first);
		assert_eq!(second_updatable_ref.as_ref(), &second);
		assert_eq!(second_updater_ref.as_ref(), &second);

		drop(updater);
		let second_updater_ref = updatable.current();
		drop(updatable);

		assert_eq!(first_updatable_ref.as_ref(), &first);
		assert_eq!(first_updater_ref.as_ref(), &first);
		assert_eq!(second_updatable_ref.as_ref(), &second);
		assert_eq!(second_updater_ref.as_ref(), &second);
	}

	#[test]
	fn thread_update() {
		let first = vec![1u8, 2u8, 3u8];

		let (updatable, mut updater) = Updatable::new(first.clone());

		assert_eq!(updatable.current().as_ref(), &first);
		assert_eq!(updater.current().as_ref(), &first);

		let handle = thread::spawn(move || {
			let second = vec![4u8, 5u8, 6u8];
			updater.update(second);
		});

		handle.join().unwrap();

		let second = vec![4u8, 5u8, 6u8];
		assert_eq!(updatable.current().as_ref(), &second);
	}

	#[test]
	fn thread_read() {
		let first = vec![1u8, 2u8, 3u8];
		let second = vec![4u8, 5u8, 6u8];

		let (updatable, mut updater) = Updatable::new(first.clone());

		assert_eq!(updatable.current().as_ref(), &first);
		assert_eq!(updater.current().as_ref(), &first);
		updater.update(second);

		let handle = thread::spawn(move || {
			let second = vec![4u8, 5u8, 6u8];
			assert_eq!(updatable.current().as_ref(), &second);
		});

		handle.join().unwrap();
	}
}
