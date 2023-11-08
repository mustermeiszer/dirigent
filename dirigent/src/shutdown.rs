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

use std::{
	future::Future,
	pin::Pin,
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
	task::{Context, Poll},
};

use futures::{future::FusedFuture, task::AtomicWaker};

use crate::traits::ExecuteOnDrop;

struct Inner {
	waker: AtomicWaker,
	is_shutdown: AtomicBool,
}

pub struct Handle {
	inner: Arc<Inner>,
}

impl ExecuteOnDrop for Handle {
	fn execute(&mut self) {
		self.inner.is_shutdown.store(true, Ordering::Relaxed);
		self.inner.waker.wake()
	}
}

impl Handle {
	pub fn shutdown(self) {
		self.inner.is_shutdown.store(true, Ordering::SeqCst);
		self.inner.waker.wake()
	}
}

pub struct Shutdown {
	inner: Arc<Inner>,
}

impl Future for Shutdown {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		self.inner.waker.register(cx.waker());

		if self.inner.is_shutdown.load(Ordering::SeqCst) {
			Poll::Ready(())
		} else {
			Poll::Pending
		}
	}
}

impl FusedFuture for Shutdown {
	fn is_terminated(&self) -> bool {
		// NOTE: Polling the Shutdown again is not a problem. At
		//       all times the Shutdown will return the right result
		false
	}
}

impl Shutdown {
	pub fn new() -> (Self, Handle) {
		let inner = Arc::new(Inner {
			waker: AtomicWaker::new(),
			is_shutdown: AtomicBool::new(false),
		});

		(
			Shutdown {
				inner: inner.clone(),
			},
			Handle { inner },
		)
	}
}
