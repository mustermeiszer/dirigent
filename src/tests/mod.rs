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
use crate as dirigent;
use crate::{
	envelope::Envelope,
	traits::{Index, Process},
};

struct AllIndexed;
impl Index for AllIndexed {
	fn indexed(t: &Envelope) -> bool {
		true
	}
}

struct TestProgram1;
#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram1 {
	type Consumes = AllIndexed;

	async fn start<C: dirigent::traits::Context>(self, mut ctx: C) -> C::Process {
		let mut p = ctx.create_process();

		let fut = async move { println!("Hello, World from Test Programm 1!") };

		p.init(fut);
		p
	}
}

struct TestProgram2;
#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram2 {
	type Consumes = AllIndexed;

	async fn start<C: dirigent::traits::Context>(self, mut ctx: C) -> C::Process {
		let mut p = ctx.create_process();

		let fut = async move { println!("Hello, World from Test Programm 1!") };

		p.init(fut);
		p
	}
}

#[test]
fn test_1() {}
