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
use crate::{envelope::Envelope, traits::Index};

struct TestProgram;

struct NoIndex;
impl Index for NoIndex {
	fn indexed(t: &Envelope) -> bool {
		true
	}
}

#[async_trait::async_trait]
impl dirigent::traits::Program for TestProgram {
	type Consumes = NoIndex;

	async fn start<C: dirigent::traits::Context>(self, ctx: C) -> C::Process {
		todo!()
	}
}

#[test]
fn test_1() {}
