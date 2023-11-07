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
	num::NonZeroUsize,
	ops::Add,
	time::{Duration, Instant},
};

use dirigent::traits::{Context, ExitStatus, IndexRegistry};
use futures::StreamExt;
use libp2p::{
	bytes::BufMut,
	identity, kad, noise,
	swarm::{Config, SwarmEvent},
	tcp, yamux, PeerId, Swarm, SwarmBuilder,
};

pub mod messages;

const BOOTNODES: [&str; 4] = [
	"QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
];

#[derive(Debug)]
pub struct Program {}

impl dirigent::traits::Program for Program {
	async fn start(
		self: Box<Self>,
		ctx: Box<dyn Context>,
		registry: Box<dyn IndexRegistry>,
	) -> ExitStatus {
		todo!()
	}
}

#[tokio::test]
async fn test() {
	let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key.clone())
		.with_tokio()
		.with_tcp(
			tcp::Config::default(),
			noise::Config::new,
			yamux::Config::default,
		)
		.unwrap()
		.with_dns()
		.unwrap()
		.with_behaviour(|key| {
			// Create a Kademlia behaviour.
			let mut cfg = kad::Config::default();
			cfg.set_query_timeout(Duration::from_secs(5 * 60));
			let store = kad::store::MemoryStore::new(key.public().to_peer_id());
			kad::Behaviour::with_config(key.public().to_peer_id(), store, cfg)
		})
		.unwrap()
		.with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(5)))
		.build();

	// Add the bootnodes to the local routing table. `libp2p-dns` built
	// into the `transport` resolves the `dnsaddr` when Kademlia tries
	// to dial these nodes.
	for peer in &BOOTNODES {
		swarm.behaviour_mut().add_address(
			&peer.parse().unwrap(),
			"/dnsaddr/bootstrap.libp2p.io".parse().unwrap(),
		);
	}

	loop {
		let event = swarm.select_next_some().await;

		match event {
			SwarmEvent::Behaviour(kad::Event::OutboundQueryProgressed {
				result: kad::QueryResult::GetClosestPeers(Ok(ok)),
				..
			}) => {
				// The example is considered failed as there
				// should always be at least 1 reachable peer.
				if ok.peers.is_empty() {
					unreachable!()
				}

				println!("Query finished with closest peers: {:?}", ok.peers);

				return;
			}
			SwarmEvent::Behaviour(kad::Event::OutboundQueryProgressed {
				result:
					kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
						..
					})),
				..
			}) => {
				unreachable!()
			}
			SwarmEvent::Behaviour(kad::Event::OutboundQueryProgressed {
				result: kad::QueryResult::PutRecord(Ok(_)),
				..
			}) => {
				println!("Successfully inserted the PK record");

				return;
			}
			SwarmEvent::Behaviour(kad::Event::OutboundQueryProgressed {
				result: kad::QueryResult::PutRecord(Err(err)),
				..
			}) => {
				unreachable!()
			}
			_ => {}
		}
	}
}
