//! Lattice stream-anchor / NET protocol opcodes.
//!
//! Mirror of the foundation/ip stream NET surface (see
//! `target/fluxor/fluxor-abi/sdk/contracts/net/net_proto.rs`). Every TCP
//! edge anchor (etcd / redis / memcached_stream) speaks this same
//! 8-opcode handshake to its NET provider, so the constants live in
//! one place and the anchors `#[path]`-mount this file the same way
//! they mount `wire.rs` / `types.rs`.
//!
//! Wire framing on the channel is the standard
//! `[msg_type:u8][len:u16 LE][payload…]` TLV per `wire.rs`.
//!
//! ## Provider → anchor (`NET_MSG_*`)
//!
//! - `ACCEPTED` — new TCP connection. Payload: `[conn_id:u8]`.
//! - `DATA` — bytes from a connection. Payload: `[conn_id:u8][bytes…]`.
//! - `CLOSED` — peer closed or provider tore down. Payload: `[conn_id:u8]`.
//! - `BOUND` — listen socket established. Payload: `[port:u16 LE]`.
//! - `CONNOK` — an outbound dial completed. Payload: `[conn_id:u8]`.
//! - `ERROR` — listen / accept / read failure. Payload: `[errno:i8]`.
//!
//! ## Anchor → provider (`NET_CMD_*`)
//!
//! - `BIND` — open the listen socket. Payload: `[port:u16 LE]`.
//! - `SEND` — write bytes to a connection. Payload: `[conn_id:u8][bytes…]`.
//! - `CLOSE` — drop a connection. Payload: `[conn_id:u8]`.
//! - `CONNECT` — dial out. Payload: `[sock_type:u8][ip:u32 LE][port:u16 LE]`.

pub const NET_MSG_ACCEPTED: u8 = 0x01;
pub const NET_MSG_DATA: u8 = 0x02;
pub const NET_MSG_CLOSED: u8 = 0x03;
pub const NET_MSG_BOUND: u8 = 0x04;
pub const NET_MSG_CONNOK: u8 = 0x05;
pub const NET_MSG_ERROR: u8 = 0x06;

pub const NET_CMD_BIND: u8 = 0x10;
pub const NET_CMD_SEND: u8 = 0x11;
pub const NET_CMD_CLOSE: u8 = 0x12;
/// Dial an outbound connection. Every anchor in this repo binds and
/// accepts; `lattice_data_client` is the first that dials, because its
/// peer is a storage graph rather than a client.
pub const NET_CMD_CONNECT: u8 = 0x13;

/// `sock_type` for a TCP stream in [`NET_CMD_CONNECT`]'s payload.
pub const NET_SOCK_STREAM: u8 = 1;
