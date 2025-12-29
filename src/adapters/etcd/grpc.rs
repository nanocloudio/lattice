//! gRPC server implementation for etcd v3 API.
//!
//! This module provides the tonic gRPC server that serves etcd requests.
//! It uses the proto module for wire-format encoding compatible with etcdctl.

use crate::core::error::{LatticeError, LatticeResult};
use crate::kpg::state_machine::KvStateMachine;
use bytes::{BufMut, Bytes, BytesMut};
use http_body_util::BodyExt;
use parking_lot::RwLock;
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tonic::codegen::http::{header, StatusCode};
use tonic::Status;

use super::proto;

/// Shared state for the gRPC services.
#[derive(Clone)]
pub struct SharedState {
    /// KV state machine.
    pub state_machine: Arc<RwLock<KvStateMachine>>,
    /// Cluster ID for response headers.
    pub cluster_id: u64,
    /// Member ID for response headers.
    pub member_id: u64,
    /// Shutdown signal receiver.
    pub shutdown_rx: watch::Receiver<bool>,
}

impl SharedState {
    /// Create a new shared state.
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            state_machine: Arc::new(RwLock::new(KvStateMachine::new())),
            cluster_id: 1,
            member_id: 1,
            shutdown_rx,
        }
    }

    /// Create a response header with current revision.
    pub fn response_header(&self) -> proto::ResponseHeader {
        let sm = self.state_machine.read();
        proto::ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: sm.current_revision() as i64,
            raft_term: 1,
        }
    }

    /// Get current revision.
    pub fn current_revision(&self) -> i64 {
        self.state_machine.read().current_revision() as i64
    }
}

/// KV service implementation.
#[derive(Clone)]
pub struct KvService {
    state: SharedState,
}

impl KvService {
    /// Create a new KV service.
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }

    /// Handle Range (Get) request.
    pub fn range(&self, req: proto::RangeRequest) -> LatticeResult<proto::RangeResponse> {
        let sm = self.state.state_machine.read();

        let kvs = if req.range_end.is_empty() {
            // Single key lookup
            sm.get(&req.key)
                .map(|record| {
                    vec![proto::KeyValue {
                        key: record.key.clone(),
                        value: record.value.clone(),
                        create_revision: record.create_revision as i64,
                        mod_revision: record.mod_revision as i64,
                        version: record.version as i64,
                        lease: record.lease_id.unwrap_or(0),
                    }]
                })
                .unwrap_or_default()
        } else {
            // Range query
            let range_end = if req.range_end == vec![0u8] {
                None // All keys
            } else {
                Some(req.range_end.as_slice())
            };
            sm.range(&req.key, range_end)
                .into_iter()
                .map(|record| proto::KeyValue {
                    key: record.key.clone(),
                    value: record.value.clone(),
                    create_revision: record.create_revision as i64,
                    mod_revision: record.mod_revision as i64,
                    version: record.version as i64,
                    lease: record.lease_id.unwrap_or(0),
                })
                .collect()
        };

        let count = kvs.len() as i64;

        Ok(proto::RangeResponse {
            header: Some(self.state.response_header()),
            kvs,
            count,
            more: false,
        })
    }

    /// Handle Put request.
    pub fn put(&self, req: proto::PutRequest) -> LatticeResult<proto::PutResponse> {
        // Capture final revision after mutation
        let (prev_kv, final_revision) = {
            let mut sm = self.state.state_machine.write();

            // Get previous value if requested
            let prev_kv = if req.prev_kv {
                sm.get(&req.key).map(|record| proto::KeyValue {
                    key: record.key.clone(),
                    value: record.value.clone(),
                    create_revision: record.create_revision as i64,
                    mod_revision: record.mod_revision as i64,
                    version: record.version as i64,
                    lease: record.lease_id.unwrap_or(0),
                })
            } else {
                None
            };

            // Perform the put
            let revision = sm.current_revision() + 1;
            let lease_id = if req.lease != 0 {
                Some(req.lease)
            } else {
                None
            };
            sm.put(req.key, req.value, revision, lease_id);

            // Capture final revision before releasing lock
            (prev_kv, sm.current_revision() as i64)
        }; // Write lock released here

        Ok(proto::PutResponse {
            header: Some(proto::ResponseHeader {
                cluster_id: self.state.cluster_id,
                member_id: self.state.member_id,
                revision: final_revision,
                raft_term: 1,
            }),
            prev_kv,
        })
    }

    /// Handle DeleteRange request.
    pub fn delete_range(
        &self,
        req: proto::DeleteRangeRequest,
    ) -> LatticeResult<proto::DeleteRangeResponse> {
        // Capture values while holding write lock
        let (prev_kvs, deleted, final_revision) = {
            let mut sm = self.state.state_machine.write();

            // Get previous values if requested
            let prev_kvs: Vec<proto::KeyValue> = if req.prev_kv {
                if req.range_end.is_empty() {
                    sm.get(&req.key)
                        .map(|record| {
                            vec![proto::KeyValue {
                                key: record.key.clone(),
                                value: record.value.clone(),
                                create_revision: record.create_revision as i64,
                                mod_revision: record.mod_revision as i64,
                                version: record.version as i64,
                                lease: record.lease_id.unwrap_or(0),
                            }]
                        })
                        .unwrap_or_default()
                } else {
                    let range_end = if req.range_end == vec![0u8] {
                        None
                    } else {
                        Some(req.range_end.as_slice())
                    };
                    sm.range(&req.key, range_end)
                        .into_iter()
                        .map(|record| proto::KeyValue {
                            key: record.key.clone(),
                            value: record.value.clone(),
                            create_revision: record.create_revision as i64,
                            mod_revision: record.mod_revision as i64,
                            version: record.version as i64,
                            lease: record.lease_id.unwrap_or(0),
                        })
                        .collect()
                }
            } else {
                vec![]
            };

            // Perform the delete
            let revision = sm.current_revision() + 1;
            let deleted = if req.range_end.is_empty() {
                if sm.delete(&req.key, revision).is_some() {
                    1
                } else {
                    0
                }
            } else {
                let range_end = if req.range_end == vec![0u8] {
                    None
                } else {
                    Some(req.range_end.as_slice())
                };
                sm.delete_range(&req.key, range_end, revision).len() as i64
            };

            // Capture final revision
            (prev_kvs, deleted, sm.current_revision() as i64)
        }; // Write lock released here

        Ok(proto::DeleteRangeResponse {
            header: Some(proto::ResponseHeader {
                cluster_id: self.state.cluster_id,
                member_id: self.state.member_id,
                revision: final_revision,
                raft_term: 1,
            }),
            deleted,
            prev_kvs: if req.prev_kv { prev_kvs } else { vec![] },
        })
    }
}

/// Lease service implementation.
#[derive(Clone)]
pub struct LeaseService {
    #[allow(dead_code)]
    state: SharedState,
    /// Next lease ID.
    #[allow(dead_code)]
    next_lease_id: Arc<std::sync::atomic::AtomicI64>,
    /// Active leases.
    #[allow(dead_code)]
    leases: Arc<RwLock<std::collections::HashMap<i64, LeaseInfo>>>,
}

/// Lease information.
#[allow(dead_code)]
struct LeaseInfo {
    id: i64,
    ttl: i64,
    granted_ttl: i64,
    keys: Vec<Vec<u8>>,
}

impl LeaseService {
    /// Create a new Lease service.
    pub fn new(state: SharedState) -> Self {
        Self {
            state,
            next_lease_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
            leases: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

/// Convert a LatticeError to a tonic Status.
pub fn lattice_error_to_status(e: LatticeError) -> Status {
    match e {
        LatticeError::InvalidRequest { message } => Status::invalid_argument(message),
        LatticeError::DirtyEpoch { .. } => Status::failed_precondition(format!("{}", e)),
        LatticeError::LinearizabilityUnavailable { .. } => Status::unavailable(format!("{}", e)),
        LatticeError::ThrottleEnvelope { message } => Status::resource_exhausted(message),
        LatticeError::PermissionDenied { message } => Status::permission_denied(message),
        LatticeError::Internal { message } => Status::internal(message),
        _ => Status::internal(format!("{}", e)),
    }
}

/// Decode gRPC message from body bytes (strips the 5-byte header).
#[allow(clippy::result_large_err)]
fn decode_grpc_message<M: Message + Default>(body: &Bytes) -> Result<M, Status> {
    if body.len() < 5 {
        return Err(Status::invalid_argument("gRPC message too short"));
    }

    let _compressed = body[0];
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;

    if body.len() < 5 + len {
        return Err(Status::invalid_argument(format!(
            "gRPC message truncated: expected {} bytes, got {}",
            len,
            body.len() - 5
        )));
    }

    let msg_bytes = &body[5..5 + len];
    M::decode(msg_bytes).map_err(|e| Status::invalid_argument(format!("decode error: {}", e)))
}

/// Encode gRPC message to bytes (adds the 5-byte header).
fn encode_grpc_message<M: Message>(msg: &M) -> Bytes {
    let encoded = msg.encode_to_vec();
    let len = encoded.len() as u32;

    let mut buf = BytesMut::with_capacity(5 + encoded.len());
    buf.put_u8(0); // not compressed
    buf.put_u32(len);
    buf.put_slice(&encoded);
    buf.freeze()
}

/// A gRPC body that includes trailers with grpc-status.
struct GrpcBody {
    data: Option<Bytes>,
    trailers_sent: bool,
}

impl http_body::Body for GrpcBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.data.take() {
            tracing::info!(data_len = data.len(), "GrpcBody sending data frame");
            return std::task::Poll::Ready(Some(Ok(http_body::Frame::data(data))));
        }
        if !self.trailers_sent {
            self.trailers_sent = true;
            let mut trailers = tonic::codegen::http::HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            tracing::info!("GrpcBody sending trailers frame");
            return std::task::Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }
        tracing::debug!("GrpcBody end of stream");
        std::task::Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none() && self.trailers_sent
    }
}

/// Build a gRPC response with proper headers and trailers.
fn grpc_response(body: Bytes) -> tonic::codegen::http::Response<tonic::body::BoxBody> {
    use tonic::body::BoxBody;

    let grpc_body = GrpcBody {
        data: Some(body),
        trailers_sent: false,
    };

    let body = BoxBody::new(grpc_body);

    tonic::codegen::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/grpc")
        .body(body)
        .unwrap()
}

/// Build a gRPC error response.
fn grpc_error_response(status: Status) -> tonic::codegen::http::Response<tonic::body::BoxBody> {
    status.into_http()
}

/// gRPC server for etcd v3 API.
pub struct EtcdGrpcServer {
    /// Bind address.
    bind_addr: SocketAddr,
    /// Shared state.
    state: SharedState,
}

impl EtcdGrpcServer {
    /// Create a new gRPC server.
    pub fn new(bind_addr: SocketAddr, shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            bind_addr,
            state: SharedState::new(shutdown_rx),
        }
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Get a reference to the shared state.
    pub fn state(&self) -> &SharedState {
        &self.state
    }

    /// Get a mutable reference to the KV state machine.
    pub fn state_machine(&self) -> &Arc<RwLock<KvStateMachine>> {
        &self.state.state_machine
    }

    /// Run the gRPC server.
    pub async fn run(self) -> LatticeResult<()> {
        use tonic::transport::Server;

        let kv_service = KvService::new(self.state.clone());
        let lease_service = LeaseService::new(self.state.clone());

        let addr = self.bind_addr;
        let mut shutdown_rx = self.state.shutdown_rx.clone();

        tracing::info!(%addr, "starting etcd gRPC server");

        // Create router with services
        let router = Server::builder()
            .add_service(EtcdKvServer::new(kv_service))
            .add_service(EtcdLeaseServer::new(lease_service));

        // Run with graceful shutdown
        router
            .serve_with_shutdown(addr, async move {
                loop {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                tracing::info!("gRPC server shutting down");
            })
            .await
            .map_err(|e| LatticeError::Internal {
                message: format!("gRPC server error: {}", e),
            })?;

        Ok(())
    }
}

// ============================================================================
// Tonic Service Wrappers
// ============================================================================

/// Wrapper to make KvService work with tonic's generated server.
#[derive(Clone)]
pub struct EtcdKvServer {
    inner: KvService,
}

impl EtcdKvServer {
    pub fn new(inner: KvService) -> Self {
        Self { inner }
    }
}

/// The service definition for tonic.
impl tonic::server::NamedService for EtcdKvServer {
    const NAME: &'static str = "etcdserverpb.KV";
}

impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for EtcdKvServer
where
    B: tonic::codegen::Body + Send + 'static,
    B::Data: Into<Bytes> + Send,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
        let inner = self.inner.clone();
        let path = req.uri().path().to_string();

        // Log immediately when service is called
        tracing::info!(path = %path, "EtcdKvServer::call invoked");

        Box::pin(async move {
            tracing::info!(path = %path, "EtcdKvServer async block started");

            // Read body frame by frame - for unary gRPC we just need the first data frame
            let body = req.into_body();
            tracing::info!("about to read body frames");

            let mut data = BytesMut::new();
            let mut pinned_body = std::pin::pin!(body);

            loop {
                match pinned_body.as_mut().frame().await {
                    Some(Ok(frame)) => {
                        if frame.is_data() {
                            if let Ok(chunk) = frame.into_data() {
                                let chunk_bytes: Bytes = chunk.into();
                                tracing::info!(
                                    chunk_len = chunk_bytes.len(),
                                    "received data frame"
                                );
                                data.extend_from_slice(&chunk_bytes);
                                // For unary RPC, we expect one message - check if we have enough
                                // gRPC frame: 1 byte compressed flag + 4 bytes length + message
                                if data.len() >= 5 {
                                    let msg_len =
                                        u32::from_be_bytes([data[1], data[2], data[3], data[4]])
                                            as usize;
                                    if data.len() >= 5 + msg_len {
                                        tracing::info!("have complete message, proceeding");
                                        break;
                                    }
                                }
                            }
                        } else if frame.is_trailers() {
                            tracing::info!("received trailers, done reading");
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("error reading body frame: {}", e.into());
                        return Ok(grpc_error_response(Status::internal(
                            "failed to read request body",
                        )));
                    }
                    None => {
                        tracing::info!("body stream ended");
                        break;
                    }
                }
            }

            let collected = data.freeze();
            tracing::info!(path = %path, body_len = collected.len(), "handling KV request");

            let response = match path.as_str() {
                "/etcdserverpb.KV/Range" => {
                    match decode_grpc_message::<proto::RangeRequest>(&collected) {
                        Ok(req) => {
                            tracing::debug!(key = ?String::from_utf8_lossy(&req.key), "Range request");
                            match inner.range(req) {
                                Ok(resp) => grpc_response(encode_grpc_message(&resp)),
                                Err(e) => grpc_error_response(lattice_error_to_status(e)),
                            }
                        }
                        Err(status) => grpc_error_response(status),
                    }
                }
                "/etcdserverpb.KV/Put" => {
                    match decode_grpc_message::<proto::PutRequest>(&collected) {
                        Ok(req) => {
                            tracing::info!(
                                key = ?String::from_utf8_lossy(&req.key),
                                value = ?String::from_utf8_lossy(&req.value),
                                "Put request decoded"
                            );
                            tracing::info!("about to call inner.put");
                            let put_result = inner.put(req);
                            tracing::info!("inner.put returned");
                            match put_result {
                                Ok(resp) => {
                                    tracing::info!("Put succeeded, building response");
                                    grpc_response(encode_grpc_message(&resp))
                                }
                                Err(e) => {
                                    tracing::error!("Put failed: {:?}", e);
                                    grpc_error_response(lattice_error_to_status(e))
                                }
                            }
                        }
                        Err(status) => {
                            tracing::error!("Put decode failed: {:?}", status);
                            grpc_error_response(status)
                        }
                    }
                }
                "/etcdserverpb.KV/DeleteRange" => {
                    match decode_grpc_message::<proto::DeleteRangeRequest>(&collected) {
                        Ok(req) => {
                            tracing::debug!(key = ?String::from_utf8_lossy(&req.key), "DeleteRange request");
                            match inner.delete_range(req) {
                                Ok(resp) => grpc_response(encode_grpc_message(&resp)),
                                Err(e) => grpc_error_response(lattice_error_to_status(e)),
                            }
                        }
                        Err(status) => grpc_error_response(status),
                    }
                }
                _ => {
                    tracing::warn!(path = %path, "unknown KV method");
                    grpc_error_response(Status::unimplemented(format!("Unknown method: {}", path)))
                }
            };

            tracing::info!("returning response");
            Ok(response)
        })
    }
}

/// Wrapper to make LeaseService work with tonic's generated server.
#[derive(Clone)]
pub struct EtcdLeaseServer {
    #[allow(dead_code)]
    inner: LeaseService,
}

impl EtcdLeaseServer {
    pub fn new(inner: LeaseService) -> Self {
        Self { inner }
    }
}

impl tonic::server::NamedService for EtcdLeaseServer {
    const NAME: &'static str = "etcdserverpb.Lease";
}

impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for EtcdLeaseServer
where
    B: tonic::codegen::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();

        Box::pin(async move {
            // Lease operations are not yet implemented
            let status = Status::unimplemented(format!("Lease method not implemented: {}", path));
            Ok(grpc_error_response(status))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_state() {
        let (_tx, rx) = watch::channel(false);
        let state = SharedState::new(rx);

        assert_eq!(state.cluster_id, 1);
        assert_eq!(state.member_id, 1);
        assert_eq!(state.current_revision(), 0);
    }

    #[test]
    fn test_kv_service_put_get() {
        let (_tx, rx) = watch::channel(false);
        let state = SharedState::new(rx);
        let kv = KvService::new(state);

        // Put a key
        let put_req = proto::PutRequest {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        };
        let put_resp = kv.put(put_req).unwrap();
        assert!(put_resp.prev_kv.is_none());

        // Get the key
        let range_req = proto::RangeRequest {
            key: b"foo".to_vec(),
            range_end: vec![],
            limit: 0,
            revision: 0,
            sort_order: 0,
            sort_target: 0,
            serializable: false,
            keys_only: false,
            count_only: false,
            min_mod_revision: 0,
            max_mod_revision: 0,
            min_create_revision: 0,
            max_create_revision: 0,
        };
        let range_resp = kv.range(range_req).unwrap();
        assert_eq!(range_resp.count, 1);
        assert_eq!(range_resp.kvs[0].key, b"foo");
        assert_eq!(range_resp.kvs[0].value, b"bar");
    }

    #[test]
    fn test_kv_service_delete() {
        let (_tx, rx) = watch::channel(false);
        let state = SharedState::new(rx);
        let kv = KvService::new(state);

        // Put a key
        kv.put(proto::PutRequest {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
        .unwrap();

        // Delete the key
        let del_req = proto::DeleteRangeRequest {
            key: b"foo".to_vec(),
            range_end: vec![],
            prev_kv: true,
        };
        let del_resp = kv.delete_range(del_req).unwrap();
        assert_eq!(del_resp.deleted, 1);
        assert_eq!(del_resp.prev_kvs.len(), 1);

        // Verify it's gone
        let range_resp = kv
            .range(proto::RangeRequest {
                key: b"foo".to_vec(),
                range_end: vec![],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(range_resp.count, 0);
    }

    #[test]
    fn test_grpc_encode_decode() {
        let req = proto::PutRequest {
            key: b"test".to_vec(),
            value: b"value".to_vec(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        };

        let encoded = encode_grpc_message(&req);
        let decoded: proto::PutRequest = decode_grpc_message(&encoded).unwrap();

        assert_eq!(decoded.key, b"test");
        assert_eq!(decoded.value, b"value");
    }
}
