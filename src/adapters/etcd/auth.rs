//! Authentication and identity verification for etcd adapter.
//!
//! Implements mTLS identity extraction and verification per §10.
//! Production profiles require mTLS for all protocol adapters.

use crate::adapters::RequestContext;
use crate::core::error::{LatticeError, LatticeResult};
use crate::net::security::ClientIdentity;

/// Identity extractor for mTLS connections.
///
/// Extracts client identity from TLS certificate during connection setup.
pub struct IdentityExtractor {
    /// Whether mTLS is required.
    require_mtls: bool,

    /// Allowed certificate issuers (empty = allow all).
    allowed_issuers: Vec<String>,
}

impl IdentityExtractor {
    /// Create a new identity extractor.
    pub fn new(require_mtls: bool) -> Self {
        Self {
            require_mtls,
            allowed_issuers: Vec::new(),
        }
    }

    /// Add an allowed certificate issuer.
    pub fn with_allowed_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.allowed_issuers.push(issuer.into());
        self
    }

    /// Check if mTLS is required.
    pub fn requires_mtls(&self) -> bool {
        self.require_mtls
    }

    /// Extract identity from certificate common name and optional SANs.
    ///
    /// Certificate format expected:
    /// - CN: `<tenant_id>/<principal_id>` or just `<principal_id>`
    /// - SAN DNS: `<tenant_id>.tenant.lattice.local` (optional)
    pub fn extract_identity(
        &self,
        common_name: &str,
        _subject_alt_names: &[String],
    ) -> LatticeResult<ClientIdentity> {
        // Parse CN format: "tenant_id/principal_id" or just "principal_id"
        let (tenant_id, principal_id) = if let Some(slash_pos) = common_name.find('/') {
            let tenant = &common_name[..slash_pos];
            let principal = &common_name[slash_pos + 1..];
            (Some(tenant.to_string()), Some(principal.to_string()))
        } else {
            (None, Some(common_name.to_string()))
        };

        Ok(ClientIdentity {
            common_name: common_name.to_string(),
            tenant_id,
            principal_id,
        })
    }

    /// Create an anonymous identity (for non-mTLS connections if allowed).
    pub fn anonymous_identity(&self) -> LatticeResult<ClientIdentity> {
        if self.require_mtls {
            return Err(LatticeError::AuthenticationRequired);
        }

        Ok(ClientIdentity {
            common_name: "anonymous".to_string(),
            tenant_id: None,
            principal_id: None,
        })
    }
}

impl Default for IdentityExtractor {
    fn default() -> Self {
        Self::new(true) // Production default: require mTLS
    }
}

/// Identity verifier for request authorization.
///
/// Verifies that the client identity is authorized to perform
/// operations on the requested tenant/keys.
pub struct IdentityVerifier {
    /// Whether to enforce tenant isolation.
    enforce_tenant_isolation: bool,
}

impl IdentityVerifier {
    /// Create a new identity verifier.
    pub fn new(enforce_tenant_isolation: bool) -> Self {
        Self {
            enforce_tenant_isolation,
        }
    }

    /// Verify that the identity can access the given tenant.
    pub fn verify_tenant_access(
        &self,
        identity: &ClientIdentity,
        tenant_id: &str,
    ) -> LatticeResult<()> {
        if !self.enforce_tenant_isolation {
            return Ok(());
        }

        // If identity has a tenant_id, it must match
        if let Some(ref identity_tenant) = identity.tenant_id {
            if identity_tenant != tenant_id {
                return Err(LatticeError::PermissionDenied {
                    message: format!(
                        "tenant mismatch: identity tenant '{}' cannot access tenant '{}'",
                        identity_tenant, tenant_id
                    ),
                });
            }
        }

        Ok(())
    }

    /// Verify identity and create a request context.
    pub fn create_context(
        &self,
        identity: ClientIdentity,
        tenant_id: &str,
        kv_epoch: u64,
    ) -> LatticeResult<RequestContext> {
        self.verify_tenant_access(&identity, tenant_id)?;

        Ok(RequestContext::new(tenant_id, kv_epoch).with_identity(identity))
    }
}

impl Default for IdentityVerifier {
    fn default() -> Self {
        Self::new(true) // Production default: enforce tenant isolation
    }
}

/// Metadata keys used for authentication in gRPC.
pub mod metadata_keys {
    /// Tenant ID header.
    pub const TENANT_ID: &str = "x-lattice-tenant-id";

    /// KV epoch header.
    pub const KV_EPOCH: &str = "x-lattice-kv-epoch";

    /// Trace ID header (W3C Trace Context).
    pub const TRACE_ID: &str = "traceparent";

    /// Request ID header.
    pub const REQUEST_ID: &str = "x-request-id";
}

/// Extract request context from gRPC metadata.
///
/// This is used by the gRPC interceptor to extract context from
/// incoming requests.
pub fn extract_context_from_metadata(
    identity: Option<ClientIdentity>,
    tenant_id: Option<&str>,
    kv_epoch: Option<u64>,
    trace_id: Option<&str>,
) -> LatticeResult<RequestContext> {
    let tenant_id = tenant_id.ok_or_else(|| LatticeError::InvalidRequest {
        message: "missing tenant ID in request metadata".to_string(),
    })?;

    let kv_epoch = kv_epoch.ok_or_else(|| LatticeError::InvalidRequest {
        message: "missing kv_epoch in request metadata".to_string(),
    })?;

    let mut ctx = RequestContext::new(tenant_id, kv_epoch);

    if let Some(id) = identity {
        ctx = ctx.with_identity(id);
    }

    if let Some(trace) = trace_id {
        // Parse W3C traceparent format: version-trace_id-span_id-flags
        if let Some((trace_id, span_id)) = parse_traceparent(trace) {
            ctx = ctx.with_trace(trace_id, span_id);
        }
    }

    Ok(ctx)
}

/// Parse W3C traceparent header.
///
/// Format: `{version}-{trace_id}-{span_id}-{flags}`
/// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
fn parse_traceparent(traceparent: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() >= 3 {
        Some((parts[1].to_string(), parts[2].to_string()))
    } else {
        None
    }
}

// ============================================================================
// Auth Service Implementation (Tasks 133-140)
// ============================================================================

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Authentication service configuration.
#[derive(Debug, Clone)]
pub struct AuthServiceConfig {
    /// Token TTL in seconds.
    pub token_ttl_seconds: u64,
    /// Auth cache TTL in milliseconds.
    pub auth_cache_ttl_ms: u64,
    /// Grace period for stale cache (aligned to CP cache_grace_ms).
    pub cache_grace_ms: u64,
    /// Whether to require authentication.
    pub require_auth: bool,
    /// Whether to enforce permissions.
    pub enforce_permissions: bool,
}

impl Default for AuthServiceConfig {
    fn default() -> Self {
        Self {
            token_ttl_seconds: 3600,   // 1 hour
            auth_cache_ttl_ms: 30_000, // 30 seconds
            cache_grace_ms: 5_000,     // 5 seconds
            require_auth: true,
            enforce_permissions: true,
        }
    }
}

/// Permission type for key operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Read permission (Range, Watch).
    Read,
    /// Write permission (Put, Delete).
    Write,
    /// Read and write permission.
    ReadWrite,
}

/// Key permission entry.
#[derive(Debug, Clone)]
pub struct KeyPermission {
    /// Key prefix (or exact key if is_prefix=false).
    pub key: Vec<u8>,
    /// Range end for range permissions.
    pub range_end: Option<Vec<u8>>,
    /// Whether this is a prefix permission.
    pub is_prefix: bool,
    /// Permission type.
    pub perm_type: Permission,
}

impl KeyPermission {
    /// Create a prefix permission.
    pub fn prefix(prefix: impl Into<Vec<u8>>, perm_type: Permission) -> Self {
        Self {
            key: prefix.into(),
            range_end: None,
            is_prefix: true,
            perm_type,
        }
    }

    /// Create an exact key permission.
    pub fn exact(key: impl Into<Vec<u8>>, perm_type: Permission) -> Self {
        Self {
            key: key.into(),
            range_end: None,
            is_prefix: false,
            perm_type,
        }
    }

    /// Create a range permission.
    pub fn range(
        key: impl Into<Vec<u8>>,
        range_end: impl Into<Vec<u8>>,
        perm_type: Permission,
    ) -> Self {
        Self {
            key: key.into(),
            range_end: Some(range_end.into()),
            is_prefix: false,
            perm_type,
        }
    }

    /// Check if this permission covers the given key for the requested operation.
    pub fn covers(&self, key: &[u8], requested_perm: Permission) -> bool {
        // Check permission type compatibility
        let perm_ok = matches!(
            (self.perm_type, requested_perm),
            (Permission::ReadWrite, _)
                | (Permission::Read, Permission::Read)
                | (Permission::Write, Permission::Write)
        );

        if !perm_ok {
            return false;
        }

        // Check key coverage
        if self.is_prefix {
            key.starts_with(&self.key)
        } else if let Some(ref range_end) = self.range_end {
            key >= self.key.as_slice() && key < range_end.as_slice()
        } else {
            key == self.key.as_slice()
        }
    }
}

/// Role definition.
#[derive(Debug, Clone)]
pub struct Role {
    /// Role name.
    pub name: String,
    /// Key permissions.
    pub permissions: Vec<KeyPermission>,
}

impl Role {
    /// Create a new role.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            permissions: Vec::new(),
        }
    }

    /// Add a permission to the role.
    pub fn with_permission(mut self, perm: KeyPermission) -> Self {
        self.permissions.push(perm);
        self
    }

    /// Check if this role has permission for the given key.
    pub fn has_permission(&self, key: &[u8], perm_type: Permission) -> bool {
        self.permissions.iter().any(|p| p.covers(key, perm_type))
    }
}

/// User entry in auth cache.
#[derive(Debug, Clone)]
pub struct UserEntry {
    /// Username.
    pub name: String,
    /// Password hash (bcrypt or similar).
    pub password_hash: String,
    /// Assigned roles.
    pub roles: Vec<String>,
}

impl UserEntry {
    /// Create a new user entry.
    pub fn new(name: impl Into<String>, password_hash: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            password_hash: password_hash.into(),
            roles: Vec::new(),
        }
    }

    /// Add a role to the user.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }
}

/// Auth token.
#[derive(Debug, Clone)]
pub struct AuthToken {
    /// Token string.
    pub token: String,
    /// Username.
    pub username: String,
    /// Expiration time.
    pub expires_at: Instant,
    /// Revision when token was issued.
    pub revision: i64,
}

impl AuthToken {
    /// Check if token is expired.
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Auth cache state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthCacheState {
    /// Fresh from CP-Raft.
    Fresh,
    /// Cached but still valid.
    Cached,
    /// Stale but within grace period.
    Stale,
    /// Expired, must fail closed.
    Expired,
}

/// Auth cache for users and roles.
struct AuthCache {
    /// Users by name.
    users: HashMap<String, UserEntry>,
    /// Roles by name.
    roles: HashMap<String, Role>,
    /// Last update time.
    last_update: Option<Instant>,
    /// Cache state.
    state: AuthCacheState,
}

impl AuthCache {
    fn new() -> Self {
        Self {
            users: HashMap::new(),
            roles: HashMap::new(),
            last_update: None,
            state: AuthCacheState::Expired,
        }
    }
}

/// Auth service for the etcd adapter.
///
/// Provides authentication and authorization per §10.
pub struct AuthService {
    /// Configuration.
    config: AuthServiceConfig,
    /// Auth cache.
    cache: RwLock<AuthCache>,
    /// Active tokens.
    tokens: RwLock<HashMap<String, AuthToken>>,
    /// Next token ID.
    next_token_id: AtomicU64,
    /// Current revision for token metadata.
    current_revision: AtomicU64,
    /// Statistics.
    stats: AuthServiceStats,
}

/// Auth service statistics.
struct AuthServiceStats {
    auth_success: AtomicU64,
    auth_failure: AtomicU64,
    permission_denied: AtomicU64,
    token_refresh: AtomicU64,
}

impl AuthServiceStats {
    fn new() -> Self {
        Self {
            auth_success: AtomicU64::new(0),
            auth_failure: AtomicU64::new(0),
            permission_denied: AtomicU64::new(0),
            token_refresh: AtomicU64::new(0),
        }
    }
}

/// Authenticate request.
#[derive(Debug, Clone)]
pub struct AuthenticateRequest {
    /// Username.
    pub name: String,
    /// Password (plaintext for request, hashed for storage).
    pub password: String,
}

/// Authenticate response.
#[derive(Debug, Clone)]
pub struct AuthenticateResponse {
    /// Auth token.
    pub token: String,
    /// Token TTL in seconds.
    pub ttl: u64,
}

/// UserAdd request.
#[derive(Debug, Clone)]
pub struct UserAddRequest {
    /// Username.
    pub name: String,
    /// Password.
    pub password: String,
    /// Whether this is a no-op if user exists.
    pub no_password: bool,
}

/// UserDelete request.
#[derive(Debug, Clone)]
pub struct UserDeleteRequest {
    /// Username.
    pub name: String,
}

/// RoleAdd request.
#[derive(Debug, Clone)]
pub struct RoleAddRequest {
    /// Role name.
    pub name: String,
}

/// RoleDelete request.
#[derive(Debug, Clone)]
pub struct RoleDeleteRequest {
    /// Role name.
    pub name: String,
}

/// UserGrantRole request.
#[derive(Debug, Clone)]
pub struct UserGrantRoleRequest {
    /// Username.
    pub user: String,
    /// Role name.
    pub role: String,
}

/// RoleGrantPermission request.
#[derive(Debug, Clone)]
pub struct RoleGrantPermissionRequest {
    /// Role name.
    pub name: String,
    /// Permission to grant.
    pub perm: KeyPermission,
}

impl AuthService {
    /// Create a new auth service.
    pub fn new(config: AuthServiceConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(AuthCache::new()),
            tokens: RwLock::new(HashMap::new()),
            next_token_id: AtomicU64::new(1),
            current_revision: AtomicU64::new(0),
            stats: AuthServiceStats::new(),
        }
    }

    /// Update the current revision.
    pub fn set_revision(&self, revision: u64) {
        self.current_revision.store(revision, Ordering::Release);
    }

    /// Update cache state based on freshness.
    pub fn update_cache_state(&self, ttl_ms: u64, grace_ms: u64) {
        let mut cache = self.cache.write().unwrap();

        if let Some(last_update) = cache.last_update {
            let elapsed = last_update.elapsed();
            let ttl = Duration::from_millis(ttl_ms);
            let grace = Duration::from_millis(ttl_ms + grace_ms);

            cache.state = if elapsed < ttl {
                AuthCacheState::Cached
            } else if elapsed < grace {
                AuthCacheState::Stale
            } else {
                AuthCacheState::Expired
            };
        }
    }

    /// Get current cache state.
    pub fn cache_state(&self) -> AuthCacheState {
        let cache = self.cache.read().unwrap();
        cache.state
    }

    /// Check if auth can proceed (not expired).
    fn check_cache_state(&self) -> LatticeResult<()> {
        let state = self.cache_state();

        if state == AuthCacheState::Expired {
            return Err(LatticeError::ControlPlaneUnavailable {
                message: "auth cache expired, cannot evaluate permissions".to_string(),
            });
        }

        Ok(())
    }

    /// Generate a new token.
    fn generate_token(&self, username: &str) -> AuthToken {
        let token_id = self.next_token_id.fetch_add(1, Ordering::SeqCst);
        let revision = self.current_revision.load(Ordering::Acquire);

        // Simple token format: base64(username:id:timestamp)
        let token_data = format!(
            "{}:{}:{}",
            username,
            token_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
        use base64::Engine;
        let token = base64::engine::general_purpose::STANDARD.encode(&token_data);

        AuthToken {
            token,
            username: username.to_string(),
            expires_at: Instant::now() + Duration::from_secs(self.config.token_ttl_seconds),
            revision: revision as i64,
        }
    }

    /// Authenticate a user.
    pub fn authenticate(&self, req: &AuthenticateRequest) -> LatticeResult<AuthenticateResponse> {
        if !self.config.require_auth {
            // Auth disabled, generate anonymous token
            let token = self.generate_token("anonymous");
            let token_str = token.token.clone();

            self.tokens
                .write()
                .unwrap()
                .insert(token_str.clone(), token);
            self.stats.auth_success.fetch_add(1, Ordering::Relaxed);

            return Ok(AuthenticateResponse {
                token: token_str,
                ttl: self.config.token_ttl_seconds,
            });
        }

        self.check_cache_state()?;

        let cache = self.cache.read().unwrap();

        // Find user
        let user = cache.users.get(&req.name).ok_or_else(|| {
            self.stats.auth_failure.fetch_add(1, Ordering::Relaxed);
            LatticeError::AuthenticationFailed {
                message: "user not found".to_string(),
            }
        })?;

        // Verify password (simplified - in production use bcrypt verify)
        // For now we just compare hashes directly (assumes pre-hashed password in request)
        if user.password_hash != req.password {
            self.stats.auth_failure.fetch_add(1, Ordering::Relaxed);
            return Err(LatticeError::AuthenticationFailed {
                message: "invalid password".to_string(),
            });
        }

        drop(cache);

        // Generate token
        let token = self.generate_token(&req.name);
        let token_str = token.token.clone();

        self.tokens
            .write()
            .unwrap()
            .insert(token_str.clone(), token);
        self.stats.auth_success.fetch_add(1, Ordering::Relaxed);

        Ok(AuthenticateResponse {
            token: token_str,
            ttl: self.config.token_ttl_seconds,
        })
    }

    /// Validate a token and return the username.
    pub fn validate_token(&self, token: &str) -> LatticeResult<String> {
        let tokens = self.tokens.read().unwrap();

        let auth_token = tokens
            .get(token)
            .ok_or(LatticeError::AuthenticationRequired)?;

        if auth_token.is_expired() {
            return Err(LatticeError::AuthenticationRequired);
        }

        Ok(auth_token.username.clone())
    }

    /// Check permission for a key operation.
    pub fn check_permission(
        &self,
        username: &str,
        key: &[u8],
        perm_type: Permission,
    ) -> LatticeResult<()> {
        if !self.config.enforce_permissions {
            return Ok(());
        }

        self.check_cache_state()?;

        let cache = self.cache.read().unwrap();

        // Find user
        let user = cache
            .users
            .get(username)
            .ok_or_else(|| LatticeError::PermissionDenied {
                message: format!("user '{}' not found", username),
            })?;

        // Check permissions through roles
        for role_name in &user.roles {
            if let Some(role) = cache.roles.get(role_name) {
                if role.has_permission(key, perm_type) {
                    return Ok(());
                }
            }
        }

        self.stats.permission_denied.fetch_add(1, Ordering::Relaxed);
        Err(LatticeError::PermissionDenied {
            message: format!(
                "user '{}' does not have {:?} permission for key",
                username, perm_type
            ),
        })
    }

    /// Check permission for a key range operation.
    pub fn check_range_permission(
        &self,
        username: &str,
        key: &[u8],
        range_end: Option<&[u8]>,
        perm_type: Permission,
    ) -> LatticeResult<()> {
        // For ranges, we check the start key
        // A more complete implementation would verify the entire range is covered
        self.check_permission(username, key, perm_type)?;

        // If range_end is specified, also check the last key
        if let Some(end) = range_end {
            if !end.is_empty() {
                self.check_permission(username, end, perm_type)?;
            }
        }

        Ok(())
    }

    /// Add a user (forwards to CP-Raft in production).
    pub fn user_add(&self, req: &UserAddRequest) -> LatticeResult<()> {
        let mut cache = self.cache.write().unwrap();

        if cache.users.contains_key(&req.name) {
            return Err(LatticeError::InvalidRequest {
                message: format!("user '{}' already exists", req.name),
            });
        }

        // In production, this would forward to CP-Raft
        // For now, update local cache directly
        let user = UserEntry::new(&req.name, &req.password);
        cache.users.insert(req.name.clone(), user);
        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;

        Ok(())
    }

    /// Delete a user (forwards to CP-Raft in production).
    pub fn user_delete(&self, req: &UserDeleteRequest) -> LatticeResult<()> {
        let mut cache = self.cache.write().unwrap();

        cache
            .users
            .remove(&req.name)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("user '{}' not found", req.name),
            })?;

        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;

        // Also remove any tokens for this user
        drop(cache);
        let mut tokens = self.tokens.write().unwrap();
        tokens.retain(|_, t| t.username != req.name);

        Ok(())
    }

    /// Add a role (forwards to CP-Raft in production).
    pub fn role_add(&self, req: &RoleAddRequest) -> LatticeResult<()> {
        let mut cache = self.cache.write().unwrap();

        if cache.roles.contains_key(&req.name) {
            return Err(LatticeError::InvalidRequest {
                message: format!("role '{}' already exists", req.name),
            });
        }

        let role = Role::new(&req.name);
        cache.roles.insert(req.name.clone(), role);
        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;

        Ok(())
    }

    /// Delete a role (forwards to CP-Raft in production).
    pub fn role_delete(&self, req: &RoleDeleteRequest) -> LatticeResult<()> {
        let mut cache = self.cache.write().unwrap();

        cache
            .roles
            .remove(&req.name)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("role '{}' not found", req.name),
            })?;

        // Remove role from all users
        for user in cache.users.values_mut() {
            user.roles.retain(|r| r != &req.name);
        }

        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;

        Ok(())
    }

    /// Grant a role to a user.
    pub fn user_grant_role(&self, req: &UserGrantRoleRequest) -> LatticeResult<()> {
        let mut cache = self.cache.write().unwrap();

        // Check role exists
        if !cache.roles.contains_key(&req.role) {
            return Err(LatticeError::InvalidRequest {
                message: format!("role '{}' not found", req.role),
            });
        }

        // Check user exists and add role
        let user = cache
            .users
            .get_mut(&req.user)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("user '{}' not found", req.user),
            })?;

        if !user.roles.contains(&req.role) {
            user.roles.push(req.role.clone());
        }

        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;

        Ok(())
    }

    /// Grant a permission to a role.
    pub fn role_grant_permission(&self, req: &RoleGrantPermissionRequest) -> LatticeResult<()> {
        let mut cache = self.cache.write().unwrap();

        let role = cache
            .roles
            .get_mut(&req.name)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("role '{}' not found", req.name),
            })?;

        role.permissions.push(req.perm.clone());

        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;

        Ok(())
    }

    /// Refresh a token.
    pub fn refresh_token(&self, token: &str) -> LatticeResult<AuthenticateResponse> {
        let username = self.validate_token(token)?;

        // Generate new token
        let new_token = self.generate_token(&username);
        let new_token_str = new_token.token.clone();

        let mut tokens = self.tokens.write().unwrap();
        tokens.remove(token);
        tokens.insert(new_token_str.clone(), new_token);

        self.stats.token_refresh.fetch_add(1, Ordering::Relaxed);

        Ok(AuthenticateResponse {
            token: new_token_str,
            ttl: self.config.token_ttl_seconds,
        })
    }

    /// Get active users (for listing).
    pub fn list_users(&self) -> Vec<String> {
        let cache = self.cache.read().unwrap();
        cache.users.keys().cloned().collect()
    }

    /// Get active roles (for listing).
    pub fn list_roles(&self) -> Vec<String> {
        let cache = self.cache.read().unwrap();
        cache.roles.keys().cloned().collect()
    }

    /// Get auth metrics.
    pub fn metrics(&self) -> AuthServiceMetrics {
        let cache = self.cache.read().unwrap();
        let tokens = self.tokens.read().unwrap();

        AuthServiceMetrics {
            active_users: cache.users.len(),
            active_roles: cache.roles.len(),
            active_tokens: tokens.len(),
            cache_state: cache.state,
            auth_success_total: self.stats.auth_success.load(Ordering::Relaxed),
            auth_failure_total: self.stats.auth_failure.load(Ordering::Relaxed),
            permission_denied_total: self.stats.permission_denied.load(Ordering::Relaxed),
            token_refresh_total: self.stats.token_refresh.load(Ordering::Relaxed),
        }
    }

    /// Cleanup expired tokens.
    pub fn cleanup_expired_tokens(&self) -> usize {
        let mut tokens = self.tokens.write().unwrap();
        let initial_count = tokens.len();
        tokens.retain(|_, t| !t.is_expired());
        initial_count - tokens.len()
    }

    /// Load auth state from CP-Raft (for cache refresh).
    pub fn load_from_cp(&self, users: Vec<UserEntry>, roles: Vec<Role>) {
        let mut cache = self.cache.write().unwrap();

        cache.users.clear();
        for user in users {
            cache.users.insert(user.name.clone(), user);
        }

        cache.roles.clear();
        for role in roles {
            cache.roles.insert(role.name.clone(), role);
        }

        cache.last_update = Some(Instant::now());
        cache.state = AuthCacheState::Fresh;
    }
}

/// Auth service metrics.
#[derive(Debug, Clone)]
pub struct AuthServiceMetrics {
    /// Number of active users in cache.
    pub active_users: usize,
    /// Number of active roles in cache.
    pub active_roles: usize,
    /// Number of active tokens.
    pub active_tokens: usize,
    /// Current cache state.
    pub cache_state: AuthCacheState,
    /// Total successful authentications.
    pub auth_success_total: u64,
    /// Total failed authentications.
    pub auth_failure_total: u64,
    /// Total permission denied.
    pub permission_denied_total: u64,
    /// Total token refreshes.
    pub token_refresh_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_extractor_parse_cn() {
        let extractor = IdentityExtractor::new(true);

        // Test tenant/principal format
        let identity = extractor
            .extract_identity("acme/alice", &[])
            .expect("should parse");
        assert_eq!(identity.common_name, "acme/alice");
        assert_eq!(identity.tenant_id, Some("acme".to_string()));
        assert_eq!(identity.principal_id, Some("alice".to_string()));

        // Test principal-only format
        let identity = extractor
            .extract_identity("bob", &[])
            .expect("should parse");
        assert_eq!(identity.common_name, "bob");
        assert_eq!(identity.tenant_id, None);
        assert_eq!(identity.principal_id, Some("bob".to_string()));
    }

    #[test]
    fn test_identity_verifier_tenant_access() {
        let verifier = IdentityVerifier::new(true);

        // Identity with matching tenant
        let identity = ClientIdentity {
            common_name: "acme/alice".to_string(),
            tenant_id: Some("acme".to_string()),
            principal_id: Some("alice".to_string()),
        };
        assert!(verifier.verify_tenant_access(&identity, "acme").is_ok());
        assert!(verifier.verify_tenant_access(&identity, "other").is_err());

        // Identity without tenant (service account)
        let identity = ClientIdentity {
            common_name: "service".to_string(),
            tenant_id: None,
            principal_id: Some("service".to_string()),
        };
        assert!(verifier.verify_tenant_access(&identity, "acme").is_ok());
    }

    #[test]
    fn test_parse_traceparent() {
        let result =
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        assert_eq!(result.0, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(result.1, "00f067aa0ba902b7");
    }

    #[test]
    fn test_anonymous_identity() {
        let extractor = IdentityExtractor::new(false);
        let identity = extractor
            .anonymous_identity()
            .expect("should allow anonymous");
        assert_eq!(identity.common_name, "anonymous");

        let strict_extractor = IdentityExtractor::new(true);
        assert!(strict_extractor.anonymous_identity().is_err());
    }

    // Auth Service tests

    fn make_auth_service() -> AuthService {
        let config = AuthServiceConfig {
            require_auth: true,
            enforce_permissions: true,
            ..Default::default()
        };
        AuthService::new(config)
    }

    #[test]
    fn test_auth_service_user_add() {
        let service = make_auth_service();

        let req = UserAddRequest {
            name: "alice".to_string(),
            password: "secret".to_string(),
            no_password: false,
        };
        service.user_add(&req).expect("should add user");

        // Adding same user again should fail
        assert!(service.user_add(&req).is_err());

        assert_eq!(service.list_users(), vec!["alice".to_string()]);
    }

    #[test]
    fn test_auth_service_role_add() {
        let service = make_auth_service();

        let req = RoleAddRequest {
            name: "admin".to_string(),
        };
        service.role_add(&req).expect("should add role");

        // Adding same role again should fail
        assert!(service.role_add(&req).is_err());

        assert_eq!(service.list_roles(), vec!["admin".to_string()]);
    }

    #[test]
    fn test_auth_service_authentication() {
        let config = AuthServiceConfig {
            require_auth: true,
            enforce_permissions: false,
            ..Default::default()
        };
        let service = AuthService::new(config);

        // Add user
        service
            .user_add(&UserAddRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
                no_password: false,
            })
            .unwrap();

        // Authenticate with correct password
        let resp = service
            .authenticate(&AuthenticateRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
            })
            .expect("should authenticate");

        assert!(!resp.token.is_empty());
        assert!(resp.ttl > 0);

        // Validate token
        let username = service
            .validate_token(&resp.token)
            .expect("should validate");
        assert_eq!(username, "alice");

        // Authenticate with wrong password
        assert!(service
            .authenticate(&AuthenticateRequest {
                name: "alice".to_string(),
                password: "wrong".to_string(),
            })
            .is_err());

        // Authenticate with unknown user
        assert!(service
            .authenticate(&AuthenticateRequest {
                name: "bob".to_string(),
                password: "secret".to_string(),
            })
            .is_err());
    }

    #[test]
    fn test_auth_service_permissions() {
        let service = make_auth_service();

        // Setup user, role, and permissions
        service
            .user_add(&UserAddRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
                no_password: false,
            })
            .unwrap();

        service
            .role_add(&RoleAddRequest {
                name: "reader".to_string(),
            })
            .unwrap();

        service
            .role_grant_permission(&RoleGrantPermissionRequest {
                name: "reader".to_string(),
                perm: KeyPermission::prefix(b"data/".to_vec(), Permission::Read),
            })
            .unwrap();

        service
            .user_grant_role(&UserGrantRoleRequest {
                user: "alice".to_string(),
                role: "reader".to_string(),
            })
            .unwrap();

        // Check permission - should succeed for read on data/ prefix
        service
            .check_permission("alice", b"data/key1", Permission::Read)
            .expect("should have read permission");

        // Check permission - should fail for write
        assert!(service
            .check_permission("alice", b"data/key1", Permission::Write)
            .is_err());

        // Check permission - should fail for different prefix
        assert!(service
            .check_permission("alice", b"other/key1", Permission::Read)
            .is_err());
    }

    #[test]
    fn test_key_permission_covers() {
        // Prefix permission
        let prefix_perm = KeyPermission::prefix(b"foo/".to_vec(), Permission::Read);
        assert!(prefix_perm.covers(b"foo/bar", Permission::Read));
        assert!(prefix_perm.covers(b"foo/bar/baz", Permission::Read));
        assert!(!prefix_perm.covers(b"bar/foo", Permission::Read));
        assert!(!prefix_perm.covers(b"foo/bar", Permission::Write));

        // Exact key permission
        let exact_perm = KeyPermission::exact(b"mykey".to_vec(), Permission::Write);
        assert!(exact_perm.covers(b"mykey", Permission::Write));
        assert!(!exact_perm.covers(b"mykey2", Permission::Write));
        assert!(!exact_perm.covers(b"mykey", Permission::Read));

        // Range permission
        let range_perm = KeyPermission::range(b"a".to_vec(), b"d".to_vec(), Permission::ReadWrite);
        assert!(range_perm.covers(b"a", Permission::Read));
        assert!(range_perm.covers(b"b", Permission::Write));
        assert!(range_perm.covers(b"c", Permission::ReadWrite));
        assert!(!range_perm.covers(b"d", Permission::Read)); // range_end is exclusive
        assert!(!range_perm.covers(b"e", Permission::Read));
    }

    #[test]
    fn test_auth_service_user_delete() {
        let service = make_auth_service();

        service
            .user_add(&UserAddRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
                no_password: false,
            })
            .unwrap();

        assert_eq!(service.list_users().len(), 1);

        service
            .user_delete(&UserDeleteRequest {
                name: "alice".to_string(),
            })
            .expect("should delete user");

        assert_eq!(service.list_users().len(), 0);

        // Deleting again should fail
        assert!(service
            .user_delete(&UserDeleteRequest {
                name: "alice".to_string(),
            })
            .is_err());
    }

    #[test]
    fn test_auth_service_role_delete() {
        let service = make_auth_service();

        service
            .role_add(&RoleAddRequest {
                name: "admin".to_string(),
            })
            .unwrap();

        // Add user with role
        service
            .user_add(&UserAddRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
                no_password: false,
            })
            .unwrap();

        service
            .user_grant_role(&UserGrantRoleRequest {
                user: "alice".to_string(),
                role: "admin".to_string(),
            })
            .unwrap();

        // Delete role
        service
            .role_delete(&RoleDeleteRequest {
                name: "admin".to_string(),
            })
            .expect("should delete role");

        assert_eq!(service.list_roles().len(), 0);
    }

    #[test]
    fn test_auth_service_metrics() {
        let config = AuthServiceConfig {
            require_auth: true,
            enforce_permissions: true,
            ..Default::default()
        };
        let service = AuthService::new(config);

        // Add user
        service
            .user_add(&UserAddRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
                no_password: false,
            })
            .unwrap();

        // Authenticate
        let resp = service
            .authenticate(&AuthenticateRequest {
                name: "alice".to_string(),
                password: "secret".to_string(),
            })
            .unwrap();
        assert!(!resp.token.is_empty());

        // Failed auth
        let _ = service.authenticate(&AuthenticateRequest {
            name: "bob".to_string(),
            password: "wrong".to_string(),
        });

        let metrics = service.metrics();
        assert_eq!(metrics.active_users, 1);
        assert_eq!(metrics.active_tokens, 1);
        assert_eq!(metrics.auth_success_total, 1);
        assert_eq!(metrics.auth_failure_total, 1);
    }

    #[test]
    fn test_auth_service_no_auth_mode() {
        let config = AuthServiceConfig {
            require_auth: false,
            enforce_permissions: false,
            ..Default::default()
        };
        let service = AuthService::new(config);

        // Should authenticate anyone when auth is disabled
        let resp = service
            .authenticate(&AuthenticateRequest {
                name: "anyone".to_string(),
                password: "anything".to_string(),
            })
            .expect("should authenticate in no-auth mode");

        assert!(!resp.token.is_empty());
    }
}
