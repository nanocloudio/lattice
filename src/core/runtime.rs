//! Main runtime orchestration.
//!
//! The runtime coordinates component lifecycle:
//! - Start order: storage → control cache → KPG runtime → adapters → listeners
//! - Shutdown order: listeners → adapters → KPG runtime → storage

use crate::core::config::Config;
use crate::storage::wal::ClustorStorage;
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Component health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentHealth {
    /// Component is starting.
    Starting,
    /// Component is healthy and operational.
    Healthy,
    /// Component is degraded but functional.
    Degraded,
    /// Component has failed.
    Failed,
    /// Component is stopping.
    Stopping,
    /// Component has stopped.
    Stopped,
}

/// Health status aggregated from all components.
#[derive(Debug, Clone)]
pub struct RuntimeHealth {
    /// Storage layer health.
    pub storage: ComponentHealth,
    /// Control plane cache health.
    pub control_plane: ComponentHealth,
    /// KPG runtime health.
    pub kpg_runtime: ComponentHealth,
    /// Adapter health.
    pub adapters: ComponentHealth,
    /// Listener health.
    pub listeners: ComponentHealth,
}

impl Default for RuntimeHealth {
    fn default() -> Self {
        Self {
            storage: ComponentHealth::Starting,
            control_plane: ComponentHealth::Starting,
            kpg_runtime: ComponentHealth::Starting,
            adapters: ComponentHealth::Starting,
            listeners: ComponentHealth::Starting,
        }
    }
}

impl RuntimeHealth {
    /// Check if the runtime is ready to serve requests.
    pub fn is_ready(&self) -> bool {
        matches!(
            (
                self.storage,
                self.control_plane,
                self.kpg_runtime,
                self.adapters,
                self.listeners
            ),
            (
                ComponentHealth::Healthy,
                ComponentHealth::Healthy | ComponentHealth::Degraded,
                ComponentHealth::Healthy,
                ComponentHealth::Healthy,
                ComponentHealth::Healthy
            )
        )
    }

    /// Check if the runtime is alive (not failed).
    pub fn is_alive(&self) -> bool {
        !matches!(
            (self.storage, self.control_plane, self.kpg_runtime),
            (ComponentHealth::Failed, _, _)
                | (_, ComponentHealth::Failed, _)
                | (_, _, ComponentHealth::Failed)
        )
    }
}

/// Lattice runtime holding all component handles.
pub struct Runtime {
    /// Configuration.
    config: Arc<Config>,

    /// Storage layer handle.
    storage: Option<ClustorStorage>,

    /// Storage directory path.
    storage_dir: PathBuf,

    /// Runtime health status.
    health: RuntimeHealth,

    /// Whether the runtime is running.
    running: Arc<AtomicBool>,

    /// Shutdown signal sender.
    shutdown_tx: watch::Sender<bool>,

    /// Shutdown signal receiver.
    shutdown_rx: watch::Receiver<bool>,

    /// gRPC server task handle.
    #[cfg(feature = "grpc")]
    grpc_handle: Option<JoinHandle<crate::core::error::LatticeResult<()>>>,
}

impl Runtime {
    /// Create a new runtime with the given configuration.
    pub fn new(config: Config) -> Result<Self> {
        config.validate().context("invalid configuration")?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let storage_dir = PathBuf::from(&config.paths.storage_dir);

        Ok(Self {
            config: Arc::new(config),
            storage: None,
            storage_dir,
            health: RuntimeHealth::default(),
            running: Arc::new(AtomicBool::new(false)),
            shutdown_tx,
            shutdown_rx,
            #[cfg(feature = "grpc")]
            grpc_handle: None,
        })
    }

    /// Get the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the storage directory path.
    pub fn storage_dir(&self) -> &PathBuf {
        &self.storage_dir
    }

    /// Get the storage layer (if initialized).
    pub fn storage(&self) -> Option<&ClustorStorage> {
        self.storage.as_ref()
    }

    /// Get the current health status.
    pub fn health(&self) -> &RuntimeHealth {
        &self.health
    }

    /// Check if the runtime is ready to serve requests.
    pub fn is_ready(&self) -> bool {
        self.health.is_ready()
    }

    /// Check if the runtime is alive.
    pub fn is_alive(&self) -> bool {
        self.health.is_alive()
    }

    /// Check if the runtime is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Get a shutdown receiver for graceful shutdown coordination.
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Initialize and start all runtime components.
    ///
    /// Components are started in order:
    /// 1. Storage layer (WAL, snapshots)
    /// 2. Control plane cache agent
    /// 3. KPG runtime
    /// 4. Protocol adapters
    /// 5. Network listeners
    pub async fn start(&mut self) -> Result<()> {
        tracing::info!(
            storage_dir = %self.storage_dir.display(),
            "starting Lattice runtime"
        );

        // 1. Initialize storage layer
        self.init_storage().await?;

        // 2. Initialize control plane cache
        self.init_control_plane().await?;

        // 3. Initialize KPG runtime
        self.init_kpg_runtime().await?;

        // 4. Initialize adapters
        self.init_adapters().await?;

        // 5. Start listeners
        self.start_listeners().await?;

        self.running.store(true, Ordering::Release);
        tracing::info!("Lattice runtime started");

        Ok(())
    }

    /// Initialize the storage layer.
    async fn init_storage(&mut self) -> Result<()> {
        tracing::debug!("initializing storage layer");

        let storage = ClustorStorage::new(self.storage_dir.clone());
        storage
            .initialize()
            .context("failed to initialize storage")?;

        self.storage = Some(storage);
        self.health.storage = ComponentHealth::Healthy;

        tracing::info!(
            storage_dir = %self.storage_dir.display(),
            "storage layer initialized"
        );
        Ok(())
    }

    /// Initialize the control plane cache.
    async fn init_control_plane(&mut self) -> Result<()> {
        tracing::debug!("initializing control plane cache");

        // Control plane cache initialization will be implemented in Phase 3
        // For now, mark as healthy for embedded mode
        if self.config.control_plane.mode == "embedded" {
            self.health.control_plane = ComponentHealth::Healthy;
        } else {
            self.health.control_plane = ComponentHealth::Degraded;
        }

        tracing::info!(
            mode = %self.config.control_plane.mode,
            "control plane cache initialized"
        );
        Ok(())
    }

    /// Initialize the KPG runtime.
    async fn init_kpg_runtime(&mut self) -> Result<()> {
        tracing::debug!("initializing KPG runtime");

        // KPG runtime initialization will be implemented in Phase 4
        self.health.kpg_runtime = ComponentHealth::Healthy;

        tracing::info!("KPG runtime initialized");
        Ok(())
    }

    /// Initialize protocol adapters.
    async fn init_adapters(&mut self) -> Result<()> {
        tracing::debug!("initializing protocol adapters");

        // Adapter initialization will be implemented in Phase 7-12
        self.health.adapters = ComponentHealth::Healthy;

        tracing::info!("protocol adapters initialized");
        Ok(())
    }

    /// Start network listeners.
    async fn start_listeners(&mut self) -> Result<()> {
        tracing::debug!("starting network listeners");

        #[cfg(feature = "grpc")]
        {
            if let Some(ref grpc_config) = self.config.listeners.grpc {
                use crate::adapters::etcd::grpc::EtcdGrpcServer;

                let bind_addr: std::net::SocketAddr = grpc_config
                    .bind
                    .parse()
                    .context("invalid gRPC bind address")?;

                // Check TLS configuration
                let has_tls =
                    grpc_config.tls_chain_path.is_some() && grpc_config.tls_key_path.is_some();

                if !has_tls && !grpc_config.insecure {
                    anyhow::bail!(
                        "gRPC listener requires TLS configuration or insecure=true. \
                        Set tls_chain_path and tls_key_path, or set insecure=true for testing."
                    );
                }

                if grpc_config.insecure {
                    tracing::warn!(
                        bind = %bind_addr,
                        "starting gRPC server in INSECURE mode (no TLS)"
                    );
                }

                let shutdown_rx = self.shutdown_rx.clone();
                let server = EtcdGrpcServer::new(bind_addr, shutdown_rx);

                // Spawn the gRPC server as a background task
                let handle = tokio::spawn(async move { server.run().await });

                self.grpc_handle = Some(handle);
                tracing::info!(bind = %bind_addr, "gRPC listener started");
            }
        }

        self.health.listeners = ComponentHealth::Healthy;
        tracing::info!("network listeners started");
        Ok(())
    }

    /// Trigger graceful shutdown.
    pub fn shutdown(&self) {
        tracing::info!("shutdown requested");
        let _ = self.shutdown_tx.send(true);
    }

    /// Wait for shutdown signal.
    pub async fn wait_for_shutdown(&mut self) {
        let mut rx = self.shutdown_rx.clone();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    /// Run the runtime until shutdown.
    pub async fn run(&mut self) -> Result<()> {
        self.start().await?;

        // Wait for shutdown signal or server failure
        #[cfg(feature = "grpc")]
        {
            if let Some(handle) = self.grpc_handle.take() {
                let mut shutdown_rx = self.shutdown_rx.clone();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::warn!("shutdown signal received (SIGINT)");
                    }
                    _ = async {
                        while !*shutdown_rx.borrow() {
                            if shutdown_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    } => {
                        tracing::info!("shutdown requested by component");
                    }
                    result = handle => {
                        match result {
                            Ok(Ok(())) => {
                                tracing::info!("gRPC server stopped normally");
                            }
                            Ok(Err(e)) => {
                                tracing::error!(error = %e, "gRPC server failed");
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "gRPC server task panicked");
                            }
                        }
                    }
                }
            } else {
                // No gRPC server, just wait for signals
                let mut shutdown_rx = self.shutdown_rx.clone();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::warn!("shutdown signal received (SIGINT)");
                    }
                    _ = async {
                        while !*shutdown_rx.borrow() {
                            if shutdown_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    } => {
                        tracing::info!("shutdown requested by component");
                    }
                }
            }
        }

        #[cfg(not(feature = "grpc"))]
        {
            let mut shutdown_rx = self.shutdown_rx.clone();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::warn!("shutdown signal received (SIGINT)");
                }
                _ = async {
                    while !*shutdown_rx.borrow() {
                        if shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                } => {
                    tracing::info!("shutdown requested by component");
                }
            }
        }

        self.stop().await?;
        Ok(())
    }

    /// Stop all runtime components.
    ///
    /// Components are stopped in reverse order:
    /// 1. Network listeners
    /// 2. Protocol adapters
    /// 3. KPG runtime
    /// 4. Control plane cache agent
    /// 5. Storage layer
    pub async fn stop(&mut self) -> Result<()> {
        tracing::info!("stopping Lattice runtime");
        self.running.store(false, Ordering::Release);

        // Signal shutdown to all components
        let _ = self.shutdown_tx.send(true);

        // 1. Stop listeners
        self.stop_listeners().await?;

        // 2. Stop adapters
        self.stop_adapters().await?;

        // 3. Stop KPG runtime
        self.stop_kpg_runtime().await?;

        // 4. Stop control plane cache
        self.stop_control_plane().await?;

        // 5. Stop storage layer
        self.stop_storage().await?;

        tracing::info!("Lattice runtime stopped");
        Ok(())
    }

    /// Stop network listeners.
    async fn stop_listeners(&mut self) -> Result<()> {
        tracing::debug!("stopping network listeners");
        self.health.listeners = ComponentHealth::Stopping;

        #[cfg(feature = "grpc")]
        {
            if let Some(handle) = self.grpc_handle.take() {
                // The server should stop when it receives the shutdown signal
                // Wait for it to complete with a timeout
                match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                    Ok(Ok(Ok(()))) => {
                        tracing::info!("gRPC server stopped");
                    }
                    Ok(Ok(Err(e))) => {
                        tracing::warn!(error = %e, "gRPC server stopped with error");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "gRPC server task panicked");
                    }
                    Err(_) => {
                        tracing::warn!("gRPC server stop timed out");
                    }
                }
            }
        }

        self.health.listeners = ComponentHealth::Stopped;
        Ok(())
    }

    /// Stop protocol adapters.
    async fn stop_adapters(&mut self) -> Result<()> {
        tracing::debug!("stopping protocol adapters");
        self.health.adapters = ComponentHealth::Stopping;
        // Adapter shutdown will be implemented in Phase 7-12
        self.health.adapters = ComponentHealth::Stopped;
        Ok(())
    }

    /// Stop the KPG runtime.
    async fn stop_kpg_runtime(&mut self) -> Result<()> {
        tracing::debug!("stopping KPG runtime");
        self.health.kpg_runtime = ComponentHealth::Stopping;
        // KPG runtime shutdown will be implemented in Phase 4
        self.health.kpg_runtime = ComponentHealth::Stopped;
        Ok(())
    }

    /// Stop the control plane cache.
    async fn stop_control_plane(&mut self) -> Result<()> {
        tracing::debug!("stopping control plane cache");
        self.health.control_plane = ComponentHealth::Stopping;
        // Control plane shutdown will be implemented in Phase 3
        self.health.control_plane = ComponentHealth::Stopped;
        Ok(())
    }

    /// Stop the storage layer.
    async fn stop_storage(&mut self) -> Result<()> {
        tracing::debug!("stopping storage layer");
        self.health.storage = ComponentHealth::Stopping;

        if let Some(storage) = self.storage.take() {
            storage.shutdown()?;
        }

        self.health.storage = ComponentHealth::Stopped;
        Ok(())
    }

    /// Start the runtime for tests (without listeners or signal handling).
    pub async fn start_for_tests(&mut self) -> Result<()> {
        self.init_storage().await?;
        self.init_control_plane().await?;
        self.init_kpg_runtime().await?;
        self.running.store(true, Ordering::Release);
        Ok(())
    }

    /// Stop the runtime for tests.
    pub async fn shutdown_for_tests(&mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.running.store(false, Ordering::Release);
        Ok(())
    }
}
