//! The `agedb` binary.
//!
//! ```text
//! agedb serve --transport stdio          # MCP over stdio, for an agent
//! agedb serve --transport http --port 8080  # REST, plus MCP at /mcp
//! agedb query "total revenue by country"  # one-shot, for demos and scripts
//! ```
//!
//! One process, two transports, one engine. Logging always goes to stderr: on
//! stdio, stdout carries JSON-RPC frames and nothing else may touch it.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use adb_core::{AdbError, DatabaseName, QueryLimits, Result, Scope, TenantId};
use adb_engine::{Engine, EngineConfig, QuerySource};
use adb_mcp::{ApiKey, AuthRegistry, McpServer};
use adb_storage::table::StorageConfig;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(
    name = "agedb",
    version,
    about = "An agent-native analytical database",
    long_about = "An agent-native analytical database. Columnar storage with a \
                  write-ahead log, a vectorized query engine, and MCP as a \
                  first-class interface."
)]
struct Cli {
    /// Where data lives. Created if absent.
    #[arg(long, env = "ADB_DATA_DIR", default_value = "./data", global = true)]
    data_dir: PathBuf,

    /// API keys and limits, as JSON. Without it, HTTP needs --api-key.
    #[arg(long, env = "ADB_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// Log filter, e.g. "info", "adb_exec=debug".
    #[arg(long, env = "ADB_LOG", default_value = "info", global = true)]
    log: String,

    /// Emit logs as JSON.
    #[arg(long, env = "ADB_LOG_JSON", global = true)]
    log_json: bool,

    /// Skip fsync on WAL append. Faster, and unsafe on power loss: benchmarks only.
    #[arg(long, env = "ADB_NO_FSYNC", global = true)]
    no_fsync: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve MCP and/or the REST API.
    Serve {
        #[arg(long, value_enum, default_value_t = Transport::Stdio)]
        transport: Transport,

        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        #[arg(long, env = "ADB_PORT", default_value_t = 8080)]
        port: u16,

        /// A single API key for HTTP, granting full access to --tenant.
        #[arg(long, env = "ADB_API_KEY")]
        api_key: Option<String>,

        /// Tenant for the stdio identity and for --api-key.
        #[arg(long, env = "ADB_TENANT", default_value = "local")]
        tenant: String,

        /// Database assumed when a call does not name one.
        #[arg(long, env = "ADB_DATABASE")]
        database: Option<String>,

        /// A browser origin allowed to call MCP over HTTP, e.g.
        /// https://app.example.com. Repeatable. Local origins are always
        /// allowed; requests without an Origin header are unaffected.
        #[arg(long = "allow-origin", value_name = "ORIGIN")]
        allow_origins: Vec<String>,
    },

    /// Run one query and print the result.
    Query {
        /// A question in plain language, or a JSON plan with --plan.
        request: String,

        #[arg(long)]
        database: Option<String>,

        #[arg(long, default_value = "local")]
        tenant: String,

        /// Treat the argument as a structured JSON plan.
        #[arg(long)]
        plan: bool,

        /// Show the physical plan instead of running it.
        #[arg(long)]
        explain: bool,
    },

    /// Print the MCP tool catalogue as JSON.
    Tools,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Transport {
    /// JSON-RPC over stdin/stdout, for an agent that launches the database.
    Stdio,
    /// REST plus MCP over Streamable HTTP.
    Http,
    /// Both at once.
    Both,
}

/// On-disk configuration.
#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    keys: Vec<ApiKeyConfig>,
    /// Identity for the stdio transport, where the process boundary is the
    /// trust boundary.
    #[serde(default)]
    local: Option<ApiKeyConfig>,
}

#[derive(Debug, Deserialize)]
struct ApiKeyConfig {
    #[serde(default)]
    key: Option<String>,
    tenant: String,
    #[serde(default)]
    user: Option<String>,
    /// Scope names, e.g. ["database:read", "data:insert"]. Empty means read-only.
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    default_database: Option<String>,
    #[serde(default)]
    max_rows: Option<usize>,
    #[serde(default)]
    max_bytes_scanned: Option<u64>,
    #[serde(default)]
    max_execution_time_ms: Option<u64>,
    #[serde(default)]
    max_write_rows: Option<usize>,
}

impl ApiKeyConfig {
    fn build(&self) -> Result<ApiKey> {
        let tenant = TenantId::new(self.tenant.clone())?;
        let mut key = ApiKey::new(self.key.clone().unwrap_or_default(), tenant);
        if let Some(user) = &self.user {
            key.user = user.clone();
        }
        if !self.scopes.is_empty() {
            let scopes: BTreeSet<Scope> = self
                .scopes
                .iter()
                .map(|scope| scope.parse::<Scope>())
                .collect::<Result<_>>()?;
            key.scopes = scopes;
        }
        if let Some(database) = &self.default_database {
            key.default_database = Some(DatabaseName::new(database.clone())?);
        }
        let defaults = QueryLimits::default();
        key.limits = QueryLimits {
            max_rows: self.max_rows.unwrap_or(defaults.max_rows),
            max_bytes_scanned: self.max_bytes_scanned.unwrap_or(defaults.max_bytes_scanned),
            max_execution_time_ms: self
                .max_execution_time_ms
                .unwrap_or(defaults.max_execution_time_ms),
            max_write_rows: self.max_write_rows.unwrap_or(defaults.max_write_rows),
        };
        Ok(key)
    }
}

fn load_config(path: Option<&PathBuf>) -> Result<FileConfig> {
    let Some(path) = path else {
        return Ok(FileConfig::default());
    };
    let bytes = std::fs::read(path)
        .map_err(|e| AdbError::bad_request(format!("cannot read {}: {e}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| AdbError::bad_request(format!("{} is not valid config: {e}", path.display())))
}

fn init_logging(filter: &str, json: bool) {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // stderr, always: stdout belongs to the MCP transport.
    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn build_auth(
    config: &FileConfig,
    tenant: &str,
    api_key: Option<&String>,
    database: Option<&String>,
) -> Result<AuthRegistry> {
    let mut registry = AuthRegistry::new();
    for key in &config.keys {
        let built = key.build()?;
        if built.key.is_empty() {
            return Err(AdbError::bad_request(
                "every entry in `keys` needs a \"key\"".to_string(),
            ));
        }
        registry = registry.with_key(built);
    }
    if let Some(key) = api_key {
        let mut built = ApiKey::new(key.clone(), TenantId::new(tenant)?).read_write();
        if let Some(database) = database {
            built.default_database = Some(DatabaseName::new(database.clone())?);
        }
        registry = registry.with_key(built);
    }

    // The stdio identity: whoever started this process already has our files,
    // so it gets full access to its tenant by default.
    let local = match &config.local {
        Some(local) => local.build()?,
        None => {
            let mut local = ApiKey::new("stdio", TenantId::new(tenant)?)
                .read_write()
                .with_limits(QueryLimits::default());
            if let Some(database) = database {
                local.default_database = Some(DatabaseName::new(database.clone())?);
            }
            local
        }
    };
    Ok(registry.with_local_identity(local))
}

fn open_engine(cli: &Cli) -> Result<Arc<Engine>> {
    let storage = StorageConfig {
        fsync: !cli.no_fsync,
        ..StorageConfig::default()
    };
    if cli.no_fsync {
        tracing::warn!("--no-fsync: acknowledged writes can be lost on power loss");
    }
    let config = EngineConfig {
        data_dir: cli.data_dir.clone(),
        storage,
        clock: None,
    };
    Ok(Arc::new(Engine::open(config)?))
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    init_logging(&cli.log, cli.log_json);
    let code = match run(cli).await {
        Ok(()) => 0,
        Err(error) => {
            tracing::error!(code = error.code(), %error, "fatal");
            eprintln!("error: {error}");
            1
        }
    };

    // Exit explicitly rather than returning and letting the runtime drop.
    //
    // The stdio transport parks a blocking thread on a read from stdin, and no
    // signal can interrupt a blocking read. Dropping a tokio runtime waits for
    // its blocking tasks to finish, so returning here would hang forever on a
    // read that will never return.
    //
    // This is safe because the graceful work has already happened by this
    // point: HTTP has stopped accepting and drained, memtables are flushed, and
    // every WAL append was fsynced when it was acknowledged. The kernel
    // releases the data directory lock as the process exits.
    std::process::exit(code)
}

async fn run(cli: Cli) -> Result<()> {
    match &cli.command {
        None | Some(Command::Serve { .. }) => {
            let (transport, bind, port, api_key, tenant, database, allow_origins) =
                match &cli.command {
                    Some(Command::Serve {
                        transport,
                        bind,
                        port,
                        api_key,
                        tenant,
                        database,
                        allow_origins,
                    }) => (
                        *transport,
                        bind.clone(),
                        *port,
                        api_key.clone(),
                        tenant.clone(),
                        database.clone(),
                        allow_origins.clone(),
                    ),
                    _ => (
                        Transport::Stdio,
                        "127.0.0.1".to_string(),
                        8080,
                        None,
                        "local".to_string(),
                        None,
                        Vec::new(),
                    ),
                };
            let config = load_config(cli.config.as_ref())?;
            let auth = build_auth(&config, &tenant, api_key.as_ref(), database.as_ref())?;
            let engine = open_engine(&cli)?;
            let http = HttpOptions {
                bind,
                port,
                allow_origins,
            };
            serve(engine, auth, transport, http).await
        }
        Some(Command::Query {
            request,
            database,
            tenant,
            plan,
            explain,
        }) => {
            let engine = open_engine(&cli)?;
            let mut ctx = adb_core::RequestContext::root(tenant);
            let database = match database {
                Some(name) => Some(DatabaseName::new(name.clone())?),
                None => {
                    // With one database there is nothing to choose.
                    let databases = engine.list_databases(&ctx)?;
                    match databases.len() {
                        1 => Some(DatabaseName::new(databases[0].clone())?),
                        _ => None,
                    }
                }
            };
            ctx.database = database;
            if ctx.database.is_none() {
                // Naming the tenant matters here: the most common cause is a
                // tenant mismatch with the server that wrote the data, not a
                // missing --database.
                let available = engine.list_databases(&ctx)?;
                return Err(AdbError::bad_request(format!(
                    "pass --database. Tenant {:?} has {}",
                    tenant,
                    if available.is_empty() {
                        "no databases (is --tenant right?)".to_string()
                    } else {
                        format!("several: {}", available.join(", "))
                    }
                )));
            }
            let source =
                if *plan {
                    QuerySource::Plan(serde_json::from_str(request).map_err(|e| {
                        AdbError::bad_request(format!("--plan needs a JSON plan: {e}"))
                    })?)
                } else {
                    QuerySource::Request(request.clone())
                };
            if *explain {
                let plan_request = match source {
                    QuerySource::Plan(plan) => plan,
                    QuerySource::Request(request) => engine.interpret(&ctx, &request)?,
                };
                let physical = engine.explain(&ctx, &plan_request)?;
                println!("{}", serde_json::to_string_pretty(&plan_request)?);
                println!("{}", physical.explain());
                return Ok(());
            }
            let outcome = engine.query(&ctx, source)?;
            println!("{}", serde_json::to_string_pretty(&outcome)?);
            Ok(())
        }
        Some(Command::Tools) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&adb_mcp::tools::definitions_json())?
            );
            Ok(())
        }
    }
}

/// How long in-flight HTTP requests get to finish before the process exits
/// anyway. Without a bound, one client that never closes its connection would
/// keep the server alive indefinitely.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Where and how the HTTP transport listens.
struct HttpOptions {
    bind: String,
    port: u16,
    allow_origins: Vec<String>,
}

async fn serve(
    engine: Arc<Engine>,
    auth: AuthRegistry,
    transport: Transport,
    options: HttpOptions,
) -> Result<()> {
    // One shutdown signal, watched by every transport.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    listen_for_signals(shutdown_tx.clone());

    let http = if matches!(transport, Transport::Http | Transport::Both) {
        if auth.is_empty() {
            return Err(AdbError::bad_request(
                "the HTTP transport needs at least one API key: pass --api-key or --config",
            ));
        }
        let state = adb_api::AppState::new(engine.clone(), auth.clone())
            .with_allowed_origins(options.allow_origins);
        tracing::info!(keys = auth.key_count(), "HTTP transport enabled");
        let stopping = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            adb_api::serve_http(
                &options.bind,
                options.port,
                state,
                stopping_signal(stopping),
            )
            .await
        }))
    } else {
        None
    };

    let mut outcome = Ok(());

    if matches!(transport, Transport::Stdio | Transport::Both) {
        let mcp = Arc::new(McpServer::new(engine.clone(), auth.clone()));
        let stdio = tokio::task::spawn_blocking(move || adb_mcp::serve_stdio(mcp));

        // The stdio loop blocks on reading stdin, and no signal can interrupt a
        // blocking read. So it is raced against the shutdown signal rather than
        // awaited: on a signal we stop waiting for it and let process exit take
        // the parked thread with it. There is nothing to drain, because a tool
        // call is handled synchronously before the next line is read.
        tokio::select! {
            finished = stdio => {
                outcome = match finished {
                    Ok(result) => result,
                    Err(error) => Err(AdbError::internal(format!("stdio task failed: {error}"))),
                };
                // stdin closed, so the client that launched this process is
                // gone. Stop the HTTP transport too rather than lingering.
                let _ = shutdown_tx.send(true);
            }
            _ = stopping_signal(shutdown_rx.clone()) => {
                tracing::info!("stopping the stdio transport");
            }
        }
    }

    if let Some(http) = http {
        // Wait for the stop signal without a deadline, then give in-flight
        // requests a bounded window to finish.
        stopping_signal(shutdown_rx.clone()).await;
        match tokio::time::timeout(DRAIN_TIMEOUT, http).await {
            Ok(Ok(result)) => {
                if outcome.is_ok() {
                    outcome = result;
                }
            }
            Ok(Err(error)) => tracing::error!(%error, "http task failed"),
            Err(_) => tracing::warn!(
                seconds = DRAIN_TIMEOUT.as_secs(),
                "in-flight requests did not finish in time, exiting anyway"
            ),
        }
    }

    checkpoint(&engine);
    outcome
}

/// Flush memtables on the way out, so a restart replays a short log rather than
/// the whole thing. Never fatal: the log already holds everything.
fn checkpoint(engine: &Engine) {
    match engine.checkpoint() {
        Ok(0) => tracing::info!("stopped"),
        Ok(segments) => tracing::info!(segments, "stopped, memtables flushed"),
        Err(error) => tracing::error!(
            %error,
            "could not checkpoint on shutdown, so the write-ahead log will be replayed on restart"
        ),
    }
}

/// Resolves once the process has been asked to stop.
async fn stopping_signal(mut stopping: watch::Receiver<bool>) {
    if *stopping.borrow() {
        return;
    }
    let _ = stopping.changed().await;
}

/// Ask every transport to stop on the first signal, and exit immediately on the
/// second: an operator who signals twice is not willing to wait.
fn listen_for_signals(shutdown: watch::Sender<bool>) {
    tokio::spawn(async move {
        next_signal().await;
        tracing::info!("shutting down, waiting for in-flight work");
        let _ = shutdown.send(true);

        next_signal().await;
        tracing::warn!("second signal, exiting now");
        std::process::exit(130);
    });
}

/// SIGINT or SIGTERM. SIGTERM matters as much as Ctrl-C, because that is what
/// Docker, Kubernetes and systemd send.
#[cfg(unix)]
async fn next_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(stream) => stream,
        Err(error) => {
            tracing::warn!(%error, "cannot listen for SIGINT");
            return std::future::pending().await;
        }
    };
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(error) => {
            tracing::warn!(%error, "cannot listen for SIGTERM, only Ctrl-C will stop this process");
            interrupt.recv().await;
            return;
        }
    };
    tokio::select! {
        _ = interrupt.recv() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn next_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
