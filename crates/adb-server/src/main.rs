//! The `agedb` binary.
//!
//! ```text
//! agedb serve --transport stdio          # MCP over stdio, for an agent
//! agedb serve --transport http --port 8080
//! agedb query "total revenue by country"  # one-shot, for demos and scripts
//! ```
//!
//! One process, two transports, one engine. Logging always goes to stderr: on
//! stdio, stdout carries JSON-RPC frames and nothing else may touch it.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use adb_core::{AdbError, DatabaseName, QueryLimits, Result, Scope, TenantId};
use adb_engine::{Engine, EngineConfig, QuerySource};
use adb_mcp::{ApiKey, AuthRegistry, McpServer};
use adb_storage::table::StorageConfig;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;

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
    /// REST plus MCP-over-HTTP.
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
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(code = error.code(), %error, "fatal");
            eprintln!("error: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    match &cli.command {
        None | Some(Command::Serve { .. }) => {
            let (transport, bind, port, api_key, tenant, database) = match &cli.command {
                Some(Command::Serve {
                    transport,
                    bind,
                    port,
                    api_key,
                    tenant,
                    database,
                }) => (
                    *transport,
                    bind.clone(),
                    *port,
                    api_key.clone(),
                    tenant.clone(),
                    database.clone(),
                ),
                _ => (
                    Transport::Stdio,
                    "127.0.0.1".to_string(),
                    8080,
                    None,
                    "local".to_string(),
                    None,
                ),
            };
            let config = load_config(cli.config.as_ref())?;
            let auth = build_auth(&config, &tenant, api_key.as_ref(), database.as_ref())?;
            let engine = open_engine(&cli)?;
            serve(engine, auth, transport, bind, port).await
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

async fn serve(
    engine: Arc<Engine>,
    auth: AuthRegistry,
    transport: Transport,
    bind: String,
    port: u16,
) -> Result<()> {
    let mcp = Arc::new(McpServer::new(engine.clone(), auth.clone()));

    let http = if matches!(transport, Transport::Http | Transport::Both) {
        if auth.is_empty() {
            return Err(AdbError::bad_request(
                "the HTTP transport needs at least one API key: pass --api-key or --config",
            ));
        }
        let state = adb_api::AppState::new(engine.clone(), auth.clone());
        tracing::info!(keys = auth.key_count(), "HTTP transport enabled");
        Some(tokio::spawn(async move {
            adb_api::serve_http(&bind, port, state, shutdown_signal()).await
        }))
    } else {
        None
    };

    if matches!(transport, Transport::Stdio | Transport::Both) {
        let mcp = mcp.clone();
        // The stdio loop blocks on reads, so it owns a thread.
        tokio::task::spawn_blocking(move || adb_mcp::serve_stdio(mcp))
            .await
            .map_err(|e| AdbError::internal(format!("stdio task failed: {e}")))??;
        return Ok(());
    }

    if let Some(http) = http {
        http.await
            .map_err(|e| AdbError::internal(format!("http task failed: {e}")))??;
    }
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
