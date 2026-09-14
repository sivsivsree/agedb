use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use adb_core::{
    AdbError, Catalog, CatalogSnapshot, DatabaseName, DdlMutation, RequestContext, Result, Scope,
    TableName, TableSchema, TenantId, Value,
};
use adb_exec::ExecStats;
use adb_planner::{physical, validate, OutputSchema, PhysicalPlan, Query};
use adb_query::{IntentTranslator, PlanRequest, RuleTranslator, TranslationContext};
use adb_storage::object_store::{LocalFsStore, ObjectStore};
use adb_storage::table::{StorageConfig, TableStore, WriteOutcome};
use adb_storage::wal::{FileLogStore, LogStore, Mutation};
use adb_storage::{paths, rows};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as Json};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub storage: StorageConfig,
    /// Fixed "now" for natural-language time windows. Only set in tests.
    pub clock: Option<DateTime<Utc>>,
}

impl EngineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            storage: StorageConfig::default(),
            clock: None,
        }
    }
}

type TableKey = (TenantId, DatabaseName, TableName);

pub struct Engine {
    /// Held for the lifetime of the engine: an exclusive advisory lock on the
    /// data directory. Two processes writing one WAL would interleave records
    /// with independent LSNs, which is unrecoverable, so this is refused rather
    /// than detected later.
    _lock: std::fs::File,
    store: Arc<dyn ObjectStore>,
    catalog: Catalog,
    /// DDL log. The catalog is a replay of this.
    system_log: FileLogStore,
    /// Serializes DDL so validation and the log append are atomic together.
    ddl_lock: Mutex<()>,
    tables: RwLock<BTreeMap<TableKey, Arc<TableStore>>>,
    translator: Box<dyn IntentTranslator>,
    /// Always available, and the fallback when a remote translator is unreachable.
    rules: RuleTranslator,
    config: EngineConfig,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("data_dir", &self.config.data_dir)
            .field("translator", &self.translator.name())
            .finish_non_exhaustive()
    }
}

/// Everything a query returns.
#[derive(Debug, Clone, Serialize)]
pub struct QueryOutcome {
    pub rows: Vec<JsonMap<String, Json>>,
    pub schema: OutputSchema,
    pub stats: ExecStats,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// The structured plan that ran. Echoed back so an agent can see exactly how
    /// its request was interpreted, and reuse or adjust the plan.
    pub plan: PlanRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interpretation: Option<String>,
    /// Physical plan, for debugging.
    pub explain: String,
}

/// How a query was asked for.
#[derive(Debug, Clone)]
pub enum QuerySource {
    /// A structured plan.
    Plan(PlanRequest),
    /// Natural language, to be translated first.
    Request(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct TableStats {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
    pub partitions: usize,
    pub segments: usize,
    pub schema_version: u32,
}

impl Engine {
    /// Open (or create) a database directory.
    ///
    /// Fails if another process already has it open.
    pub fn open(config: EngineConfig) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsStore::new(&config.data_dir)?);
        let lock = Self::lock_data_dir(&config.data_dir)?;
        let system_wal = store.local_path(&paths::system_wal()).ok_or_else(|| {
            AdbError::Unsupported("v0.1 needs a filesystem-backed object store".to_string())
        })?;
        let recovered = FileLogStore::open(&system_wal, config.storage.fsync)?;

        // The catalog is exactly a replay of the DDL log.
        let mut mutations: Vec<DdlMutation> = Vec::with_capacity(recovered.entries.len());
        for entry in &recovered.entries {
            match &entry.mutation {
                Mutation::Ddl { .. } => mutations.push(entry.mutation.as_ddl()?),
                other => {
                    return Err(AdbError::Corruption(format!(
                        "the system log contains a {} record, which belongs to a table",
                        other.kind()
                    )))
                }
            }
        }
        let catalog = Catalog::replay(&mutations)?;
        tracing::info!(
            ddl_records = mutations.len(),
            data_dir = %config.data_dir.display(),
            "recovered catalog from the system log"
        );

        let translator: Box<dyn IntentTranslator> = match adb_query::AnthropicTranslator::from_env()
        {
            Some(anthropic) => {
                tracing::info!(model = anthropic.model(), "natural language: Claude");
                Box::new(anthropic)
            }
            None => {
                tracing::info!(
                    "natural language: built-in rules (set ANTHROPIC_API_KEY to use Claude)"
                );
                Box::new(RuleTranslator::new())
            }
        };

        let engine = Self {
            _lock: lock,
            store,
            catalog,
            system_log: recovered.store,
            ddl_lock: Mutex::new(()),
            tables: RwLock::new(BTreeMap::new()),
            translator,
            rules: RuleTranslator::new(),
            config,
        };
        engine.open_all_tables()?;
        engine.remove_orphaned_table_data()?;
        Ok(engine)
    }

    /// Take an exclusive lock on the data directory.
    fn lock_data_dir(data_dir: &std::path::Path) -> Result<std::fs::File> {
        use std::io::Write;

        let path = data_dir.join("LOCK");
        let mut file = std::fs::File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock().map_err(|_| {
            AdbError::bad_request(format!(
                "{} is already open by another agedb process",
                data_dir.display()
            ))
        })?;
        // Best-effort breadcrumb for whoever finds a stale directory. The lock
        // itself is the OS's, not this content.
        let _ = file.set_len(0);
        let _ = writeln!(file, "pid {}", std::process::id());
        let _ = file.flush();
        Ok(file)
    }

    pub fn translator_name(&self) -> &'static str {
        self.translator.name()
    }

    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.catalog.snapshot()
    }

    /// Open storage for every table the catalog knows about, which also replays
    /// each table's WAL.
    fn open_all_tables(&self) -> Result<()> {
        let snapshot = self.catalog.snapshot();
        let mut tables = self.write_tables()?;
        for (tenant, databases) in &snapshot.tenants {
            for (database, meta) in databases {
                for (name, schema) in &meta.tables {
                    let store = TableStore::open(
                        self.store.clone(),
                        tenant,
                        database,
                        schema.clone(),
                        self.config.storage.clone(),
                    )?;
                    tables.insert(
                        (tenant.clone(), database.clone(), name.clone()),
                        Arc::new(store),
                    );
                }
            }
        }
        tracing::info!(tables = tables.len(), "opened tables");
        Ok(())
    }

    /// Delete data belonging to tables the catalog no longer contains.
    ///
    /// A crash between logging a `DropTable` and deleting its files would
    /// otherwise leak them forever.
    fn remove_orphaned_table_data(&self) -> Result<()> {
        let snapshot = self.catalog.snapshot();
        let mut known: HashSet<String> = HashSet::new();
        for (tenant, databases) in &snapshot.tenants {
            for (database, meta) in databases {
                for name in meta.tables.keys() {
                    known.insert(paths::table_prefix(tenant, database, name));
                }
            }
        }
        let mut orphans: HashSet<String> = HashSet::new();
        for key in self.store.list("tenants")? {
            // tenants/{t}/databases/{d}/tables/{tbl}/...
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() < 6 || parts[2] != "databases" || parts[4] != "tables" {
                continue;
            }
            let prefix = parts[..6].join("/");
            if !known.contains(&prefix) {
                orphans.insert(prefix);
            }
        }
        for prefix in orphans {
            tracing::warn!(prefix = %prefix, "removing data for a table that is not in the catalog");
            self.store.delete_prefix(&prefix)?;
        }
        Ok(())
    }

    fn read_tables(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, BTreeMap<TableKey, Arc<TableStore>>>> {
        self.tables
            .read()
            .map_err(|_| AdbError::internal("table registry lock poisoned"))
    }

    fn write_tables(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, BTreeMap<TableKey, Arc<TableStore>>>> {
        self.tables
            .write()
            .map_err(|_| AdbError::internal("table registry lock poisoned"))
    }

    /// Validate, log and apply one DDL mutation.
    fn commit_ddl(&self, mutation: DdlMutation) -> Result<()> {
        let _guard = self
            .ddl_lock
            .lock()
            .map_err(|_| AdbError::internal("DDL lock poisoned"))?;
        // Reject illegal DDL *before* it reaches the log: the log is the source
        // of truth and must only contain applicable records.
        self.catalog.snapshot().apply(&mutation)?;
        self.system_log.append(&Mutation::ddl(&mutation)?)?;
        self.catalog.apply(&mutation)?;
        Ok(())
    }

    // --- databases -------------------------------------------------------

    pub fn create_database(&self, ctx: &RequestContext, name: &DatabaseName) -> Result<()> {
        ctx.require(Scope::DatabaseWrite)?;
        self.commit_ddl(DdlMutation::CreateDatabase {
            tenant: ctx.tenant.clone(),
            database: name.clone(),
        })
    }

    pub fn list_databases(&self, ctx: &RequestContext) -> Result<Vec<String>> {
        ctx.require(Scope::DatabaseRead)?;
        Ok(self
            .catalog
            .snapshot()
            .databases(&ctx.tenant)
            .into_iter()
            .map(|name| name.to_string())
            .collect())
    }

    /// Drop a database. `cascade` is required when it still has tables, so an
    /// agent cannot delete a populated database by accident.
    pub fn drop_database(
        &self,
        ctx: &RequestContext,
        name: &DatabaseName,
        cascade: bool,
    ) -> Result<usize> {
        ctx.require(Scope::DatabaseWrite)?;
        let snapshot = self.catalog.snapshot();
        let tables = snapshot.tables(&ctx.tenant, name)?;
        if !tables.is_empty() && !cascade {
            return Err(AdbError::bad_request(format!(
                "database {name} still has {} table(s); pass cascade to delete them too",
                tables.len()
            )));
        }
        self.commit_ddl(DdlMutation::DropDatabase {
            tenant: ctx.tenant.clone(),
            database: name.clone(),
        })?;

        let mut registry = self.write_tables()?;
        for schema in &tables {
            registry.remove(&(ctx.tenant.clone(), name.clone(), schema.name.clone()));
        }
        drop(registry);
        // Data is removed after the catalog change is durable; a crash in
        // between is cleaned up by `remove_orphaned_table_data` on restart.
        self.store
            .delete_prefix(&paths::database_prefix(&ctx.tenant, name))?;
        Ok(tables.len())
    }

    // --- tables ----------------------------------------------------------

    pub fn create_table(
        &self,
        ctx: &RequestContext,
        schema: TableSchema,
    ) -> Result<Arc<TableSchema>> {
        ctx.require(Scope::SchemaWrite)?;
        let database = ctx.require_database()?.clone();
        schema.validate()?;
        self.commit_ddl(DdlMutation::CreateTable {
            tenant: ctx.tenant.clone(),
            database: database.clone(),
            schema: schema.clone(),
        })?;
        let created = self
            .catalog
            .snapshot()
            .table(&ctx.tenant, &database, &schema.name)?;
        let store = TableStore::open(
            self.store.clone(),
            &ctx.tenant,
            &database,
            created.clone(),
            self.config.storage.clone(),
        )?;
        self.write_tables()?.insert(
            (ctx.tenant.clone(), database, schema.name.clone()),
            Arc::new(store),
        );
        Ok(created)
    }

    pub fn list_tables(&self, ctx: &RequestContext) -> Result<Vec<Arc<TableSchema>>> {
        ctx.require(Scope::SchemaRead)?;
        let database = ctx.require_database()?;
        self.catalog.snapshot().tables(&ctx.tenant, database)
    }

    pub fn describe_table(
        &self,
        ctx: &RequestContext,
        name: &TableName,
    ) -> Result<Arc<TableSchema>> {
        ctx.require(Scope::SchemaRead)?;
        let database = ctx.require_database()?;
        self.catalog.snapshot().table(&ctx.tenant, database, name)
    }

    pub fn update_schema(
        &self,
        ctx: &RequestContext,
        schema: TableSchema,
    ) -> Result<Arc<TableSchema>> {
        ctx.require(Scope::SchemaWrite)?;
        let database = ctx.require_database()?.clone();
        self.commit_ddl(DdlMutation::UpdateSchema {
            tenant: ctx.tenant.clone(),
            database: database.clone(),
            schema: schema.clone(),
        })?;
        let updated = self
            .catalog
            .snapshot()
            .table(&ctx.tenant, &database, &schema.name)?;
        self.table(ctx, &schema.name)?.set_schema(updated.clone())?;
        Ok(updated)
    }

    pub fn drop_table(&self, ctx: &RequestContext, name: &TableName) -> Result<()> {
        ctx.require(Scope::SchemaWrite)?;
        let database = ctx.require_database()?.clone();
        // Fail before logging if it does not exist.
        self.catalog
            .snapshot()
            .table(&ctx.tenant, &database, name)?;
        self.commit_ddl(DdlMutation::DropTable {
            tenant: ctx.tenant.clone(),
            database: database.clone(),
            table: name.clone(),
        })?;
        let removed =
            self.write_tables()?
                .remove(&(ctx.tenant.clone(), database.clone(), name.clone()));
        match removed {
            Some(store) => match Arc::try_unwrap(store) {
                Ok(owned) => owned.destroy()?,
                // Someone still holds a snapshot; delete the files directly.
                Err(_) => {
                    self.store
                        .delete_prefix(&paths::table_prefix(&ctx.tenant, &database, name))?
                }
            },
            None => self
                .store
                .delete_prefix(&paths::table_prefix(&ctx.tenant, &database, name))?,
        }
        Ok(())
    }

    pub fn table(&self, ctx: &RequestContext, name: &TableName) -> Result<Arc<TableStore>> {
        let database = ctx.require_database()?;
        let key = (ctx.tenant.clone(), database.clone(), name.clone());
        self.read_tables()?
            .get(&key)
            .cloned()
            .ok_or_else(|| AdbError::not_found("table", format!("{database}.{name}")))
    }

    pub fn table_stats(&self, ctx: &RequestContext, name: &TableName) -> Result<TableStats> {
        ctx.require(Scope::SchemaRead)?;
        let table = self.table(ctx, name)?;
        let snapshots = table.snapshots()?;
        Ok(TableStats {
            table: name.to_string(),
            rows: snapshots.iter().map(|s| s.visible_rows()).sum(),
            bytes: table.stored_bytes()?,
            partitions: snapshots.len(),
            segments: snapshots.iter().map(|s| s.segments.len()).sum(),
            schema_version: table.schema().version,
        })
    }

    // --- data ------------------------------------------------------------

    pub fn insert(
        &self,
        ctx: &RequestContext,
        name: &TableName,
        rows_json: &[JsonMap<String, Json>],
    ) -> Result<WriteOutcome> {
        ctx.require(Scope::DataInsert)?;
        self.write_rows(ctx, name, rows_json, false)
    }

    pub fn upsert(
        &self,
        ctx: &RequestContext,
        name: &TableName,
        rows_json: &[JsonMap<String, Json>],
    ) -> Result<WriteOutcome> {
        ctx.require(Scope::DataUpdate)?;
        self.write_rows(ctx, name, rows_json, true)
    }

    fn write_rows(
        &self,
        ctx: &RequestContext,
        name: &TableName,
        rows_json: &[JsonMap<String, Json>],
        upsert: bool,
    ) -> Result<WriteOutcome> {
        if rows_json.len() > ctx.limits.max_write_rows {
            return Err(AdbError::LimitExceeded {
                limit: "write:max_rows",
                detail: format!(
                    "{} rows in one call, limit is {}",
                    rows_json.len(),
                    ctx.limits.max_write_rows
                ),
            });
        }
        let table = self.table(ctx, name)?;
        let schema = table.schema();
        let batch = rows::batch_from_json_rows(&schema, rows_json)?;
        if upsert {
            table.upsert(batch)
        } else {
            table.insert(batch)
        }
    }

    pub fn delete(&self, ctx: &RequestContext, name: &TableName, keys: &[Json]) -> Result<usize> {
        ctx.require(Scope::DataDelete)?;
        let table = self.table(ctx, name)?;
        let schema = table.schema();
        let parsed = parse_keys(&schema, keys)?;
        table.delete(&parsed)
    }

    /// Point lookup by primary key.
    pub fn get(
        &self,
        ctx: &RequestContext,
        name: &TableName,
        keys: &[Json],
    ) -> Result<Vec<JsonMap<String, Json>>> {
        ctx.require(Scope::DatabaseRead)?;
        let table = self.table(ctx, name)?;
        let schema = table.schema();
        let parsed = parse_keys(&schema, keys)?;
        let batch = table.get(&parsed)?;
        rows::batch_to_json_rows(&batch)
    }

    pub fn flush(&self, ctx: &RequestContext, name: &TableName) -> Result<usize> {
        ctx.require(Scope::DataInsert)?;
        self.table(ctx, name)?.flush()
    }

    pub fn compact(&self, ctx: &RequestContext, name: &TableName) -> Result<usize> {
        ctx.require(Scope::DataInsert)?;
        self.table(ctx, name)?.compact()
    }

    /// Flush every open table's memtable and checkpoint it.
    ///
    /// This is a lifecycle operation rather than a request, so it takes no
    /// [`RequestContext`]: it runs on shutdown, across every tenant. Nothing is
    /// at risk if it is skipped, because the write-ahead log is the source of
    /// truth, but checkpointing means a restart replays a short log instead of
    /// the whole thing.
    ///
    /// A failure on one table is logged and the rest still flush: a shutdown
    /// that gives up halfway is worse than one that does what it can.
    pub fn checkpoint(&self) -> Result<usize> {
        let mut segments = 0;
        for ((tenant, database, table), store) in self.read_tables()?.iter() {
            match store.flush() {
                Ok(created) => segments += created,
                Err(error) => tracing::error!(
                    %tenant, %database, %table, %error,
                    "could not flush this table during checkpoint"
                ),
            }
        }
        Ok(segments)
    }

    // --- queries ---------------------------------------------------------

    /// Translation context for the current database.
    fn translation_context(&self, ctx: &RequestContext) -> Result<TranslationContext> {
        let tables = self.list_tables(ctx)?;
        let mut context = TranslationContext::new(tables);
        if let Some(clock) = self.config.clock {
            context = context.at(clock);
        }
        Ok(context)
    }

    /// Run a query, from either a structured plan or natural language.
    pub fn query(&self, ctx: &RequestContext, source: QuerySource) -> Result<QueryOutcome> {
        ctx.require(Scope::DatabaseRead)?;
        let mut warnings = Vec::new();
        let (plan_request, interpretation) = match source {
            QuerySource::Plan(plan) => (plan, None),
            QuerySource::Request(request) => {
                let context = self.translation_context(ctx)?;
                let translation = match self.translator.translate(&request, &context) {
                    Ok(translation) => translation,
                    Err(error) if self.translator.name() != "rules" && !error.is_client_error() => {
                        // The remote translator is unavailable; the deterministic
                        // one may still handle the request. Say which was used.
                        tracing::warn!(%error, "falling back to the rule translator");
                        warnings.push(format!(
                            "the language model was unavailable ({error}); the request was \
                             interpreted by the built-in rules"
                        ));
                        self.rules.translate(&request, &context)?
                    }
                    Err(error) => return Err(error),
                };
                let interpretation = translation.interpretation.clone();
                (translation.plan, Some(interpretation))
            }
        };

        let table_name = TableName::new(plan_request.table.clone())?;
        let schema = self.describe_table_for_query(ctx, &table_name)?;
        let ir = plan_request.to_ir()?;
        self.run_plan(
            ctx,
            &table_name,
            &schema,
            &ir,
            plan_request,
            interpretation,
            warnings,
        )
    }

    /// Schema lookup for a query. Uses `database:read` rather than
    /// `schema:read`: reading a table implies seeing its shape.
    fn describe_table_for_query(
        &self,
        ctx: &RequestContext,
        name: &TableName,
    ) -> Result<Arc<TableSchema>> {
        let database = ctx.require_database()?;
        self.catalog.snapshot().table(&ctx.tenant, database, name)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_plan(
        &self,
        ctx: &RequestContext,
        name: &TableName,
        schema: &TableSchema,
        ir: &Query,
        plan_request: PlanRequest,
        interpretation: Option<String>,
        mut warnings: Vec<String>,
    ) -> Result<QueryOutcome> {
        let validated = validate(ir, schema, &ctx.limits)?;
        warnings.extend(validated.warnings.iter().cloned());
        let plan = physical::build(&validated)?;
        let table = self.table(ctx, name)?;
        let snapshots = table.snapshots()?;

        tracing::debug!(
            request_id = %ctx.request_id,
            tenant = %ctx.tenant,
            table = %name,
            plan = %plan.explain(),
            "executing query"
        );
        let result = adb_exec::execute(&plan, &snapshots, ctx.limits)?;
        warnings.extend(result.warnings.iter().cloned());

        Ok(QueryOutcome {
            rows: result.to_json_rows()?,
            schema: validated.schema.clone(),
            stats: result.stats.clone(),
            warnings,
            plan: plan_request,
            interpretation,
            explain: plan.explain(),
        })
    }

    /// Lower a query without running it.
    pub fn explain(
        &self,
        ctx: &RequestContext,
        plan_request: &PlanRequest,
    ) -> Result<PhysicalPlan> {
        ctx.require(Scope::DatabaseRead)?;
        let name = TableName::new(plan_request.table.clone())?;
        let schema = self.describe_table_for_query(ctx, &name)?;
        let ir = plan_request.to_ir()?;
        let validated = validate(&ir, &schema, &ctx.limits)?;
        physical::build(&validated)
    }

    /// Translate a request without running it, for "how did you read this?".
    pub fn interpret(&self, ctx: &RequestContext, request: &str) -> Result<PlanRequest> {
        ctx.require(Scope::DatabaseRead)?;
        let context = self.translation_context(ctx)?;
        Ok(self.translator.translate(request, &context)?.plan)
    }

    /// Schema context an agent (or a model) can read.
    pub fn schema_context(&self, ctx: &RequestContext) -> Result<String> {
        let tables = self.list_tables(ctx)?;
        Ok(adb_query::schema_context(&tables))
    }
}

/// Parse caller-supplied primary keys.
///
/// Accepts `{"id": 1}` (recommended, and required for composite keys), a bare
/// scalar `1`, or a positional array `[1, "x"]`.
fn parse_keys(schema: &TableSchema, keys: &[Json]) -> Result<Vec<Vec<Value>>> {
    if schema.is_append_only() {
        return Err(AdbError::bad_request(format!(
            "table {} has no primary key, so rows cannot be addressed individually",
            schema.name
        )));
    }
    let pk = &schema.primary_key;
    keys.iter()
        .enumerate()
        .map(|(index, key)| match key {
            Json::Object(map) => pk
                .iter()
                .map(|column| {
                    let value = map.get(column).ok_or_else(|| {
                        AdbError::bad_request(format!(
                            "key {index} is missing primary key column {column:?}"
                        ))
                    })?;
                    let data_type = schema.require_column(column)?.data_type;
                    Value::from_json(value, data_type)
                })
                .collect::<Result<Vec<_>>>(),
            Json::Array(values) => {
                if values.len() != pk.len() {
                    return Err(AdbError::bad_request(format!(
                        "key {index} has {} value(s) but the primary key has {}",
                        values.len(),
                        pk.len()
                    )));
                }
                values
                    .iter()
                    .zip(pk)
                    .map(|(value, column)| {
                        let data_type = schema.require_column(column)?.data_type;
                        Value::from_json(value, data_type)
                    })
                    .collect()
            }
            scalar => {
                if pk.len() != 1 {
                    return Err(AdbError::bad_request(format!(
                        "table {} has a composite primary key ({}); pass an object per key",
                        schema.name,
                        pk.join(", ")
                    )));
                }
                let data_type = schema.require_column(&pk[0])?.data_type;
                Ok(vec![Value::from_json(scalar, data_type)?])
            }
        })
        .collect()
}
