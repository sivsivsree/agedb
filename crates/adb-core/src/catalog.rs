use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

use crate::error::{AdbError, Result};
use crate::ids::{DatabaseName, TableName, TenantId};
use crate::schema::TableSchema;

/// Every structural change to the catalog, as a value.
///
/// DDL reaches the catalog only by way of a logged mutation, which is what makes
/// the catalog reconstructible from the system WAL and, later, replicable
/// through Raft. Variants must stay deterministic: no clocks, no UUIDs, no
/// defaults resolved during `apply`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DdlMutation {
    CreateDatabase {
        tenant: TenantId,
        database: DatabaseName,
    },
    /// Cascades: dropping a database drops its tables. The tool layer requires
    /// an explicit `cascade` flag from the caller before emitting this.
    DropDatabase {
        tenant: TenantId,
        database: DatabaseName,
    },
    CreateTable {
        tenant: TenantId,
        database: DatabaseName,
        schema: TableSchema,
    },
    DropTable {
        tenant: TenantId,
        database: DatabaseName,
        table: TableName,
    },
    UpdateSchema {
        tenant: TenantId,
        database: DatabaseName,
        schema: TableSchema,
    },
}

impl DdlMutation {
    pub fn tenant(&self) -> &TenantId {
        match self {
            Self::CreateDatabase { tenant, .. }
            | Self::DropDatabase { tenant, .. }
            | Self::CreateTable { tenant, .. }
            | Self::DropTable { tenant, .. }
            | Self::UpdateSchema { tenant, .. } => tenant,
        }
    }

    pub fn database(&self) -> &DatabaseName {
        match self {
            Self::CreateDatabase { database, .. }
            | Self::DropDatabase { database, .. }
            | Self::CreateTable { database, .. }
            | Self::DropTable { database, .. }
            | Self::UpdateSchema { database, .. } => database,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseMeta {
    pub tables: BTreeMap<TableName, Arc<TableSchema>>,
}

/// An immutable point-in-time view of all tenants, databases and tables.
///
/// Readers clone an `Arc` of this and never take a lock (see "Concurrency" in ARCHITECTURE.md);
/// writers build a successor and swap it in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub tenants: BTreeMap<TenantId, BTreeMap<DatabaseName, DatabaseMeta>>,
    /// Monotonic counter, bumped on each applied mutation. Useful for debugging
    /// and for asserting that a plan was built against the schema it ran on.
    pub epoch: u64,
}

impl CatalogSnapshot {
    pub fn databases(&self, tenant: &TenantId) -> Vec<&DatabaseName> {
        self.tenants
            .get(tenant)
            .map(|dbs| dbs.keys().collect())
            .unwrap_or_default()
    }

    pub fn database(&self, tenant: &TenantId, db: &DatabaseName) -> Result<&DatabaseMeta> {
        self.tenants
            .get(tenant)
            .and_then(|dbs| dbs.get(db))
            .ok_or_else(|| AdbError::not_found("database", db))
    }

    pub fn has_database(&self, tenant: &TenantId, db: &DatabaseName) -> bool {
        self.database(tenant, db).is_ok()
    }

    pub fn tables(&self, tenant: &TenantId, db: &DatabaseName) -> Result<Vec<Arc<TableSchema>>> {
        Ok(self
            .database(tenant, db)?
            .tables
            .values()
            .cloned()
            .collect())
    }

    pub fn table(
        &self,
        tenant: &TenantId,
        db: &DatabaseName,
        table: &TableName,
    ) -> Result<Arc<TableSchema>> {
        self.database(tenant, db)?
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| AdbError::not_found("table", format!("{db}.{table}")))
    }

    /// Pure transition: validate `m` against this snapshot and return its
    /// successor. Returns an error rather than a snapshot when the mutation is
    /// illegal, which is how the engine rejects DDL *before* it is logged.
    pub fn apply(&self, m: &DdlMutation) -> Result<CatalogSnapshot> {
        let mut next = self.clone();
        next.epoch += 1;
        match m {
            DdlMutation::CreateDatabase { tenant, database } => {
                let dbs = next.tenants.entry(tenant.clone()).or_default();
                if dbs.contains_key(database) {
                    return Err(AdbError::already_exists("database", database));
                }
                dbs.insert(database.clone(), DatabaseMeta::default());
            }
            DdlMutation::DropDatabase { tenant, database } => {
                let dbs = next
                    .tenants
                    .get_mut(tenant)
                    .ok_or_else(|| AdbError::not_found("database", database))?;
                dbs.remove(database)
                    .ok_or_else(|| AdbError::not_found("database", database))?;
            }
            DdlMutation::CreateTable {
                tenant,
                database,
                schema,
            } => {
                schema.validate()?;
                let db = next
                    .tenants
                    .get_mut(tenant)
                    .and_then(|dbs| dbs.get_mut(database))
                    .ok_or_else(|| AdbError::not_found("database", database))?;
                if db.tables.contains_key(&schema.name) {
                    return Err(AdbError::already_exists("table", &schema.name));
                }
                db.tables
                    .insert(schema.name.clone(), Arc::new(schema.clone()));
            }
            DdlMutation::DropTable {
                tenant,
                database,
                table,
            } => {
                let db = next
                    .tenants
                    .get_mut(tenant)
                    .and_then(|dbs| dbs.get_mut(database))
                    .ok_or_else(|| AdbError::not_found("database", database))?;
                db.tables
                    .remove(table)
                    .ok_or_else(|| AdbError::not_found("table", format!("{database}.{table}")))?;
            }
            DdlMutation::UpdateSchema {
                tenant,
                database,
                schema,
            } => {
                let db = next
                    .tenants
                    .get_mut(tenant)
                    .and_then(|dbs| dbs.get_mut(database))
                    .ok_or_else(|| AdbError::not_found("database", database))?;
                let existing = db.tables.get(&schema.name).ok_or_else(|| {
                    AdbError::not_found("table", format!("{database}.{}", schema.name))
                })?;
                existing.check_evolution(schema)?;
                let mut updated = schema.clone();
                // The version is engine-owned, not caller-supplied.
                updated.version = existing.version + 1;
                db.tables.insert(schema.name.clone(), Arc::new(updated));
            }
        }
        Ok(next)
    }
}

/// Lock-free versioned catalog: an `ArcSwap` over immutable snapshots.
#[derive(Debug, Default)]
pub struct Catalog {
    inner: ArcSwap<CatalogSnapshot>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_snapshot(snapshot: CatalogSnapshot) -> Self {
        Self {
            inner: ArcSwap::from_pointee(snapshot),
        }
    }

    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.inner.load_full()
    }

    /// Apply a mutation, retrying if another writer swapped in a new snapshot
    /// first. Callers still serialize DDL upstream (the engine holds a DDL lock
    /// so validation and WAL append are atomic); this loop only guarantees the
    /// swap itself never loses an update.
    pub fn apply(&self, m: &DdlMutation) -> Result<Arc<CatalogSnapshot>> {
        loop {
            let current = self.inner.load_full();
            let next = Arc::new(current.apply(m)?);
            let prev = self.inner.compare_and_swap(&current, next.clone());
            if Arc::ptr_eq(&prev, &current) {
                return Ok(next);
            }
        }
    }

    /// Replay a sequence of logged mutations into a fresh catalog.
    pub fn replay(mutations: &[DdlMutation]) -> Result<Self> {
        let mut snapshot = CatalogSnapshot::default();
        for m in mutations {
            snapshot = snapshot.apply(m)?;
        }
        Ok(Self::from_snapshot(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;
    use crate::types::DataType;

    fn tenant() -> TenantId {
        TenantId::new("t1").unwrap()
    }

    fn db() -> DatabaseName {
        DatabaseName::new("crm").unwrap()
    }

    fn schema(name: &str) -> TableSchema {
        TableSchema::new(
            TableName::new(name).unwrap(),
            vec![ColumnSchema::new("id", DataType::Int64).required()],
        )
        .with_primary_key(vec!["id".to_string()])
    }

    fn ddl() -> Vec<DdlMutation> {
        vec![
            DdlMutation::CreateDatabase {
                tenant: tenant(),
                database: db(),
            },
            DdlMutation::CreateTable {
                tenant: tenant(),
                database: db(),
                schema: schema("customers"),
            },
        ]
    }

    #[test]
    fn replay_rebuilds_the_catalog() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        let snap = catalog.snapshot();
        assert_eq!(snap.databases(&tenant()).len(), 1);
        let t = snap
            .table(&tenant(), &db(), &TableName::new("customers").unwrap())
            .unwrap();
        assert_eq!(t.columns.len(), 1);
        assert_eq!(snap.epoch, 2);
    }

    #[test]
    fn snapshots_are_immutable_across_mutations() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        let before = catalog.snapshot();
        catalog
            .apply(&DdlMutation::CreateTable {
                tenant: tenant(),
                database: db(),
                schema: schema("orders"),
            })
            .unwrap();
        // The snapshot a reader is holding does not change under it.
        assert_eq!(before.tables(&tenant(), &db()).unwrap().len(), 1);
        assert_eq!(
            catalog.snapshot().tables(&tenant(), &db()).unwrap().len(),
            2
        );
    }

    #[test]
    fn tenants_are_isolated() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        let other = TenantId::new("t2").unwrap();
        assert!(catalog.snapshot().database(&other, &db()).is_err());
        assert!(catalog
            .snapshot()
            .table(&other, &db(), &TableName::new("customers").unwrap())
            .is_err());
    }

    #[test]
    fn duplicate_and_missing_objects_are_rejected() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        assert_eq!(
            catalog
                .apply(&DdlMutation::CreateDatabase {
                    tenant: tenant(),
                    database: db()
                })
                .unwrap_err()
                .code(),
            "already_exists"
        );
        assert_eq!(
            catalog
                .apply(&DdlMutation::DropTable {
                    tenant: tenant(),
                    database: db(),
                    table: TableName::new("nope").unwrap(),
                })
                .unwrap_err()
                .code(),
            "not_found"
        );
    }

    #[test]
    fn invalid_ddl_never_reaches_a_snapshot() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        let mut bad = schema("bad");
        bad.columns.clear();
        assert!(catalog
            .apply(&DdlMutation::CreateTable {
                tenant: tenant(),
                database: db(),
                schema: bad
            })
            .is_err());
        assert_eq!(
            catalog.snapshot().tables(&tenant(), &db()).unwrap().len(),
            1
        );
    }

    #[test]
    fn update_schema_bumps_the_engine_owned_version() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        let mut next = schema("customers");
        next.columns
            .push(ColumnSchema::new("email", DataType::Utf8));
        next.version = 999; // caller-supplied version is ignored
        catalog
            .apply(&DdlMutation::UpdateSchema {
                tenant: tenant(),
                database: db(),
                schema: next,
            })
            .unwrap();
        let t = catalog
            .snapshot()
            .table(&tenant(), &db(), &TableName::new("customers").unwrap())
            .unwrap();
        assert_eq!(t.version, 2);
        assert_eq!(t.columns.len(), 2);
    }

    #[test]
    fn drop_database_cascades() {
        let catalog = Catalog::replay(&ddl()).unwrap();
        catalog
            .apply(&DdlMutation::DropDatabase {
                tenant: tenant(),
                database: db(),
            })
            .unwrap();
        assert!(catalog.snapshot().database(&tenant(), &db()).is_err());
    }
}
