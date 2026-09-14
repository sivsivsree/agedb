//! Storage key layout (see "Multi-tenancy" in ARCHITECTURE.md).
//!
//! Keys are S3-shaped even though v0.1 writes them to a local filesystem, so
//! pointing the object store at a bucket later is a configuration change:
//!
//! ```text
//! tenants/{tenant}/databases/{db}/tables/{table}/p{partition}/manifest.json
//! tenants/{tenant}/databases/{db}/tables/{table}/p{partition}/wal/current.log
//! tenants/{tenant}/databases/{db}/tables/{table}/p{partition}/segments/seg-00000001.parquet
//! system/wal/current.log        <- the DDL log the catalog is replayed from
//! ```
//!
//! Every component is a validated identifier (see `adb_core::ids`), so these
//! strings cannot contain `..` or `/`.

use adb_core::{DatabaseName, TableName, TenantId};

pub fn system_wal() -> String {
    "system/wal/current.log".to_string()
}

pub fn tenant_prefix(tenant: &TenantId) -> String {
    format!("tenants/{tenant}")
}

pub fn database_prefix(tenant: &TenantId, db: &DatabaseName) -> String {
    format!("tenants/{tenant}/databases/{db}")
}

pub fn table_prefix(tenant: &TenantId, db: &DatabaseName, table: &TableName) -> String {
    format!("{}/tables/{table}", database_prefix(tenant, db))
}

pub fn partition_prefix(
    tenant: &TenantId,
    db: &DatabaseName,
    table: &TableName,
    partition: u32,
) -> String {
    format!("{}/p{partition}", table_prefix(tenant, db, table))
}

pub fn manifest(tenant: &TenantId, db: &DatabaseName, table: &TableName, partition: u32) -> String {
    format!(
        "{}/manifest.json",
        partition_prefix(tenant, db, table, partition)
    )
}

pub fn wal(tenant: &TenantId, db: &DatabaseName, table: &TableName, partition: u32) -> String {
    format!(
        "{}/wal/current.log",
        partition_prefix(tenant, db, table, partition)
    )
}

pub fn segment(
    tenant: &TenantId,
    db: &DatabaseName,
    table: &TableName,
    partition: u32,
    segment_id: u64,
) -> String {
    format!(
        "{}/segments/seg-{segment_id:08}.parquet",
        partition_prefix(tenant, db, table, partition)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_keys_are_tenant_scoped_and_zero_padded() {
        let key = segment(
            &TenantId::new("tenant_123").unwrap(),
            &DatabaseName::new("crm").unwrap(),
            &TableName::new("events").unwrap(),
            2,
            7,
        );
        assert_eq!(
            key,
            "tenants/tenant_123/databases/crm/tables/events/p2/segments/seg-00000007.parquet"
        );
        // Zero padding keeps lexical order equal to numeric order, which is what
        // arrival-order reasoning in `table.rs` depends on.
        assert!(
            segment(
                &TenantId::new("t").unwrap(),
                &DatabaseName::new("d").unwrap(),
                &TableName::new("x").unwrap(),
                0,
                9
            ) < segment(
                &TenantId::new("t").unwrap(),
                &DatabaseName::new("d").unwrap(),
                &TableName::new("x").unwrap(),
                0,
                10
            )
        );
    }
}
