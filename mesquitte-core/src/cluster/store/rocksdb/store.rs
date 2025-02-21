use std::{collections::BTreeMap, path::Path, sync::Arc};

use log::debug;
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError,
    StoredMembership,
    alias::SnapshotDataOf,
    entry::RaftEntry as _,
    storage::{RaftStateMachine, Snapshot},
};
use parking_lot::RwLock;
use rand::Rng as _;
use rust_rocksdb::{ColumnFamilyDescriptor, DB, Options};
use serde::{Deserialize, Serialize};

use crate::cluster::{LogStore, TypeConfig, typ};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: String },
}

impl Request {
    pub fn set(key: impl ToString, value: impl ToString) -> Self {
        Self::Set {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TypeConfig>,
    pub data: typ::SnapshotData,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<TypeConfig>>,
    pub last_membership: StoredMembership<TypeConfig>,
    pub data: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct StateMachineStore {
    pub sm: Arc<RwLock<StateMachineData>>,
    db: Arc<DB>,
}

impl StateMachineStore {
    async fn new(db: Arc<DB>) -> Arc<Self> {
        let state_machine = Self {
            db,
            sm: Default::default(),
        };
        let mut state_machine = Arc::new(state_machine);
        let snapshot = state_machine.get_current_snapshot().await.unwrap();
        if let Some(s) = snapshot {
            let prev: StateMachineData = s.snapshot;
            let mut sm = state_machine.sm.write();
            *sm = prev;
        }

        state_machine
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let sm;
        let last_applied_log;
        let last_membership;
        {
            sm = self.sm.read();
            last_applied_log = sm.last_applied_log;
            last_membership = sm.last_membership.clone();
        }
        let snapshot_idx: u64 = rand::rng().random_range(0..1000);
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: sm.clone(),
        };

        let serialized_snapshot = bincode::serialize(&snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;

        self.db
            .put_cf(
                self.db.cf_handle("sm_meta").unwrap(),
                "snapshot",
                serialized_snapshot,
            )
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;

        Ok(Snapshot {
            meta,
            snapshot: sm.clone(),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
    {
        let state_machine = self.sm.read();
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let entries_iter = entries.into_iter();
        let mut res = Vec::with_capacity(entries_iter.size_hint().0);
        let mut sm = self.sm.write();

        for entry in entries_iter {
            debug!("{} replicate to sm", entry.log_id);
            sm.last_applied_log = Some(entry.log_id());
            match entry.payload {
                EntryPayload::Blank => res.push(Response { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    Request::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(Response {
                            value: Some(value.clone()),
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<TypeConfig>, StorageError<TypeConfig>> {
        Ok(Default::default())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: SnapshotDataOf<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot,
        };
        let updated_state_machine: StateMachineData = new_snapshot.data.clone();
        {
            let mut sm = self.sm.write();
            *sm = updated_state_machine;
        }
        let serialized_snapshot = bincode::serialize(&new_snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        self.db
            .put_cf(
                self.db.cf_handle("sm_meta").unwrap(),
                "snapshot",
                serialized_snapshot,
            )
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        let x = self
            .db
            .get_cf(self.db.cf_handle("sm_meta").unwrap(), "snapshot")
            .map_err(|e| StorageError::write_snapshot(None, &e))?;
        let bytes = match x {
            Some(x) => x,
            None => return Ok(None),
        };
        let snapshot: StoredSnapshot =
            bincode::deserialize(&bytes).map_err(|e| StorageError::write_snapshot(None, &e))?;
        let data = snapshot.data.clone();

        Ok(Some(Snapshot {
            meta: snapshot.meta,
            snapshot: data,
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

pub async fn new<C: RaftTypeConfig, P: AsRef<Path>>(
    db_path: P,
) -> (LogStore<C>, Arc<StateMachineStore>) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let meta = ColumnFamilyDescriptor::new("meta", Options::default());
    let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let db = DB::open_cf_descriptors(&db_opts, db_path, vec![meta, sm_meta, logs]).unwrap();
    let db = Arc::new(db);

    (LogStore::new(db.clone()), StateMachineStore::new(db).await)
}

#[cfg(test)]
mod tests {
    use openraft::{
        StorageError,
        testing::log::{StoreBuilder, Suite},
    };
    use tempfile::TempDir;

    use crate::cluster::*;

    struct RocksBuilder {}

    impl StoreBuilder<TypeConfig, LogStore<TypeConfig>, Arc<StateMachineStore>, TempDir>
        for RocksBuilder
    {
        async fn build(
            &self,
        ) -> Result<(TempDir, LogStore<TypeConfig>, Arc<StateMachineStore>), StorageError<TypeConfig>>
        {
            let td = TempDir::new().expect("couldn't create temp dir");
            let (log_store, sm) = super::new(td.path()).await;
            Ok((td, log_store, sm))
        }
    }

    #[tokio::test]
    pub async fn test_rocks_store() -> Result<(), StorageError<TypeConfig>> {
        Suite::test_all(RocksBuilder {}).await?;
        Ok(())
    }
}
