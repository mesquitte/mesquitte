use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use log::debug;
use openraft::{
    alias::SnapshotDataOf,
    storage::{RaftStateMachine, Snapshot},
    Entry, EntryPayload, LogId, RaftLogId as _, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership,
};
use rand::Rng as _;
use rust_rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};

use super::{typ, LogStore, NodeId, TypeConfig};

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

    /// The data of the state machine at the time of this snapshot.
    pub data: Box<typ::SnapshotData>,
}

/// Data contained in the Raft state machine.
///
/// Note that we are using `serde` to serialize the
/// `data`, which has a implementation to be serialized. Note that for this test we set both the key
/// and value as String, but you could set any type of value that has the serialization impl.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<TypeConfig>,

    /// Application data.
    pub data: BTreeMap<String, String>,
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug, Clone)]
pub struct StateMachineStore {
    sm: StateMachineData,
    db: Arc<DB>,
}

impl StateMachineStore {
    async fn new(db: Arc<DB>) -> Self {
        let mut state_machine = Self {
            db,
            sm: Default::default(),
        };
        let snapshot = state_machine.get_current_snapshot().await.unwrap();

        // Restore previous state from snapshot
        if let Some(s) = snapshot {
            let prev: StateMachineData = *s.snapshot;
            state_machine.sm = prev;
        }

        state_machine
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let last_applied_log = self.sm.last_applied_log;
        let last_membership = self.sm.last_membership.clone();
        // Generate a random snapshot index.
        let snapshot_idx: u64 = rand::thread_rng().gen_range(0..1000);

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
            data: Box::new(self.sm.clone()),
        };

        let serialized_snapshot = serde_json::to_vec(&snapshot)
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
            snapshot: Box::new(self.sm.clone()),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
    {
        Ok((self.sm.last_applied_log, self.sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let entries_iter = entries.into_iter();
        let mut res = Vec::with_capacity(entries_iter.size_hint().0);

        let sm = &mut self.sm;

        for entry in entries_iter {
            debug!("{} replicate to sm", entry.log_id);

            sm.last_applied_log = Some(*entry.get_log_id());

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
    ) -> Result<Box<SnapshotDataOf<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(Box::default())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: Box<SnapshotDataOf<TypeConfig>>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot,
        };

        // Update the state machine.
        let updated_state_machine: StateMachineData = *new_snapshot.data.clone();
        // .map_err(|e| StorageError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;

        self.sm = updated_state_machine;

        // Save snapshot

        let serialized_snapshot = serde_json::to_vec(&new_snapshot)
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
            serde_json::from_slice(&bytes).map_err(|e| StorageError::write_snapshot(None, &e))?;

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

pub async fn new<P: Into<PathBuf>>(db_path: P) -> (LogStore, StateMachineStore) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let meta = ColumnFamilyDescriptor::new("meta", Options::default());
    let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let db = DB::open_cf_descriptors(&db_opts, db_path.into(), vec![meta, sm_meta, logs]).unwrap();

    let db = Arc::new(db);
    (LogStore::new(db.clone()), StateMachineStore::new(db).await)
}

#[cfg(test)]
mod tests {
    use openraft::{
        testing::log::{StoreBuilder, Suite},
        StorageError,
    };
    use tempfile::TempDir;

    use super::*;

    struct RocksBuilder {}

    impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, TempDir> for RocksBuilder {
        async fn build(
            &self,
        ) -> Result<(TempDir, LogStore, StateMachineStore), StorageError<TypeConfig>> {
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
