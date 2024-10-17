use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use heed::{byteorder::BE, types::*, Database, Env, EnvOpenOptions};
use log::debug;
use openraft::{
    alias::SnapshotDataOf,
    storage::{RaftStateMachine, Snapshot},
    Entry, EntryPayload, LogId, RaftLogId as _, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership,
};
use parking_lot::RwLock;
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::cluster::{typ, LogStore, NodeId, TypeConfig};

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
    pub data: Box<typ::SnapshotData>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<TypeConfig>,
    pub data: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct StateMachineStore {
    pub sm: Arc<RwLock<StateMachineData>>,
    env: Arc<Env>,
}

impl StateMachineStore {
    async fn new(env: Arc<Env>) -> Arc<Self> {
        let state_machine = Self {
            env,
            sm: Default::default(),
        };
        let mut state_machine = Arc::new(state_machine);
        let snapshot = state_machine.get_current_snapshot().await.unwrap();
        if let Some(s) = snapshot {
            let prev: StateMachineData = *s.snapshot;
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
            data: Box::new(sm.clone()),
        };

        let serialized_snapshot = bincode::serialize(&snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;

        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        let db: Database<Str, Bytes> = self
            .env
            .create_database(&mut wtxn, Some("sm_meta"))
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        db.put(&mut wtxn, "snapshot", &serialized_snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        wtxn.commit()
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(sm.clone()),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
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
        let updated_state_machine: StateMachineData = *new_snapshot.data.clone();
        {
            let mut sm = self.sm.write();
            *sm = updated_state_machine;
        }
        let serialized_snapshot = bincode::serialize(&new_snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        let db: Database<Str, Bytes> = self
            .env
            .create_database(&mut wtxn, Some("sm_meta"))
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        db.put(&mut wtxn, "snapshot", &serialized_snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        wtxn.commit()
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| StorageError::write_snapshot(None, &e))?;
        let db = self
            .env
            .open_database::<Str, Bytes>(&rtxn, Some("sm_meta"))
            .map_err(|e| StorageError::write_snapshot(None, &e))?
            .unwrap();
        // let mut wtxn = self
        //     .env
        //     .write_txn()
        //     .map_err(|e| StorageError::write_snapshot(None, &e))?;
        // let db: Database<Str, Bytes> = self
        //     .env
        //     .create_database(&mut wtxn, Some("sm_meta"))
        //     .map_err(|e| StorageError::write_snapshot(None, &e))?;
        let x: Option<&[u8]> = db
            .get(&rtxn, "snapshot")
            .map_err(|e| StorageError::write_snapshot(None, &e))?;
        let bytes = match x {
            Some(x) => x,
            None => return Ok(None),
        };
        let snapshot: StoredSnapshot =
            bincode::deserialize(bytes).map_err(|e| StorageError::write_snapshot(None, &e))?;
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

pub async fn new<P: Into<PathBuf>>(db_path: P) -> (LogStore, Arc<StateMachineStore>) {
    let p = db_path.into();
    fs::create_dir_all(p.clone()).await.unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024)
            .max_dbs(3000)
            .open(p)
            .unwrap()
    };
    let mut wtxn = env.write_txn().unwrap();
    env.create_database::<Str, Bytes>(&mut wtxn, Some("meta"))
        .unwrap();
    env.create_database::<Str, Bytes>(&mut wtxn, Some("sm_meta"))
        .unwrap();
    env.create_database::<U64<BE>, Bytes>(&mut wtxn, Some("logs"))
        .unwrap();
    wtxn.commit().unwrap();
    let env = Arc::new(env);

    (
        LogStore::new(env.clone()),
        StateMachineStore::new(env).await,
    )
}

#[cfg(test)]
mod tests {
    use openraft::{
        testing::log::{StoreBuilder, Suite},
        StorageError,
    };
    use tempfile::{tempdir, TempDir};

    use crate::cluster::*;

    struct HeedBuilder {}

    impl StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>, TempDir> for HeedBuilder {
        async fn build(
            &self,
        ) -> Result<(TempDir, LogStore, Arc<StateMachineStore>), StorageError<TypeConfig>> {
            let tmp_dir = tempdir().expect("could not create temp dir");
            let file_path = tmp_dir.path().join("cluster.mdb");
            let (log_store, sm) = super::new(file_path.as_path()).await;
            Ok((tmp_dir, log_store, sm))
        }
    }

    #[tokio::test]
    pub async fn test_heed_store() -> Result<(), StorageError<TypeConfig>> {
        Suite::test_all(HeedBuilder {}).await?;
        Ok(())
    }
}
