use std::{
    collections::BTreeMap, error::Error, fmt::Debug, ops::RangeBounds, path::Path, sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use openraft::{
    alias::SnapshotDataOf,
    storage::{IOFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot},
    AnyError, Entry, EntryPayload, ErrorVerb, LogId, OptionalSend, RaftLogId, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use rand::Rng;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, Options, DB};
use serde::{Deserialize, Serialize};

use super::{typ, TypeConfig};

pub type RocksNodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Set { key: String, value: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RocksSnapshot {
    pub meta: SnapshotMeta<TypeConfig>,
    pub data: Box<typ::SnapshotData>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateMachine {
    pub last_applied_log: Option<LogId<RocksNodeId>>,
    pub last_membership: StoredMembership<TypeConfig>,
    pub data: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct RocksStateMachine {
    db: Arc<DB>,
    pub sm: StateMachine,
}

impl RocksStateMachine {
    async fn new(db: Arc<DB>) -> RocksStateMachine {
        let mut state_machine = Self {
            db,
            sm: Default::default(),
        };
        let snapshot = state_machine.get_current_snapshot().await.unwrap();
        if let Some(s) = snapshot {
            let prev: StateMachine = *s.snapshot;
            state_machine.sm = prev;
        }
        state_machine
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let data = &self.sm;
        let last_applied_log = self.sm.last_applied_log;
        let last_membership = self.sm.last_membership.clone();
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
        let snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: Box::new(data.clone()),
        };
        let serialized_snapshot = serde_json::to_vec(&snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;
        self.db
            .put_cf(
                self.db.cf_handle("sm_meta").unwrap(),
                "snapshot",
                serialized_snapshot,
            )
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(data.clone()),
        })
    }
}

impl RaftStateMachine<TypeConfig> for RocksStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<RocksNodeId>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
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

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
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
        let new_snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: snapshot,
        };
        let updated_state_machine: StateMachine = *new_snapshot.data.clone();
        self.sm = updated_state_machine;
        let serialized_snapshot = serde_json::to_vec(&new_snapshot)
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;
        self.db
            .put_cf(
                self.db.cf_handle("sm_meta").unwrap(),
                "snapshot",
                serialized_snapshot,
            )
            .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;
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
            .map_err(|e| StorageError::write_snapshot(None, AnyError::new(&e)))?;
        let bytes = match x {
            Some(x) => x,
            None => return Ok(None),
        };
        let snapshot: RocksSnapshot = serde_json::from_slice(&bytes)
            .map_err(|e| StorageError::write_snapshot(None, AnyError::new(&e)))?;
        let data = snapshot.data.clone();
        Ok(Some(Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(*data),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct RocksLogStore {
    db: Arc<DB>,
}

type StorageResult<T> = Result<T, StorageError<TypeConfig>>;

fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

mod meta {
    use openraft::ErrorSubject;
    use openraft::LogId;

    use super::RocksNodeId;
    use super::TypeConfig;

    pub(crate) trait StoreMeta {
        const KEY: &'static str;
        type Value: serde::Serialize + serde::de::DeserializeOwned;
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<TypeConfig>;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct Vote {}

    impl StoreMeta for LastPurged {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogId<u64>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
            ErrorSubject::Store
        }
    }

    impl StoreMeta for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<RocksNodeId>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
            ErrorSubject::Vote
        }
    }
}

impl RocksLogStore {
    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn get_meta<M: meta::StoreMeta>(&self) -> Result<Option<M::Value>, StorageError<TypeConfig>> {
        let v = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(|e| StorageError::new(M::subject(None), ErrorVerb::Read, AnyError::new(&e)))?;

        let t = match v {
            Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(|e| {
                StorageError::new(M::subject(None), ErrorVerb::Read, AnyError::new(&e))
            })?),
            None => None,
        };
        Ok(t)
    }

    fn put_meta<M: meta::StoreMeta>(
        &self,
        value: &M::Value,
    ) -> Result<(), StorageError<TypeConfig>> {
        let json_value = serde_json::to_vec(value).map_err(|e| {
            StorageError::new(M::subject(Some(value)), ErrorVerb::Write, AnyError::new(&e))
        })?;
        self.db
            .put_cf(self.cf_meta(), M::KEY, json_value)
            .map_err(|e| {
                StorageError::new(M::subject(Some(value)), ErrorVerb::Write, AnyError::new(&e))
            })?;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for RocksLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        let mut res = Vec::new();
        let it = self.db.iterator_cf(
            self.cf_logs(),
            rocksdb::IteratorMode::From(&start, Direction::Forward),
        );
        for item_res in it {
            let (id, val) = item_res.map_err(read_logs_err)?;
            let id = bin_to_id(&id);
            if !range.contains(&id) {
                break;
            }
            let entry: Entry<_> = serde_json::from_slice(&val).map_err(read_logs_err)?;
            assert_eq!(id, entry.log_id.index);
            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RocksNodeId>>, StorageError<TypeConfig>> {
        self.get_meta::<meta::Vote>()
    }
}

impl RaftLogStorage<TypeConfig> for RocksLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
            .next();
        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
                let ent = serde_json::from_slice::<Entry<TypeConfig>>(&entry_bytes)
                    .map_err(read_logs_err)?;
                Some(ent.log_id)
            }
        };
        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;
        let last_log_id = match last_log_id {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<RocksNodeId>,
    ) -> Result<(), StorageError<TypeConfig>> {
        self.put_meta::<meta::Vote>(vote)?;
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_vote(&e))?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            self.db
                .put_cf(
                    self.cf_logs(),
                    id,
                    serde_json::to_vec(&entry).map_err(|e| StorageError::write_logs(&e))?,
                )
                .map_err(|e| StorageError::write_logs(&e))?;
        }
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_logs(&e))?;
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RocksNodeId>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageError::write_logs(&e))?;
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<RocksNodeId>) -> Result<(), StorageError<TypeConfig>> {
        self.put_meta::<meta::LastPurged>(&log_id)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageError::write_logs(&e))?;
        Ok(())
    }
}

pub async fn new<P: AsRef<Path>>(db_path: P) -> (RocksLogStore, RocksStateMachine) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    let meta = ColumnFamilyDescriptor::new("meta", Options::default());
    let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());
    let db = DB::open_cf_descriptors(&db_opts, db_path, vec![meta, sm_meta, logs]).unwrap();
    let db = Arc::new(db);
    (
        RocksLogStore { db: db.clone() },
        RocksStateMachine::new(db).await,
    )
}

fn read_logs_err(e: impl Error + 'static) -> StorageError<TypeConfig> {
    StorageError::read_logs(&e)
}
