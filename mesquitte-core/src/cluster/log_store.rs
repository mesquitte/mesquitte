use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::debug;
use openraft::{
    storage::{IOFlushed, RaftLogStorage},
    Entry, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, StorageError, Vote,
};
use rust_rocksdb::{ColumnFamily, Direction, IteratorMode, DB};

use super::{NodeId, TypeConfig};

type StorageResult<T> = Result<T, StorageError<TypeConfig>>;

#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<DB>,
}

fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

impl LogStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

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
            .map_err(|e| StorageError::new(M::subject(None), ErrorVerb::Read, &e))?;

        let t = match v {
            None => None,
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| StorageError::new(M::subject(None), ErrorVerb::Read, &e))?,
            ),
        };
        Ok(t)
    }

    fn put_meta<M: meta::StoreMeta>(
        &self,
        value: &M::Value,
    ) -> Result<(), StorageError<TypeConfig>> {
        let encoded = bincode::serialize(value)
            .map_err(|e| StorageError::new(M::subject(Some(value)), ErrorVerb::Write, &e))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, encoded)
            .map_err(|e| StorageError::new(M::subject(Some(value)), ErrorVerb::Write, &e))?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };

        let mut res = Vec::new();

        let it = self.db.iterator_cf(
            self.cf_logs(),
            IteratorMode::From(&start, Direction::Forward),
        );
        for item_res in it {
            let (id, val) = item_res.map_err(|e| StorageError::read_logs(&e))?;

            let id = bin_to_id(&id);
            if !range.contains(&id) {
                break;
            }

            let entry: Entry<_> =
                bincode::deserialize(&val).map_err(|e| StorageError::read_logs(&e))?;

            assert_eq!(id, entry.log_id.index);

            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<TypeConfig>> {
        self.get_meta::<meta::Vote>()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), IteratorMode::End)
            .next();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(|e| StorageError::read_logs(&e))?;
                let ent = bincode::deserialize::<Entry<TypeConfig>>(&entry_bytes)
                    .map_err(|e| StorageError::read_logs(&e))?;
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

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<TypeConfig>> {
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
                    bincode::serialize(&entry).map_err(|e| StorageError::write_logs(&e))?,
                )
                .map_err(|e| StorageError::write_logs(&e))?;
        }

        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_logs(&e))?;

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        debug!("truncate: [{:?}, +oo)", log_id);

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

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        debug!("delete_log: [0, {:?}]", log_id);

        self.put_meta::<meta::LastPurged>(&log_id)?;

        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }
}

mod meta {
    use openraft::{ErrorSubject, LogId};
    use serde::{de::DeserializeOwned, Serialize};

    use crate::cluster::{NodeId, TypeConfig};

    pub(crate) trait StoreMeta {
        const KEY: &'static str;
        type Value: Serialize + DeserializeOwned;
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<TypeConfig>;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct Vote {}

    impl StoreMeta for LastPurged {
        const KEY: &'static str = "last_purged_log";
        type Value = LogId<u64>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
            ErrorSubject::Store
        }
    }

    impl StoreMeta for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<NodeId>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
            ErrorSubject::Vote
        }
    }
}
