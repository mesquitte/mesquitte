use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

use heed::{byteorder::BE, types::*, Database, Env};
use log::{debug, info};
use openraft::{
    storage::{IOFlushed, RaftLogStorage},
    Entry, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, StorageError, Vote,
};

use crate::cluster::{NodeId, TypeConfig};

type StorageResult<T> = Result<T, StorageError<TypeConfig>>;

#[derive(Debug, Clone)]
pub struct LogStore {
    env: Arc<Env>,
}

impl LogStore {
    pub fn new(env: Arc<Env>) -> Self {
        Self { env }
    }

    fn get_meta<M: meta::StoreMeta>(&self) -> Result<Option<M::Value>, StorageError<TypeConfig>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| StorageError::new(M::subject(None), ErrorVerb::Read, &e))?;
        let db = self
            .env
            .open_database::<Str, Bytes>(&rtxn, Some("meta"))
            .map_err(|e| StorageError::new(M::subject(None), ErrorVerb::Read, &e))?
            .unwrap();
        let v: Option<&[u8]> = db
            .get(&rtxn, M::KEY)
            .map_err(|e| StorageError::new(M::subject(None), ErrorVerb::Read, &e))?;

        let t = match v {
            None => None,
            Some(bytes) => Some(
                bincode::deserialize(bytes)
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
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| StorageError::new(M::subject(Some(value)), ErrorVerb::Write, &e))?;
        let db: Database<Str, Bytes> = self
            .env
            .create_database(&mut wtxn, Some("meta"))
            .map_err(|e| StorageError::new(M::subject(Some(value)), ErrorVerb::Write, &e))?;
        db.put(&mut wtxn, M::KEY, &encoded)
            .map_err(|e| StorageError::new(M::subject(Some(value)), ErrorVerb::Write, &e))?;
        wtxn.commit()
            .map_err(|e| StorageError::new(M::subject(Some(value)), ErrorVerb::Write, &e))?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
        debug!("log range: {:?}", range);
        let mut res = Vec::new();
        let rtxn = self.env.read_txn().unwrap();
        let db = self
            .env
            .open_database::<U64<BE>, Bytes>(&rtxn, Some("logs"))
            .map_err(|e| StorageError::write_logs(&e))?
            .unwrap();
        for item in db.range(&rtxn, &range).unwrap() {
            let (id, val) = item.unwrap();
            if !range.contains(&id) {
                break;
            }
            let entry: Entry<_> =
                bincode::deserialize(val).map_err(|e| StorageError::read_logs(&e))?;
            assert_eq!(id, entry.log_id.index);
            debug!("log entry: {:?}", entry);
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
        let rtxn = self.env.read_txn().unwrap();
        let db = self
            .env
            .open_database::<U64<BE>, Bytes>(&rtxn, Some("logs"))
            .map_err(|e| StorageError::write_logs(&e))?
            .unwrap();
        let last = db.last(&rtxn).unwrap();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res;
                let ent = bincode::deserialize::<Entry<TypeConfig>>(entry_bytes)
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
            debug!("append entries: {:?}", entry);
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|e| StorageError::write_logs(&e))?;
            let db: Database<U64<BE>, Bytes> = self
                .env
                .create_database(&mut wtxn, Some("logs"))
                .map_err(|e| StorageError::write_logs(&e))?;
            db.put(
                &mut wtxn,
                &entry.log_id.index,
                &bincode::serialize(&entry).map_err(|e| StorageError::write_logs(&e))?,
            )
            .map_err(|e| StorageError::write_logs(&e))?;
            wtxn.commit().map_err(|e| StorageError::write_logs(&e))?;
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        debug!("truncate: [{:?}, +oo)", log_id);
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| StorageError::write_logs(&e))?;
        let db: Database<U64<BE>, Bytes> = self
            .env
            .create_database(&mut wtxn, Some("logs"))
            .map_err(|e| StorageError::write_logs(&e))?;
        let range = log_id.index..;
        let deleted: usize = db
            .delete_range(&mut wtxn, &range)
            .map_err(|e| StorageError::write_logs(&e))?;
        info!("truncate logs: {}", deleted);
        wtxn.commit().map_err(|e| StorageError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        debug!("delete_log: [0, {:?}]", log_id);

        self.put_meta::<meta::LastPurged>(&log_id)?;

        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| StorageError::write_logs(&e))?;
        let db: Database<U64<BE>, Bytes> = self
            .env
            .create_database(&mut wtxn, Some("logs"))
            .map_err(|e| StorageError::write_logs(&e))?;
        let range = 0..=log_id.index;
        let deleted: usize = db
            .delete_range(&mut wtxn, &range)
            .map_err(|e| StorageError::write_logs(&e))?;
        wtxn.commit().map_err(|e| StorageError::write_logs(&e))?;
        info!("purged logs: {}", deleted);
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
