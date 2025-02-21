use std::{fmt::Debug, marker::PhantomData, ops::RangeBounds, sync::Arc};

use heed::{Database, Env, byteorder::BE, types::*};
use log::{debug, info};
use openraft::{
    LogState, OptionalSend, RaftLogReader, RaftTypeConfig, StorageError,
    alias::{EntryOf, LogIdOf, VoteOf},
    entry::RaftEntry as _,
    storage::{IOFlushed, RaftLogStorage},
};

#[derive(Debug, Clone)]
pub struct LogStore<C: RaftTypeConfig> {
    env: Arc<Env>,
    _p: PhantomData<C>,
}

impl<C> LogStore<C>
where
    C: RaftTypeConfig,
{
    pub fn new(env: Arc<Env>) -> Self {
        Self {
            env,
            _p: Default::default(),
        }
    }

    fn get_meta<M: meta::StoreMeta<C>>(&self) -> Result<Option<M::Value>, StorageError<C>> {
        let rtxn = self.env.read_txn().map_err(M::read_err)?;
        let db = self
            .env
            .open_database::<Str, Bytes>(&rtxn, Some("meta"))
            .map_err(M::read_err)?
            .unwrap();
        let v: Option<&[u8]> = db.get(&rtxn, M::KEY).map_err(M::read_err)?;

        let t = match v {
            None => None,
            Some(bytes) => Some(bincode::deserialize(bytes).map_err(M::read_err)?),
        };
        Ok(t)
    }

    fn put_meta<M: meta::StoreMeta<C>>(&self, value: &M::Value) -> Result<(), StorageError<C>> {
        let encoded = bincode::serialize(value).map_err(|e| M::write_err(value, e))?;
        let mut wtxn = self.env.write_txn().map_err(|e| M::write_err(value, e))?;
        let db: Database<Str, Bytes> = self
            .env
            .create_database(&mut wtxn, Some("meta"))
            .map_err(|e| M::write_err(value, e))?;
        db.put(&mut wtxn, M::KEY, &encoded)
            .map_err(|e| M::write_err(value, e))?;
        wtxn.commit().map_err(|e| M::write_err(value, e))?;

        Ok(())
    }
}

impl<C> RaftLogReader<C> for LogStore<C>
where
    C: RaftTypeConfig,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C>> {
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
            let entry: EntryOf<C> =
                bincode::deserialize(val).map_err(|e| StorageError::read_logs(&e))?;
            assert_eq!(id, entry.index());
            debug!("log entry: {:?}", entry);
            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
        self.get_meta::<meta::Vote>()
    }
}

impl<C> RaftLogStorage<C> for LogStore<C>
where
    C: RaftTypeConfig,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
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
                let ent = bincode::deserialize::<EntryOf<C>>(entry_bytes)
                    .map_err(|e| StorageError::read_logs(&e))?;
                Some(ent.log_id())
            }
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

        let last_log_id = match last_log_id {
            None => last_purged_log_id.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), StorageError<C>> {
        self.put_meta::<meta::Vote>(vote)?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), StorageError<C>>
    where
        I: IntoIterator<Item = EntryOf<C>> + Send,
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
                &entry.index(),
                &bincode::serialize(&entry).map_err(|e| StorageError::write_logs(&e))?,
            )
            .map_err(|e| StorageError::write_logs(&e))?;
            wtxn.commit().map_err(|e| StorageError::write_logs(&e))?;
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
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

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
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
    use openraft::{
        AnyError, ErrorSubject, ErrorVerb, RaftTypeConfig, StorageError,
        alias::{LogIdOf, VoteOf},
    };
    use serde::{Serialize, de::DeserializeOwned};

    pub(crate) trait StoreMeta<C: RaftTypeConfig> {
        const KEY: &'static str;
        type Value: Serialize + DeserializeOwned;
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<C>;
        fn read_err(e: impl std::error::Error + 'static) -> StorageError<C> {
            StorageError::new(Self::subject(None), ErrorVerb::Read, AnyError::new(&e))
        }
        fn write_err(v: &Self::Value, e: impl std::error::Error + 'static) -> StorageError<C> {
            StorageError::new(Self::subject(Some(v)), ErrorVerb::Write, AnyError::new(&e))
        }
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct Vote {}

    impl<C> StoreMeta<C> for LastPurged
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "last_purged_log";
        type Value = LogIdOf<C>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<C> {
            ErrorSubject::Store
        }
    }

    impl<C> StoreMeta<C> for Vote
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "vote";
        type Value = VoteOf<C>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<C> {
            ErrorSubject::Vote
        }
    }
}
