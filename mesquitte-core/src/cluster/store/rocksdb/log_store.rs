use std::{fmt::Debug, marker::PhantomData, ops::RangeBounds, sync::Arc};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::debug;
use openraft::{
    alias::{EntryOf, LogIdOf, VoteOf},
    entry::RaftEntry as _,
    storage::{IOFlushed, RaftLogStorage},
    LogState, OptionalSend, RaftLogReader, RaftTypeConfig, StorageError,
};
use rust_rocksdb::{ColumnFamily, Direction, IteratorMode, DB};

#[derive(Debug, Clone)]
pub struct LogStore<C: RaftTypeConfig> {
    db: Arc<DB>,
    _p: PhantomData<C>,
}

fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

impl<C: RaftTypeConfig> LogStore<C> {
    pub fn new(db: Arc<DB>) -> Self {
        db.cf_handle("meta")
            .expect("column family `meta` not found");
        db.cf_handle("logs")
            .expect("column family `logs` not found");
        Self {
            db,
            _p: Default::default(),
        }
    }

    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn get_meta<M: meta::StoreMeta<C>>(&self) -> Result<Option<M::Value>, StorageError<C>> {
        let v = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(M::read_err)?;

        let t = match v {
            None => None,
            Some(bytes) => Some(bincode::deserialize(&bytes).map_err(M::read_err)?),
        };
        Ok(t)
    }

    fn put_meta<M: meta::StoreMeta<C>>(&self, value: &M::Value) -> Result<(), StorageError<C>> {
        let encoded = bincode::serialize(value).map_err(|e| M::write_err(value, e))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, encoded)
            .map_err(|e| M::write_err(value, e))?;

        Ok(())
    }
}

impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C>> {
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

            let entry: EntryOf<C> =
                bincode::deserialize(&val).map_err(|e| StorageError::read_logs(&e))?;

            assert_eq!(id, entry.index());

            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
        self.get_meta::<meta::Vote>()
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), IteratorMode::End)
            .next();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(|e| StorageError::read_logs(&e))?;
                let ent = bincode::deserialize::<EntryOf<C>>(&entry_bytes)
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
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_vote(&e))?;
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
            let id = id_to_bin(entry.index());
            assert_eq!(bin_to_id(&id), entry.index());
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

    async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
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

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
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
    use openraft::{
        alias::{LogIdOf, VoteOf},
        AnyError, ErrorSubject, ErrorVerb, RaftTypeConfig, StorageError,
    };
    use serde::{de::DeserializeOwned, Serialize};

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

    impl<C: RaftTypeConfig> StoreMeta<C> for LastPurged {
        const KEY: &'static str = "last_purged_log";
        type Value = LogIdOf<C>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<C> {
            ErrorSubject::Store
        }
    }

    impl<C: RaftTypeConfig> StoreMeta<C> for Vote {
        const KEY: &'static str = "vote";
        type Value = VoteOf<C>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<C> {
            ErrorSubject::Vote
        }
    }
}
