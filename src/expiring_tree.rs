use chrono::{offset::Utc, DateTime};
use log::debug;
use sled::IVec;
use std::{collections::HashSet, marker::PhantomData};

use crate::{
    encoding::Encoding,
    error::Result,
    structured_tree::{
        CompareAndSwapError, StructuredBatch, StructuredIter, StructuredTransactionalTree,
        StructuredTree,
    },
};

#[derive(Clone)]
/// A flash-sympathetic persistent lock-free B+ tree
///
/// This tree keeps track of insert and update times if requested
pub struct ExpiringTree<V, E, F> {
    data: StructuredTree<V, F>,
    expires_at: StructuredTree<DateTime<Utc>, E>,
    expires_at_inverse: StructuredTree<HashSet<IVec>, E>,
    extend_on_update: bool,
    extend_on_fetch: bool,
    expiration_length: chrono::Duration,
}

/// A builder for creating expiring trees.
///
/// This allows setting expiration duration and whether to extend the duration of a value on
/// fetch or update.
pub struct ExpiringTreeBuilder<V, E, F> {
    db: sled::Db,
    data: String,
    extend_on_update: bool,
    extend_on_fetch: bool,
    expiration_length: chrono::Duration,
    value: PhantomData<V>,
    encoding: PhantomData<E>,
    data_encoding: PhantomData<F>,
}

/// An iterator over keys and values in a `Tree`.
pub struct ExpiringIter<'a, V, E, F>(StructuredIter<V, F>, &'a ExpiringTree<V, E, F>);

#[derive(Clone, Debug, Default)]
/// A batch of updates that will be applied atomically to the Tree.
pub struct ExpiringBatch<V, F>(StructuredBatch<V, F>, HashSet<IVec>);

#[derive(Clone)]
/// A transaction that will be applied atomically to the Tree.
pub struct ExpiringTransactionalTree<'a, V, E, F>(
    StructuredTransactionalTree<'a, V, F>,
    &'a ExpiringTree<V, E, F>,
);

impl<V, E, F> ExpiringTree<V, E, F>
where
    E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    F: Encoding<V> + 'static,
{
    /// Clone for structures where V, E, and F aren't Clone
    pub fn cloned(&self) -> Self {
        ExpiringTree {
            data: self.data.cloned(),
            expires_at: self.expires_at.cloned(),
            expires_at_inverse: self.expires_at_inverse.cloned(),
            extend_on_update: self.extend_on_update,
            extend_on_fetch: self.extend_on_fetch,
            expiration_length: self.expiration_length,
        }
    }

    /// Perform a multi-key serializable transaction.
    ///
    /// Transactions also work on tuples of Trees, preserving serializable ACID semantics! In this
    /// example, we treat two trees like a work queue, atomically apply updates to data and move
    /// them from the unprocessed Tree to the processed Tree.
    pub fn transaction<G, R>(&self, g: G) -> sled::TransactionResult<Result<R>>
    where
        G: Fn(ExpiringTransactionalTree<V, E, F>) -> sled::ConflictableTransactionResult<Result<R>>,
    {
        self.data
            .transaction(move |trans_tree| (g)(ExpiringTransactionalTree(trans_tree, &self)))
    }

    /// Create a new batched update that can be atomically applied.
    ///
    /// It is possible to apply a Batch in a transaction as well, which is the way you can apply a Batch to multiple Trees atomically.
    pub fn apply_batch(&self, batch: ExpiringBatch<V, F>) -> Result<()> {
        let keys = batch.1;
        self.data.apply_batch(batch.0)?;

        if self.extend_on_update {
            let now = Utc::now();
            for key in keys {
                self.update_expires_at(key, now)?;
            }
        }

        Ok(())
    }

    /// Compare and swap. Capable of unique creation, conditional modification, or deletion. If
    /// old is None, this will only set the value if it doesn't exist yet. If new is None, will
    /// delete the value if old is correct. If both old and new are Some, will modify the value
    /// if old is correct.
    ///
    /// It returns Ok(Ok(())) if operation finishes successfully.
    ///
    /// If it fails it returns: - Ok(Err(CompareAndSwapError(current, proposed))) if operation
    /// failed to setup a new value. CompareAndSwapError contains current and proposed values.
    /// - Err(Error::Unsupported) if the database is opened in read-only mode.
    pub fn compare_and_swap<K>(
        &self,
        key: K,
        old: Option<V>,
        new: Option<V>,
    ) -> Result<std::result::Result<(), CompareAndSwapError<V>>>
    where
        K: AsRef<[u8]>,
    {
        let to_delete = old.is_some() && new.is_none();
        let to_update = new.is_some();

        let ivec = IVec::from(key.as_ref());

        let res = self.data.compare_and_swap(key, old, new)?;

        let success = res.is_ok();

        if to_delete && success {
            self.remove_expires_at(ivec)?;
        } else if to_update && success && self.extend_on_update {
            self.update_expires_at(ivec, Utc::now())?;
        }

        Ok(res)
    }

    /// Retrieve a value from the Tree if it exists.
    pub fn get<K>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
    {
        let ivec = IVec::from(key.as_ref());
        let opt = self.data.get(key)?;

        if self.extend_on_fetch {
            self.update_expires_at(ivec, Utc::now())?;
        }

        Ok(opt)
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert<K>(&self, key: K, value: V) -> Result<Option<V>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let ivec: IVec = key.as_ref().into();
        let opt = self.data.insert::<K>(key, value)?;

        if self.extend_on_update {
            self.update_expires_at(ivec, Utc::now())?;
        }

        Ok(opt)
    }

    /// Delete a value, returning the old value if it existed.
    pub fn remove<K>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
    {
        let ivec = IVec::from(key.as_ref());
        let opt = self.data.remove(key)?;

        self.remove_expires_at(ivec)?;

        Ok(opt)
    }

    /// Fetch the value, apply a function to it and return the result.
    ///
    /// ### Note
    /// This may call the function multiple times if the value has been changed from other threads
    /// in the meantime.
    pub fn update_and_fetch<K>(
        &self,
        key: K,
        f: impl Fn(Option<V>) -> Option<V>,
    ) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
    {
        let ivec = IVec::from(key.as_ref());
        let opt = self.data.update_and_fetch(key, f)?;

        if opt.is_some() && self.extend_on_update {
            self.update_expires_at(ivec, Utc::now())?;
        } else {
            self.remove_expires_at(ivec)?;
        }

        Ok(opt)
    }

    /// Fetch the value, apply a function to it and return the previous value.
    ///
    /// ### Note
    /// This may call the function multiple times if the value has been changed from other threads in the meantime.
    pub fn fetch_and_update<K>(
        &self,
        key: K,
        f: impl Fn(Option<V>) -> Option<V>,
    ) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
    {
        let ivec = IVec::from(key.as_ref());
        let opt = self.data.fetch_and_update(key, f)?;

        if opt.is_some() && self.extend_on_update {
            self.update_expires_at(ivec, Utc::now())?;
        } else {
            self.remove_expires_at(ivec)?;
        }

        Ok(opt)
    }

    /// Synchronously flushes all dirty IO buffers and calls fsync. If this succeeds, it is guaranteed that all previous writes will be recovered if the system crashes. Returns the number of bytes flushed during this call.
    ///
    /// Flushing can take quite a lot of time, and you should measure the performance impact of using it on realistic sustained workloads running on realistic hardware.
    pub fn flush(&self) -> Result<()> {
        self.data.flush()?;
        self.expires_at.flush()?;
        self.expires_at_inverse.flush()?;
        Ok(())
    }

    /// Returns `true` if the `Tree` contains a value for the specified key.
    pub fn contains_key<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        self.data.contains_key(key)
    }

    /// Create a double-ended iterator over the tuples of keys and values in this tree.
    pub fn iter<'a>(&'a self) -> ExpiringIter<'a, V, E, F> {
        ExpiringIter(self.data.iter(), &self)
    }

    /// Create a double-ended iterator over tuples of keys and values, where the keys fall
    /// within the specified range.
    pub fn range<'a, K, R>(&'a self, range: R) -> ExpiringIter<'a, V, E, F>
    where
        K: AsRef<[u8]>,
        R: std::ops::RangeBounds<K>,
    {
        ExpiringIter(self.data.range(range), &self)
    }

    /// Retrieve the key and value before the provided key, if one exists.
    pub fn get_lt<K>(&self, key: K) -> Result<Option<(IVec, V)>>
    where
        K: AsRef<[u8]>,
    {
        if let Some((k, v)) = self.data.get_lt(key)? {
            if self.extend_on_fetch {
                self.update_expires_at(k.clone(), Utc::now())?;
            }

            return Ok(Some((k, v)));
        }

        Ok(None)
    }

    /// Retrieve the next key and value from the Tree after the provided key.
    ///
    /// ### Note
    /// The order follows the Ord implementation for Vec<u8>:
    ///
    /// `[] < [0] < [255] < [255, 0] < [255, 255] ...`
    ///
    /// To retain the ordering of numerical types use big endian reprensentation
    pub fn get_gt<K>(&self, key: K) -> Result<Option<(IVec, V)>>
    where
        K: AsRef<[u8]>,
    {
        if let Some((k, v)) = self.data.get_gt(key)? {
            if self.extend_on_fetch {
                self.update_expires_at(k.clone(), Utc::now())?;
            }

            return Ok(Some((k, v)));
        }

        Ok(None)
    }

    /// Create an iterator over tuples of keys and values, where the all the keys starts with the
    /// given prefix.
    pub fn scan_prefix<'a, P>(&'a self, prefix: P) -> ExpiringIter<'a, V, E, F>
    where
        P: AsRef<[u8]>,
    {
        ExpiringIter(self.data.scan_prefix(prefix), &self)
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    pub fn pop_max(&self) -> Result<Option<(IVec, V)>> {
        if let Some((k, v)) = self.data.pop_max()? {
            self.remove_expires_at(k.clone())?;

            return Ok(Some((k, v)));
        }

        Ok(None)
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    pub fn pop_min(&self) -> Result<Option<(IVec, V)>> {
        if let Some((k, v)) = self.data.pop_min()? {
            self.remove_expires_at(k.clone())?;

            return Ok(Some((k, v)));
        }

        Ok(None)
    }

    /// Returns the number of elements in this tree.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the `Tree` contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    pub fn clear(&self) -> Result<()> {
        self.data.clear()?;
        self.expires_at.clear()?;
        self.expires_at_inverse.clear()?;
        Ok(())
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> String {
        self.data.name()
    }

    /// Create an iterator over the keys of expired records
    pub fn expired<'a>(&'a self) -> impl 'a + Iterator<Item = IVec> {
        let now: IVec = Utc::now().to_string().into_bytes().into();
        debug!("now: {:?}", now);

        self.expires_at_inverse
            .range(..now)
            .values()
            .filter_map(|res| res.ok())
            .flat_map(|res| res.into_iter())
    }

    fn remove_expires_at(&self, key: IVec) -> Result<()> {
        if let Some(prev) = self.expires_at.remove(key.clone())? {
            self.expires_at_inverse
                .update_and_fetch(prev.to_string().into_bytes(), |opt| {
                    opt.and_then(|mut hs| {
                        hs.remove(&key);
                        if hs.is_empty() {
                            None
                        } else {
                            Some(hs)
                        }
                    })
                })?;
        }

        Ok(())
    }

    fn update_expires_at(&self, key: IVec, now: DateTime<Utc>) -> Result<()> {
        let expires_at = now + self.expiration_length;

        if let Some(prev) = self.expires_at.insert(key.clone(), expires_at)? {
            self.expires_at_inverse
                .update_and_fetch(prev.to_string().into_bytes(), |opt| {
                    opt.and_then(|mut hs| {
                        hs.remove(&key);
                        if hs.is_empty() {
                            None
                        } else {
                            Some(hs)
                        }
                    })
                })?;
        }

        self.expires_at_inverse
            .update_and_fetch(expires_at.to_string().into_bytes(), |opt| {
                let mut hs = opt.unwrap_or(HashSet::new());
                hs.insert(key.clone());
                Some(hs)
            })?;

        Ok(())
    }
}

impl<V, E, F> ExpiringTreeBuilder<V, E, F>
where
    E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    F: Encoding<V> + 'static,
{
    pub(crate) fn new(db: &sled::Db, data: &str) -> Self {
        ExpiringTreeBuilder {
            db: db.clone(),
            data: data.to_owned(),
            extend_on_update: false,
            extend_on_fetch: false,
            expiration_length: chrono::Duration::hours(12),
            value: PhantomData,
            encoding: PhantomData,
            data_encoding: PhantomData,
        }
    }

    /// Create a tree that extends the duration of a record when that record is updated
    pub fn extend_on_update(&mut self) -> &mut Self {
        self.extend_on_update = true;
        self
    }

    /// Create a tree that extends the duration of a record when that record is fetched
    pub fn extend_on_fetch(&mut self) -> &mut Self {
        self.extend_on_fetch = true;
        self
    }

    /// Create a tree with a custom record duration
    pub fn expiration_length(&mut self, expiration_length: chrono::Duration) -> &mut Self {
        self.expiration_length = expiration_length;
        self
    }

    /// Create the tree
    pub fn build(&self) -> Result<ExpiringTree<V, E, F>> {
        Ok(ExpiringTree {
            data: StructuredTree::new(&self.db, &self.data)?,
            expires_at: StructuredTree::new(&self.db, &format!("{}-expires-at", self.data))?,
            expires_at_inverse: StructuredTree::new(
                &self.db,
                &format!("{}-expires-at-inverse", self.data),
            )?,
            extend_on_update: self.extend_on_update,
            extend_on_fetch: self.extend_on_fetch,
            expiration_length: self.expiration_length,
        })
    }
}

impl<'a, V, E, F> ExpiringIter<'a, V, E, F>
where
    E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    F: Encoding<V> + 'static,
{
    /// Iterate over the keys of this Tree
    pub fn keys(self) -> impl 'a + DoubleEndedIterator<Item = Result<IVec>> {
        self.map(|res| res.map(|(key, _)| key))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl 'a + DoubleEndedIterator<Item = Result<V>> {
        self.map(|res| res.map(|(_, v)| v))
    }
}

impl<V, F> ExpiringBatch<V, F>
where
    F: Encoding<V> + 'static,
{
    /// Set a key to a new value
    pub fn insert<K>(&mut self, key: K, value: V) -> Result<()>
    where
        IVec: From<K>,
    {
        let k = IVec::from(key);
        self.1.insert(k.clone());
        self.0.insert::<IVec>(k, value)
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
    where
        IVec: From<K>,
    {
        let k = IVec::from(key);
        self.1.remove(&k);
        self.0.remove::<IVec>(k)
    }
}

impl<'a, V, E, F> ExpiringTransactionalTree<'a, V, E, F>
where
    E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    F: Encoding<V> + 'static,
{
    /// Set a key to a new value
    pub fn insert<K>(
        &self,
        key: K,
        value: V,
    ) -> sled::ConflictableTransactionResult<Result<Option<V>>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let k = IVec::from(key);
        let r = self.0.insert::<IVec>(k.clone(), value)?;

        if self.1.extend_on_update {
            if let Err(e) = self.1.update_expires_at(k, Utc::now()) {
                return Ok(Err(e));
            }
        }

        Ok(r)
    }

    /// Remove a key
    pub fn remove<K>(&self, key: K) -> sled::ConflictableTransactionResult<Result<Option<V>>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let k = IVec::from(key);
        let r = self.0.remove::<IVec>(k.clone())?;

        if let Err(e) = self.1.remove_expires_at(k) {
            return Ok(Err(e));
        }

        Ok(r)
    }

    /// Get the value associated with a key
    pub fn get<K>(&self, key: K) -> sled::ConflictableTransactionResult<Result<Option<V>>>
    where
        K: AsRef<[u8]>,
    {
        let k = key.as_ref().to_vec();

        let r = self.0.get(key)?;

        if self.1.extend_on_fetch {
            if let Err(e) = self.1.update_expires_at(k.into(), Utc::now()) {
                return Ok(Err(e));
            }
        }

        Ok(r)
    }

    /// Atomically apply multiple inserts and removals.
    pub fn apply_batch(
        &self,
        batch: ExpiringBatch<V, F>,
    ) -> sled::ConflictableTransactionResult<Result<()>> {
        let keys = batch.1;
        self.0.apply_batch(batch.0)?;

        if self.1.extend_on_update {
            let now = Utc::now();
            for key in keys {
                if let Err(e) = self.1.update_expires_at(key, now) {
                    return Ok(Err(e));
                }
            }
        }

        Ok(Ok(()))
    }
}

impl<'a, V, E, F> Iterator for ExpiringIter<'a, V, E, F>
where
    E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    F: Encoding<V> + 'static,
{
    type Item = Result<(IVec, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.and_then(move |(k, v)| {
            if self.1.extend_on_fetch {
                self.1
                    .update_expires_at(k.clone(), Utc::now())
                    .map(move |_| (k, v))
            } else {
                Ok((k, v))
            }
        }))
    }
}

impl<'a, V, E, F> DoubleEndedIterator for ExpiringIter<'a, V, E, F>
where
    E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    F: Encoding<V> + 'static,
{
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        Some(self.0.next_back()?.and_then(move |(k, v)| {
            if self.1.extend_on_fetch {
                self.1
                    .update_expires_at(k.clone(), Utc::now())
                    .map(move |_| (k, v))
            } else {
                Ok((k, v))
            }
        }))
    }
}
