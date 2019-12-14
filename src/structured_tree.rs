use sled::IVec;
use std::{marker::PhantomData, ops::RangeBounds};

use crate::{
    encoding::Encoding,
    error::{coerce, Result},
};

/// Compare and swap error.
pub struct CompareAndSwapError<V> {
    /// Current value.
    pub current: Option<V>,
    /// New proposed value.
    pub proposed: Option<V>,
}

#[derive(Clone)]
/// A flash-sympathetic persistent lock-free B+ tree
pub struct StructuredTree<V, E>(sled::Tree, String, PhantomData<V>, PhantomData<E>);

/// An iterator over keys and values in a `Tree`.
pub struct StructuredIter<V, E>(sled::Iter, PhantomData<V>, PhantomData<E>);

#[derive(Clone, Debug, Default)]
/// A batch of updates that will be applied atomically to the Tree.
pub struct StructuredBatch<V, E>(sled::Batch, PhantomData<V>, PhantomData<E>);

#[derive(Clone)]
/// A transaction that will be applied atomically to the Tree.
pub struct StructuredTransactionalTree<'a, V, E>(
    &'a sled::TransactionalTree,
    PhantomData<V>,
    PhantomData<E>,
);

impl<V, E> StructuredTree<V, E>
where
    E: Encoding<V> + 'static,
{
    pub(crate) fn new(db: &sled::Db, name: &str) -> Result<Self> {
        Ok(StructuredTree(
            db.open_tree(name)?,
            name.to_owned(),
            PhantomData,
            PhantomData,
        ))
    }

    /// Clone for structures where V and E aren't Clone
    pub fn cloned(&self) -> Self {
        StructuredTree(self.0.clone(), self.1.clone(), PhantomData, PhantomData)
    }

    /// Perform a multi-key serializable transaction.
    ///
    /// Transactions also work on tuples of Trees, preserving serializable ACID semantics! In this
    /// example, we treat two trees like a work queue, atomically apply updates to data and move
    /// them from the unprocessed Tree to the processed Tree.
    pub fn transaction<F, R>(&self, f: F) -> sled::TransactionResult<Result<R>>
    where
        F: Fn(StructuredTransactionalTree<V, E>) -> sled::ConflictableTransactionResult<Result<R>>,
    {
        self.0.transaction(move |trans_tree| {
            (f)(StructuredTransactionalTree(
                trans_tree,
                PhantomData,
                PhantomData,
            ))
        })
    }

    /// Create a new batched update that can be atomically applied.
    ///
    /// It is possible to apply a Batch in a transaction as well, which is the way you can apply a Batch to multiple Trees atomically.
    pub fn apply_batch(&self, batch: StructuredBatch<V, E>) -> Result<()> {
        Ok(self.0.apply_batch(batch.0)?)
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
        let ov = coerce(old.map(|value| E::encode(&value)))?;
        let nv = coerce(new.map(|value| E::encode(&value)))?;

        match self.0.compare_and_swap(key, ov, nv)? {
            Ok(()) => Ok(Ok(())),
            Err(sled::CompareAndSwapError { current, proposed }) => {
                let current = if let Some(current) = current {
                    Some(E::decode(&current)?)
                } else {
                    None
                };
                let proposed = if let Some(proposed) = proposed {
                    Some(E::decode(&proposed)?)
                } else {
                    None
                };

                Ok(Err(CompareAndSwapError { current, proposed }))
            }
        }
    }

    /// Retrieve a value from the Tree if it exists.
    pub fn get<K>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
    {
        let opt = self.0.get(key)?;

        if let Some(v) = opt {
            Ok(Some(E::decode(&v)?))
        } else {
            Ok(None)
        }
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert<K>(&self, key: K, value: V) -> Result<Option<V>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let v = E::encode(&value)?;

        let opt = self.0.insert::<K, Vec<u8>>(key, v)?;

        if let Some(v) = opt {
            Ok(Some(E::decode(&v)?))
        } else {
            Ok(None)
        }
    }

    /// Delete a value, returning the old value if it existed.
    pub fn remove<K>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]>,
    {
        let opt = self.0.remove(key)?;

        if let Some(v) = opt {
            Ok(Some(E::decode(&v)?))
        } else {
            Ok(None)
        }
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
        let opt = self.0.update_and_fetch(key, |opt| {
            let o = opt.and_then(|v| E::decode(&v).ok());

            (f)(o).and_then(|value| E::encode(&value).ok())
        })?;

        if let Some(v) = opt {
            Ok(Some(E::decode(&v)?))
        } else {
            Ok(None)
        }
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
        let opt = self.0.fetch_and_update(key, |opt| {
            let o = opt.and_then(|v| E::decode(&v).ok());

            (f)(o).and_then(|value| E::encode(&value).ok())
        })?;

        if let Some(v) = opt {
            Ok(Some(E::decode(&v)?))
        } else {
            Ok(None)
        }
    }

    /// Subscribe to `Event`s that happen to keys that have the specified prefix. Events for
    /// particular keys are guaranteed to be witnessed in the same order by all threads, but
    /// threads may witness different interleavings of `Event`s across different keys. If
    /// subscribers don't keep up with new writes, they will cause new writes to block. There is a
    /// buffer of 1024 items per `Subscriber`. This can be used to build reactive and replicated
    /// systems.
    pub fn watch_prefix(&self, prefix: Vec<u8>) -> sled::Subscriber {
        self.0.watch_prefix(prefix)
    }

    /// Synchronously flushes all dirty IO buffers and calls fsync. If this succeeds, it is guaranteed that all previous writes will be recovered if the system crashes. Returns the number of bytes flushed during this call.
    ///
    /// Flushing can take quite a lot of time, and you should measure the performance impact of using it on realistic sustained workloads running on realistic hardware.
    pub fn flush(&self) -> Result<()> {
        self.0.flush()?;
        Ok(())
    }

    /// Returns `true` if the `Tree` contains a value for the specified key.
    pub fn contains_key<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.0.contains_key(key)?)
    }

    /// Create a double-ended iterator over the tuples of keys and values in this tree.
    pub fn iter(&self) -> StructuredIter<V, E> {
        StructuredIter::new(self.0.iter())
    }

    /// Create a double-ended iterator over tuples of keys and values, where the keys fall
    /// within the specified range.
    pub fn range<K, R>(&self, range: R) -> StructuredIter<V, E>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        StructuredIter::new(self.0.range(range))
    }

    /// Retrieve the key and value before the provided key, if one exists.
    pub fn get_lt<K>(&self, key: K) -> Result<Option<(IVec, V)>>
    where
        K: AsRef<[u8]>,
    {
        match self.0.get_lt(key)? {
            Some((k, v)) => {
                let value = E::decode(&v)?;
                Ok(Some((k, value)))
            }
            None => Ok(None),
        }
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
        match self.0.get_gt(key)? {
            Some((k, v)) => {
                let value = E::decode(&v)?;
                Ok(Some((k, value)))
            }
            None => Ok(None),
        }
    }

    /// Create an iterator over tuples of keys and values, where the all the keys starts with the
    /// given prefix.
    pub fn scan_prefix<P>(&self, prefix: P) -> StructuredIter<V, E>
    where
        P: AsRef<[u8]>,
    {
        StructuredIter::new(self.0.scan_prefix(prefix))
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    pub fn pop_max(&self) -> Result<Option<(IVec, V)>> {
        match self.0.pop_max()? {
            Some((k, v)) => {
                let value = E::decode(&v)?;
                Ok(Some((k, value)))
            }
            None => Ok(None),
        }
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    pub fn pop_min(&self) -> Result<Option<(IVec, V)>> {
        match self.0.pop_min()? {
            Some((k, v)) => {
                let value = E::decode(&v)?;
                Ok(Some((k, value)))
            }
            None => Ok(None),
        }
    }

    /// Returns the number of elements in this tree.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the `Tree` contains no elements.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    pub fn clear(&self) -> Result<()> {
        Ok(self.0.clear()?)
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> String {
        self.1.clone()
    }
}

impl<V, E> StructuredIter<V, E>
where
    E: Encoding<V> + 'static,
{
    fn new(iter: sled::Iter) -> Self {
        StructuredIter(iter, PhantomData, PhantomData)
    }

    /// Iterate over the keys of this Tree
    pub fn keys(self) -> impl DoubleEndedIterator<Item = Result<IVec>> {
        self.map(|res| res.map(|(key, _)| key))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl DoubleEndedIterator<Item = Result<V>> {
        self.map(|res| res.map(|(_, v)| v))
    }
}

impl<V, E> StructuredBatch<V, E>
where
    E: Encoding<V>,
{
    /// Set a key to a new value
    pub fn insert<K>(&mut self, key: K, value: V) -> Result<()>
    where
        IVec: From<K>,
    {
        let v = E::encode(&value)?;
        self.0.insert::<_, Vec<u8>>(key, v);
        Ok(())
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
    where
        IVec: From<K>,
    {
        self.0.remove(key)
    }
}

impl<'a, V, E> StructuredTransactionalTree<'a, V, E>
where
    E: Encoding<V>,
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
        let v = match E::encode(&value) {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };

        let opt = self.0.insert::<_, Vec<_>>(key, v)?;

        if let Some(v) = opt {
            match E::decode(&v) {
                Ok(i) => return Ok(Ok(Some(i))),
                Err(e) => return Ok(Err(e)),
            }
        } else {
            Ok(Ok(None))
        }
    }

    /// Remove a key
    pub fn remove<K>(&self, key: K) -> sled::ConflictableTransactionResult<Result<Option<V>>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let opt = self.0.remove(key)?;

        if let Some(v) = opt {
            match E::decode(&v) {
                Ok(i) => return Ok(Ok(Some(i))),
                Err(e) => return Ok(Err(e)),
            }
        } else {
            Ok(Ok(None))
        }
    }

    /// Get the value associated with a key
    pub fn get<K>(&self, key: K) -> sled::ConflictableTransactionResult<Result<Option<V>>>
    where
        K: AsRef<[u8]>,
    {
        let opt = self.0.get(key)?;

        if let Some(v) = opt {
            match E::decode(&v) {
                Ok(i) => return Ok(Ok(Some(i))),
                Err(e) => return Ok(Err(e)),
            }
        } else {
            Ok(Ok(None))
        }
    }

    /// Atomically apply multiple inserts and removals.
    pub fn apply_batch(
        &self,
        batch: StructuredBatch<V, E>,
    ) -> sled::ConflictableTransactionResult<()> {
        Ok(self.0.apply_batch(batch.0)?)
    }
}

impl<V, E> Iterator for StructuredIter<V, E>
where
    E: Encoding<V>,
{
    type Item = Result<(IVec, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next()? {
            Ok((key, v)) => Some(E::decode(&v).map(move |value| (key, value))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl<V, E> DoubleEndedIterator for StructuredIter<V, E>
where
    E: Encoding<V>,
{
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.0.next_back()? {
            Ok((key, v)) => Some(E::decode(&v).map(move |value| (key, value))),
            Err(e) => Some(Err(e.into())),
        }
    }
}
