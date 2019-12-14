use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, ser::Serialize};
use sled::IVec;
use std::collections::HashSet;

use crate::{encoding::Encoding, error::Result, expiring, structured};

/// Extensions for the sled Db type that provides different ways of opening trees for storing
/// structured data.
///
/// Example:
/// ```rust
/// use sled_extensions::{Config, DbExt};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Config::default().temporary(true).open()?;
/// let tree = db.open_json_tree::<()>("json-tree")?;
/// # Ok(())
/// # }
/// ```
pub trait DbExt {
    /// Open an expiring tree
    ///
    /// Expiring trees are trees that keep track of insert and update times in order to help cache
    /// implementations. While this library on it's own doesn't provide a fully-featured cache,
    /// implementing a cache on top of an expiring tree is simple.
    ///
    /// This tree can have numerous types, for example,
    /// ```rust
    /// use sled_extensions::{Config, DbExt, bincode, cbor, json};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Config::default().temporary(true).open()?;
    /// let tree: bincode::expiring::Tree<()> = db.open_expiring_tree("bincode-tree").build()?;
    /// let tree: cbor::expiring::Tree<()> = db.open_expiring_tree("cbor-tree").build()?;
    /// let tree: json::expiring::Tree<()> = db.open_expiring_tree("json-tree").build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// While it is possible to use only this method to open many kinds of expiring trees, it may
    /// be easier to use a more specific tree opening method.
    fn open_expiring_tree<V, E, F>(&self, name: &str) -> expiring::TreeBuilder<V, E, F>
    where
        E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
        F: Encoding<V> + 'static;

    /// Open a structured tree
    ///
    /// Structured trees are a thin wrapper on top of basic sled trees that provides encoding and
    /// decoding for the data stored in the tree.
    ///
    /// This tree can have numerous types, for example,
    /// ```rust
    /// use sled_extensions::{Config, DbExt, bincode, cbor, json};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Config::default().temporary(true).open()?;
    /// let tree: bincode::Tree<()> = db.open_structured_tree("bincode-tree")?;
    /// let tree: cbor::Tree<()> = db.open_structured_tree("cbor-tree")?;
    /// let tree: json::Tree<()> = db.open_structured_tree("json-tree")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// While it is possible to use only this method to open many kinds of structured trees, it may
    /// be easier to use a more specific tree opening method.
    fn open_structured_tree<V, E>(&self, name: &str) -> Result<structured::Tree<V, E>>
    where
        E: Encoding<V> + 'static;

    /// Open an expiring tree using an encoding for both metadata storage and value storage
    fn open_expiring_structured_tree<V, E>(
        &self,
        name: &str,
    ) -> structured::expiring::TreeBuilder<V, E>
    where
        E: Encoding<V> + Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_expiring_tree(name)
    }

    /// Open an expiring tree using an encoding for metadata storage
    fn open_expiring_plain_tree<E>(&self, name: &str) -> expiring::plain::TreeBuilder<E>
    where
        E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
    {
        self.open_expiring_tree(name)
    }

    #[cfg(feature = "bincode")]
    /// Open a tree that stores it's values as bincode
    fn open_bincode_tree<V>(&self, name: &str) -> Result<crate::bincode::Tree<V>>
    where
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_structured_tree(name)
    }

    #[cfg(feature = "bincode")]
    /// Open an expiring tree that stores it's values as bincode
    fn open_expiring_bincode_tree<V>(&self, name: &str) -> crate::bincode::expiring::TreeBuilder<V>
    where
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_expiring_tree(name)
    }

    #[cfg(feature = "bincode")]
    /// Open an expiring tree that stores it's metadata as bincode
    fn open_expiring_plain_bincode_tree(
        &self,
        name: &str,
    ) -> expiring::plain::TreeBuilder<crate::bincode::BincodeEncoding> {
        self.open_expiring_tree(name)
    }

    #[cfg(feature = "cbor")]
    /// Open a tree that stores it's values as cbor
    fn open_cbor_tree<V>(&self, name: &str) -> Result<crate::cbor::Tree<V>>
    where
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_structured_tree(name)
    }

    #[cfg(feature = "cbor")]
    /// Open an expiring tree that stores it's values as cbor
    fn open_expiring_cbor_tree<V>(&self, name: &str) -> crate::cbor::expiring::TreeBuilder<V>
    where
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_expiring_tree(name)
    }

    #[cfg(feature = "cbor")]
    /// Open an expiring tree that stores it's metadata as cbor
    fn open_expiring_plain_cbor_tree(
        &self,
        name: &str,
    ) -> expiring::plain::TreeBuilder<crate::cbor::CborEncoding> {
        self.open_expiring_tree(name)
    }

    #[cfg(feature = "json")]
    /// Open a tree that stores it's values as json
    fn open_json_tree<V>(&self, name: &str) -> Result<crate::json::Tree<V>>
    where
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_structured_tree(name)
    }

    #[cfg(feature = "json")]
    /// Open an expiring tree that stores it's values as json
    fn open_expiring_json_tree<V>(&self, name: &str) -> crate::json::expiring::TreeBuilder<V>
    where
        V: DeserializeOwned + Serialize + 'static,
    {
        self.open_expiring_tree(name)
    }

    #[cfg(feature = "json")]
    /// Open an expiring tree that stores it's metadata as json
    fn open_expiring_plain_json_tree(
        &self,
        name: &str,
    ) -> expiring::plain::TreeBuilder<crate::json::JsonEncoding> {
        self.open_expiring_tree(name)
    }
}

impl DbExt for sled::Db {
    fn open_expiring_tree<V, E, F>(&self, name: &str) -> expiring::TreeBuilder<V, E, F>
    where
        E: Encoding<HashSet<IVec>> + Encoding<DateTime<Utc>> + 'static,
        F: Encoding<V> + 'static,
    {
        expiring::TreeBuilder::new(self, name)
    }

    fn open_structured_tree<V, E>(&self, name: &str) -> Result<structured::Tree<V, E>>
    where
        E: Encoding<V> + 'static,
    {
        structured::Tree::new(self, name)
    }
}
