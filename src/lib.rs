#![deny(missing_docs)]

//! # Sled Extensions
//! _Wrappers around the [Sled embedded database](https://docs.rs/sled/0.28.0/sled) to permit
//! storing structured data_
//! - [Join the discussion on Matrix](https://matrix.to/#/!skqvSdiKcFwIdaQoLD:asonix.dog?via=asonix.dog)
//!
//! Using Sled Extensions is much like using Sled. The Tree API mirrors Sled's directly, and the
//! [`Db`] type is extended through traits.
//!
//! ```rust
//! use sled_extensions::{Config, Db, DbExt};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Config::default().temporary(true).open()?;
//! let tree = db.open_json_tree::<usize>("json-tree")?;
//!
//! tree.insert(b"hey", 32)?;
//!
//! if let Some(num) = tree.get(b"hey")? {
//!     assert_eq!(num, 32);
//! } else {
//!     unreachable!("Shouldn't be empty");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Available features
//! - `bincode` - Enable storing bincode-encoded data
//! - `cbor` - Enable storing cbor-encoded data
//! - `json` - Enable storing json-encoded data

mod db;
mod encoding;
mod error;
mod expiring_tree;
mod structured_tree;

pub use sled::{abort, Config, Db, IVec, TransactionError};

pub use self::{
    db::DbExt,
    encoding::Encoding,
    error::{Error, Result},
    structured_tree::CompareAndSwapError,
};

/// Basic structured trees
///
/// This module contains the base types that other trees are built on. In fact, most trees are
/// just type aliases for the basic trees here with pre-defined encodings.
pub mod structured {
    pub use crate::structured_tree::{
        StructuredBatch as Batch, StructuredIter as Iter,
        StructuredTransactionalTree as TransactionalTree, StructuredTree as Tree,
    };

    /// This module names types for more easily interacting with Expiring Trees
    ///
    /// The number of type parameters are reduced by asserting that the encoder used for the
    /// expiring tree's metadata is the same encoder used for values stored in the tree.
    pub mod expiring {
        use crate::expiring;

        /// A basic expiring strutured tree
        ///
        /// This tree is an alias for a generic expiring tree, but asserts that the metadata
        /// encoding is the same as the value encoding.
        pub type Tree<V, E> = expiring::Tree<V, E, E>;

        /// The builder associated with the Tree
        pub type TreeBuilder<V, E> = expiring::TreeBuilder<V, E, E>;

        /// The Iterator associated with the Tree
        pub type Iter<'a, V, E> = expiring::Iter<'a, V, E, E>;

        /// The Batch associated with the Tree
        pub type Batch<V, E> = expiring::Batch<V, E>;

        /// The TransactionalTree associated with the Tree
        pub type TransactionalTree<'a, V, E> = expiring::TransactionalTree<'a, V, E, E>;
    }
}

/// Basic expiring trees
///
/// The trees defined here are generic over Encoder types for the metadata and values stored in
/// the tree.
pub mod expiring {
    pub use crate::expiring_tree::{
        ExpiringBatch as Batch, ExpiringIter as Iter,
        ExpiringTransactionalTree as TransactionalTree, ExpiringTree as Tree,
        ExpiringTreeBuilder as TreeBuilder,
    };

    /// "Plain" expiring trees
    ///
    /// This module is generic over the metadata encoding, but assumes values stored in the tree
    /// are IVecs. This allows for binary types to be stored without impacting performance
    pub mod plain {
        use sled::IVec;

        pub use crate::encoding::PlainEncoding;

        /// The "Plain" Tree
        pub type Tree<E> = super::Tree<IVec, E, PlainEncoding>;

        /// The "Plain" Tree's Builder
        pub type TreeBuilder<E> = super::TreeBuilder<IVec, E, PlainEncoding>;

        /// The "Plain" Tree's Batch
        pub type Batch = super::Batch<IVec, PlainEncoding>;

        /// The "Plain" Tree's Iterator
        pub type Iter<'a, E> = super::Iter<'a, IVec, E, PlainEncoding>;

        /// The "Plain" Tree's TransactionalTree
        pub type TransactionalTree<'a, E> = super::TransactionalTree<'a, IVec, E, PlainEncoding>;
    }
}

#[cfg(feature = "bincode")]
/// A module containing trees that are pre-configured to store Bincode-encoded data
pub mod bincode {
    use crate::structured;

    pub use crate::encoding::BincodeEncoding;

    /// A tree that stores data of type V encoded as Bincode
    pub type Tree<V> = structured::Tree<V, BincodeEncoding>;

    /// The bincode tree's iterator
    pub type Iter<V> = structured::Iter<V, BincodeEncoding>;

    /// The bincode tree's batch
    pub type Batch<V> = structured::Batch<V, BincodeEncoding>;

    /// The bincode tree's transaction
    pub type TransactionalTree<'a, V> = structured::TransactionalTree<'a, V, BincodeEncoding>;

    /// A module containing expiring trees that store Bincode-encoded data
    pub mod expiring {
        use crate::expiring;

        use super::BincodeEncoding;

        /// An expiring tree that stores data of type V encoded as Bincode
        pub type Tree<V> = expiring::Tree<V, BincodeEncoding, BincodeEncoding>;

        /// The expiring bincode tree's builder
        pub type TreeBuilder<V> = expiring::TreeBuilder<V, BincodeEncoding, BincodeEncoding>;

        /// The expiring bincode tree's iterator
        pub type Iter<'a, V> = expiring::Iter<'a, V, BincodeEncoding, BincodeEncoding>;

        /// The expiring bincode tree's batch
        pub type Batch<V> = expiring::Batch<V, BincodeEncoding>;

        /// The expiring bincode tree's transaction
        pub type TransactionalTree<'a, V> =
            expiring::TransactionalTree<'a, V, BincodeEncoding, BincodeEncoding>;
    }
}

#[cfg(feature = "cbor")]
/// A module containing trees that are pre-configured to store Cbor-encoded data
pub mod cbor {
    use crate::structured_tree::{
        StructuredBatch, StructuredIter, StructuredTransactionalTree, StructuredTree,
    };

    pub use crate::encoding::CborEncoding;

    /// A tree that stores data of type V encoded as Cbor
    pub type Tree<V> = StructuredTree<V, CborEncoding>;

    /// The cbor tree's iterator
    pub type Iter<V> = StructuredIter<V, CborEncoding>;

    /// The cbor tree's batch
    pub type Batch<V> = StructuredBatch<V, CborEncoding>;

    /// The cbor tree's transaction
    pub type TransactionalTree<'a, V> = StructuredTransactionalTree<'a, V, CborEncoding>;

    /// A module containing expiring trees that store Cbor-encoded data
    pub mod expiring {
        use crate::expiring;

        use super::CborEncoding;

        /// An expiring tree that stores data of type V encoded as Cbor
        pub type Tree<V> = expiring::Tree<V, CborEncoding, CborEncoding>;

        /// The expiring cbor tree's builder
        pub type TreeBuilder<V> = expiring::TreeBuilder<V, CborEncoding, CborEncoding>;

        /// The expiring cbor tree's iterator
        pub type Iter<'a, V> = expiring::Iter<'a, V, CborEncoding, CborEncoding>;

        /// The expiring cbor tree's batch
        pub type Batch<V> = expiring::Batch<V, CborEncoding>;

        /// The expiring cbor tree's transaction
        pub type TransactionalTree<'a, V> =
            expiring::TransactionalTree<'a, V, CborEncoding, CborEncoding>;
    }
}

#[cfg(feature = "json")]
/// A module containing trees that are pre-configured to store Json-encoded data
pub mod json {
    use crate::structured_tree::{
        StructuredBatch, StructuredIter, StructuredTransactionalTree, StructuredTree,
    };

    pub use crate::encoding::JsonEncoding;

    /// A tree that stores data of type V encoded as Json
    pub type Tree<V> = StructuredTree<V, JsonEncoding>;

    /// The json tree's iterator
    pub type Iter<V> = StructuredIter<V, JsonEncoding>;

    /// The json tree's batch
    pub type Batch<V> = StructuredBatch<V, JsonEncoding>;

    /// The json tree's transaction
    pub type TransactionalTree<'a, V> = StructuredTransactionalTree<'a, V, JsonEncoding>;

    /// A module containing expiring trees that store Json-encoded data
    pub mod expiring {
        use crate::expiring;

        use super::JsonEncoding;

        /// An expiring tree that stores data of type V encoded as Json
        pub type Tree<V> = expiring::Tree<V, JsonEncoding, JsonEncoding>;

        /// The expiring json tree's builder
        pub type TreeBuilder<V> = expiring::TreeBuilder<V, JsonEncoding, JsonEncoding>;

        /// The expiring json tree's iterator
        pub type Iter<'a, V> = expiring::Iter<'a, V, JsonEncoding, JsonEncoding>;

        /// The expiring json tree's batch
        pub type Batch<V> = expiring::Batch<V, JsonEncoding>;

        /// The expiring json tree's transaction
        pub type TransactionalTree<'a, V> =
            expiring::TransactionalTree<'a, V, JsonEncoding, JsonEncoding>;
    }
}
