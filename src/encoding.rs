#[cfg(any(feature = "bincode", feature = "cbor", feature = "json"))]
use serde::{de::DeserializeOwned, ser::Serialize};

#[cfg(any(feature = "bincode", feature = "cbor", feature = "json"))]
use crate::error::Error;

use crate::error::Result;

/// The Encoding trait
///
/// By implementing this trait, a custom data format can be used to store information in Sled
pub trait Encoding<T> {
    /// Encoding data to bytes
    fn encode(t: &T) -> Result<Vec<u8>>;

    /// Decoding data from bytes
    fn decode(slice: &[u8]) -> Result<T>;
}

#[derive(Clone, Debug, Default)]
/// A 'Plain' Encoding that only works on values that are already bytes
pub struct PlainEncoding;

#[cfg(feature = "bincode")]
#[derive(Clone, Debug, Default)]
/// An Encoding backed by bincode to store serde-compatible types
///
/// Note that Bincode cannot store certain kinds of types, such as untagged enums
pub struct BincodeEncoding;

#[cfg(feature = "cbor")]
#[derive(Clone, Debug, Default)]
/// An Encoding backed by bincode to store serde-compatible types
pub struct CborEncoding;

#[cfg(feature = "json")]
#[derive(Clone, Debug, Default)]
/// An Encoding backed by json to store serde-compatible types
pub struct JsonEncoding;

impl<T> Encoding<T> for PlainEncoding
where
    T: AsRef<[u8]>,
    for<'a> T: From<&'a [u8]>,
{
    fn encode(t: &T) -> Result<Vec<u8>> {
        Ok(t.as_ref().to_vec())
    }

    fn decode(slice: &[u8]) -> Result<T> {
        Ok(slice.into())
    }
}

#[cfg(feature = "bincode")]
impl<T> Encoding<T> for BincodeEncoding
where
    T: DeserializeOwned + Serialize + 'static,
{
    fn encode(t: &T) -> Result<Vec<u8>> {
        bincode::serialize(t).map_err(Error::BincodeSerialize)
    }

    fn decode(slice: &[u8]) -> Result<T> {
        bincode::deserialize(slice).map_err(Error::BincodeDeserialize)
    }
}

#[cfg(feature = "cbor")]
impl<T> Encoding<T> for CborEncoding
where
    T: DeserializeOwned + Serialize + 'static,
{
    fn encode(t: &T) -> Result<Vec<u8>> {
        serde_cbor::to_vec(t).map_err(Error::CborSerialize)
    }

    fn decode(slice: &[u8]) -> Result<T> {
        serde_cbor::from_slice(slice).map_err(Error::CborDeserialize)
    }
}

#[cfg(feature = "json")]
impl<T> Encoding<T> for JsonEncoding
where
    T: DeserializeOwned + Serialize + 'static,
{
    fn encode(t: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(t).map_err(Error::JsonSerialize)
    }

    fn decode(slice: &[u8]) -> Result<T> {
        serde_json::from_slice(slice).map_err(Error::JsonDeserialize)
    }
}
