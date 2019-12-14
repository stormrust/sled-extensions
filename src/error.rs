use std::{error::Error as StdError, fmt};

/// An alias for Result<T, sled_extensions::Error>
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for this library
///
/// If different features are enabled, different variantes of this enum will be present.
/// - `json` -- `JsonSerialize` | `JsonDeserialize`
/// - `cbor` -- `CborSerialize` | `CborDeserialize`
/// - `bincode` -- `BincodeSerialize` | `BincodeDeserialize`
///
#[derive(Debug)]
pub enum Error {
    #[cfg(feature = "json")]
    /// Json Serialization error
    JsonSerialize(serde_json::error::Error),

    #[cfg(feature = "json")]
    /// Json Deserialization error
    JsonDeserialize(serde_json::error::Error),

    #[cfg(feature = "cbor")]
    /// Cbor Serialization error
    CborSerialize(serde_cbor::error::Error),
    #[cfg(feature = "cbor")]
    /// Cbor Deserialization error
    CborDeserialize(serde_cbor::error::Error),

    #[cfg(feature = "bincode")]
    /// Bincode Serialization error
    BincodeSerialize(bincode::Error),
    #[cfg(feature = "bincode")]
    /// Bincode Deserialization error
    BincodeDeserialize(bincode::Error),

    /// Custom errors provided by users of this crate
    Custom(Box<dyn StdError + Send + Sync>),
    /// Errors in the Sled database
    Sled(sled::Error),
}

impl Error {
    /// Helper to automatically box custom error types
    pub fn custom<E>(error: E) -> Error
    where
        E: StdError + Send + Sync + 'static,
    {
        Error::Custom(Box::new(error))
    }
}

pub(crate) fn coerce<T>(opt: Option<Result<T>>) -> Result<Option<T>> {
    match opt {
        Some(Ok(t)) => Ok(Some(t)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Self {
        Error::Sled(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            #[cfg(feature = "json")]
            Error::JsonSerialize(ref e) => write!(f, "There was an error serializing data, {}", e),
            #[cfg(feature = "json")]
            Error::JsonDeserialize(ref e) => {
                write!(f, "There was an error deserializing data, {}", e)
            }

            #[cfg(feature = "cbor")]
            Error::CborSerialize(ref e) => write!(f, "There was an error serializing data, {}", e),
            #[cfg(feature = "cbor")]
            Error::CborDeserialize(ref e) => {
                write!(f, "There was an error deserializing data, {}", e)
            }

            #[cfg(feature = "bincode")]
            Error::BincodeSerialize(ref e) => {
                write!(f, "There was an error serializing data, {}", e)
            }
            #[cfg(feature = "bincode")]
            Error::BincodeDeserialize(ref e) => {
                write!(f, "There was an error deserializing data, {}", e)
            }

            Error::Custom(ref e) => write!(f, "There was a custom error, {}", e),
            Error::Sled(ref e) => write!(f, "There was an error in the database, {}", e),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            #[cfg(feature = "json")]
            Error::JsonSerialize(_) => "There was an error serializing data",
            #[cfg(feature = "json")]
            Error::JsonDeserialize(_) => "There was an error deserializing data",

            #[cfg(feature = "cbor")]
            Error::CborSerialize(_) => "There was an error serializing data",
            #[cfg(feature = "cbor")]
            Error::CborDeserialize(_) => "There was an error deserializing data",

            #[cfg(feature = "bincode")]
            Error::BincodeSerialize(ref e) => e.description(),
            #[cfg(feature = "bincode")]
            Error::BincodeDeserialize(ref e) => e.description(),

            Error::Custom(ref e) => e.description(),
            Error::Sled(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&dyn StdError> {
        match *self {
            Error::Sled(ref e) => Some(e),
            Error::Custom(_) => None,

            #[cfg(feature = "bincode")]
            Error::BincodeSerialize(ref e) | Error::BincodeDeserialize(ref e) => Some(e),

            #[cfg(feature = "json")]
            Error::JsonSerialize(ref e) | Error::JsonDeserialize(ref e) => Some(e),

            #[cfg(feature = "cbor")]
            Error::CborSerialize(ref e) | Error::CborDeserialize(ref e) => Some(e),
        }
    }
}
