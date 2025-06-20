// The MIT License (MIT)

// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

//! Deserializer

mod error;
mod raw;
mod serde;

pub use self::{
    error::{Error, Result},
    serde::Deserializer,
};

use std::io::Read;

use crate::{
    bson::{Bson, Document, Timestamp},
    ser::write_i32,
    spec::BinarySubtype,
};

#[rustfmt::skip]
use ::serde::{
    de::{DeserializeOwned, Error as _},
    Deserialize,
};

pub(crate) use self::serde::{convert_unsigned_to_signed_raw, BsonVisitor};

pub(crate) use self::raw::Deserializer as RawDeserializer;

pub(crate) const MAX_BSON_SIZE: i32 = i32::MAX;
pub(crate) const MIN_BSON_DOCUMENT_SIZE: i32 = 4 + 1; // 4 bytes for length, one byte for null terminator
pub(crate) const MIN_BSON_STRING_SIZE: i32 = 4 + 1; // 4 bytes for length, one byte for null terminator
pub(crate) const MIN_CODE_WITH_SCOPE_SIZE: i32 = 4 + MIN_BSON_STRING_SIZE + MIN_BSON_DOCUMENT_SIZE;

/// Hint provided to the deserializer via `deserialize_newtype_struct` as to the type of thing
/// being deserialized.
#[derive(Debug, Clone, Copy)]
enum DeserializerHint {
    /// No hint provided, deserialize normally.
    None,

    /// The type being deserialized expects the BSON to contain a binary value with the provided
    /// subtype. This is currently used to deserialize [`bson::Uuid`] values.
    BinarySubtype(BinarySubtype),

    /// The type being deserialized is raw BSON, meaning no allocations should occur as part of
    /// deserializing and everything should be visited via borrowing or [`Copy`] if possible.
    RawBson,
}

impl Timestamp {
    pub(crate) fn from_reader<R: Read>(mut reader: R) -> Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Timestamp::from_le_bytes(bytes))
    }
}

/// Deserialize a `T` from the provided [`Bson`] value.
///
/// The [`Deserializer`] used by this function presents itself as human readable, whereas the
/// one used in [`from_slice`] does not. This means that this function may deserialize differently
/// than [`from_slice`] for types that change their deserialization logic depending on whether
/// the format is human readable or not.
pub fn from_bson<T>(bson: Bson) -> Result<T>
where
    T: DeserializeOwned,
{
    let de = Deserializer::new(bson);
    #[cfg(feature = "serde_path_to_error")]
    {
        serde_path_to_error::deserialize(de).map_err(Error::with_path)
    }
    #[cfg(not(feature = "serde_path_to_error"))]
    {
        Deserialize::deserialize(de)
    }
}

/// Deserialize a `T` from the provided [`Document`].
///
/// The [`Deserializer`] used by this function presents itself as human readable, whereas the
/// one used in [`from_slice`] does not. This means that this function may deserialize differently
/// than [`from_slice`] for types that change their deserialization logic depending on whether
/// the format is human readable or not.
pub fn from_document<T>(doc: Document) -> Result<T>
where
    T: DeserializeOwned,
{
    from_bson(Bson::Document(doc))
}

pub(crate) fn reader_to_vec<R: Read>(mut reader: R) -> Result<Vec<u8>> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    let length = i32::from_le_bytes(buf);

    if length < MIN_BSON_DOCUMENT_SIZE {
        return Err(Error::custom("document size too small"));
    }

    let mut bytes = Vec::with_capacity(length as usize);
    write_i32(&mut bytes, length).map_err(Error::custom)?;

    reader.take(length as u64 - 4).read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Deserialize an instance of type `T` from an I/O stream of BSON.
pub fn from_reader<R, T>(reader: R) -> Result<T>
where
    T: DeserializeOwned,
    R: Read,
{
    let bytes = reader_to_vec(reader)?;
    from_slice(bytes.as_slice())
}

/// Deserialize an instance of type `T` from a slice of BSON bytes.
pub fn from_slice<'de, T>(bytes: &'de [u8]) -> Result<T>
where
    T: Deserialize<'de>,
{
    from_raw(raw::Deserializer::new(bytes)?)
}

pub(crate) fn from_raw<'de, T: Deserialize<'de>>(
    deserializer: raw::Deserializer<'de>,
) -> Result<T> {
    #[cfg(feature = "serde_path_to_error")]
    {
        serde_path_to_error::deserialize(deserializer).map_err(Error::with_path)
    }
    #[cfg(not(feature = "serde_path_to_error"))]
    {
        T::deserialize(deserializer)
    }
}
