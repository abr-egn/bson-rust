use std::{borrow::Cow, convert::TryFrom};

use super::{
    Error as RawError,
    RawBinaryRef,
    RawBsonRef,
    RawDocument,
    RawIter,
    RawRegexRef,
    Result as RawResult,
};
use crate::{
    error::{Error, Result},
    oid::ObjectId,
    spec::ElementType,
    Bson,
    DateTime,
    RawArrayBuf,
    Timestamp,
};

/// A slice of a BSON document containing a BSON array value (akin to [`std::str`]). This can be
/// retrieved from a [`RawDocument`] via [`RawDocument::get`].
///
/// This is an _unsized_ type, meaning that it must always be used behind a pointer like `&`.
///
/// Accessing elements within a [`RawArray`] is similar to element access in [`crate::Document`],
/// but because the contents are parsed during iteration instead of at creation time, format errors
/// can happen at any time during use.
///
/// Iterating over a [`RawArray`] yields either an error or a value that borrows from the
/// original document without making any additional allocations.
///
/// ```
/// use bson::{doc, raw::RawDocument};
///
/// let doc = doc! {
///     "x": [1, true, "two", 5.5]
/// };
/// let bytes = bson::serialize_to_vec(&doc)?;
///
/// let rawdoc = RawDocument::from_bytes(bytes.as_slice())?;
/// let rawarray = rawdoc.get_array("x")?;
///
/// for v in rawarray {
///     println!("{:?}", v?);
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Individual elements can be accessed using [`RawArray::get`] or any of
/// the type-specific getters, such as [`RawArray::get_object_id`] or
/// [`RawArray::get_str`]. Note that accessing elements is an O(N) operation, as it
/// requires iterating through the array from the beginning to find the requested index.
///
/// ```
/// use bson::{doc, raw::RawDocument};
///
/// let doc = doc! {
///     "x": [1, true, "two", 5.5]
/// };
/// let bytes = doc.to_vec()?;
///
/// let rawdoc = RawDocument::from_bytes(bytes.as_slice())?;
/// let rawarray = rawdoc.get_array("x")?;
///
/// assert_eq!(rawarray.get_bool(1)?, true);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(PartialEq)]
#[repr(transparent)]
pub struct RawArray {
    pub(crate) doc: RawDocument,
}

impl RawArray {
    pub(crate) fn from_doc(doc: &RawDocument) -> &RawArray {
        // SAFETY:
        //
        // Dereferencing a raw pointer requires unsafe due to the potential that the pointer is
        // null, dangling, or misaligned. We know the pointer is not null or dangling due to the
        // fact that it's created by a safe reference. Converting &RawDocument to *const
        // RawDocument will be properly aligned due to them being references to the same type,
        // and converting *const RawDocument to *const RawArray is aligned due to the fact that
        // the only field in a RawArray is a RawDocument, meaning the structs are represented
        // identically at the byte level.
        unsafe { &*(doc as *const RawDocument as *const RawArray) }
    }

    #[cfg(feature = "serde")]
    pub(crate) fn as_doc(&self) -> &RawDocument {
        &self.doc
    }

    /// Gets a reference to the value at the given index.
    pub fn get(&self, index: usize) -> RawResult<Option<RawBsonRef<'_>>> {
        self.into_iter().nth(index).transpose()
    }

    fn get_with<'a, T>(
        &'a self,
        index: usize,
        expected_type: ElementType,
        f: impl FnOnce(RawBsonRef<'a>) -> Option<T>,
    ) -> Result<T> {
        let bson = self
            .get(index)
            .map_err(|e| Error::value_access_invalid_bson(format!("{:?}", e)))?
            .ok_or_else(Error::value_access_not_present)
            .map_err(|e| e.with_index(index))?;
        match f(bson) {
            Some(t) => Ok(t),
            None => Err(
                Error::value_access_unexpected_type(bson.element_type(), expected_type)
                    .with_index(index),
            ),
        }
    }

    /// Gets the BSON double at the given index or returns an error if the value at that index isn't
    /// a double.
    pub fn get_f64(&self, index: usize) -> Result<f64> {
        self.get_with(index, ElementType::Double, RawBsonRef::as_f64)
    }

    /// Gets a reference to the string at the given index or returns an error if the
    /// value at that index isn't a string.
    pub fn get_str(&self, index: usize) -> Result<&str> {
        self.get_with(index, ElementType::String, RawBsonRef::as_str)
    }

    /// Gets a reference to the document at the given index or returns an error if the
    /// value at that index isn't a document.
    pub fn get_document(&self, index: usize) -> Result<&RawDocument> {
        self.get_with(
            index,
            ElementType::EmbeddedDocument,
            RawBsonRef::as_document,
        )
    }

    /// Gets a reference to the array at the given index or returns an error if the
    /// value at that index isn't a array.
    pub fn get_array(&self, index: usize) -> Result<&RawArray> {
        self.get_with(index, ElementType::Array, RawBsonRef::as_array)
    }

    /// Gets a reference to the BSON binary value at the given index or returns an error if the
    /// value at that index isn't a binary.
    pub fn get_binary(&self, index: usize) -> Result<RawBinaryRef<'_>> {
        self.get_with(index, ElementType::Binary, RawBsonRef::as_binary)
    }

    /// Gets the ObjectId at the given index or returns an error if the value at that index isn't an
    /// ObjectId.
    pub fn get_object_id(&self, index: usize) -> Result<ObjectId> {
        self.get_with(index, ElementType::ObjectId, RawBsonRef::as_object_id)
    }

    /// Gets the boolean at the given index or returns an error if the value at that index isn't a
    /// boolean.
    pub fn get_bool(&self, index: usize) -> Result<bool> {
        self.get_with(index, ElementType::Boolean, RawBsonRef::as_bool)
    }

    /// Gets the DateTime at the given index or returns an error if the value at that index isn't a
    /// DateTime.
    pub fn get_datetime(&self, index: usize) -> Result<DateTime> {
        self.get_with(index, ElementType::DateTime, RawBsonRef::as_datetime)
    }

    /// Gets a reference to the BSON regex at the given index or returns an error if the
    /// value at that index isn't a regex.
    pub fn get_regex(&self, index: usize) -> Result<RawRegexRef<'_>> {
        self.get_with(index, ElementType::RegularExpression, RawBsonRef::as_regex)
    }

    /// Gets a reference to the BSON timestamp at the given index or returns an error if the
    /// value at that index isn't a timestamp.
    pub fn get_timestamp(&self, index: usize) -> Result<Timestamp> {
        self.get_with(index, ElementType::Timestamp, RawBsonRef::as_timestamp)
    }

    /// Gets the BSON int32 at the given index or returns an error if the value at that index isn't
    /// a 32-bit integer.
    pub fn get_i32(&self, index: usize) -> Result<i32> {
        self.get_with(index, ElementType::Int32, RawBsonRef::as_i32)
    }

    /// Gets BSON int64 at the given index or returns an error if the value at that index isn't a
    /// 64-bit integer.
    pub fn get_i64(&self, index: usize) -> Result<i64> {
        self.get_with(index, ElementType::Int64, RawBsonRef::as_i64)
    }

    /// Gets a reference to the raw bytes of the [`RawArray`].
    pub fn as_bytes(&self) -> &[u8] {
        self.doc.as_bytes()
    }

    /// Whether this array contains any elements or not.
    pub fn is_empty(&self) -> bool {
        self.doc.is_empty()
    }

    /// Gets an iterator over the elements in the [`RawArray`],
    /// which yields `Result<RawElement<'_>>` values. These hold a
    /// reference to the underlying array but do not explicitly
    /// resolve the values.
    ///
    /// This iterator, which underpins the implementation of the
    /// default iterator, produces `RawElement` objects that hold a
    /// view onto the array but do not parse out or construct
    /// values until the `.value()` or `.try_into()` methods are
    /// called.
    pub fn iter_elements(&self) -> RawIter {
        RawIter::new(&self.doc)
    }
}

impl std::fmt::Debug for RawArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawArray")
            .field("data", &hex::encode(self.doc.as_bytes()))
            .finish()
    }
}

impl TryFrom<&RawArray> for Vec<Bson> {
    type Error = RawError;

    fn try_from(arr: &RawArray) -> RawResult<Vec<Bson>> {
        arr.into_iter()
            .map(|result| {
                let rawbson = result?;
                Bson::try_from(rawbson)
            })
            .collect()
    }
}

impl ToOwned for RawArray {
    type Owned = RawArrayBuf;

    fn to_owned(&self) -> Self::Owned {
        RawArrayBuf::from_raw_document_buf(self.doc.to_owned())
    }
}

impl<'a> From<&'a RawArray> for Cow<'a, RawArray> {
    fn from(rdr: &'a RawArray) -> Self {
        Cow::Borrowed(rdr)
    }
}

impl<'a> IntoIterator for &'a RawArray {
    type IntoIter = RawArrayIter<'a>;
    type Item = RawResult<RawBsonRef<'a>>;

    fn into_iter(self) -> RawArrayIter<'a> {
        RawArrayIter {
            inner: RawIter::new(&self.doc),
        }
    }
}

/// An iterator over borrowed raw BSON array values.
pub struct RawArrayIter<'a> {
    inner: RawIter<'a>,
}

impl<'a> Iterator for RawArrayIter<'a> {
    type Item = RawResult<RawBsonRef<'a>>;

    fn next(&mut self) -> Option<RawResult<RawBsonRef<'a>>> {
        match self.inner.next() {
            Some(Ok(elem)) => match elem.value() {
                Ok(value) => Some(Ok(value)),
                Err(e) => Some(Err(e)),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(feature = "serde")]
impl<'de: 'a, 'a> serde::Deserialize<'de> for &'a RawArray {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use super::serde::OwnedOrBorrowedRawArray;
        match OwnedOrBorrowedRawArray::deserialize(deserializer)? {
            OwnedOrBorrowedRawArray::Borrowed(b) => Ok(b),
            o => Err(serde::de::Error::custom(format!(
                "expected borrowed raw array, instead got owned {:?}",
                o
            ))),
        }
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for &RawArray {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct SeqSerializer<'a>(&'a RawArray);

        impl serde::Serialize for SeqSerializer<'_> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeSeq as _;
                if serializer.is_human_readable() {
                    let mut seq = serializer.serialize_seq(None)?;
                    for v in self.0 {
                        let v = v.map_err(serde::ser::Error::custom)?;
                        seq.serialize_element(&v)?;
                    }
                    seq.end()
                } else {
                    serializer.serialize_bytes(self.0.as_bytes())
                }
            }
        }

        serializer.serialize_newtype_struct(crate::raw::RAW_ARRAY_NEWTYPE, &SeqSerializer(self))
    }
}
