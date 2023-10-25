use crate::{spec::BinarySubtype, RawBinaryRef};
use std::{
    error,
    fmt::{self, Display},
};

/// Represents a BSON binary value.
#[derive(Debug, Clone, PartialEq)]
pub struct Binary {
    /// The subtype of the bytes.
    pub subtype: BinarySubtype,

    /// The binary bytes.
    pub bytes: Vec<u8>,
}

impl Display for Binary {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "Binary({:#x}, {})",
            u8::from(self.subtype),
            base64::encode(&self.bytes)
        )
    }
}

impl Binary {
    /// Creates a [`Binary`] from a base64 string and optional [`BinarySubtype`]. If the
    /// `subtype` argument is [`None`], the [`Binary`] constructed will default to
    /// [`BinarySubtype::Generic`].
    ///
    /// ```rust
    /// # use bson::{Binary, binary::Result};
    /// # fn example() -> Result<()> {
    /// let input = base64::encode("hello");
    /// let binary = Binary::from_base64(input, None)?;
    /// println!("{:?}", binary);
    /// // binary: Binary { subtype: Generic, bytes: [104, 101, 108, 108, 111] }
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_base64(
        input: impl AsRef<str>,
        subtype: impl Into<Option<BinarySubtype>>,
    ) -> Result<Self> {
        let bytes = base64::decode(input.as_ref()).map_err(|e| Error::DecodingError {
            message: e.to_string(),
        })?;
        let subtype = match subtype.into() {
            Some(s) => s,
            None => BinarySubtype::Generic,
        };
        Ok(Binary { subtype, bytes })
    }

    /// Borrow the contents as a [`RawBinaryRef`].
    pub fn as_raw_binary(&self) -> RawBinaryRef<'_> {
        RawBinaryRef {
            bytes: self.bytes.as_slice(),
            subtype: self.subtype,
        }
    }
}

/// Possible errors that can arise during [`Binary`] construction.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Error {
    /// While trying to decode from base64, an error was returned.
    DecodingError { message: String },
}

impl error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::DecodingError { message: m } => fmt.write_str(m),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
