use serde::ser::{
    self,
    Serialize,
    SerializeMap,
    SerializeStruct,
};
use serde_bytes::Bytes;

use crate::{
    bson::{Bson, DbPointer, Document, JavaScriptCodeWithScope, Regex, Timestamp},
    datetime::DateTime,
    extjson,
    oid::ObjectId,
    raw::{RawDbPointerRef, RawRegexRef},
    spec::BinarySubtype,
    Binary,
    Decimal128,
};

impl Serialize for ObjectId {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut ser = serializer.serialize_struct("$oid", 1)?;
        ser.serialize_field("$oid", &self.to_string())?;
        ser.end()
    }
}

impl Serialize for Document {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self {
            state.serialize_entry(k, v)?;
        }
        state.end()
    }
}

impl Serialize for Bson {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Bson::Double(v) => serializer.serialize_f64(*v),
            Bson::String(v) => serializer.serialize_str(v),
            Bson::Array(v) => v.serialize(serializer),
            Bson::Document(v) => v.serialize(serializer),
            Bson::Boolean(v) => serializer.serialize_bool(*v),
            Bson::Null => serializer.serialize_unit(),
            Bson::Int32(v) => serializer.serialize_i32(*v),
            Bson::Int64(v) => serializer.serialize_i64(*v),
            Bson::ObjectId(oid) => oid.serialize(serializer),
            Bson::DateTime(dt) => dt.serialize(serializer),
            Bson::Binary(b) => b.serialize(serializer),
            Bson::JavaScriptCode(c) => {
                let mut state = serializer.serialize_struct("$code", 1)?;
                state.serialize_field("$code", c)?;
                state.end()
            }
            Bson::JavaScriptCodeWithScope(code_w_scope) => code_w_scope.serialize(serializer),
            Bson::DbPointer(dbp) => dbp.serialize(serializer),
            Bson::Symbol(s) => {
                let mut state = serializer.serialize_struct("$symbol", 1)?;
                state.serialize_field("$symbol", s)?;
                state.end()
            }
            Bson::RegularExpression(re) => re.serialize(serializer),
            Bson::Timestamp(t) => t.serialize(serializer),
            Bson::Decimal128(d) => {
                let mut state = serializer.serialize_struct("$numberDecimal", 1)?;
                state.serialize_field("$numberDecimalBytes", Bytes::new(&d.bytes))?;
                state.end()
            }
            Bson::Undefined => {
                let mut state = serializer.serialize_struct("$undefined", 1)?;
                state.serialize_field("$undefined", &true)?;
                state.end()
            }
            Bson::MaxKey => {
                let mut state = serializer.serialize_struct("$maxKey", 1)?;
                state.serialize_field("$maxKey", &1)?;
                state.end()
            }
            Bson::MinKey => {
                let mut state = serializer.serialize_struct("$minKey", 1)?;
                state.serialize_field("$minKey", &1)?;
                state.end()
            }
        }
    }
}

/// Options used to configure a [`Serializer`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SerializerOptions {
    /// Whether the [`Serializer`] should present itself as human readable or not.
    /// The default value is true.
    pub human_readable: Option<bool>,
}

impl SerializerOptions {
    /// Create a builder used to construct a new [`SerializerOptions`].
    pub fn builder() -> SerializerOptionsBuilder {
        SerializerOptionsBuilder {
            options: Default::default(),
        }
    }
}

/// A builder used to construct new [`SerializerOptions`] structs.
pub struct SerializerOptionsBuilder {
    options: SerializerOptions,
}

impl SerializerOptionsBuilder {
    /// Set the value for [`SerializerOptions::is_human_readable`].
    pub fn human_readable(mut self, value: impl Into<Option<bool>>) -> Self {
        self.options.human_readable = value.into();
        self
    }

    /// Consume this builder and produce a [`SerializerOptions`].
    pub fn build(self) -> SerializerOptions {
        self.options
    }
}

impl Serialize for Timestamp {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut state = serializer.serialize_struct("$timestamp", 1)?;
        let body = extjson::models::TimestampBody {
            t: self.time,
            i: self.increment,
        };
        state.serialize_field("$timestamp", &body)?;
        state.end()
    }
}

impl Serialize for Regex {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let raw = RawRegexRef {
            pattern: self.pattern.as_str(),
            options: self.options.as_str(),
        };
        raw.serialize(serializer)
    }
}

impl Serialize for JavaScriptCodeWithScope {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut state = serializer.serialize_struct("$codeWithScope", 2)?;
        state.serialize_field("$code", &self.code)?;
        state.serialize_field("$scope", &self.scope)?;
        state.end()
    }
}

impl Serialize for Binary {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        if let BinarySubtype::Generic = self.subtype {
            serializer.serialize_bytes(self.bytes.as_slice())
        } else {
            let mut state = serializer.serialize_struct("$binary", 1)?;
            let body = extjson::models::BinaryBody {
                base64: base64::encode(self.bytes.as_slice()),
                subtype: hex::encode([self.subtype.into()]),
            };
            state.serialize_field("$binary", &body)?;
            state.end()
        }
    }
}

impl Serialize for Decimal128 {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        if serializer.is_human_readable() {
            let mut state = serializer.serialize_map(Some(1))?;
            state.serialize_entry("$numberDecimal", &self.to_string())?;
            state.end()
        } else {
            let mut state = serializer.serialize_struct("$numberDecimal", 1)?;
            state.serialize_field("$numberDecimalBytes", serde_bytes::Bytes::new(&self.bytes))?;
            state.end()
        }
    }
}

impl Serialize for DateTime {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut state = serializer.serialize_struct("$date", 1)?;
        let body = extjson::models::DateTimeBody::from_millis(self.timestamp_millis());
        state.serialize_field("$date", &body)?;
        state.end()
    }
}

impl Serialize for DbPointer {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let raw = RawDbPointerRef {
            namespace: self.namespace.as_str(),
            id: self.id,
        };
        raw.serialize(serializer)
    }
}
