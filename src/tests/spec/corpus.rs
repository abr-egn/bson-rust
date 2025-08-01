use std::{
    convert::{TryFrom, TryInto},
    marker::PhantomData,
    str::FromStr,
};

use crate::{
    raw::{RawBsonRef, RawDocument},
    tests::LOCK,
    Bson,
    Document,
    RawBson,
    RawDocumentBuf,
    Utf8Lossy,
};
use pretty_assertions::assert_eq;
use serde::{Deserialize, Deserializer};

use super::run_spec_test;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestFile {
    description: String,
    bson_type: String,
    test_key: Option<String>,

    #[serde(default)]
    valid: Vec<Valid>,

    #[serde(rename = "decodeErrors")]
    #[serde(default)]
    decode_errors: Vec<DecodeError>,

    #[serde(rename = "parseErrors")]
    #[serde(default)]
    parse_errors: Vec<ParseError>,

    #[allow(dead_code)]
    deprecated: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Valid {
    description: String,
    canonical_bson: String,
    canonical_extjson: String,
    relaxed_extjson: Option<String>,
    degenerate_bson: Option<String>,
    degenerate_extjson: Option<String>,
    #[allow(dead_code)]
    converted_bson: Option<String>,
    #[allow(dead_code)]
    converted_extjson: Option<String>,
    lossy: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DecodeError {
    description: String,
    bson: String,
}

#[derive(Debug, Deserialize)]
struct ParseError {
    description: String,
    string: String,
}

struct FieldVisitor<'a, T>(&'a str, PhantomData<T>);

impl<'de, T> serde::de::Visitor<'de> for FieldVisitor<'_, T>
where
    T: Deserialize<'de>,
{
    type Value = T;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "expecting RawBson at field {}", self.0)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        while let Some((k, v)) = map.next_entry::<String, T>()? {
            if k.as_str() == self.0 {
                return Ok(v);
            }
        }
        Err(serde::de::Error::custom(format!(
            "missing field: {}",
            self.0
        )))
    }
}

fn run_test(test: TestFile) {
    let _guard = LOCK.run_concurrently();
    for valid in test.valid {
        #[cfg(not(feature = "large_dates"))]
        if valid.description == "Y10K" {
            continue;
        }

        let description = format!("{}: {}", test.description, valid.description);

        let canonical_bson = hex::decode(&valid.canonical_bson).expect(&description);

        // these four cover the four ways to create a [`Document`] from the provided BSON.
        let documentfromreader_cb =
            Document::from_reader(canonical_bson.as_slice()).expect(&description);

        let fromreader_cb: Document =
            crate::deserialize_from_reader(canonical_bson.as_slice()).expect(&description);

        let fromdocument_documentfromreader_cb: Document =
            crate::deserialize_from_document(documentfromreader_cb.clone()).expect(&description);

        let todocument_documentfromreader_cb: Document =
            crate::serialize_to_document(&documentfromreader_cb).expect(&description);

        let canonical_raw_document =
            RawDocument::from_bytes(canonical_bson.as_slice()).expect(&description);
        let document_from_raw_document: Document =
            canonical_raw_document.try_into().expect(&description);

        let canonical_raw_bson_from_slice =
            crate::deserialize_from_slice::<RawBsonRef>(canonical_bson.as_slice())
                .expect(&description)
                .as_document()
                .expect(&description);

        let canonical_owned_raw_bson_from_slice =
            crate::deserialize_from_slice::<RawBson>(canonical_bson.as_slice())
                .expect(&description);

        let canonical_raw_document_from_slice =
            crate::deserialize_from_slice::<&RawDocument>(canonical_bson.as_slice())
                .expect(&description);

        // These cover the ways to serialize those [`Documents`] back to BSON.
        let mut documenttowriter_documentfromreader_cb = Vec::new();
        documentfromreader_cb
            .to_writer(&mut documenttowriter_documentfromreader_cb)
            .expect(&description);

        let mut documenttowriter_fromreader_cb = Vec::new();
        fromreader_cb
            .to_writer(&mut documenttowriter_fromreader_cb)
            .expect(&description);

        let mut documenttowriter_fromdocument_documentfromreader_cb = Vec::new();
        fromdocument_documentfromreader_cb
            .to_writer(&mut documenttowriter_fromdocument_documentfromreader_cb)
            .expect(&description);

        let mut documenttowriter_todocument_documentfromreader_cb = Vec::new();
        todocument_documentfromreader_cb
            .to_writer(&mut documenttowriter_todocument_documentfromreader_cb)
            .expect(&description);

        let tovec_documentfromreader_cb =
            crate::serialize_to_vec(&documentfromreader_cb).expect(&description);

        let mut documenttowriter_document_from_raw_document = Vec::new();
        document_from_raw_document
            .to_writer(&mut documenttowriter_document_from_raw_document)
            .expect(&description);

        // Serialize the raw versions "back" to BSON also.
        let tovec_rawdocument =
            crate::serialize_to_vec(&canonical_raw_document).expect(&description);
        let tovec_rawdocument_from_slice =
            crate::serialize_to_vec(&canonical_raw_document_from_slice).expect(&description);
        let tovec_rawbson =
            crate::serialize_to_vec(&canonical_raw_bson_from_slice).expect(&description);
        let tovec_ownedrawbson =
            crate::serialize_to_vec(&canonical_owned_raw_bson_from_slice).expect(&description);

        // test Bson / RawBson field deserialization
        if let Some(ref test_key) = test.test_key {
            // skip regex tests that don't have the value at the test key
            if !description.contains("$regex query operator") {
                // deserialize the field from raw Bytes into a RawBson
                let deserializer_raw =
                    crate::RawDeserializer::new(canonical_bson.as_slice()).unwrap();
                let raw_bson_field = deserializer_raw
                    .deserialize_any(FieldVisitor(test_key.as_str(), PhantomData::<RawBsonRef>))
                    .expect(&description);
                // convert to an owned Bson and put into a Document
                let bson: Bson = raw_bson_field.try_into().expect(&description);
                let from_raw_doc = doc! {
                    test_key: bson
                };

                // deserialize the field from raw Bytes into an OwnedRawBson
                let deserializer_raw =
                    crate::RawDeserializer::new(canonical_bson.as_slice()).unwrap();
                let owned_raw_bson_field = deserializer_raw
                    .deserialize_any(FieldVisitor(test_key.as_str(), PhantomData::<RawBson>))
                    .expect(&description);
                let test_key_cstr: &crate::raw::CStr = test_key.as_str().try_into().unwrap();
                let from_slice_owned_vec =
                    RawDocumentBuf::from_iter([(test_key_cstr, owned_raw_bson_field)]).into_bytes();

                // deserialize the field from raw Bytes into a Bson
                let deserializer_value =
                    crate::RawDeserializer::new(canonical_bson.as_slice()).unwrap();
                let bson_field = deserializer_value
                    .deserialize_any(FieldVisitor(test_key.as_str(), PhantomData::<Bson>))
                    .expect(&description);
                // put into a Document
                let from_slice_value_doc = doc! {
                    test_key: bson_field,
                };

                // deserialize the field from a Bson into a Bson
                let deserializer_value_value =
                    crate::Deserializer::new(Bson::Document(documentfromreader_cb.clone()));
                let bson_field = deserializer_value_value
                    .deserialize_any(FieldVisitor(test_key.as_str(), PhantomData::<Bson>))
                    .expect(&description);
                // put into a Document
                let from_value_value_doc = doc! {
                    test_key: bson_field,
                };

                // deserialize the field from a Bson into a RawBson
                let deserializer_value_raw =
                    crate::Deserializer::new(Bson::Document(documentfromreader_cb.clone()));
                let raw_bson_field = deserializer_value_raw
                    .deserialize_any(FieldVisitor(test_key.as_str(), PhantomData::<RawBson>))
                    .expect(&description);
                let from_value_raw_doc = doc! {
                    test_key: Bson::try_from(raw_bson_field).expect(&description),
                };

                // convert back into raw BSON for comparison with canonical BSON
                let from_raw_vec = crate::serialize_to_vec(&from_raw_doc).expect(&description);
                let from_slice_value_vec =
                    crate::serialize_to_vec(&from_slice_value_doc).expect(&description);
                let from_bson_value_vec =
                    crate::serialize_to_vec(&from_value_value_doc).expect(&description);
                let from_value_raw_vec =
                    crate::serialize_to_vec(&from_value_raw_doc).expect(&description);

                assert_eq!(from_raw_vec, canonical_bson, "{}", description);
                assert_eq!(from_slice_value_vec, canonical_bson, "{}", description);
                assert_eq!(from_bson_value_vec, canonical_bson, "{}", description);
                assert_eq!(from_slice_owned_vec, canonical_bson, "{}", description);
                assert_eq!(from_value_raw_vec, canonical_bson, "{}", description);
            }
        }

        // native_to_bson( bson_to_native(cB) ) = cB

        // now we ensure the hex for all 5 are equivalent to the canonical BSON provided by the
        // test.
        assert_eq!(
            hex::encode(documenttowriter_documentfromreader_cb).to_lowercase(),
            valid.canonical_bson.to_lowercase(),
            "{}",
            description,
        );

        assert_eq!(
            hex::encode(documenttowriter_fromreader_cb).to_lowercase(),
            valid.canonical_bson.to_lowercase(),
            "{}",
            description,
        );

        assert_eq!(
            hex::encode(documenttowriter_fromdocument_documentfromreader_cb).to_lowercase(),
            valid.canonical_bson.to_lowercase(),
            "{}",
            description,
        );

        // bson::to_document(&Document) may be lossy due to round-tripping extjson
        if valid.lossy != Some(true) {
            assert_eq!(
                hex::encode(documenttowriter_todocument_documentfromreader_cb).to_lowercase(),
                valid.canonical_bson.to_lowercase(),
                "{}",
                description,
            );
        }

        assert_eq!(
            hex::encode(tovec_documentfromreader_cb).to_lowercase(),
            valid.canonical_bson.to_lowercase(),
            "{}",
            description,
        );

        assert_eq!(
            hex::encode(documenttowriter_document_from_raw_document).to_lowercase(),
            valid.canonical_bson.to_lowercase(),
            "{}",
            description,
        );

        assert_eq!(tovec_rawdocument, tovec_rawbson, "{}", description);
        assert_eq!(
            tovec_rawdocument, tovec_rawdocument_from_slice,
            "{}",
            description
        );
        assert_eq!(tovec_rawdocument, tovec_ownedrawbson, "{}", description);

        assert_eq!(
            hex::encode(tovec_rawdocument).to_lowercase(),
            valid.canonical_bson.to_lowercase(),
            "{}",
            description,
        );

        // NaN == NaN is false, so we skip document comparisons that contain NaN
        if !description.to_ascii_lowercase().contains("nan") && !description.contains("decq541") {
            assert_eq!(documentfromreader_cb, fromreader_cb, "{}", description);

            assert_eq!(
                documentfromreader_cb, fromdocument_documentfromreader_cb,
                "{}",
                description
            );

            // bson::to_document(&Document) may be lossy due to round-tripping extjson
            if valid.lossy != Some(true) {
                assert_eq!(
                    documentfromreader_cb, todocument_documentfromreader_cb,
                    "{}",
                    description
                );
            }

            assert_eq!(
                document_from_raw_document, documentfromreader_cb,
                "{}",
                description
            );
        }

        // native_to_bson( bson_to_native(dB) ) = cB

        if let Some(db) = valid.degenerate_bson {
            let db = hex::decode(&db).expect(&description);

            let bson_to_native_db = Document::from_reader(db.as_slice()).expect(&description);
            let mut native_to_bson_bson_to_native_db = Vec::new();
            bson_to_native_db
                .to_writer(&mut native_to_bson_bson_to_native_db)
                .unwrap();
            assert_eq!(
                hex::encode(native_to_bson_bson_to_native_db).to_lowercase(),
                valid.canonical_bson.to_lowercase(),
                "{}",
                description,
            );

            let bson_to_native_db_serde: Document =
                crate::deserialize_from_reader(db.as_slice()).expect(&description);
            let mut native_to_bson_bson_to_native_db_serde = Vec::new();
            bson_to_native_db_serde
                .to_writer(&mut native_to_bson_bson_to_native_db_serde)
                .unwrap();
            assert_eq!(
                hex::encode(native_to_bson_bson_to_native_db_serde).to_lowercase(),
                valid.canonical_bson.to_lowercase(),
                "{}",
                description,
            );

            let document_from_raw_document: Document = RawDocument::from_bytes(db.as_slice())
                .expect(&description)
                .try_into()
                .expect(&description);
            let mut documenttowriter_document_from_raw_document = Vec::new();
            document_from_raw_document
                .to_writer(&mut documenttowriter_document_from_raw_document)
                .expect(&description);
            assert_eq!(
                hex::encode(documenttowriter_document_from_raw_document).to_lowercase(),
                valid.canonical_bson.to_lowercase(),
                "{}",
                description,
            );

            // NaN == NaN is false, so we skip document comparisons that contain NaN
            if !description.contains("NaN") {
                assert_eq!(
                    bson_to_native_db_serde, documentfromreader_cb,
                    "{}",
                    description
                );

                assert_eq!(
                    document_from_raw_document, documentfromreader_cb,
                    "{}",
                    description
                );
            }
        }

        let cej: serde_json::Value =
            serde_json::from_str(&valid.canonical_extjson).expect(&description);

        // native_to_canonical_extended_json( bson_to_native(cB) ) = cEJ

        let mut cej_updated_float = cej.clone();

        // Rust doesn't format f64 with exponential notation by default, and the spec doesn't give
        // guidance on when to use it, so we manually parse any $numberDouble fields with
        // exponential notation and replace them with non-exponential notation.
        if let Some(ref key) = test.test_key {
            if let Some(serde_json::Value::Object(subdoc)) = cej_updated_float.get_mut(key) {
                if let Some(&mut serde_json::Value::String(ref mut s)) =
                    subdoc.get_mut("$numberDouble")
                {
                    if s.to_lowercase().contains('e') {
                        let d = f64::from_str(s).unwrap();
                        let mut fixed_string = format!("{}", d);

                        if d.fract() == 0.0 {
                            fixed_string.push_str(".0");
                        }

                        *s = fixed_string;
                    }
                }
            }
        }

        assert_eq!(
            Bson::Document(documentfromreader_cb.clone()).into_canonical_extjson(),
            cej_updated_float,
            "{}",
            description
        );

        // native_to_relaxed_extended_json( bson_to_native(cB) ) = cEJ

        if let Some(ref relaxed_extjson) = valid.relaxed_extjson {
            let rej: serde_json::Value = serde_json::from_str(relaxed_extjson).expect(&description);

            assert_eq!(
                Bson::Document(documentfromreader_cb.clone()).into_relaxed_extjson(),
                rej,
                "{}",
                description
            );
        }

        // native_to_canonical_extended_json( json_to_native(cEJ) ) = cEJ

        let json_to_native_cej: Bson = cej.clone().try_into().expect("cej into bson should work");

        let native_to_canonical_extended_json_bson_to_native_cej =
            json_to_native_cej.clone().into_canonical_extjson();

        assert_eq!(
            native_to_canonical_extended_json_bson_to_native_cej, cej_updated_float,
            "{}",
            description,
        );

        // native_to_bson( json_to_native(cEJ) ) = cB (unless lossy)

        if valid.lossy != Some(true) {
            let mut native_to_bson_json_to_native_cej = Vec::new();
            json_to_native_cej
                .as_document()
                .unwrap()
                .to_writer(&mut native_to_bson_json_to_native_cej)
                .unwrap();

            assert_eq!(
                hex::encode(native_to_bson_json_to_native_cej).to_lowercase(),
                valid.canonical_bson.to_lowercase(),
                "{}",
                description,
            );
        }

        if let Some(ref degenerate_extjson) = valid.degenerate_extjson {
            let dej: serde_json::Value =
                serde_json::from_str(degenerate_extjson).expect(&description);

            let json_to_native_dej: Bson = dej.clone().try_into().unwrap();

            // native_to_canonical_extended_json( json_to_native(dEJ) ) = cEJ

            let native_to_canonical_extended_json_json_to_native_dej =
                json_to_native_dej.clone().into_canonical_extjson();

            assert_eq!(
                native_to_canonical_extended_json_json_to_native_dej, cej,
                "{}",
                description,
            );

            // native_to_bson( json_to_native(dEJ) ) = cB (unless lossy)

            if valid.lossy != Some(true) {
                let mut native_to_bson_json_to_native_dej = Vec::new();
                json_to_native_dej
                    .as_document()
                    .unwrap()
                    .to_writer(&mut native_to_bson_json_to_native_dej)
                    .unwrap();

                assert_eq!(
                    hex::encode(native_to_bson_json_to_native_dej).to_lowercase(),
                    valid.canonical_bson.to_lowercase(),
                    "{}",
                    description,
                );
            }
        }

        // native_to_relaxed_extended_json( json_to_native(rEJ) ) = rEJ

        if let Some(ref rej) = valid.relaxed_extjson {
            let rej: serde_json::Value = serde_json::from_str(rej).unwrap();

            let json_to_native_rej: Bson = rej.clone().try_into().unwrap();

            let native_to_relaxed_extended_json_bson_to_native_rej =
                json_to_native_rej.clone().into_relaxed_extjson();

            assert_eq!(
                native_to_relaxed_extended_json_bson_to_native_rej, rej,
                "{}",
                description,
            );
        }
    }

    for decode_error in test.decode_errors.iter() {
        let description = format!(
            "{} decode error: {}",
            test.bson_type, decode_error.description
        );
        let bson = hex::decode(&decode_error.bson).expect("should decode from hex");

        if let Ok(doc) = RawDocument::from_bytes(bson.as_slice()) {
            Document::try_from(doc).expect_err(description.as_str());
        }

        // No meaningful definition of "byte count" for an arbitrary reader.
        if decode_error.description
            == "Stated length less than byte count, with garbage after envelope"
        {
            continue;
        }

        Document::from_reader(bson.as_slice()).expect_err(&description);
        crate::deserialize_from_reader::<_, Document>(bson.as_slice())
            .expect_err(description.as_str());

        if decode_error.description.contains("invalid UTF-8") {
            RawDocumentBuf::from_reader(bson.as_slice())
                .expect(&description)
                .to_document_utf8_lossy()
                .unwrap_or_else(|err| {
                    panic!(
                        "{}: utf8_lossy should not fail (failed with {:?})",
                        description, err
                    )
                });
            crate::deserialize_from_slice::<Utf8Lossy<Document>>(bson.as_slice())
                .expect(&description);
        }
    }

    for parse_error in test.parse_errors {
        // no special support for dbref convention
        if parse_error.description.contains("DBRef") {
            continue;
        }

        let json: serde_json::Value = serde_json::Value::String(parse_error.string);

        if let Ok(bson) = Bson::try_from(json.clone()) {
            // if converting to bson succeeds, assert that translating that bson to bytes fails
            assert!(crate::serialize_to_vec(&bson).is_err());
        }
    }
}

#[test]
fn run() {
    run_spec_test(&["bson-corpus"], run_test);
}
