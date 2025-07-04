use crate::{
    spec::BinarySubtype,
    uuid::{Uuid, UuidRepresentation},
    Binary,
    Bson,
};

#[cfg(feature = "serde")]
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct U {
    uuid: Uuid,
}

#[test]
fn into_bson() {
    let uuid = Uuid::new();

    let bson: Bson = uuid.into();
    let binary = Binary {
        bytes: uuid.bytes().to_vec(),
        subtype: BinarySubtype::Uuid,
    };

    assert_eq!(bson, Bson::Binary(binary));
}

#[cfg(feature = "serde")]
#[test]
fn raw_serialization() {
    let u = U { uuid: Uuid::new() };
    let bytes = crate::serialize_to_vec(&u).unwrap();

    let doc: crate::Document = crate::deserialize_from_slice(bytes.as_slice()).unwrap();
    assert_eq!(doc, doc! { "uuid": u.uuid });

    let u_roundtrip: U = crate::deserialize_from_slice(bytes.as_slice()).unwrap();
    assert_eq!(u_roundtrip, u);
}

#[cfg(feature = "serde")]
#[test]
fn bson_serialization() {
    let u = U { uuid: Uuid::new() };
    let correct = doc! {
        "uuid": Binary {
            bytes: u.uuid.bytes().to_vec(),
            subtype: BinarySubtype::Uuid
        }
    };

    assert_eq!(doc! { "uuid": u.uuid }, correct);

    let doc = crate::serialize_to_document(&u).unwrap();
    assert_eq!(doc, correct);

    let u_roundtrip: U = crate::deserialize_from_document(doc).unwrap();
    assert_eq!(u_roundtrip, u);
}

#[cfg(feature = "serde")]
#[test]
fn json() {
    let u = U { uuid: Uuid::new() };

    let json = serde_json::to_value(&u).unwrap();
    assert_eq!(json, serde_json::json!({ "uuid": u.uuid.to_string() }));

    let u_roundtrip_json: U = serde_json::from_value(json).unwrap();
    assert_eq!(u_roundtrip_json, u);
}

#[cfg(feature = "serde")]
#[test]
fn wrong_subtype() {
    let generic = doc! {
        "uuid": Binary {
            bytes: Uuid::new().bytes().to_vec(),
            subtype: BinarySubtype::Generic
        }
    };
    crate::deserialize_from_document::<U>(generic.clone()).unwrap_err();
    let generic_bytes = crate::serialize_to_vec(&generic).unwrap();
    crate::deserialize_from_slice::<U>(&generic_bytes).unwrap_err();

    let old = doc! {
        "uuid": Binary {
            bytes: Uuid::new().bytes().to_vec(),
            subtype: BinarySubtype::UuidOld
        }
    };
    crate::deserialize_from_document::<U>(old.clone()).unwrap_err();
    let old_bytes = crate::serialize_to_vec(&old).unwrap();
    crate::deserialize_from_slice::<U>(&old_bytes).unwrap_err();

    let other = doc! {
        "uuid": Binary {
            bytes: Uuid::new().bytes().to_vec(),
            subtype: BinarySubtype::UserDefined(100)
        }
    };
    crate::deserialize_from_document::<U>(other.clone()).unwrap_err();
    let other_bytes = crate::serialize_to_vec(&other).unwrap();
    crate::deserialize_from_slice::<U>(&other_bytes).unwrap_err();
}

#[test]
fn test_binary_constructors() {
    let uuid = crate::Uuid::parse_str("00112233445566778899AABBCCDDEEFF").unwrap();
    let bin = Binary::from_uuid(uuid);
    assert_eq!(bin.bytes, uuid.bytes());
    assert_eq!(bin.subtype, BinarySubtype::Uuid);

    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::Standard);
    assert_eq!(bin.bytes, uuid.bytes());
    assert_eq!(bin.subtype, BinarySubtype::Uuid);

    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::JavaLegacy);
    assert_eq!(
        bin.bytes,
        Uuid::parse_str("7766554433221100FFEEDDCCBBAA9988")
            .unwrap()
            .bytes()
    );
    assert_eq!(bin.subtype, BinarySubtype::UuidOld);

    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::CSharpLegacy);
    assert_eq!(
        bin.bytes,
        Uuid::parse_str("33221100554477668899AABBCCDDEEFF")
            .unwrap()
            .bytes()
    );
    assert_eq!(bin.subtype, BinarySubtype::UuidOld);

    // Same byte ordering as standard representation
    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::PythonLegacy);
    assert_eq!(
        bin.bytes,
        Uuid::parse_str("00112233445566778899AABBCCDDEEFF")
            .unwrap()
            .bytes()
    );
    assert_eq!(bin.subtype, BinarySubtype::UuidOld);
}

#[test]
fn test_binary_to_uuid_standard_rep() {
    let uuid = crate::Uuid::parse_str("00112233445566778899AABBCCDDEEFF").unwrap();
    let bin = Binary::from_uuid(uuid);

    assert_eq!(bin.to_uuid().unwrap(), uuid);
    assert_eq!(
        bin.to_uuid_with_representation(UuidRepresentation::Standard)
            .unwrap(),
        uuid
    );

    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::CSharpLegacy)
        .is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::PythonLegacy)
        .is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::PythonLegacy)
        .is_err());
}

#[test]
fn test_binary_to_uuid_explicitly_standard_rep() {
    let uuid = crate::Uuid::parse_str("00112233445566778899AABBCCDDEEFF").unwrap();
    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::Standard);

    assert_eq!(bin.to_uuid().unwrap(), uuid);
    assert_eq!(
        bin.to_uuid_with_representation(UuidRepresentation::Standard)
            .unwrap(),
        uuid
    );

    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::CSharpLegacy)
        .is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::PythonLegacy)
        .is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::PythonLegacy)
        .is_err());
}

#[test]
fn test_binary_to_uuid_java_rep() {
    let uuid = crate::Uuid::parse_str("00112233445566778899AABBCCDDEEFF").unwrap();
    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::JavaLegacy);

    assert!(bin.to_uuid().is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::Standard)
        .is_err());

    assert_eq!(
        bin.to_uuid_with_representation(UuidRepresentation::JavaLegacy)
            .unwrap(),
        uuid
    );
}

#[test]
fn test_binary_to_uuid_csharp_legacy_rep() {
    let uuid = crate::Uuid::parse_str("00112233445566778899AABBCCDDEEFF").unwrap();
    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::CSharpLegacy);

    assert!(bin.to_uuid().is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::Standard)
        .is_err());

    assert_eq!(
        bin.to_uuid_with_representation(UuidRepresentation::CSharpLegacy)
            .unwrap(),
        uuid
    );
}

#[test]
fn test_binary_to_uuid_python_legacy_rep() {
    let uuid = crate::Uuid::parse_str("00112233445566778899AABBCCDDEEFF").unwrap();
    let bin = Binary::from_uuid_with_representation(uuid, UuidRepresentation::PythonLegacy);

    assert!(bin.to_uuid().is_err());
    assert!(bin
        .to_uuid_with_representation(UuidRepresentation::Standard)
        .is_err());

    assert_eq!(
        bin.to_uuid_with_representation(UuidRepresentation::PythonLegacy)
            .unwrap(),
        uuid
    );
}

#[cfg(feature = "uuid-1")]
#[test]
fn interop_1() {
    let uuid = crate::Uuid::new();
    let uuid_uuid = uuid.to_uuid_1();
    assert_eq!(uuid.to_string(), uuid_uuid.to_string());
    assert_eq!(&uuid.bytes(), uuid_uuid.as_bytes());

    let back: crate::Uuid = uuid_uuid.into();
    assert_eq!(back, uuid);

    let d_bson = doc! { "uuid": uuid };
    let d_uuid = doc! { "uuid": uuid_uuid };
    assert_eq!(d_bson, d_uuid);
}

#[cfg(feature = "serde")]
#[test]
fn deserialize_uuid_from_string() {
    #[derive(serde::Deserialize)]
    struct UuidWrapper {
        uuid: Uuid,
    }

    let uuid = Uuid::new();

    let doc = doc! { "uuid": uuid.to_string() };
    let wrapper: UuidWrapper =
        crate::deserialize_from_document(doc).expect("failed to deserialize document");
    assert_eq!(wrapper.uuid, uuid);

    let raw_doc = rawdoc! { "uuid": uuid.to_string() };
    let wrapper: UuidWrapper = crate::deserialize_from_slice(raw_doc.as_bytes())
        .expect("failed to deserialize raw document");
    assert_eq!(wrapper.uuid, uuid);
}
