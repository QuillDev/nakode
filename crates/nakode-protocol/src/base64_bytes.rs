use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

pub fn serialize<SerializerType>(
    bytes: &[u8],
    serializer: SerializerType,
) -> Result<SerializerType::Ok, SerializerType::Error>
where
    SerializerType: Serializer,
{
    serializer.serialize_str(&STANDARD.encode(bytes))
}

pub fn deserialize<'de, DeserializerType>(
    deserializer: DeserializerType,
) -> Result<Vec<u8>, DeserializerType::Error>
where
    DeserializerType: Deserializer<'de>,
{
    let encoded = String::deserialize(deserializer)?;
    STANDARD
        .decode(encoded)
        .map_err(DeserializerType::Error::custom)
}
