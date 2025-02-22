use crate::models::models::MyStruct;

pub fn my_struct_deserializer(payload: &[u8]) -> Option<MyStruct> {
    serde_json::from_slice(payload).ok()
}

pub fn string_deserializer(payload: &[u8]) -> Option<String> {
    let string = String::from_utf8_lossy(payload).to_string();
    Some(string)
}
