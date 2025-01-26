use crate::models::models::MyStruct;

pub fn my_struct_deserializer() -> fn(&[u8]) -> Option<MyStruct> {
    |payload| serde_json::from_slice::<MyStruct>(payload).ok()
}

pub fn string_deserializer() -> fn(&[u8]) -> Option<String> {
    |payload| Some(String::from_utf8_lossy(payload).to_string())
}
