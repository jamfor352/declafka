use log::info;
use crate::models::models::MyStruct;

pub fn handle_normal_string(message: String) {
    info!("Received text message: {}", message);
}

pub fn handle_my_struct(message: MyStruct) {
    info!("Received JSON message with field 1 as: {} and field 2 as: {}", message.field1, message.field2);
}
