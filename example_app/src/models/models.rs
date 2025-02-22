use serde::{Deserialize, Serialize};

// Define the structure for the JWT claims
#[derive(Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

// Define the structure for the User
#[derive(Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Clone)]
pub struct MyStruct {
    pub field1: String,
    pub field2: i32,
}