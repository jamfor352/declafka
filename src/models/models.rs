use serde::{Deserialize, Serialize};

// Define the structure for the JWT claims
#[derive(Serialize, Deserialize)]
pub struct Claims {
    pub(crate) sub: String,
    pub(crate) exp: usize,
}

// Define the structure for the User
#[derive(Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password: String,
}
