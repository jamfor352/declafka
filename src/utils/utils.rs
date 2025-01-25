use crate::models::models::Claims;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};

const SECRET_KEY: &[u8] = b"secret_key";

// Function to create the JWT token
pub fn create_jwt(username: &str) -> String {
    let claims = Claims {
        sub: username.to_owned(),
        exp: 10000000000,
    };

    let header = Header::new(Algorithm::HS256);
    let encoding_key = EncodingKey::from_secret(SECRET_KEY);

    encode(&header, &claims, &encoding_key).unwrap()
}


// Function to validate the JWT token
pub fn validate_jwt(token: &str) -> bool {
    let decoding_key = DecodingKey::from_secret(SECRET_KEY);
    let validation = Validation::new(Algorithm::HS256);

    match decode::<Claims>(token, &decoding_key, &validation) {
        Ok(_) => true,
        Err(_) => false,
    }
}
