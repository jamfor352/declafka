use actix_web::{web, HttpResponse, Responder};
use crate::models::models::User;
use crate::utils::utils::{create_jwt, validate_jwt};

// Expose login and protected_route handlers
pub async fn login(user: web::Json<User>) -> impl Responder {
    if user.username == "admin" && user.password == "password" {
        let token = create_jwt(&user.username);
        HttpResponse::Ok().body(token)
    } else {
        HttpResponse::Unauthorized().finish()
    }
}

pub async fn protected_route(req: actix_web::HttpRequest) -> impl Responder {
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            let token = auth_str.strip_prefix("Bearer ").unwrap_or(auth_str);
            if validate_jwt(token) {
                return HttpResponse::Ok().body("You have access to the protected resource!");
            }
        }
    }
    HttpResponse::Unauthorized().finish()
}