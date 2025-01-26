use crate::routes::routes::{login, protected_route};
use actix_service::ServiceFactory;
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Logger;
use actix_web::{web, App, Error};

mod models;
mod routes;
mod utils;

