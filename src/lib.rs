use crate::routes::routes::{login, protected_route};
use actix_service::ServiceFactory;
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Logger;
use actix_web::{web, App, Error};

mod models;
mod routes;
mod utils;

pub fn app() -> App<
    impl ServiceFactory<
        ServiceRequest,
        Response = ServiceResponse<impl MessageBody>,
        Config = (),
        InitError = (),
        Error = Error,
    >> {
    App::new()
        .wrap(Logger::default())
        .route("/login", web::post().to(login))
        .route("/protected", web::get().to(protected_route))
}

