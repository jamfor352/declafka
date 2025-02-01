
use crate::routes::routes::app;
use actix_web::HttpServer;
use listeners::listeners::{handle_my_struct_listener, handle_normal_string_listener};

mod listeners;
mod models;
mod utils;
mod routes;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // init logs
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Start the Kafka Listeners
    handle_normal_string_listener().start();
    handle_my_struct_listener().start();

    HttpServer::new(|| {app() })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
