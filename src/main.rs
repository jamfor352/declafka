use actix_web::HttpServer;
use customersvc::app;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    HttpServer::new(|| {app() })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
