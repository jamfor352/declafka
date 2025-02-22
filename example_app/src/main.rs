use actix_web::{HttpServer, web, HttpResponse};
use example_app::listeners::listeners::{handle_my_struct_listener, handle_normal_string_listener};
use example_app::routes::routes::app;
use serde_json::json;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_level(log::LevelFilter::Info)
        .init();

    // Start the Kafka Listeners
    let string_listener = handle_normal_string_listener().expect("Failed to create string listener");
    string_listener.start();

    let json_listener = handle_my_struct_listener().expect("Failed to create json listener");
    json_listener.start();

    // Start HTTP server
    HttpServer::new(move || {
        app()
            .route("/metrics", web::get().to(|| async { 
                HttpResponse::Ok().body("Metrics coming soon") 
            }))
            .route("/health", web::get().to(|| async { 
                HttpResponse::Ok().json(json!({
                    "status": "healthy",
                    "kafka_listeners": "running"
                }))
            }))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

/*
If you don't want to run a HTTP server, you can do something like:

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // init logs
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Fire-and-forget style: start() spawns tasks
    handle_normal_string_listener().start();
    handle_my_struct_listener().start();

    // Keep main alive by some means, or let it exit if that's what you want
    tokio::signal::ctrl_c().await?;
    Ok(())
}
 */
