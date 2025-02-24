use actix_web::{HttpServer, web, HttpResponse};
use example_app::listeners::listeners::{handle_my_struct_listener, handle_normal_string_listener};
use example_app::routes::routes::app;
use serde_json::json;


// With Actix Web, HTTP server:
#[declafka_macro::begin_listeners(
    listeners = [
        handle_normal_string_listener, 
        handle_my_struct_listener
    ]
)]
#[actix_web::main]
async fn main() -> std::io::Result<()> {
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
    .await?;

    Ok(())
}


// With Tokio, no web server:
// #[declafka_macro::begin_listeners(
//     listeners = [
//         handle_normal_string_listener, 
//         handle_my_struct_listener
//     ]
// )]
// #[tokio::main]
// async fn main() -> std::io::Result<()> {
//     // Keep main alive by some means, or let it exit if that's what you want
//     tokio::signal::ctrl_c().await?;
//     Ok(())
// }
