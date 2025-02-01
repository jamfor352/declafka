use actix_web::body::to_bytes;
use actix_web::{http::StatusCode, test};
use serde_json::json;
use std::str;
use my_kafka_app::routes::routes::app;

/// Test successful login
#[actix_web::test]
async fn test_login_success() {
    let app = test::init_service(app()).await; // No into_service nonsense

    let req = test::TestRequest::post()
        .uri("/login")
        .set_json(json!({
              "username": "admin",
              "password": "password"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = to_bytes(resp.into_body()).await.unwrap_or_else(|_| panic!("Failed to read response body"));
    let body_str = str::from_utf8(&body_bytes).unwrap();
    assert!(body_str.starts_with("eyJ"), "Response did not start with expected prefix: {}", body_str);
}
