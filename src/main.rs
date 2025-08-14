use axum::{routing::{get, post}, Router, Json};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/lootpacks", get(get_lootpacks))
        .route("/lootpacks/create", post(create_lootpack))
        .route("/lootpacks/:id/open", post(open_lootpack))
        .route("/rewards", get(get_rewards))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3005").await.unwrap();
    println!("🎁 Lootpacks Service running on port 3005");
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<Value> {
    Json(json!({"status": "healthy", "service": "lootpacks-service", "features": ["lootpacks", "rewards", "gamification"]}))
}

async fn get_lootpacks() -> Json<Value> {
    Json(json!({
        "lootpacks": [
            {"id": "loot_1", "name": "Daily Pack", "cost": 100, "rewards": 5},
            {"id": "loot_2", "name": "Premium Pack", "cost": 500, "rewards": 25}
        ],
        "service": "lootpacks-service"
    }))
}

async fn create_lootpack() -> Json<Value> {
    Json(json!({"message": "Lootpack created", "id": "loot_123", "service": "lootpacks-service"}))
}

async fn open_lootpack() -> Json<Value> {
    Json(json!({
        "rewards": [
            {"type": "coupon", "value": "SAVE20", "rarity": "common"},
            {"type": "points", "value": 50, "rarity": "rare"}
        ],
        "service": "lootpacks-service"
    }))
}

async fn get_rewards() -> Json<Value> {
    Json(json!({
        "rewards": [
            {"id": "reward_1", "type": "coupon", "value": "SAVE10"},
            {"id": "reward_2", "type": "points", "value": 100}
        ],
        "service": "lootpacks-service"
    }))
}
