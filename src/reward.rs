use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Reward {
    pub user_id: Option<Uuid>,
    pub goal: Option<String>,
    pub target_points: Option<i32>,
    pub reward_value: Option<f64>,
    pub unlocked: Option<bool>,
}
