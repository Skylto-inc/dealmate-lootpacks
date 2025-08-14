use crate::models::lootpacks::*;
use crate::error::Result;
use sqlx::PgPool;
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};
use rand::Rng;
use std::collections::HashMap;
use tracing::{info, warn, error};

pub struct LootpackService {
    db: PgPool,
    reward_cache: tokio::sync::RwLock<HashMap<Uuid, RewardPool>>, // Cache for pack-specific reward pools
}

impl LootpackService {
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            reward_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get all available pack types
    pub async fn get_pack_types(&self) -> Result<Vec<PackType>> {
        let packs = sqlx::query_as!(
            PackType,
            r#"
            SELECT id, name, type, description, icon, color_gradient, 
                   price_coins, cooldown_hours, min_rewards, max_rewards,
                   possible_reward_types, is_active, created_at, updated_at
            FROM pack_types 
            WHERE is_active = true
            ORDER BY 
                CASE WHEN type = 'free' THEN 0 ELSE 1 END,
                price_coins ASC NULLS FIRST
            "#
        )
        .fetch_all(&self.db)
        .await?;

        Ok(packs)
    }

    /// Get user lootpack statistics
    pub async fn get_user_stats(&self, user_id: &str) -> Result<UserStatsResponse> {
        // Try to get existing stats
        let stats = sqlx::query_as!(
            UserLootpackStats,
            r#"
            SELECT user_id, deal_coins, daily_streak, last_daily_claim,
                   total_packs_opened, level, level_progress, total_savings_inr,
                   member_status, puzzle_pieces, puzzle_packs_claimed, created_at, updated_at
            FROM user_lootpack_stats 
            WHERE user_id = $1
            "#,
            user_id
        )
        .fetch_optional(&self.db)
        .await?;

        let stats = match stats {
            Some(s) => s,
            None => {
                // Create default stats for new user
                let new_stats = sqlx::query_as!(
                    UserLootpackStats,
                    r#"
                    INSERT INTO user_lootpack_stats 
                    (user_id, deal_coins, daily_streak, total_packs_opened, level, 
                     level_progress, total_savings_inr, member_status, puzzle_pieces, puzzle_packs_claimed)
                    VALUES ($1, 500, 1, 0, 1, 0, 0, 'Bronze', 0, 0)
                    RETURNING user_id, deal_coins, daily_streak, last_daily_claim,
                             total_packs_opened, level, level_progress, total_savings_inr,
                             member_status, puzzle_pieces, puzzle_packs_claimed, created_at, updated_at
                    "#,
                    user_id
                )
                .fetch_one(&self.db)
                .await?;
                new_stats
            }
        };

        // Check if user can claim daily pack
        let now = Utc::now();
        let can_claim_daily = stats.last_daily_claim
            .map(|last_claim| now.signed_duration_since(last_claim) >= Duration::hours(24))
            .unwrap_or(true);

        let next_daily_claim = if can_claim_daily {
            None
        } else {
            stats.last_daily_claim.map(|last| last + Duration::hours(24))
        };

        Ok(UserStatsResponse {
            deal_coins: stats.deal_coins.unwrap_or(500),
            daily_streak: stats.daily_streak.unwrap_or(1),
            total_packs_opened: stats.total_packs_opened.unwrap_or(0),
            level: stats.level.unwrap_or(1),
            level_progress: stats.level_progress.unwrap_or(0),
            member_status: stats.member_status.unwrap_or_else(|| "Bronze".to_string()),
            can_claim_daily,
            next_daily_claim,
        })
    }

    /// Open a pack and generate rewards using DSA-optimized selection
    /// Enhanced to support ad requirements for free packs
    pub async fn open_pack(&self, user_id: &str, pack_type_id: Uuid) -> Result<OpenPackResponse> {
        let mut tx = self.db.begin().await?;

        // Get pack type and validate
        let pack_type = sqlx::query_as!(
            PackType,
            r#"
            SELECT id, name, type, description, icon, color_gradient, 
                   price_coins, cooldown_hours, min_rewards, max_rewards,
                   possible_reward_types, is_active, created_at, updated_at
            FROM pack_types 
            WHERE id = $1 AND is_active = true
            "#,
            pack_type_id
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| crate::error::AppError::NotFound("Pack type not found".to_string()))?;

        // Get user stats
        let user_stats = sqlx::query_as!(
            UserLootpackStats,
            "SELECT * FROM user_lootpack_stats WHERE user_id = $1",
            user_id
        )
        .fetch_optional(&mut *tx)
        .await?;

        // Enhanced validation for free packs - check if ad was watched recently
        if pack_type.r#type == "free" {
            if let Some(stats) = &user_stats {
                if let Some(last_claim) = stats.last_daily_claim {
                    let time_since_last = Utc::now().signed_duration_since(last_claim);
                    if time_since_last < Duration::hours(24) {
                        return Err(crate::error::AppError::BadRequest(
                            "Daily pack still on cooldown".to_string()
                        ));
                    }
                }
            }
            
            // Check if user has watched ad for daily pack in the last hour
            // This provides flexibility while preventing abuse
            let recent_daily_ad = sqlx::query!(
                r#"
                SELECT id FROM user_ad_interactions 
                WHERE user_id = $1 AND ad_placement = 'daily_pack_ad' 
                AND is_completed = true AND completed_at > NOW() - INTERVAL '1 hour'
                ORDER BY completed_at DESC LIMIT 1
                "#,
                user_id
            )
            .fetch_optional(&mut *tx)
            .await?;

            if recent_daily_ad.is_none() {
                return Err(crate::error::AppError::BadRequest(
                    "Please watch an ad to claim your daily free pack".to_string()
                ));
            }
        } else if let Some(price) = pack_type.price_coins {
            if let Some(stats) = &user_stats {
                let user_coins = stats.deal_coins.unwrap_or(0);
                if user_coins < price {
                    return Err(crate::error::AppError::BadRequest(
                        "Insufficient DealCoins".to_string()
                    ));
                }
            } else {
                return Err(crate::error::AppError::BadRequest(
                    "Insufficient DealCoins".to_string()
                ));
            }
        }

        // Get or build reward pool for this pack type
        let reward_pool = self.get_reward_pool_for_pack(pack_type_id).await?;

        // Generate rewards using DSA-optimized selection
        let num_rewards = {
            let mut rng = rand::thread_rng();
            rng.gen_range(pack_type.min_rewards..=pack_type.max_rewards)
        };

        let generated_rewards = self.generate_rewards(&reward_pool, num_rewards, &pack_type).await?;

        // Record pack opening
        let pack_history = sqlx::query!(
            r#"
            INSERT INTO user_pack_history (user_id, pack_type_id, rewards_count, total_value_inr)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            user_id,
            pack_type_id,
            generated_rewards.len() as i32,
            bigdecimal::BigDecimal::from(0) // TODO: Calculate actual value
        )
        .fetch_one(&mut *tx)
        .await?;

        // Insert rewards into user inventory
        for reward in &generated_rewards {
            let expires_at = if reward.r#type == "points" {
                None
            } else {
                Some(Utc::now() + Duration::days(30)) // Default 30 days
            };

            sqlx::query!(
                r#"
                INSERT INTO user_rewards 
                (user_id, pack_history_id, type, title, value, description, code, 
                 rarity, source, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
                user_id,
                pack_history.id,
                reward.r#type,
                reward.title,
                reward.value,
                reward.description,
                reward.code,
                reward.rarity,
                pack_type.name,
                expires_at
            )
            .execute(&mut *tx)
            .await?;
        }

        // Update user stats
        let coin_bonus = generated_rewards.iter()
            .filter(|r| r.r#type == "points")
            .map(|r| r.value.trim_start_matches('+').parse::<i32>().unwrap_or(0))
            .sum::<i32>();

        let pack_cost = if pack_type.r#type == "premium" {
            pack_type.price_coins.unwrap_or(0)
        } else {
            0
        };

        let level_progress_gain = 10;
        
        let updated_stats = if let Some(mut stats) = user_stats {
            let mut current_coins = stats.deal_coins.unwrap_or(500);
            let mut current_packs = stats.total_packs_opened.unwrap_or(0);
            let mut current_level = stats.level.unwrap_or(1);
            let mut current_progress = stats.level_progress.unwrap_or(0);
            let mut current_streak = stats.daily_streak.unwrap_or(1);
            
            current_coins = current_coins + coin_bonus - pack_cost;
            current_packs += 1;
            current_progress += level_progress_gain;
            
            // Handle level up
            if current_progress >= 100 {
                current_level += 1;
                current_progress = 0;
                current_coins += 100; // Level up bonus
            }

            // Update daily streak for free packs
            if pack_type.r#type == "free" {
                let now = Utc::now();
                if let Some(last_claim) = stats.last_daily_claim {
                    let hours_diff = now.signed_duration_since(last_claim).num_hours();
                    if hours_diff >= 24 && hours_diff < 48 {
                        current_streak += 1;
                    } else if hours_diff >= 48 {
                        current_streak = 1; // Reset streak
                    }
                } else {
                    current_streak = 1;
                }
                stats.last_daily_claim = Some(now);
            }

            sqlx::query!(
                r#"
                UPDATE user_lootpack_stats 
                SET deal_coins = $2, total_packs_opened = $3, level = $4, 
                    level_progress = $5, daily_streak = $6, last_daily_claim = $7,
                    updated_at = NOW()
                WHERE user_id = $1
                "#,
                user_id,
                current_coins,
                current_packs,
                current_level,
                current_progress,
                current_streak,
                stats.last_daily_claim
            )
            .execute(&mut *tx)
            .await?;

            // Update the stats for response
            stats.deal_coins = Some(current_coins);
            stats.total_packs_opened = Some(current_packs);
            stats.level = Some(current_level);
            stats.level_progress = Some(current_progress);
            stats.daily_streak = Some(current_streak);
            stats
        } else {
            return Err(crate::error::AppError::InternalError(
                "Failed to update user stats".to_string()
            ));
        };

        tx.commit().await?;

        info!("User {} opened pack {} and received {} rewards", 
              user_id, pack_type.name, generated_rewards.len());

        let stats_response = UserStatsResponse {
            deal_coins: updated_stats.deal_coins.unwrap_or(500),
            daily_streak: updated_stats.daily_streak.unwrap_or(1),
            total_packs_opened: updated_stats.total_packs_opened.unwrap_or(0),
            level: updated_stats.level.unwrap_or(1),
            level_progress: updated_stats.level_progress.unwrap_or(0),
            member_status: updated_stats.member_status.unwrap_or_else(|| "Bronze".to_string()),
            can_claim_daily: pack_type.r#type == "free" || 
                updated_stats.last_daily_claim
                    .map(|last| Utc::now().signed_duration_since(last) >= Duration::hours(24))
                    .unwrap_or(true),
            next_daily_claim: if pack_type.r#type == "free" {
                Some(Utc::now() + Duration::hours(24))
            } else {
                updated_stats.last_daily_claim.map(|last| last + Duration::hours(24))
            },
        };

        Ok(OpenPackResponse {
            rewards: generated_rewards,
            updated_stats: stats_response,
        })
    }

    /// Get user's rewards inventory
    pub async fn get_user_inventory(&self, user_id: &str) -> Result<UserInventoryResponse> {
        let rewards = sqlx::query_as!(
            UserReward,
            r#"
            SELECT id, user_id, pack_history_id, template_id, type, title, value,
                   description, code, rarity, source, expires_at, is_used, used_at, created_at
            FROM user_rewards 
            WHERE user_id = $1
            ORDER BY created_at DESC
            "#,
            user_id
        )
        .fetch_all(&self.db)
        .await?;

        let now = Utc::now();
        let active_count = rewards.iter().filter(|r| !r.is_used.unwrap_or(false)).count() as i32;
        let used_count = rewards.iter().filter(|r| r.is_used.unwrap_or(false)).count() as i32;
        let expiring_soon_count = rewards.iter()
            .filter(|r| !r.is_used.unwrap_or(false) && r.expires_at.map(|exp| (exp - now).num_days() <= 3).unwrap_or(false))
            .count() as i32;

        let stats = InventoryStats {
            active_count,
            used_count,
            expiring_soon_count,
            total_value_estimate: bigdecimal::BigDecimal::from(850), // TODO: Calculate actual value
        };

        Ok(UserInventoryResponse { rewards, stats })
    }

    /// Get reward pool for a pack type with caching
    async fn get_reward_pool_for_pack(&self, pack_type_id: Uuid) -> Result<RewardPool> {
        // Check cache first
        {
            let cache = self.reward_cache.read().await;
            if let Some(pool) = cache.get(&pack_type_id) {
                return Ok(pool.clone());
            }
        }

        // Build reward pool
        let mappings = sqlx::query!(
            r#"
            SELECT rt.id, rt.type, rt.title, rt.value, rt.description, rt.rarity,
                   rt.code_pattern, rt.validity_days, rt.metadata, rt.is_active, rt.created_at,
                   prm.weight
            FROM reward_templates rt
            JOIN pack_reward_mappings prm ON rt.id = prm.reward_template_id
            WHERE prm.pack_type_id = $1 AND rt.is_active = true
            ORDER BY prm.weight DESC
            "#,
            pack_type_id
        )
        .fetch_all(&self.db)
        .await?;

        let mut weighted_rewards = Vec::new();
        let mut cumulative_weight = 0;

        for mapping in mappings {
            cumulative_weight += mapping.weight.unwrap_or(1);
            
            let template = RewardTemplate {
                id: mapping.id,
                r#type: mapping.r#type,
                title: mapping.title,
                value: mapping.value,
                description: mapping.description,
                rarity: mapping.rarity,
                code_pattern: mapping.code_pattern,
                validity_days: mapping.validity_days,
                metadata: Some(mapping.metadata.unwrap_or_default()),
                is_active: Some(mapping.is_active.unwrap_or(true)),
                created_at: Some(mapping.created_at.unwrap_or_else(Utc::now)),
            };

            weighted_rewards.push(WeightedReward {
                template,
                weight: mapping.weight.unwrap_or(1),
                cumulative_weight,
            });
        }

        let pool = RewardPool::new(weighted_rewards);

        // Cache the pool
        {
            let mut cache = self.reward_cache.write().await;
            cache.insert(pack_type_id, pool.clone());
        }

        Ok(pool)
    }

    /// Generate rewards using DSA-optimized weighted selection
    async fn generate_rewards(
        &self,
        pool: &RewardPool,
        count: i32,
        pack_type: &PackType,
    ) -> Result<Vec<GeneratedReward>> {
        let mut rewards = Vec::new();

        // Guarantee at least one rare+ reward for premium packs
        if pack_type.r#type == "premium" && pack_type.price_coins.unwrap_or(0) >= 299 {
            let rare_rewards = pool.get_by_rarity("rare");
            let epic_rewards = pool.get_by_rarity("epic");
            let legendary_rewards = pool.get_by_rarity("legendary");
            
            let mut guaranteed_pool = Vec::new();
            guaranteed_pool.extend(rare_rewards);
            guaranteed_pool.extend(epic_rewards);
            guaranteed_pool.extend(legendary_rewards);
            
            if !guaranteed_pool.is_empty() {
                let idx = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(0..guaranteed_pool.len())
                };
                let template = guaranteed_pool[idx];
                rewards.push(self.template_to_generated_reward(template).await?);
            }
        }

        // Fill remaining slots with weighted random selection
        let remaining_count = count - rewards.len() as i32;
        for _ in 0..remaining_count {
            if pool.total_weight > 0 {
                let target_weight = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(1..=pool.total_weight)
                };
                if let Some(template) = pool.select_by_weight(target_weight) {
                    rewards.push(self.template_to_generated_reward(template).await?);
                }
            }
        }

        Ok(rewards)
    }

    /// Convert reward template to generated reward
    async fn template_to_generated_reward(&self, template: &RewardTemplate) -> Result<GeneratedReward> {
        let code = if template.r#type == "coupon" || template.r#type == "voucher" {
            Some(self.generate_coupon_code(&template.r#type).await)
        } else {
            None
        };

        let expires_at = if template.r#type == "points" {
            None
        } else {
            template.validity_days.map(|days| Utc::now() + Duration::days(days as i64))
        };

        Ok(GeneratedReward {
            id: Uuid::new_v4().to_string(),
            r#type: template.r#type.clone(),
            title: template.title.clone(),
            value: template.value.clone(),
            description: template.description.clone().unwrap_or_default(),
            code,
            rarity: template.rarity.clone(),
            expires_at,
        })
    }

    /// Generate unique coupon codes
    async fn generate_coupon_code(&self, reward_type: &str) -> String {
        let prefixes = match reward_type {
            "coupon" => vec!["DEAL", "SAVE", "SHOP", "MEGA", "SUPER"],
            "voucher" => vec!["GIFT", "FREE", "ENJOY", "TREAT", "BONUS"],
            _ => vec!["DEAL"],
        };

        let mut rng = rand::thread_rng();
        let prefix = prefixes[rng.gen_range(0..prefixes.len())];
        let suffix = rng.gen_range(100..999);
        
        format!("{}{}", prefix, suffix)
    }
}

// Implement Clone for RewardPool to support caching
impl Clone for RewardPool {
    fn clone(&self) -> Self {
        Self {
            rewards: self.rewards.clone(),
            total_weight: self.total_weight,
            rarity_pools: self.rarity_pools.clone(),
        }
    }
}
