//! poly-sentient-exec — minimal Polymarket CLOB execution microservice.
//!
//! Forked from poly-dash's order-signing path (`main.rs` client setup +
//! `api.rs::exec_order` + the tick-exact Decimal helpers). It is STATELESS: it
//! owns no market/window state and runs no streams. poly-sentient (the
//! autonomous Node app) already has the live window + token ids + tick size from
//! poly-dash's shared `/ws` feed, so it passes those in per order. This service's
//! only job is to SIGN with the autonomous trader's OWN wallet and POST to the
//! CLOB — independent funds/orders from poly-dash.
//!
//! Endpoints (all 127.0.0.1):
//!   GET  /health
//!   POST /buy   /sell           {token_id, price_cents, size, order_type?, tick_size?, neg_risk?}
//!   POST /market-buy /market-sell {token_id, size, tick_size?, neg_risk?}   (FAK sweep)
//!   POST /cancel/{order_id}
//!   POST /cancel-all
//!   GET  /balance

use std::sync::Arc;

use axum::{
    extract::{Json, Path, State},
    routing::{get, post},
    Router,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::info;

/// Holds the polyfill-rs client used for order execution (one wallet).
struct TradingClient {
    polyfill: polyfill_rs::ClobClient,
}

// ---------------------------------------------------------------------------
// Tick-exact Decimal helpers (ported verbatim from poly-dash api.rs).
// `Decimal::from_f64_retain(0.01)` carries binary drift and fails the CLOB's
// `is_price_tick_aligned` modulo check — so we snap to the canonical
// `Decimal::new(1, n)` forms and build prices from snapped integer ticks.
// ---------------------------------------------------------------------------

fn tick_size_exact_decimal(f: f64) -> Decimal {
    const EPS: f64 = 1e-9;
    if (f - 0.01).abs() < EPS {
        return Decimal::new(1, 2);
    }
    if (f - 0.001).abs() < EPS {
        return Decimal::new(1, 3);
    }
    if (f - 0.0001).abs() < EPS {
        return Decimal::new(1, 4);
    }
    if (f - 0.1).abs() < EPS {
        return Decimal::new(1, 1);
    }
    tracing::warn!(tick_size = f, "unknown tick_size; falling back to f64 round-trip");
    Decimal::from_f64_retain(f).unwrap_or(Decimal::new(1, 2))
}

fn price_exact_decimal(price: f64, tick: Decimal) -> Decimal {
    use std::str::FromStr;
    let scale = tick.scale();
    let ticks_per_unit = 10f64.powi(scale as i32);
    let snapped_ticks = (price * ticks_per_unit).round() as i64;
    Decimal::from_str(&format!("{}", snapped_ticks))
        .ok()
        .map(|d| d / Decimal::new(ticks_per_unit as i64, 0))
        .unwrap_or_else(|| Decimal::new(snapped_ticks, scale))
}

/// Parse a string order type into polyfill's enum (default GTC for limits).
fn parse_order_type(s: &Option<String>, default: polyfill_rs::OrderType) -> polyfill_rs::OrderType {
    match s.as_deref().map(str::trim).map(str::to_uppercase).as_deref() {
        Some("GTC") => polyfill_rs::OrderType::GTC,
        Some("FAK") | Some("IOC") => polyfill_rs::OrderType::FAK,
        Some("FOK") => polyfill_rs::OrderType::FOK,
        Some("GTD") => polyfill_rs::OrderType::GTD,
        _ => default,
    }
}

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LimitReq {
    token_id: String,
    /// Price in cents, 1..99.
    price_cents: u32,
    /// Share count.
    size: f64,
    #[serde(default)]
    order_type: Option<String>,
    /// Market tick size (0.01 / 0.001 / 0.0001 / 0.1). Default 0.01.
    #[serde(default)]
    tick_size: Option<f64>,
    #[serde(default)]
    neg_risk: Option<bool>,
}

#[derive(Deserialize)]
struct MarketReq {
    token_id: String,
    size: f64,
    #[serde(default)]
    tick_size: Option<f64>,
    #[serde(default)]
    neg_risk: Option<bool>,
}

// ---------------------------------------------------------------------------
// Core
// ---------------------------------------------------------------------------

async fn exec_order(
    t: &TradingClient,
    token_id: &str,
    price_cents: u32,
    size: f64,
    side: polyfill_rs::Side,
    order_type: polyfill_rs::OrderType,
    tick_size: f64,
    neg_risk: bool,
) -> Value {
    let price_f64 = (price_cents as f64) / 100.0;
    let tick_dec = tick_size_exact_decimal(tick_size);
    let price_dec = price_exact_decimal(price_f64, tick_dec);
    let size_dec = Decimal::try_from(size).unwrap_or_default();
    let args = polyfill_rs::OrderArgs::new(token_id, price_dec, size_dec, side);

    let opts = polyfill_rs::types::CreateOrderOptions {
        tick_size: Some(tick_dec),
        neg_risk: Some(neg_risk),
    };
    let post_options = polyfill_rs::types::PostOrderOptions {
        order_type,
        post_only: false,
        defer_exec: false,
    };

    match t.polyfill.create_order(&args, Some(&opts)).await {
        Ok(signed) => match t.polyfill.post_order(signed, Some(&post_options)).await {
            Ok(resp) => json!({
                "success": resp.success,
                "state": if resp.status.is_empty() {
                    if resp.success { "FILLED" } else { "FAILED" }.to_string()
                } else { resp.status.clone() },
                "order_id": resp.order_id,
                "error": resp.error_msg,
                "taking_amount": resp.taking_amount,
                "making_amount": resp.making_amount,
                "tx_hashes": resp.transactions_hashes,
                "trade_ids": resp.trade_ids,
            }),
            Err(e) => json!({ "success": false, "error": format!("post: {e}") }),
        },
        Err(e) => json!({ "success": false, "error": format!("create: {e}") }),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health() -> &'static str {
    "ok"
}

async fn post_buy(State(t): State<Arc<TradingClient>>, Json(b): Json<LimitReq>) -> Json<Value> {
    let ot = parse_order_type(&b.order_type, polyfill_rs::OrderType::GTC);
    Json(
        exec_order(
            &t,
            &b.token_id,
            b.price_cents,
            b.size,
            polyfill_rs::Side::BUY,
            ot,
            b.tick_size.unwrap_or(0.01),
            b.neg_risk.unwrap_or(false),
        )
        .await,
    )
}

async fn post_sell(State(t): State<Arc<TradingClient>>, Json(b): Json<LimitReq>) -> Json<Value> {
    let ot = parse_order_type(&b.order_type, polyfill_rs::OrderType::GTC);
    Json(
        exec_order(
            &t,
            &b.token_id,
            b.price_cents,
            b.size,
            polyfill_rs::Side::SELL,
            ot,
            b.tick_size.unwrap_or(0.01),
            b.neg_risk.unwrap_or(false),
        )
        .await,
    )
}

async fn post_market_buy(State(t): State<Arc<TradingClient>>, Json(b): Json<MarketReq>) -> Json<Value> {
    Json(
        exec_order(
            &t,
            &b.token_id,
            99,
            b.size,
            polyfill_rs::Side::BUY,
            polyfill_rs::OrderType::FAK,
            b.tick_size.unwrap_or(0.01),
            b.neg_risk.unwrap_or(false),
        )
        .await,
    )
}

async fn post_market_sell(State(t): State<Arc<TradingClient>>, Json(b): Json<MarketReq>) -> Json<Value> {
    Json(
        exec_order(
            &t,
            &b.token_id,
            1,
            b.size,
            polyfill_rs::Side::SELL,
            polyfill_rs::OrderType::FAK,
            b.tick_size.unwrap_or(0.01),
            b.neg_risk.unwrap_or(false),
        )
        .await,
    )
}

async fn post_cancel(State(t): State<Arc<TradingClient>>, Path(id): Path<String>) -> Json<Value> {
    if id.is_empty() {
        return Json(json!({ "success": false, "error": "empty order_id" }));
    }
    match t.polyfill.cancel(&id).await {
        Ok(resp) => Json(json!({
            "success": true,
            "cancelled": resp.canceled,
            "not_cancelled": resp.not_canceled,
        })),
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

async fn post_cancel_all(State(t): State<Arc<TradingClient>>) -> Json<Value> {
    match t.polyfill.cancel_all().await {
        Ok(resp) => Json(json!({ "success": true, "canceled": resp.canceled.len() })),
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

async fn get_balance(State(t): State<Arc<TradingClient>>) -> Json<Value> {
    let params = polyfill_rs::types::BalanceAllowanceParams {
        asset_type: Some(polyfill_rs::types::AssetType::COLLATERAL),
        token_id: None,
        signature_type: Some(1),
    };
    match t.polyfill.get_balance_allowance(Some(params)).await {
        Ok(v) => {
            let raw = v.get("balance").and_then(|x| match x {
                Value::String(s) => s.parse::<f64>().ok(),
                Value::Number(n) => n.as_f64(),
                _ => None,
            });
            // USDC is 6 decimals; values >= 1e6 are base units.
            let usdc = raw.map(|r| r / 1_000_000.0);
            Json(json!({ "success": true, "usdc": usdc }))
        }
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("poly_sentient_exec=info")
        .init();

    let _ = dotenvy::dotenv_override();

    let private_key = std::env::var("POLY_PRIVATE_KEY")?;
    let funder: Option<alloy_primitives::Address> = std::env::var("POLY_ADDRESS")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse())
        .transpose()?;
    let api_key = std::env::var("POLY_API_KEY")?;
    let api_secret = std::env::var("POLY_API_SECRET")?;
    let passphrase = std::env::var("POLY_PASSPHRASE")?;

    let creds = polyfill_rs::ApiCredentials {
        api_key,
        secret: api_secret,
        passphrase,
    };
    let polyfill = polyfill_rs::ClobClient::from_config(polyfill_rs::types::ClientConfig {
        base_url: "https://clob.polymarket.com".to_string(),
        chain: 137,
        private_key: Some(private_key),
        api_credentials: Some(creds),
        signature_type: Some(1u8), // POLY_PROXY
        funder: funder.map(|f| format!("{:#x}", f)),
        ..polyfill_rs::types::ClientConfig::default()
    })
    .expect("failed to build polyfill ClobClient");

    let trading = Arc::new(TradingClient { polyfill });

    let app = Router::new()
        .route("/health", get(health))
        .route("/buy", post(post_buy))
        .route("/sell", post(post_sell))
        .route("/market-buy", post(post_market_buy))
        .route("/market-sell", post(post_market_sell))
        .route("/cancel/{order_id}", post(post_cancel))
        .route("/cancel-all", post(post_cancel_all))
        .route("/balance", get(get_balance))
        .with_state(trading);

    let port: u16 = std::env::var("EXEC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4321);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    info!("poly-sentient-exec listening on 127.0.0.1:{port} (local only)");
    axum::serve(listener, app).await?;
    Ok(())
}
