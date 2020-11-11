use log::info;
use pathfinding::prelude::astar;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::http::StatusCode;
use warp::{reject, Filter, Rejection, Reply};

use crate::db::Database;

type DatabasePool = Arc<Mutex<Database>>;

#[derive(Deserialize)]
struct QueryParams {
    lat1: f32,
    lon1: f32,
    lat2: f32,
    lon2: f32,
}

pub async fn serve(database: Database) -> () {
    let dbpool = Arc::new(Mutex::new(database));
    let env = warp::any().map(move || dbpool.clone());

    let route = warp::get()
        .and(warp::path("route"))
        .and(warp::query::<QueryParams>())
        .and(env.clone())
        .and_then(route);

    let routes = route.recover(handle_rejection);

    let server = warp::serve(routes);
    let s = server.run(([127, 0, 0, 1], 8081));
    info!("Server listening on http://localhost:8081");
    s.await;
}

/// An API error serializable to JSON.
#[derive(Serialize)]
struct ErrorMessage {
    code: u16,
    message: String,
}

#[derive(Serialize)]
struct RouteResult {
    linestring: Vec<(f64, f64)>,
    distance: i32,
}

#[derive(Debug)]
struct NodeNotFound;

impl reject::Reject for NodeNotFound {}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND";
    } else if let Some(NodeNotFound) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "Node not found";
    } else {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION";
    }

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });
    Ok(warp::reply::with_status(json, code))
}

async fn route(
    params: QueryParams,
    db_pool: DatabasePool,
) -> std::result::Result<impl Reply, Rejection> {
    let database = db_pool.lock().await;
    let maybe_start = database.find_near(params.lat1 as f64, params.lon1 as f64);
    let maybe_goal = database.find_near(params.lat2 as f64, params.lon2 as f64);

    if maybe_start.is_none() || maybe_goal.is_none() {
        return Err(reject::custom(NodeNotFound));
    }

    let start = maybe_start.unwrap();
    let goal = maybe_goal.unwrap();

    let result = astar(
        &start,
        |n| database.neighbours(n),
        |n| n.distance_to(&goal),
        |n| *n == goal || n.distance_to(&goal) < 100,
    );

    if let Some((nodes, distance)) = result {
        let linestring: Vec<(f64, f64)> = nodes.iter().map(|n| (n.lat, n.lon)).collect();
        let json = warp::reply::json(&RouteResult {
            linestring: linestring,
            distance: distance,
        });
        Ok(warp::reply::with_status(json, StatusCode::OK))
    } else {
        return Err(reject());
    }
}
