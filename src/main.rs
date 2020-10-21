mod db;
mod quadkey;
use anyhow::Result;
use clap::{App, Arg, SubCommand};
use log::info;
use pathfinding::prelude::astar;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::Filter;

use db::Database;

#[macro_use]
extern crate anyhow;

pub type DatabasePool = Arc<Mutex<Database>>;

#[derive(Deserialize)]
struct QueryParams {
    lat1: f32,
    lon1: f32,
    lat2: f32,
    lon2: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let matches = App::new("osm rust")
        .arg(
            Arg::with_name("db")
                .short("d")
                .long("db")
                .value_name("DB")
                .help("Path to sqlite db file")
                .default_value("osm.db")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("import")
                .about("imports OSM data")
                .arg(
                    Arg::with_name("pbf")
                        .help("Path to input osm.pbf file")
                        .required(true),
                ),
        )
        .get_matches();

    let db_filename = matches.value_of("db").unwrap();
    let database = Database::new(db_filename)?;

    if let Some(matches) = matches.subcommand_matches("import") {
        let pbf_filename = matches.value_of("pbf").unwrap();
        database.import(pbf_filename)?;
        info!(
            "Nodes: {:?}, Links: {:?}",
            database.node_count()?,
            database.link_count()?
        );
    } else {
        let dbpool = Arc::new(Mutex::new(database));
        let env = warp::any().map(move || dbpool.clone());

        let hello = warp::get()
            .and(warp::path("route"))
            .and(warp::query::<QueryParams>())
            .and(env.clone())
            .and_then(route);

        let server = warp::serve(hello);
        server.run(([127, 0, 0, 1], 8081)).await;
    }

    Ok(())
}

async fn route(
    params: QueryParams,
    dbPool: DatabasePool,
) -> std::result::Result<impl warp::Reply, warp::Rejection> {
    let database = dbPool.lock().await;
    let start = database
        .find_near(params.lat1 as f64, params.lon1 as f64)
        .unwrap();
    let goal = database
        .find_near(params.lat2 as f64, params.lon2 as f64)
        .unwrap();

    let result = astar(
        &start,
        |n| database.neighbours(n),
        |n| n.distance_to(&goal),
        |n| *n == goal || n.distance_to(&goal) < 100,
    );

    if let Some((nodes, distance)) = result {
        let linestring: Vec<(f64, f64)> = nodes.iter().map(|n| (n.lat, n.lon)).collect();
        let foo = json!({
            "linestring": linestring,
            "distance": distance,
        });
        return Ok(foo.to_string());
    } else {
        let linestring: Vec<(f64, f64)> = Vec::new();
        let foo = json!({
            "linestring": linestring,
            "distance": 0
        });
        return Ok(foo.to_string());
    }
}
