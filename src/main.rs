mod db;
mod quadkey;
mod web;
use anyhow::Result;
use clap::{App, Arg, SubCommand};
use log::info;

use db::Database;

#[macro_use]
extern crate anyhow;

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
        web::serve(database).await;
    }

    Ok(())
}
