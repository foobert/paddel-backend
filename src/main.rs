use anyhow::Result;
use clap::{App, Arg};
use log::{debug, info};
use osmpbfreader::objects::{Node, Way};
use osmpbfreader::{OsmObj, OsmPbfReader};
use rusqlite::{params, Connection};
use std::fs::File;

fn filter_object(obj: &OsmObj) -> bool {
    obj.is_way()
        && obj.tags().contains_key("waterway")
        && obj
            .tags()
            .values()
            .any(|v| v == "river" || v == "stream" || v == "canal")
}

fn update_node(node: &Node, conn: &Connection) -> Result<()> {
    debug!("Updating node {:?}", node);
    conn.execute(
        "INSERT OR REPLACE INTO nodes (id, lat, lon) VALUES (?, ?, ?)",
        params![node.id.0, node.lat(), node.lon()],
    )?;

    Ok(())
}

fn update_way(way: &Way, conn: &Connection) -> Result<()> {
    debug!("Updating way {:?} {:?}", way.id, way.tags);

    for node_pair in way.nodes.windows(2) {
        debug!("Inserting link between {:?}", node_pair);
        conn.execute(
            "INSERT OR REPLACE INTO links (source, destination) VALUES (?, ?)",
            params![node_pair[0].0, node_pair[1].0],
        )?;
    }

    Ok(())
}

fn init_db(conn: &Connection) -> Result<()> {
    debug!("Initializing database");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS nodes ( \
            id INTEGER PRIMARY KEY, \
            lat DOUBLE, \
            lon DOUBLE, \
            quadkey CHARACTER(10) \
            )",
        params![],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS links ( \
            id INTEGER PRIMARY KEY AUTOINCREMENT, \
            source INTEGER, \
            destination INTEGER, \
            FOREIGN KEY(source) REFERENCES nodes(id), \
            FOREIGN KEY(destination) REFERENCES nodes(id) \
            )",
        params![],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS links_source ON links(source)",
        params![],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS links_destination ON links(destination)",
        params![],
    )?;

    Ok(())
}

fn stats_db(conn: &Connection) -> Result<()> {
    let node_count: i64 =
        conn.query_row("SELECT count(*) FROM nodes", params![], |row| row.get(0))?;
    let link_count: i64 =
        conn.query_row("SELECT count(*) FROM links", params![], |row| row.get(0))?;
    info!("Nodes: {:?}, Links: {:?}", node_count, link_count);
    Ok(())
}

fn load_osm_pbf<T>(input: T, conn: &Connection) -> Result<()>
where
    T: std::io::Read + std::io::Seek,
{
    let mut pbf = OsmPbfReader::new(input);
    let objs = pbf.get_objs_and_deps(filter_object)?;

    info!("Updating database with {:?} objects...", objs.len());
    for (_, obj) in &objs {
        match obj {
            OsmObj::Node(node) => update_node(node, &conn)?,
            OsmObj::Way(way) => update_way(way, &conn)?,
            _ => (),
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();
    let matches = App::new("osm load")
        .arg(
            Arg::with_name("db")
                .short("d")
                .long("db")
                .value_name("DB")
                .help("Path to sqlite db file")
                .default_value("osm.db")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("pbf")
                .help("Path to input osm.pbf file")
                .required(true),
        )
        .get_matches();

    let db_filename = matches.value_of("db").unwrap();
    let pbf_filename = matches.value_of("pbf").unwrap();

    let conn = Connection::open(db_filename)?;
    init_db(&conn)?;

    let file = File::open(pbf_filename)?;
    info!("Loading {:?}. This may take a while...", pbf_filename);
    load_osm_pbf(file, &conn)?;

    stats_db(&conn)?;

    Ok(())
}
