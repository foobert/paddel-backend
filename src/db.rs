use anyhow::Result;
use log::{debug, error, info};
use osmpbfreader::objects::{Node, Way};
use osmpbfreader::{OsmObj, OsmPbfReader};
use rusqlite::{params, Connection};
use std::f64::consts::PI;
use std::fs::File;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct Database {
    conn: Connection,
    filename: String,
}

#[derive(Debug, Clone)]
pub struct RouteNode {
    id: i64,
    pub lat: f64,
    pub lon: f64,
}
impl Hash for RouteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for RouteNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RouteNode {}

impl RouteNode {
    pub fn distance_to(&self, other: &Self) -> i32 {
        distance_between(self, other)
    }
}

impl Database {
    pub fn new(filename: &str) -> Result<Database> {
        let db = Database {
            conn: Connection::open(filename)?,
            filename: filename.into(),
        };
        db.init()?;
        db.fixup_quadkeys()?;
        return Ok(db);
    }

    fn init(&self) -> Result<()> {
        debug!("Initializing database");
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS nodes ( \
            id INTEGER PRIMARY KEY, \
            lat DOUBLE, \
            lon DOUBLE, \
            quadkey CHARACTER(10) \
            )",
            params![],
        )?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS links ( \
            id INTEGER PRIMARY KEY AUTOINCREMENT, \
            source INTEGER, \
            destination INTEGER, \
            FOREIGN KEY(source) REFERENCES nodes(id), \
            FOREIGN KEY(destination) REFERENCES nodes(id) \
            )",
            params![],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS links_source ON links(source)",
            params![],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS links_destination ON links(destination)",
            params![],
        )?;

        Ok(())
    }

    fn fixup_quadkeys(&self) -> Result<()> {
        info!("Fixing quadkeys...");
        let mut stmt = self
            .conn
            .prepare("SELECT id, lat, lon FROM nodes WHERE quadkey IS NULL")?;
        let nodes_iter = stmt.query_map(params![], |row| {
            let id: i64 = row.get(0)?;
            let lat: f64 = row.get(1)?;
            let lon: f64 = row.get(2)?;
            let quadkey = super::quadkey::Quadkey::new(lat, lon, 13);

            Ok((id, quadkey))
        });
        let mut stmt2 = self
            .conn
            .prepare("UPDATE nodes SET quadkey = ? WHERE id = ?")?;
        let mut count = 0;
        for res in nodes_iter? {
            if let Ok((id, quadkey)) = res {
                //debug!("Fix node {:?} with quadkey {:?}", id, quadkey.to_string());
                count += 1;
                stmt2.execute(params![quadkey.to_string(), id])?;
            } else {
                error!("Cannot fix quadkey: {:?}", res);
            }
        }
        info!("Fixed {:?} quadkeys", count);
        Ok(())
    }

    pub fn node_count(&self) -> Result<i64> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM nodes", params![], |row| row.get(0))?;
        Ok(count)
    }

    pub fn link_count(&self) -> Result<i64> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM links", params![], |row| row.get(0))?;
        Ok(count)
    }

    pub fn import(&self, filename: &str) -> Result<()> {
        info!("Parsing OSM pbf {:?}...", filename);
        let file = File::open(filename)?;
        self.load_osm_pbf(file)?;
        Ok(())
    }

    fn filter_object(&self, obj: &OsmObj) -> bool {
        obj.is_way()
            && obj.tags().contains_key("waterway")
            && obj
                .tags()
                .values()
                .any(|v| v == "river" || v == "stream" || v == "canal")
    }

    fn update_node(&self, node: &Node) -> Result<()> {
        debug!("Updating node {:?}", node);
        let quadkey = super::quadkey::Quadkey::new(node.lat(), node.lon(), 13);
        self.conn.execute(
            "INSERT OR REPLACE INTO nodes (id, lat, lon, quadkey) VALUES (?, ?, ?, ?)",
            params![node.id.0, node.lat(), node.lon(), quadkey.to_string()],
        )?;

        Ok(())
    }

    fn update_way(&self, way: &Way) -> Result<()> {
        debug!("Updating way {:?} {:?}", way.id, way.tags);

        for node_pair in way.nodes.windows(2) {
            debug!("Inserting link between {:?}", node_pair);
            self.conn.execute(
                "INSERT OR REPLACE INTO links (source, destination) VALUES (?, ?)",
                params![node_pair[0].0, node_pair[1].0],
            )?;
        }

        Ok(())
    }

    fn load_osm_pbf<T>(&self, input: T) -> Result<()>
    where
        T: std::io::Read + std::io::Seek,
    {
        let mut pbf = OsmPbfReader::new(input);
        let objs = pbf.get_objs_and_deps(|x| self.filter_object(x))?;

        info!("Updating database with {:?} objects...", objs.len());
        let mut count = 0;
        for (_, obj) in &objs {
            match obj {
                OsmObj::Node(node) => self.update_node(node)?,
                OsmObj::Way(way) => self.update_way(way)?,
                _ => (),
            }
            if count % 10000 == 0 {
                info!("Updated {:?} objects so far", count);
            }
            count += 1;
        }

        Ok(())
    }

    pub fn find_near(&self, lat: f64, lon: f64) -> Option<RouteNode> {
        match self.find_near_err(lat, lon) {
            Ok(node) => Some(node),
            Err(_) => None,
        }
    }

    fn find_near_err(&self, lat: f64, lon: f64) -> Result<RouteNode> {
        // compute quadkey, find all nodes near, sort by distance, pick first
        debug!("Looking for node near {:?} {:?}", lat, lon);
        let quadkey = super::quadkey::Quadkey::new(lat, lon, 12);
        debug!("Quadkey: {:?}", quadkey);
        let mut stmt = self
            .conn
            .prepare("SELECT id, lat, lon FROM nodes WHERE substr(quadkey, 1, ?) = ?")?;
        let nodes_iter = stmt
            .query_map(params![12, quadkey.to_string()], |row| {
                Ok(RouteNode {
                    id: row.get(0)?,
                    lat: row.get(1)?,
                    lon: row.get(2)?,
                })
            })?
            .map(|n| n.unwrap());
        let goal = RouteNode {
            id: 0,
            lat: lat,
            lon: lon,
        };
        let min_node = nodes_iter.min_by(|a, b| {
            let dist_a = distance_between(a, &goal);
            let dist_b = distance_between(b, &goal);
            dist_a.cmp(&dist_b)
        });
        debug!("min node: {:?}", min_node);

        return min_node.ok_or(anyhow!("No node near {:?} {:?}", lat, lon));
    }

    pub fn neighbours(&self, node: &RouteNode) -> Vec<(RouteNode, i32)> {
        self.neighbours_res(node).unwrap_or(Vec::new())
    }

    fn neighbours_res(&self, node: &RouteNode) -> Result<Vec<(RouteNode, i32)>> {
        let mut stmt_src = self.conn.prepare(
            "SELECT n.id, n.lat, n.lon FROM links l
        left join nodes n on l.destination = n.id
        where l.source = ?",
        )?;
        let mut stmt_dst = self.conn.prepare(
            "SELECT n.id, n.lat, n.lon FROM links l
        left join nodes n on l.source = n.id
        where l.destination = ?",
        )?;

        let nodes_iter = stmt_src
            .query_map(params![node.id], |row| {
                Ok(RouteNode {
                    id: row.get(0)?,
                    lat: row.get(1)?,
                    lon: row.get(2)?,
                })
            })?
            .chain(stmt_dst.query_map(params![node.id], |row| {
                Ok(RouteNode {
                    id: row.get(0)?,
                    lat: row.get(1)?,
                    lon: row.get(2)?,
                })
            })?)
            .filter(|n| n.is_ok())
            .map(|n| n.unwrap())
            .map(|n| (n.clone(), distance_between(&node, &n)))
            .collect();
        return Ok(nodes_iter);
    }
}

fn distance_between(a: &RouteNode, b: &RouteNode) -> i32 {
    let r = 6371e3; // metres
    let phi1 = (a.lat * PI) / 180.0; // φ, λ in radians
    let phi2 = (b.lat * PI) / 180.0;
    let delta_phi = ((b.lat - a.lat) * PI) / 180.0;
    let delta_lambda = ((b.lon - a.lon) * PI) / 180.0;

    let a = (delta_phi / 2.0).sin() * (delta_phi / 2.0).sin()
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin() * (delta_lambda / 2.0).sin();
    let c = 2.0 * (a.sqrt().atan2((1.0 - a).sqrt()));
    let d = r * c; // in metres
    return d.round() as i32;
}
