use std::io::{self, Write};
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::{collections::HashMap, fs::read_to_string};
use toml::Value;
use walkdir::{DirEntry, WalkDir};

use serde::{Deserialize, Deserializer};

#[derive(Debug)]
struct OsmConn {
    conn: String,
    schema: String,
}

impl<'de> Deserialize<'de> for OsmConn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let connection_params = Value::deserialize(deserializer)?;

        let host = connection_params
            .get("host")
            .and_then(|v| v.as_str())
            .expect("Parameter host is not a string");
        let user = connection_params
            .get("user")
            .and_then(|v| v.as_str())
            .expect("Parameter user is not a string");
        let password = connection_params
            .get("password")
            .and_then(|v| v.as_str())
            .expect("Parameter password is not a string");
        let port = connection_params
            .get("port")
            .and_then(|v| v.as_integer())
            .expect("Parameter port error");
        let name = connection_params
            .get("name")
            .and_then(|v| v.as_str())
            .expect("Parameter name is not a string");

        let schema = connection_params
            .get("schema")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
            .expect("Parameter schema is not a string");

        let ogr_conn = format!(
            r#"PG:dbname='{name}' host='{host}' port={port} user='{user}' password='{password}'"#
        );

        Ok(Self {
            conn: ogr_conn,
            schema: schema,
        })
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    tables: HashMap<String, Vec<String>>,
    connection: OsmConn,
}

fn process_table(
    table_name: &String,
    iso3_codes: &Vec<String>,
    temp_dir: &PathBuf,
    connection: &OsmConn,
) {
    let iso3_code = iso3_codes[0].clone();

    let roads_layer = format!("{}_trs_roads_osm", iso3_code.to_lowercase());
    let streets_layer = format!("{iso3_code}_trs_streets_osm");

    let output_dir_roads = temp_dir.join(&roads_layer);

    let schema = &connection.schema;
    let roads_sql = format!("SELECT * FROM {schema}.{table_name} where iso3 = '{iso3_code}'");
    let mut cmd = Command::new("ogr2ogr");

    let output = cmd
        .args([
            "-f",
            "ESRI Shapefile",
            &roads_layer,
            &connection.conn,
            "-sql",
            &roads_sql,
        ])
        .output()
        .expect("Failed to execute process");

    io::stderr().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();

    // zip Element.
    let zip_file = format!("{roads_layer}.zip");
    let file = File::create(zip_file).unwrap();

    let path = Path::new(&zip_file);
    let walkdir = WalkDir::new(roads_layer);
    let it = walkdir.into_iter();

    //zip_dir(&mut it.filter_map(|e| e.ok()), src_dir, file, method);

    //.output()
    //.expect("Failed running command");

    //println!("{:?}", output_dir_roads);
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let temp_dir = std::env::temp_dir();

    // Read config
    let config = read_to_string("osm.toml")
        .and_then(|content| {
            toml::from_str::<Config>(&content)
                .map_err(|_| Error::new(ErrorKind::Other, "failed parsing config"))
        })
        .expect("Failed parsing config");

    config.tables.iter().for_each(|(table_name, iso3_codes)| {
        process_table(table_name, iso3_codes, &temp_dir, &config.connection)
    });

    //log::info!("{:?}", config);
}
