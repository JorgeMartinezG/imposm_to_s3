use s3::creds::Credentials;
use s3::error::S3Error;
use s3::Bucket;

use std::collections::HashMap;
use std::fs::read_to_string;
use std::fs::File;
use std::io::{self, Error, ErrorKind, Read, Seek, Write};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::process::Command;
use zip::result::ZipError;
use zip::write::FileOptions;

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

        let db_fields = ["host", "user", "password", "name", "port", "schema"];

        let items = db_fields
            .iter()
            .map(|i| {
                connection_params
                    .get(i)
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("Failed parsing database params {i}"))
            })
            .collect::<Vec<&str>>();

        let [host, user, password, name, port, schema] = items.as_slice() else { panic!("Failed to get all database values") };

        let ogr_conn = format!(
            r#"PG:dbname='{name}' host='{host}' port={port} user='{user}' password='{password}'"#
        );

        Ok(Self {
            conn: ogr_conn,
            schema: schema.to_string(),
        })
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    tables: HashMap<String, Vec<String>>,
    connection: OsmConn,
}

fn zip_dir<T>(
    it: &mut dyn Iterator<Item = DirEntry>,
    prefix: &str,
    writer: T,
    method: zip::CompressionMethod,
) -> zip::result::ZipResult<()>
where
    T: Write + Seek,
{
    let mut zip_writer = zip::ZipWriter::new(writer);
    let options = FileOptions::default()
        .compression_method(method)
        .unix_permissions(0o755);

    let mut buffer = Vec::new();

    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(Path::new(prefix)).unwrap();

        if path.is_file() == false {
            continue;
        }
        println!("adding file {path:?} as {name:?} ...");
        zip_writer.start_file(name.to_str().unwrap(), options)?;

        let mut f = File::open(path)?;
        f.read_to_end(&mut buffer)
            .and_then(|_| zip_writer.write_all(&buffer))
            .map(|_| buffer.clear())?;
    }
    zip_writer.finish()?;
    Ok(())
}

async fn put_s3(path: &String, zip_path: &str) -> Result<(), S3Error> {
    let creds = Credentials::new(Some(""), Some(""), None, None, None)?;

    let bucket = Bucket::new("osm-roads-dumps", "eu-west-2".parse()?, creds)?;

    println!("{:?}", bucket);

    let mut f = tokio::fs::File::open(zip_path)
        .await
        .expect("Failed openning");

    //println!("{}-{}", path, zip_path);

    let response_data = bucket.put_object_stream(&mut f, path).await?;

    println!("{:?}", response_data);

    return Ok(());
}

async fn process_table(
    table_name: &String,
    iso3_codes: &Vec<String>,
    temp_dir: &PathBuf,
    connection: &OsmConn,
) {
    let iso3_code = iso3_codes[0].clone();

    let roads_layer = format!("{}_trs_roads_osm", iso3_code.to_lowercase());
    let _streets_layer = format!("{iso3_code}_trs_streets_osm");

    let output_dir_roads = temp_dir.join(&roads_layer);

    let schema = &connection.schema;
    let roads_sql = format!("SELECT * FROM {schema}.{table_name} where iso3 = '{iso3_code}'");
    let mut cmd = Command::new("ogr2ogr");

    let zip_file = format!("{roads_layer}.zip");

    let output = cmd
        .args([
            "-f",
            "ESRI Shapefile",
            &roads_layer,
            &connection.conn,
            "-sql",
            &roads_sql,
            "-nln",
            &roads_layer,
        ])
        .output()
        .expect("Failed to execute process");

    io::stderr().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();

    // zip Element.
    let zip_file = format!("{roads_layer}.zip");
    let file = File::create(&zip_file).unwrap();

    let walkdir = WalkDir::new(&roads_layer);
    let it = walkdir.into_iter();

    zip_dir(
        &mut it.filter_map(|e| e.ok()),
        &roads_layer,
        file,
        zip::CompressionMethod::Stored,
    )
    .unwrap();

    put_s3(&roads_layer, &zip_file).await.unwrap();

    //.output()
    //.expect("Failed running command");

    println!("{:?}", output_dir_roads);
}

#[tokio::main]
async fn main() {
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

    for (table_name, iso3_codes) in config.tables {
        process_table(&table_name, &iso3_codes, &temp_dir, &config.connection).await
    }

    //log::info!("{:?}", config);
}
