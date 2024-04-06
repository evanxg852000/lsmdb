use std::path::Path;

use lsmdb_storage::{LiteDb, LiteDbOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("./data");
    let db = LiteDb::open(db_path, LiteDbOptions::default()).unwrap();
    for i in 0..=10 {
        let k = format!("k_{:01$}", i, 3);
        let v = format!("v_{:01$}", i, 3);
        db.set(k.as_bytes(), v.as_bytes()).unwrap();
    }

    for result in db.scan(&None, &None)? {
        let (k, v) = result?;
        println!("{} -> {}", String::from_utf8(k)?, String::from_utf8(v)?);
    }
    Ok(())
}
