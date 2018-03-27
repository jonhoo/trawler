extern crate glob;

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("statistics.rs");
    let mut f = File::create(&dest_path).unwrap();

    for stats in glob::glob("data/*.dat").unwrap().filter_map(Result::ok) {
        let name = stats.file_name().unwrap();
        let name = name.to_str().unwrap().replace(".dat", "");

        if name == "requests" {
            continue;
        }

        f.write_all(
            format!(
                "const {}: &'static [(usize, usize)] = &[",
                name.to_uppercase().replace('-', "_")
            ).as_bytes(),
        ).unwrap();

        let stats = File::open(&*stats).map(BufReader::new).unwrap();
        for line in stats.lines().filter_map(Result::ok) {
            let mut fields = line.split_whitespace();
            f.write_all(
                format!("({}, {}),", fields.next().unwrap(), fields.next().unwrap()).as_bytes(),
            ).unwrap();
        }

        f.write_all(b"];\n").unwrap();
    }
}
