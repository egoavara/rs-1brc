use std::io::{BufRead, BufReader};
use std::path::Path;
use hashbrown::HashMap;

struct Data {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

pub fn run(path: &Path) {
    let file = std::fs::File::open(path).unwrap();
    let reader = BufReader::new(file);
    let mut result: HashMap<String, Data> = HashMap::new();
    for line in reader.lines() {
        let line = line.unwrap();
        let mut fields = line.split(';');
        let key = fields.next().unwrap();
        let key = key.to_string();
        match fields.next() {
            None => {
                println!("Invalid line: {} \n", line);
                continue;
            }
            Some(data) => {
                let value = data.parse::<f64>().unwrap();
                result.entry(key)
                    .and_modify(|e| {
                        e.min = e.min.min(value);
                        e.max = e.max.max(value);
                        e.sum += value;
                        e.count += 1;
                    })
                    .or_insert_with(|| Data {
                        min: value,
                        max: value,
                        sum: value,
                        count: 1,
                    });
            }
        }
    }
    let keys = result.keys().map(|x| x.to_owned()).collect::<Vec<_>>();
    for key in keys {
        let data = result.get(&key).unwrap();
        // println!("{}: min={}, max={}, avg={}", key, data.min, data.max, data.sum / data.count as f64);
    }
}

