use memmap2::Mmap;
use std::collections::HashMap;

fn memory_map(file: &str) -> anyhow::Result<memmap2::Mmap> {
    let file = std::fs::File::open(file)?;
    let map = unsafe { memmap2::Mmap::map(&file)? };
    Ok(map)
}

fn split_map(file: &Mmap, count: usize) -> Vec<&[u8]> {
    let len = file.len();
    file.chunks(len / count).collect()
}

fn handle_measurement<'a>(
    stations: &mut HashMap<&'a str, (i64, i64, i64, i64)>,
    station: &'a str,
    measure: i64,
) {
    if let Some((n, total, min, max)) = stations.get_mut(station) {
        *n += 1;
        *total += measure;
        *min = measure.min(*min);
        *max = measure.max(*max);
    } else {
        stations.insert(station, (1, measure, measure, measure));
    }
}

fn print_result(stations: &HashMap<&str, (i64, i64, i64, i64)>) {
    let mut count = 0i64;
    let mut results = vec![];
    for (s, (n, total, min, max)) in stations.iter() {
        count += n;
        let mean = total / n; // todo fix rounding
        results.push((s, min, max, mean))
    }

    results.sort_by(|(k1, ..), (k2, ..)| k1.cmp(k2));

    print!("{{");
    for (station, min, max, mean) in results {
        print!("{station}={min}/{mean}/{max}, ");
    }
    println!("}}");
    println!("n: {count}");
}

fn join_maps(
    maps: Vec<HashMap<&str, (i64, i64, i64, i64)>>,
) -> HashMap<&str, (i64, i64, i64, i64)> {
    let mut complete = HashMap::new();
    for map in maps {
        for (k, (a, b, c, d)) in map.into_iter() {
            if let Some((a2, b2, c2, d2)) = complete.get_mut(k) {
                *a2 += a;
                *b2 += b;
                *c2 = c.min(*c2);
                *d2 = d.max(*d2);
            } else {
                complete.insert(k, (a, b, c, d));
            }
        }
    }
    complete
}

struct Reader<'a> {
    slice: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(slice: &'a [u8]) -> Self {
        // TODO: fix
        let offset = slice
            .iter()
            .enumerate()
            .find(|(_, c)| **c == b'\n')
            .map(|(i, _)| i)
            .unwrap()
            + 1;
        Self { slice, offset }
    }

    fn next(&mut self) -> Option<(&'a str, i64)> {
        let name = {
            let name_start = self.offset;
            loop {
                if self.offset >= self.slice.len() {
                    return None;
                }
                if self.slice[self.offset] == b';' {
                    break &self.slice[name_start..self.offset];
                }
                self.offset += 1;
            }
        };

        let measurement = {
            let mut negative = false;
            let mut n = 0;
            loop {
                if self.offset >= self.slice.len() {
                    return None;
                }
                match self.slice[self.offset] {
                    b'-' => {
                        negative = true;
                    }
                    b'0'..=b'9' => {
                        n += n * 10 + (self.slice[self.offset] - b'0') as i64;
                    }
                    b'\n' => {
                        self.offset += 1;
                        break;
                    }
                    _ => {}
                }
                self.offset += 1;
            }
            if negative {
                n = -n;
            }
            n
        };
        let name = unsafe { std::str::from_utf8_unchecked(name) };
        Some((name, measurement))
    }
}

fn main() -> anyhow::Result<()> {
    let map = memory_map("../1brc/measurements.txt")?;
    let chunks = split_map(&map, 24);

    let maps = std::thread::scope(|scope| {
        let mut handles = vec![];
        for chunk in chunks {
            let chunk = chunk;
            handles.push(scope.spawn(move || {
                let mut stations: HashMap<&str, (i64, i64, i64, i64)> = HashMap::new();

                let mut reader = Reader::new(chunk);
                while let Some((station, measure)) = reader.next() {
                    handle_measurement(&mut stations, station, measure);
                }
                stations
            }));
        }
        handles.into_iter().map(|r| r.join().unwrap()).collect()
    });

    let stations = join_maps(maps);
    print_result(&stations);
    Ok(())
}
