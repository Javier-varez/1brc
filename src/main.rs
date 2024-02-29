use memmap2::Mmap;
use std::collections::HashMap;
use std::fmt::Display;

fn memory_map(file: &str) -> anyhow::Result<memmap2::Mmap> {
    let file = std::fs::File::open(file)?;
    let map = unsafe { memmap2::Mmap::map(&file)? };
    Ok(map)
}

fn split_map(file: &Mmap, count: usize) -> Vec<&[u8]> {
    let len = file.len();
    let boundaries = (1..count).map(|i| {
        let mut cur = len * i / count;
        while file[cur] != b'\n' {
            cur += 1;
        }
        cur + 1
    });

    let boundaries = std::iter::once(0)
        .chain(boundaries)
        .chain(std::iter::once(len));

    boundaries
        .clone()
        .zip(boundaries.skip(1))
        .map(|(b, e)| &file[b..e])
        .collect()
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

struct Float(i64);

impl Display for Float {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = if self.0 < 0 { -self.0 } else { self.0 };
        let neg = if self.0 < 0 { "-" } else { "" };
        let i = v / 10;
        let d = v % 10;
        write!(f, "{}{}.{}", neg, i, d)
    }
}

fn print_result(stations: HashMap<&str, (i64, i64, i64, i64)>) {
    let mut count = 0i64;
    let mut results = vec![];
    for (s, (n, total, min, max)) in stations.into_iter() {
        count += n;
        let mean = if total < 0 {
            (total - n / 2) / n
        } else {
            (total + n / 2) / n
        };
        results.push((s, Float(min), Float(max), Float(mean)))
    }

    results.sort_by(|(k1, ..), (k2, ..)| k1.cmp(k2));

    print!("{{");
    for (i, (station, min, max, mean)) in results.into_iter().enumerate() {
        if i != 0 {
            print!(", ");
        }
        print!("{station}={min}/{mean}/{max}");
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
}

impl<'a> Reader<'a> {
    fn new(slice: &'a [u8]) -> Self {
        Self { slice }
    }
}
impl<'a> Iterator for Reader<'a> {
    type Item = (&'a str, i64);

    fn next(&mut self) -> Option<(&'a str, i64)> {
        if self.slice.is_empty() {
            return None;
        }

        let name = {
            let count = unsafe {
                self.slice
                    .iter()
                    .enumerate()
                    .find(|(_, c)| **c == b';')
                    .map(|(i, _)| i)
                    .unwrap_unchecked()
            };
            let res = &self.slice[..count];
            self.slice = &self.slice[count..];
            res
        };

        let measurement = {
            let mut negative = false;
            let mut n = 0;

            let mut skip = 0;

            self.slice
                .iter()
                .take_while(|c| **c != b'\n')
                .for_each(|c| {
                    match *c {
                        b'-' => {
                            negative = true;
                        }
                        b'0'..=b'9' => {
                            n = n * 10 + (*c - b'0') as i64;
                        }
                        _ => {}
                    }
                    skip += 1;
                });

            self.slice = &self.slice[skip + 1..];
            if negative {
                -n
            } else {
                n
            }
        };
        // We know the input is well formed utf8, don't bother checking and wasting time...
        let name = unsafe { std::str::from_utf8_unchecked(name) };
        Some((name, measurement))
    }
}

fn main() -> anyhow::Result<()> {
    let begin_time = std::time::Instant::now();

    let file = std::env::args().skip(1).next().ok_or(anyhow::anyhow!(
        "Please provide the path to the input file as an argument"
    ))?;
    let map = memory_map(&file)?;
    let chunks = split_map(&map, num_cpus::get());

    let maps = std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                scope.spawn(|| {
                    let mut stations: HashMap<&str, (i64, i64, i64, i64)> = HashMap::new();

                    let reader = Reader::new(chunk);
                    for (station, measure) in reader {
                        handle_measurement(&mut stations, station, measure);
                    }

                    stations
                })
            })
            .collect();

        handles.into_iter().map(|r| r.join().unwrap()).collect()
    });

    let stations = join_maps(maps);
    print_result(stations);

    let end_time = std::time::Instant::now();
    let elapsed = end_time - begin_time;

    println!("elapsed seconds: {}", elapsed.as_secs_f32());
    Ok(())
}
