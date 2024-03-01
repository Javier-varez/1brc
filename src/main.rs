use memmap2::Mmap;
use std::fmt::Display;

#[derive(Clone)]
struct Entry<'a> {
    k: &'a str,
    v: (i64, i64, i64, i64),
}

struct HashMap<'a> {
    entries: Vec<Vec<Entry<'a>>>,
}

impl<'a> HashMap<'a> {
    const CAP: usize = 256 * 1024;
    fn new() -> Self {
        Self {
            entries: vec![vec![]; Self::CAP],
        }
    }

    fn join(&mut self, other: Self) {
        for (s, o) in self.entries.iter_mut().zip(other.entries.into_iter()) {
            for oe in o {
                if let Some(se) = s.iter_mut().find(|e| e.k == oe.k) {
                    let (n, total, min, max) = &mut (se.v);
                    *n += oe.v.0;
                    *total += oe.v.1;
                    *min = oe.v.2.min(*min);
                    *max = oe.v.3.max(*max);
                } else {
                    s.push(oe);
                }
            }
        }
    }

    // Hash is calculated externally to avoid iterating through the key again, if possible
    fn update(&mut self, hash: usize, k: &'a str, measure: i64) {
        let pos = hash % Self::CAP;
        if let Some(e) = self.entries[pos].iter_mut().find(|e| e.k == k) {
            let (n, total, min, max) = &mut (e.v);
            *n += 1;
            *total += measure;
            *min = measure.min(*min);
            *max = measure.max(*max);
        } else {
            self.entries[pos].push(Entry {
                k,
                v: (1, measure, measure, measure),
            });
        }
    }

    fn into_iter(self) -> impl Iterator<Item = (&'a str, (i64, i64, i64, i64))> {
        self.entries
            .into_iter()
            .map(|e| e.into_iter().map(|e| (e.k, e.v)))
            .flatten()
    }
}

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

fn print_result(stations: HashMap) {
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

fn join_maps(maps: Vec<HashMap>) -> HashMap {
    let mut complete = HashMap::new();
    for map in maps {
        complete.join(map);
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
    type Item = (usize, &'a str, i64);

    fn next(&mut self) -> Option<(usize, &'a str, i64)> {
        if self.slice.is_empty() {
            return None;
        }

        let (hash, name) = {
            let mut count = 0;
            let mut hash = 0usize;
            self.slice.iter().take_while(|c| **c != b';').for_each(|c| {
                hash ^= (*c as usize) << (count % 8);
                count += 1;
            });
            // We know the input is well formed utf8, don't bother checking and wasting time...
            let name = unsafe { std::str::from_utf8_unchecked(&self.slice[..count]) };
            self.slice = &self.slice[count..];
            (hash, name)
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

        Some((hash, name, measurement))
    }
}

fn main() -> anyhow::Result<()> {
    let begin_time = std::time::Instant::now();

    let file = std::env::args().skip(1).next().ok_or(anyhow::anyhow!(
        "Please provide the path to the input file as an argument"
    ))?;
    let map = memory_map(&file)?;
    let chunks = split_map(&map, num_cpus::get());

    let maps: Vec<HashMap> = std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                scope.spawn(|| {
                    let mut stations: HashMap = HashMap::new();

                    let reader = Reader::new(chunk);
                    for (hash, station, measure) in reader {
                        stations.update(hash, station, measure);
                    }

                    stations
                })
            })
            .collect();

        handles.into_iter().map(|r| r.join().unwrap()).collect()
    });

    let mid_time = std::time::Instant::now();

    let stations = join_maps(maps);
    print_result(stations);

    let end_time = std::time::Instant::now();
    let elapsed = end_time - begin_time;

    println!("elapsed seconds: {}", elapsed.as_secs_f32());
    println!("par time : {}", (mid_time - begin_time).as_secs_f32());
    println!("serial time : {}", (end_time - mid_time).as_secs_f32());
    Ok(())
}
