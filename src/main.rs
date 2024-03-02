use memmap2::Mmap;
use std::env::args;
use std::fmt::Display;
use std::io::{Read, Write};

#[derive(Clone)]
struct Entry<'a> {
    k: &'a [u8],
    v: (i64, i64, i64, i64),
}

const EMPTY_VEC: Vec<Entry<'_>> = vec![];
const HASH_CAP: usize = 1024;

struct HashMap<'a> {
    entries: [Vec<Entry<'a>>; 1024],
}

impl<'a> HashMap<'a> {
    fn new() -> Self {
        Self {
            entries: [EMPTY_VEC; HASH_CAP],
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
    fn update(&mut self, hash: usize, k: &'a [u8], measure: i64) {
        let pos = hash % HASH_CAP;
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

    fn into_iter(self) -> impl Iterator<Item = (&'a [u8], (i64, i64, i64, i64))> {
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

fn print_result(stations: HashMap) -> anyhow::Result<()> {
    let mut results = vec![];
    for (s, (n, total, min, max)) in stations.into_iter() {
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
        let station = std::str::from_utf8(&station)?;
        print!("{station}={min}/{mean}/{max}");
    }
    println!("}}");
    Ok(())
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
    len: usize,
}

impl<'a> Reader<'a> {
    fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            len: slice.len(),
        }
    }

    fn advance(&mut self, count: usize) -> &'a [u8] {
        unsafe {
            let base = self.slice.as_ptr();
            let part = &*std::ptr::slice_from_raw_parts(base, count);
            self.slice =
                &*std::ptr::slice_from_raw_parts(base.add(count + 1), self.len - count - 1);
            self.len -= count + 1;
            part
        }
    }
}
impl<'a> Iterator for Reader<'a> {
    type Item = (usize, &'a [u8], i64);

    fn next(&mut self) -> Option<(usize, &'a [u8], i64)> {
        if self.slice.is_empty() {
            return None;
        }

        let (hash, name) = {
            let mut count = 0;
            let mut hash = 0usize;
            self.slice.iter().take_while(|c| **c != b';').for_each(|c| {
                hash ^= (*c as usize) << (count % 64);
                count += 1;
            });
            let name = self.advance(count);
            (hash, name)
        };

        let measurement = {
            let mut iter = self.slice.iter();
            let (mut n, negative) = match unsafe { iter.next().unwrap_unchecked() } {
                b'-' => (0, true),
                c => ((*c - b'0') as i64, false),
            };

            let mut count = 1;
            iter.take_while(|c| **c != b'\n').for_each(|c| {
                if *c != b'.' {
                    n = n * 10 + (*c - b'0') as i64;
                }
                count += 1;
            });

            self.advance(count);
            if negative {
                -n
            } else {
                n
            }
        };

        Some((hash, name, measurement))
    }
}

const NUM_CHUNKS: usize = 72;

fn main() -> anyhow::Result<()> {
    if args().find(|a| a == "subprocess").is_none() {
        let res = std::process::Command::new(args().next().unwrap())
            .args(args().skip(1))
            .arg("subprocess")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        let stdout = res.stdout.unwrap();
        let res = stdout
            .bytes()
            .take_while(|c| c.as_ref().is_ok_and(|c| *c != b'\n'))
            .map(|c| c.unwrap())
            .collect::<Vec<_>>();
        std::io::stdout().lock().write(&res)?;
        return Ok(());
    }

    let file = std::env::args().skip(1).next().ok_or(anyhow::anyhow!(
        "Please provide the path to the input file as an argument"
    ))?;
    let map = memory_map(&file)?;
    let chunks = split_map(&map, NUM_CHUNKS);

    let maps: Vec<HashMap> = std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .chunks(chunks.len() / num_cpus::get())
            .into_iter()
            .map(|chunks| {
                scope.spawn(|| {
                    chunks
                        .into_iter()
                        .map(|chunk| {
                            let mut stations: HashMap = HashMap::new();

                            let reader = Reader::new(chunk);
                            for (hash, station, measure) in reader {
                                stations.update(hash, station, measure);
                            }

                            stations
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|r| r.join().unwrap())
            .flatten()
            .collect()
    });

    let stations = join_maps(maps);
    print_result(stations)?;

    Ok(())
}
