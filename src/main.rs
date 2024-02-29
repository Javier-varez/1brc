use std::collections::HashMap;
use std::io::Read;
use std::sync::Mutex;

const SIZE: usize = 128 * 1024;
struct BufferedFile {
    file: std::fs::File,
    buffer: Vec<u8>,
    offset: usize,
    len: usize,
}

impl BufferedFile {
    fn next(&mut self) -> Option<&str> {
        if let Some(linebreak) = self.buffer[self.offset..self.offset + self.len]
            .iter()
            .enumerate()
            .find_map(|(idx, c)| if *c == b'\n' { Some(idx) } else { None })
        {
            if linebreak == 0 {
                return None;
            }
            let line = &self.buffer[self.offset..self.offset + linebreak];
            self.len -= line.len() + 1;
            self.offset += line.len() + 1;
            return Some(unsafe { std::str::from_utf8_unchecked(line) });
        };

        for i in 0..self.len {
            self.buffer[i] = self.buffer[i + self.offset];
        }
        self.offset = 0;

        let n = self.file.read(&mut self.buffer[self.len..]).ok()?;
        if n == 0 {
            return None;
        }
        self.len += n;
        self.next()
    }
}

fn main() -> anyhow::Result<()> {
    // let file = std::fs::File::open("example.txt")?;
    let file = std::fs::File::open("../1brc/measurements.txt")?;

    let mut file = BufferedFile {
        file,
        buffer: vec![0; SIZE],
        offset: 0,
        len: 0,
    };

    let stations: Mutex<HashMap<String, (i64, i64, i64, i64)>> = Mutex::new(HashMap::new());
    let mut i = 0usize;
    loop {
        let Some(line) = file.next() else {
            break;
        };

        let mut iter = line.split(';');
        let station = iter.next().unwrap();
        let measure: i64 = iter
            .next()
            .map(|s| s.chars().filter(|c| *c != '.').collect::<String>())
            .unwrap()
            .parse()?;

        let mut stations = stations.lock().unwrap();
        if let Some((n, total, min, max)) = stations.get_mut(station) {
            *n += 1;
            *total += measure;
            *min = measure.min(*min);
            *max = measure.max(*max);
        } else {
            stations.insert(station.to_owned(), (1, measure, measure, measure));
        }

        if i % (1024 * 1024) == 0 {
            println!("Processed {}Mi rows", i / (1024 * 1024));
        }

        i += 1;
    }

    let mut results = vec![];
    let stations = stations.lock().unwrap();
    for (s, (n, total, min, max)) in stations.iter() {
        let mean = total / n; // todo fix rounding
        results.push((s, min, max, mean))
    }

    results.sort_by(|(k1, ..), (k2, ..)| k1.cmp(k2));
    print!("{{");
    for (station, min, max, mean) in results {
        print!("{station}={min}/{mean}/{max}, ");
    }
    print!("}}");

    Ok(())
}
