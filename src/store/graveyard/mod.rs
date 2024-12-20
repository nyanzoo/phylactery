use std::{collections::BTreeMap, ops::Range, path::PathBuf, rc::Rc};

use crate::{deque::Location, u64_to_usize};

pub(crate) mod tombstone;
use tombstone::Tombstone;

pub struct Graveyard {
    tombs: Vec<Tombstone>,
    max_disk_usage: u64,
    amount_dead: u64,
    dir: Rc<PathBuf>,
}

// Store will need to be something like an event loop anyway
// we will use graveyard to collect tombstones from across files in the Deque
// and then we will create new sub-collections to make new files with.
// we need to have at least 1 deque node worth of disk space to make sure we can
// write the new file to disk.
// should be something like:
// 1. determine if need to compact data
// 2. if so, collect tombstones from all data files and make copy ranges
// 3. create new file(s) with copy ranges (data-store) (uncommitted!) can use .new as a suffix
// 4. update the metadata file to point to the new locations (not flushed yet)
//    we might not be able to safely use the mmap file approach, instead,
//    we might need to write to a separate file first then rename as .old
//    then we can modify the metadata file as we have a backup.
// 5. rename & flush the data-store file(s)
// 6. flush the metadata file
// if any step fails we can crash and recover from the .old file

pub struct Compaction {
    file_to_tombs: BTreeMap<Location, Vec<Range<usize>>>,
}

impl Compaction {
    pub fn shards(&self) -> impl Iterator<Item = usize> + '_ {
        self.file_to_tombs.keys().map(|l| u64_to_usize(l.file))
    }

    pub fn file_to_tombs(&self) -> &BTreeMap<Location, Vec<Range<usize>>> {
        &self.file_to_tombs
    }
}

impl Graveyard {
    pub(crate) fn new(dir: PathBuf, max_disk_usage: u64) -> Self {
        Self {
            tombs: vec![],
            max_disk_usage,
            amount_dead: 0,
            dir: Rc::new(dir),
        }
    }

    pub(crate) fn bury(&mut self, tomb: Tombstone) {
        self.tombs.push(tomb);
        self.amount_dead += tomb.len;
    }

    pub(crate) fn should_compact(&self) -> bool {
        self.amount_dead > ((self.max_disk_usage * 10) / 100)
    }

    pub(crate) fn compact(&mut self) -> Compaction {
        let Self { tombs, .. } = self;

        // map the tombstones to the files
        Compaction {
            file_to_tombs: map_tombs_to_files(
                self.dir.clone(),
                reduce_tombs(std::mem::take(tombs)),
            ),
        }
    }
}

fn reduce_tombs(tombs: Vec<Tombstone>) -> Vec<Tombstone> {
    let mut reduction = vec![];

    let mut map = BTreeMap::new();
    for tomb in tombs {
        map.entry(tomb.file).or_insert_with(Vec::new).push(tomb);
    }

    for tombs in map.values_mut() {
        reduction.extend(reduce_tombs_for_file(tombs));
    }

    reduction
}

fn reduce_tombs_for_file(tombs: &mut Vec<Tombstone>) -> Vec<Tombstone> {
    let mut reduction = vec![];
    tombs.sort_by(|a, b| a.offset.cmp(&b.offset));
    for tomb in tombs.drain(..) {
        if reduction.is_empty() {
            reduction.push(tomb);
        } else {
            let last = reduction.last().expect("no tombstones");
            if last.offset + last.len == tomb.offset {
                reduction.last_mut().expect("no tombstones").len += tomb.len;
            } else if last.offset == tomb.offset {
                // skip
            } else {
                reduction.push(tomb);
            }
        }
    }
    reduction
}

fn map_tombs_to_files(
    dir: Rc<PathBuf>,
    mut reduction: Vec<Tombstone>,
) -> BTreeMap<Location, Vec<Range<usize>>> {
    let mut tomb_map = BTreeMap::new();
    for tomb in reduction.drain(..) {
        let location = Location {
            dir: dir.clone(),
            file: tomb.file,
        };
        let range = Range {
            start: tomb.offset as usize,
            end: (tomb.offset + tomb.len) as usize,
        };
        tomb_map
            .entry(location)
            .or_insert_with(Vec::new)
            .push(range);
    }
    tomb_map
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_tomb(file: u64, offset: u64, len: u64) -> Tombstone {
        Tombstone {
            crc: 0,
            file,
            offset,
            len,
        }
    }

    #[test]
    fn test_reduce_tombs() {
        let tombs = vec![
            test_tomb(0, 0, 10),
            test_tomb(0, 10, 10),
            test_tomb(0, 30, 10),
            test_tomb(1, 30, 10),
            test_tomb(1, 50, 10),
            test_tomb(1, 70, 10),
            test_tomb(2, 0, 60),
            test_tomb(2, 60, 10),
            test_tomb(2, 80, 10),
            test_tomb(2, 90, 10),
        ];

        assert_eq!(
            reduce_tombs(tombs),
            [
                test_tomb(0, 0, 20),
                test_tomb(0, 30, 10),
                test_tomb(1, 30, 10),
                test_tomb(1, 50, 10),
                test_tomb(1, 70, 10),
                test_tomb(2, 0, 70),
                test_tomb(2, 80, 20),
            ]
            .to_vec()
        );
    }

    #[test]
    fn test_map_tombs_to_files() {
        let mut expected = BTreeMap::new();
        let dir = PathBuf::from("/tmp");
        expected.insert(Location::new(dir.clone(), 0), vec![0..20, 40..50]);
        expected.insert(Location::new(dir.clone(), 1), vec![30..40, 50..60]);

        assert_eq!(
            map_tombs_to_files(
                Rc::new(dir),
                vec![
                    test_tomb(0, 0, 20),
                    test_tomb(0, 40, 10),
                    test_tomb(1, 30, 10),
                    test_tomb(1, 50, 10),
                ]
            ),
            expected
        );
    }
}
