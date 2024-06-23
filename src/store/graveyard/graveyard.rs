use std::collections::VecDeque;

use super::tombstone::Tombstone;

pub struct Graveyard {
    tombs: Vec<Tombstone>,
    node_size: u64,
    max_disk_usage: u64,
    amount_dead: u64,
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

#[derive(Copy, Clone, Debug)]
pub struct CopyRange {
    file: u64,
    start: u64,
    end: u64,
}

pub struct NewNode {
    ranges: Vec<CopyRange>,
    fill: u64,
}

pub struct Compaction {
    nodes: Vec<NewNode>,
}

impl Graveyard {
    fn new(max_disk_usage: u64, node_size: u64) -> Self {
        Self {
            tombs: Vec::new(),
            max_disk_usage,
            amount_dead: 0,
            node_size,
        }
    }

    fn bury(&mut self, tomb: Tombstone) {
        self.tombs.push(tomb);
        self.amount_dead += tomb.len;
    }

    fn compact(self, file_start: u64) -> Compaction {
        let Self {
            tombs,
            max_disk_usage,
            amount_dead,
            node_size,
        } = self;

        let reduction = reduce_tombs(tombs);

        // map the tombstones to the files
        let tomb_map = map_tombs(reduction);

        // now we need to create the copy ranges
        let ranges = copy_ranges(tomb_map, self.node_size);

        // now we need to create the new nodes
        let mut new_nodes = vec![];
        let mut current_file = file_start;

        for range in ranges {
            if new_nodes.is_empty() {
                let new_node = NewNode {
                    ranges: vec![range],
                    fill: range.end - range.start,
                };
                new_nodes.push(new_node);

                current_file = range.file;
            } else {
                let mut new_node = new_nodes.last_mut().expect("no new nodes");
                let len = range.end - range.start;
                if new_node.fill + len < node_size {
                    new_node.ranges.push(range);
                    new_node.fill += len;
                } else {
                    let new_node = NewNode {
                        ranges: vec![range],
                        fill: len,
                    };
                    new_nodes.push(new_node);
                }
            }
        }

        Compaction { nodes: new_nodes }
    }
}

fn reduce_tombs(mut tombs: Vec<Tombstone>) -> VecDeque<Tombstone> {
    let mut reduction = VecDeque::new();

    // compact the tombstones first
    tombs.sort_by(|a, b| a.offset.cmp(&b.offset));

    for tomb in tombs {
        if reduction.is_empty() {
            reduction.push_back(tomb);
        } else {
            let last = reduction.back().expect("no tombstones");
            if last.offset + last.len == tomb.offset {
                reduction.back_mut().expect("no tombstones").len += tomb.len;
            } else if last.offset == tomb.offset {
                // skip
            } else {
                reduction.push_back(tomb);
            }
        }
    }

    reduction
}

fn map_tombs(mut reduction: VecDeque<Tombstone>) -> Vec<Vec<Tombstone>> {
    let mut tomb_map = vec![];
    for tomb in reduction.drain(..) {
        if tomb_map.is_empty() {
            tomb_map.push(vec![tomb]);
        } else {
            let last = tomb_map.last_mut().expect("no tombstones");
            let current_file = last.first().expect("no tombstone").file;
            if current_file == tomb.file {
                tomb_map.last_mut().expect("no tombstones").push(tomb);
            } else {
                tomb_map.push(vec![tomb]);
            }
        }
    }
    tomb_map
}

fn copy_ranges(mut tomb_map: Vec<Vec<Tombstone>>, node_size: u64) -> Vec<CopyRange> {
    let mut ranges = vec![];
    let mut start = 0;
    let mut current_file = 0;
    for tombs in tomb_map.drain(..) {
        for tomb in tombs {
            if ranges.is_empty() {
                if tomb.offset == 0 {
                    start = tomb.len;
                } else {
                    ranges.push(CopyRange {
                        file: tomb.file,
                        start,
                        end: tomb.offset,
                    });
                    start = tomb.offset + tomb.len;
                }
            } else {
                ranges.push(CopyRange {
                    file: tomb.file,
                    start,
                    end: tomb.offset,
                });
                start = tomb.offset + tomb.len;
            }
            current_file = tomb.file;
        }
    }

    ranges.push(CopyRange {
        file: current_file,
        start,
        end: node_size,
    });

    ranges
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

        let reduction = reduce_tombs(tombs);
        assert_eq!(reduction.len(), 5);
        let tomb = reduction.front().expect("no tombstone");
        assert_eq!(tomb.file, 0);
        assert_eq!(tomb.offset, 0);
        assert_eq!(tomb.len, 100);
    }
}
