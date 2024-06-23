use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

pub(super) struct LRU<K, V> {
    map: HashMap<K, usize>,
    list: VecDeque<(K, V)>,
    capacity: usize,
}

impl<K, V> LRU<K, V>
where
    K: Clone + Eq + Hash,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            list: Default::default(),
            capacity,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(node) = self.map.get_mut(key) {
            let pair = self.list.remove(*node).expect("have key but no value");
            self.list.push_front(pair);
            self.list.front().map(|(_, v)| v)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.map.len() == self.capacity {
            if let Some((key, _)) = self.list.pop_back() {
                self.map.remove(&key);
            }
        }

        let pair = (key, value);
        if let Some(idx) = self.map.insert(pair.0.clone(), self.list.len()) {
            self.list.remove(idx);
        }
        self.list.push_front(pair);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(node) = self.map.remove(key) {
            let pair = self.list.remove(node).expect("have key but no value");
            Some(pair.1)
        } else {
            None
        }
    }
}
