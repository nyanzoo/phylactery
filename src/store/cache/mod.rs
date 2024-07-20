use std::hash::Hash;

use hashlink::LruCache;

pub(super) struct LRU<K, V> {
    inner: LruCache<K, V>,
}

impl<K, V> LRU<K, V>
where
    K: Clone + Eq + Hash,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: LruCache::new(capacity),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn put(&mut self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }
}
