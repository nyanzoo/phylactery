use std::hash::Hash;

use hashlink::LruCache;

pub(super) struct Lru<K, V> {
    inner: LruCache<K, V>,
}

impl<K, V> Lru<K, V>
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

    #[allow(dead_code)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }
}
