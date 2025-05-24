use std::{collections::HashMap, hash::Hash, iter::Map};


#[derive(Debug, Clone, PartialEq)]
pub struct OrderedCell<T> {
    order: u64,
    inner: T,
}

impl<T> OrderedCell<T> {
    #[inline]
    pub fn new(order: u64, inner: T) -> Self {
        Self { order, inner }
    }

    #[inline]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> PartialOrd for OrderedCell<T>
where T: PartialEq {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.order.partial_cmp(&other.order)
    }
}

#[derive(Debug, Clone)]
pub struct OrderedHashMap<K, V> {
    inner: HashMap<K, OrderedCell<V>>,
    next_order: u64,
}

impl<K, V> PartialEq for OrderedHashMap<K, V>
where K: Eq + Hash, V: PartialEq {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<'a, K, V> IntoIterator for &'a OrderedHashMap<K, V>
where K: Eq + Hash {
    type Item = (&'a K, &'a V);
    type IntoIter = Map<
        <Vec<(&'a K, &'a OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((&'a K, &'a OrderedCell<V>)) -> Self::Item
    >;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K, V> IntoIterator for OrderedHashMap<K, V>
where K: Eq + Hash {
    type Item = (K, V);
    type IntoIter = Map<
        <Vec<(K, OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((K, OrderedCell<V>)) -> Self::Item
    >;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        OrderedHashMap::into_iter(self)
    }
}

impl<'a, K, V> IntoIterator for &'a mut OrderedHashMap<K, V>
where K: Eq + Hash {
    type Item = (&'a K, &'a mut V);
    type IntoIter = Map<
        <Vec<(&'a K, &'a mut OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((&'a K, &'a mut OrderedCell<V>)) -> Self::Item
    >;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<K, V> OrderedHashMap<K, V>
where K: Eq + Hash {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            next_order: 0,
        }
    }

    #[inline]
    pub fn with_capacity(size: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(size),
            next_order: 0,
        }
    }

    #[inline]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let order = self.next_order;
        let old = self.inner.insert(key, OrderedCell::new(order, value));
        self.next_order += 1;
        old.map(OrderedCell::into_inner)
    }

    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key).map(OrderedCell::into_inner)
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key).map(OrderedCell::inner)
    }

    #[inline]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key).map(OrderedCell::inner_mut)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    #[inline]
    pub fn iter<'a>(&'a self) -> Map<
        <Vec<(&'a K, &'a OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((&'a K, &'a OrderedCell<V>)) -> (&'a K, &'a V)
    > {
        let mut items: Vec<(&K, &OrderedCell<V>)> = self.inner.iter().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(k, v)| (k, v.inner()))
    }

    #[inline]
    pub fn iter_mut<'a>(&'a mut self) -> Map<
        <Vec<(&'a K, &'a mut OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((&'a K, &'a mut OrderedCell<V>)) -> (&'a K, &'a mut V)
    > {
        let mut items: Vec<(&K, &mut OrderedCell<V>)> = self.inner.iter_mut().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(k, v)| (k, v.inner_mut()))
    }

    #[inline]
    pub fn into_iter(self) -> Map<
        <Vec<(K, OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((K, OrderedCell<V>)) -> (K, V)
    > {
        let mut items: Vec<(K, OrderedCell<V>)> = self.inner.into_iter().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(k, v)| (k, v.into_inner()))
    }

    #[inline]
    pub fn into_values(self) -> Map<
        <Vec<(K, OrderedCell<V>)> as IntoIterator>::IntoIter,
        fn((K, OrderedCell<V>)) -> V
    > {
        let mut items: Vec<(K, OrderedCell<V>)> = self.inner.into_iter().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(_, v)| v.into_inner())
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &V> {
        let mut items: Vec<(&K, &OrderedCell<V>)> = self.inner.iter().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(_, v)| v.inner())
    }

    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        let mut items: Vec<(&K, &OrderedCell<V>)> = self.inner.iter().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(k, _)| k)
    }

    #[inline]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        let mut items: Vec<(&K, &mut OrderedCell<V>)> = self.inner.iter_mut().collect();
        items.sort_by(|a, b| a.1.order.cmp(&b.1.order) );
        items.into_iter().map(|(_, v)| v.inner_mut())
    }

    #[inline]
    pub fn values_unordered(&self) -> impl Iterator<Item = &V> {
        self.inner.values().map(OrderedCell::inner)
    }

    #[inline]
    pub fn values_unordered_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.inner.values_mut().map(OrderedCell::inner_mut)
    }

    #[inline]
    pub fn into_values_unordered(self) -> impl Iterator<Item = V> {
        self.inner.into_values().map(OrderedCell::into_inner)
    }
}

impl<K, V> OrderedHashMap<K, V> {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}
