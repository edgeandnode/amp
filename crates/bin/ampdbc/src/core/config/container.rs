use std::{
    any::type_name,
    collections::HashSet,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::{BuildHasher, Hash, Hasher},
    iter::once as iter_once,
    marker::PhantomData,
    mem::MaybeUninit,
};

use crate::{Assert, IsTrue, Result};

pub const fn subtract_one<const M: usize>() -> usize {
    subtract_k::<M, 1>()
}

pub const fn subtract_k<const M: usize, const K: usize>() -> usize {
    M.saturating_sub(K)
}

pub type AtLeastOne<T> = AtLeastN<1, T>;

#[derive(Clone)]
pub struct AtLeastN<const N: usize, T: MemoizedHash>
where
    Assert<{ N > 0 }>: IsTrue,
    [T; subtract_one::<N>()]: Sized,
{
    /// The first item (which always exists since N > 0)
    first: T,
    /// The next N-1 items (as an array)
    /// If N == 1, this is an empty array.
    next: Box<[T; subtract_one::<N>()]>,
    /// Any additional items (beyond N)
    /// This can be empty.
    last: Vec<T>,
}

impl<T: Default + MemoizedHash> Default for AtLeastOne<T> {
    fn default() -> Self {
        Self {
            first: T::default(),
            next: Box::new([]),
            last: Vec::new(),
        }
    }
}

impl<T: Clone + MemoizedHash, const N: usize> AtLeastN<N, T>
where
    Assert<{ N > 0 }>: IsTrue,
    [T; subtract_one::<N>()]: Sized,
{
    pub fn try_new(mut items: impl ExactSizeIterator<Item = T>) -> Result<Self> {
        let size = items.len();
        assert!(size >= N, "Expected at least {N} items, got {size}",);

        let mut visited: MemoizedHashSet<T> = MemoizedHashSet::with_capacity(size);

        // UNWRAP: We have already asserted that N > 0 and items.len() >= N, so there is at least one item.
        let first = items.next().unwrap();

        visited.insert(first.clone());

        let mut next_array: MaybeUninit<[T; subtract_one::<N>()]> = MaybeUninit::uninit();

        let mut last_vec: Vec<T> = Vec::with_capacity(size.saturating_sub(subtract_one::<N>()));

        let next_size = subtract_one::<N>();

        let items_processed = items
            .scan(0usize, |state, item| {
                // Only add unique items to next_array until it is full
                if !visited.insert(item.clone()) {
                    return Some(*state);
                }

                if *state < next_size {
                    // SAFETY: We are only writing to indices within bounds.
                    unsafe {
                        let array_ptr = next_array.as_mut_ptr() as *mut T;
                        array_ptr.add(*state).write(item);
                    }
                } else {
                    last_vec.push(item);
                }

                *state += 1;
                Some(*state)
            })
            .max()
            .unwrap_or_default()
            .saturating_add(1);

        if items_processed < N {
            Err(crate::DriverError::missing_config(N, items_processed).into())
        } else {
            Ok(Self {
                first,
                next: Box::new(unsafe {
                    // SAFETY: We have filled all N-1 items in next_array.
                    next_array.assume_init()
                }),
                last: last_vec,
            })
        }
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn len(&self) -> usize {
        1 + self.next.len() + self.last.len()
    }

    pub fn min_len(&self) -> usize {
        N
    }

    /// Truncates the collection to only contain the minimum N items.
    pub fn truncate(&mut self) {
        self.last.clear();
    }

    /// Returns an iterator over all items.
    /// The iterator yields references to the items.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        iter_once(&self.first)
            .chain(self.next.iter())
            .chain(self.last.iter())
    }

    /// Returns a mutable iterator over all items.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        iter_once(&mut self.first)
            .chain(self.next.iter_mut())
            .chain(self.last.iter_mut())
    }

    pub fn into_iter(self) -> impl Iterator<Item = T> {
        iter_once(self.first)
            .chain(self.next.into_iter())
            .chain(self.last.into_iter())
    }

    /// Returns a vector containing all items.
    ///
    /// All items are cloned into the new vector.
    pub fn to_vec(&self) -> Vec<T> {
        let mut vec = Vec::with_capacity(self.len());
        vec.push(self.first.clone());
        vec.extend_from_slice(&self.next[..]);
        vec.extend_from_slice(&self.last);
        vec
    }
}

impl<const N: usize, T: Clone + HasName> AtLeastN<N, T>
where
    Assert<{ N > 0 }>: IsTrue,
    [T; subtract_one::<N>()]: Sized,
{
    pub fn get(&self, name: &T::NameType) -> Option<&T> {
        if self.first.name() == name {
            return Some(&self.first);
        }

        for item in self.next.iter() {
            if item.name() == name {
                return Some(item);
            }
        }

        for item in self.last.iter() {
            if item.name() == name {
                return Some(item);
            }
        }

        None
    }

    pub fn get_mut(&mut self, name: &T::NameType) -> Option<&mut T> {
        if self.first.name() == name {
            return Some(&mut self.first);
        }

        for item in self.next.iter_mut() {
            if item.name() == name {
                return Some(item);
            }
        }

        for item in self.last.iter_mut() {
            if item.name() == name {
                return Some(item);
            }
        }

        None
    }
}

impl<const N: usize, T: Debug + MemoizedHash> Debug for AtLeastN<N, T>
where
    Assert<{ N > 0 }>: IsTrue,
    [T; subtract_one::<N>()]: Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let items = f
            .debug_list()
            .entry(&self.first)
            .entries(self.next.iter())
            .entries(self.last.iter())
            .finish();

        f.debug_struct(format!("AtLeastN<{}, {}>", N, type_name::<T>()).as_str())
            .field("size", &N)
            .field("items", &items)
            .finish()
    }
}

impl<const N: usize, T: Display + MemoizedHash> Display for AtLeastN<N, T>
where
    Assert<{ N > 0 }>: IsTrue,
    [T; subtract_one::<N>()]: Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "[{}", self.first)?;
        for item in self.next.iter() {
            write!(f, ", {item}")?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MemoizedHasher<T: MemoizedHash> {
    state: u64,
    signed: bool,
    _marker: PhantomData<T>,
}

#[derive(Debug, Clone)]
pub struct MemoizedHashSet<T: MemoizedHash>(HashSet<T, MemoizedHasher<T>>);

impl<T: MemoizedHash> MemoizedHashSet<T> {
    pub fn with_capacity(capacity: usize) -> MemoizedHashSet<T> {
        let hasher = MemoizedHasher::default();

        MemoizedHashSet(HashSet::with_capacity_and_hasher(capacity, hasher))
    }

    pub fn insert(&mut self, value: T) -> bool {
        self.0.insert(value)
    }
}

impl<T: MemoizedHash> Default for MemoizedHasher<T> {
    fn default() -> Self {
        Self {
            state: 0,
            signed: false,
            _marker: PhantomData,
        }
    }
}

impl<T: MemoizedHash> Hasher for MemoizedHasher<T> {
    fn write_u64(&mut self, i: u64) {
        self.signed = false;
        self.state = i;
    }

    fn write_u32(&mut self, i: u32) {
        self.signed = false;
        self.state = i as u64;
    }

    fn write_u16(&mut self, i: u16) {
        self.signed = false;
        self.state = i as u64;
    }

    fn write_u8(&mut self, i: u8) {
        self.signed = false;
        self.state = i as u64;
    }

    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, _bytes: &[u8]) {
        panic!(
            "MemoizedHasher does not support write with byte slices, use the typed write methods instead"
        );
    }

    fn write_i64(&mut self, i: i64) {
        self.signed = true;
        self.state = u64::from_le_bytes(i.to_le_bytes());
    }

    fn write_i32(&mut self, i: i32) {
        self.signed = true;
        self.state = u32::from_le_bytes(i.to_le_bytes()) as u64;
    }

    fn write_i16(&mut self, i: i16) {
        self.signed = true;
        self.state = u16::from_le_bytes(i.to_le_bytes()) as u64;
    }

    fn write_i8(&mut self, i: i8) {
        self.signed = true;
        self.state = u8::from_le_bytes(i.to_le_bytes()) as u64;
    }
}

impl<T: MemoizedHash> BuildHasher for MemoizedHasher<T> {
    type Hasher = MemoizedHasher<T>;

    fn build_hasher(&self) -> Self::Hasher {
        MemoizedHasher::default()
    }
}

pub trait MemoizedHash: Eq + Hash {
    fn get_hash(&self) -> u64;
    fn set_hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.get_hash());
    }
}

pub trait HasName: MemoizedHash {
    type NameType: Eq;
    fn name(&self) -> &Self::NameType;
}
