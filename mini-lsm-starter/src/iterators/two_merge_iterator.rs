// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    is_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, is_a: true };
        iter.skip_b()?;
        iter.is_a = iter.choose_a();
        Ok(iter)
    }

    fn choose_a(&mut self) -> bool {
        if !self.a.is_valid() {
            return false;
        }
        if !self.b.is_valid() {
            return true;
        }
        self.a.key() <= self.b.key()
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.is_a {
            true => self.a.key(),
            false => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.is_a {
            true => self.a.value(),
            false => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.is_a {
            true => self.a.is_valid(),
            false => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.is_a {
            true => self.a.next()?,
            false => self.b.next()?,
        }
        self.skip_b()?;
        self.is_a = self.choose_a();

        Ok(())
    }
}
