use auto_impl::auto_impl;
use bytes::Bytes;
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Generator, GeneratorState},
    pin::Pin,
};

pub trait TableEncode: Send + Sync + Sized {
    type Encoded: AsRef<[u8]> + Send + Sync;

    fn encode(self) -> Self::Encoded;
}

pub trait TableDecode: Send + Sync + Sized {
    fn decode(b: &[u8]) -> anyhow::Result<Self>;
}

pub trait TableObject: TableEncode + TableDecode {}

impl<T> TableObject for T where T: TableEncode + TableDecode {}

#[auto_impl(Box, Arc)]
pub trait Table: Send + Sync + Debug + 'static {
    type Key: TableEncode;
    type Value: TableObject;
    type SeekKey: TableEncode;

    fn db_name(&self) -> string::String<Bytes>;
}

pub trait DupSort: Table {
    type SeekBothKey: TableObject;
}

#[derive(Copy, Clone, Debug)]
pub struct TryGenIter<'a, G, E>
where
    G: Generator<Return = Result<(), E>> + Unpin + 'a,
{
    done: bool,
    inner: G,
    _marker: PhantomData<&'a ()>,
}

impl<'a, G, E> Iterator for TryGenIter<'a, G, E>
where
    G: Generator<Return = Result<(), E>> + Unpin + 'a,
{
    type Item = Result<G::Yield, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match Pin::new(&mut self.inner).resume(()) {
            GeneratorState::Yielded(n) => Some(Ok(n)),
            GeneratorState::Complete(res) => {
                self.done = true;
                if let Err(e) = res {
                    Some(Err(e))
                } else {
                    None
                }
            }
        }
    }
}

impl<'a, G, E> From<G> for TryGenIter<'a, G, E>
where
    G: Generator<Return = Result<(), E>> + Unpin,
{
    fn from(gen: G) -> Self {
        Self {
            done: false,
            inner: gen,
            _marker: PhantomData,
        }
    }
}

pub fn ttw<'a, T, E>(f: impl Fn(&T) -> bool + 'a) -> impl Fn(&Result<T, E>) -> bool + 'a {
    move |res| match res {
        Ok(v) => (f)(v),
        Err(_) => true,
    }
}
