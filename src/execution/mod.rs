use std::{mem, time};
use std::collections::HashMap;
use hdrhistogram::Histogram;
use LobstersRequest;

type Stats = HashMap<mem::Discriminant<LobstersRequest>, Histogram<u64>>;

#[inline]
fn id_to_slug(mut id: u32) -> [u8; 6] {
    // convert id to unique string
    // 26 possible characters (a-z0-9)
    let mut slug = [0; 6];
    let mut digit: u8;
    digit = (id % 26) as u8;
    slug[5] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 26;
    digit = (id % 26) as u8;
    slug[4] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 26;
    digit = (id % 26) as u8;
    slug[3] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 26;
    digit = (id % 26) as u8;
    slug[2] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 26;
    digit = (id % 26) as u8;
    slug[1] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 26;
    digit = (id % 26) as u8;
    slug[0] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 26;
    debug_assert_eq!(id, 0);
    slug
}

#[derive(Clone, Debug)]
pub(crate) struct Workload {
    pub(crate) mem_scale: f64,
    pub(crate) req_scale: f64,
    pub(crate) threads: usize,
    pub(crate) warmup: time::Duration,
    pub(crate) runtime: time::Duration,
}

pub(crate) mod issuer;
pub(crate) mod generator;
pub(crate) mod harness;

pub use self::issuer::MAX_IN_FLIGHT;
