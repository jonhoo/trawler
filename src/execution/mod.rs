use std::{mem, time};
use std::collections::HashMap;
use hdrhistogram::Histogram;
use LobstersRequest;
use histogram_sampler;
use {COMMENTS_PER_STORY, VOTES_PER_STORY, VOTES_PER_USER};

type Stats = HashMap<mem::Discriminant<LobstersRequest>, Histogram<u64>>;

#[derive(Clone, Debug)]
struct Sampler {
    votes_per_user: histogram_sampler::Sampler,
    votes_per_story: histogram_sampler::Sampler,
    comments_per_story: histogram_sampler::Sampler,
    ncomments: u32,
}

use rand;
use rand::distributions::IndependentSample;
impl Sampler {
    fn new(scale: f64) -> Self {
        // compute how many of each thing there will be in the database after scaling by mem_scale
        let scale = |hist: &'static [(usize, usize)]| {
            hist.into_iter()
                .map(|&(bin, n)| (bin, (scale * n as f64).round() as usize))
        };

        let votes_per_user = scale(VOTES_PER_USER);
        let votes_per_story = scale(VOTES_PER_STORY);
        let comments_per_story = scale(COMMENTS_PER_STORY);
        // XXX: use VOTES_PER_COMMENT
        // TODO: https://lobste.rs/s/cqnzl5/lobste_rs_access_pattern_statistics_for#c_btw1qb
        let mut ncomments = 0;

        let mut sampler = {
            let ncomments = &mut ncomments;
            let comments_per_story = comments_per_story.map(|(bin, n)| {
                *ncomments += (bin + 10) * n;
                (bin, n)
            });

            // make the samplers we'll need
            Sampler {
                votes_per_user: histogram_sampler::Sampler::from_bins(votes_per_user, 100),
                votes_per_story: histogram_sampler::Sampler::from_bins(votes_per_story, 10),
                comments_per_story: histogram_sampler::Sampler::from_bins(comments_per_story, 10),
                ncomments: 0,
            }
        };
        sampler.ncomments = ncomments as u32;
        sampler
    }

    fn user<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_user.ind_sample(rng) as u32
    }

    fn nusers(&self) -> u32 {
        self.votes_per_user.nvalues() as u32
    }

    fn story_for_vote<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_story.ind_sample(rng) as u32
    }

    fn nstories(&self) -> u32 {
        self.votes_per_story.nvalues() as u32
    }

    fn story_for_comment<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.comments_per_story.ind_sample(rng) as u32
    }

    fn ncomments(&self) -> u32 {
        self.ncomments
    }
}

#[inline]
fn id_to_slug(mut id: u32) -> [u8; 6] {
    // convert id to unique string
    // 36 possible characters (a-z0-9)
    let mut slug = [0; 6];
    let mut digit: u8;
    digit = (id % 36) as u8;
    slug[5] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 36;
    digit = (id % 36) as u8;
    slug[4] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 36;
    digit = (id % 36) as u8;
    slug[3] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 36;
    digit = (id % 36) as u8;
    slug[2] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 36;
    digit = (id % 36) as u8;
    slug[1] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 36;
    digit = (id % 36) as u8;
    slug[0] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
    id /= 36;
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
