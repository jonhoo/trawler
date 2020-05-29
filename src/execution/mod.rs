use crate::LobstersRequest;
use crate::{COMMENTS_PER_STORY, VOTES_PER_COMMENT, VOTES_PER_STORY, VOTES_PER_USER};
use histogram_sampler;
use rand::distributions::Distribution;
use std::collections::HashMap;
use std::{mem, time};

type Stats = HashMap<mem::Discriminant<LobstersRequest>, crate::timing::Timeline>;

#[derive(Clone, Debug)]
struct Sampler {
    votes_per_user: histogram_sampler::Sampler,
    votes_per_story: histogram_sampler::Sampler,
    votes_per_comment: histogram_sampler::Sampler,
    comments_per_story: histogram_sampler::Sampler,
}

use rand;
impl Sampler {
    fn new(scale: f64) -> Self {
        fn adjust<'a, F>(
            hist: &'static [(usize, usize)],
            f: F,
        ) -> impl Iterator<Item = (usize, usize)>
        where
            F: Fn(f64) -> f64,
        {
            hist.into_iter()
                .map(move |&(bin, n)| (bin, f(n as f64).round() as usize))
        }

        // we need to scale up the workload by the given factor. but what does that _mean_?
        //
        // we assume that when the user gives a scaling factor `f`, what they expect is for there
        // to be f * BASE_OPS_PER_MIN requests/second to the backend. however, if we just naively
        // issued more requests per second, but kept all the distributions the same size, then that
        // is equivalent to make each user do f times as many requests per second. in reality, that
        // is not how load scales. instead, higher load generally corresponds to more users
        // producing that load. if we double the number of users, the number of requests per second
        // should roughly double. if we previously had x "active" users, we will now have f * x
        // active users. the relationship to other attributes like #votes, #stories, and #comments
        // is trickier, and tackled below.
        //
        // one thing that's useful to keep in mind here is how to interpret the "X_per_Y"
        // variables. it is a binned histogram of a _specific point in time_, where the x-axis is
        // Y, and the y-axis is X (confusing, I know). or, said differently, all the X_per_Y stats
        // are taken at the same point in time, and measure "the number of Ys that have N Xs",
        // where N is the bin value. for example, each bin (v, count) in votes_per_user says that
        // there are `count` users with approximately `v` votes.

        // when the scale increases, we want there to more users with each vote count. that is, in
        // each vote count bin, we want there to be f times more users.
        let votes_per_user = adjust(VOTES_PER_USER, |n| n * scale);

        // at 2x scale, it is not clear that there are 2x as many stories with a given vote count.
        // there is probably a weak correlation with scale (a tiny site with 2 users probably
        // doesn't have many users), but in general the number of stories is _probably_ fairly
        // independent of scale.
        // TODO: scale this value
        let votes_per_story = adjust(VOTES_PER_STORY, |n| n);

        // at 2x the scale, there are probably markedly more comments, but it's unclear that it is
        // quite double.
        // TODO: scale this value
        let votes_per_comment = adjust(VOTES_PER_COMMENT, |n| n);

        // the number of stories with N comments probably changes similarly to the number of
        // stories with N votes.
        // TODO: scale this value
        let comments_per_story = adjust(COMMENTS_PER_STORY, |n| n);

        // NOTE: we _don't_ scale the bin widths belo, since we didn't alter them above.
        Sampler {
            votes_per_user: histogram_sampler::Sampler::from_bins(votes_per_user, 100),
            votes_per_story: histogram_sampler::Sampler::from_bins(votes_per_story, 10),
            votes_per_comment: histogram_sampler::Sampler::from_bins(votes_per_comment, 10),
            comments_per_story: histogram_sampler::Sampler::from_bins(comments_per_story, 10),
        }
    }

    fn user<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_user.sample(rng) as u32
    }

    fn nusers(&self) -> u32 {
        self.votes_per_user.nvalues() as u32
    }

    fn comment_for_vote<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_comment.sample(rng) as u32
    }

    fn story_for_vote<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_story.sample(rng) as u32
    }

    fn nstories(&self) -> u32 {
        std::cmp::max(
            self.votes_per_story.nvalues(),
            self.comments_per_story.nvalues(),
        ) as u32
    }

    fn story_for_comment<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.comments_per_story.sample(rng) as u32
    }

    fn ncomments(&self) -> u32 {
        self.votes_per_comment.nvalues() as u32
    }
}

const MAX_SLUGGABLE_ID: u32 = 2176782336; // 36 ^ 6;

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
    pub(crate) scale: f64,
    pub(crate) runtime: time::Duration,
}

pub(crate) mod harness;
