use WorkerCommand;
use client::{LobstersClient, LobstersRequest, Vote};
use execution::{self, id_to_slug, Sampler, MAX_SLUGGABLE_ID};
use futures::Sink;
use multiqueue;
use rand::{self, Rng};
use std::sync::atomic;
use std::time;
use tokio_core;

pub(super) fn run<C>(
    load: execution::Workload,
    sampler: Sampler,
    mut pool: multiqueue::MPMCFutSender<WorkerCommand>,
    target: f64,
) -> usize
where
    C: LobstersClient + 'static,
{
    let warmup = load.warmup;
    let runtime = load.runtime;

    let start = time::Instant::now();
    let end = start + warmup + runtime;

    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();
    let nusers = sampler.nusers();

    let mut ops = 0;
    let mut rng = rand::thread_rng();
    let interarrival_ns = rand::distributions::exponential::Exp::new(target * 1e-9);

    let mut core = tokio_core::reactor::Core::new().unwrap();
    let mut next = time::Instant::now();
    while next < end {
        use rand::distributions::IndependentSample;
        let now = time::Instant::now();

        if next > now {
            atomic::spin_loop_hint();
            continue;
        }

        // randomly pick next request type based on relative frequency
        let mut seed: isize = rng.gen_range(0, 100000);
        let seed = &mut seed;
        let mut pick = |f| {
            let applies = *seed <= f;
            *seed -= f;
            applies
        };

        let req = if pick(55842) {
            // XXX: we're assuming here that stories with more votes are viewed more
            LobstersRequest::Story(id_to_slug(sampler.story_for_vote(&mut rng)))
        } else if pick(30105) {
            LobstersRequest::Frontpage
        } else if pick(6702) {
            // XXX: we're assuming that users who vote a lot are also "popular"
            LobstersRequest::User(sampler.user(&mut rng))
        } else if pick(4674) {
            LobstersRequest::Comments
        } else if pick(967) {
            LobstersRequest::Recent
        } else if pick(630) {
            LobstersRequest::CommentVote(
                sampler.user(&mut rng),
                id_to_slug(sampler.comment_for_vote(&mut rng)),
                Vote::Up,
            )
        } else if pick(475) {
            LobstersRequest::StoryVote(
                sampler.user(&mut rng),
                id_to_slug(sampler.story_for_vote(&mut rng)),
                Vote::Up,
            )
        } else if pick(316) {
            // comments without a parent
            let id = rng.gen_range(ncomments, MAX_SLUGGABLE_ID);
            // XXX: we're assuming that users who vote a lot also comment a lot
            LobstersRequest::Comment {
                id: id_to_slug(id),
                user: sampler.user(&mut rng),
                story: id_to_slug(sampler.story_for_comment(&mut rng)),
                parent: None,
            }
        } else if pick(87) {
            LobstersRequest::Login(rng.gen_range(0, nusers))
        } else if pick(71) {
            // comments with a parent
            let id = rng.gen_range(ncomments, MAX_SLUGGABLE_ID);
            let story = sampler.story_for_comment(&mut rng);
            // we need to pick a comment that's on the chosen story
            // we know that every nth comment from prepopulation is to the same story
            let comments_per_story = ncomments / nstories;
            let parent = story + nstories * rng.gen_range(0, comments_per_story);
            // XXX: we're assuming that users who vote a lot also comment a lot
            LobstersRequest::Comment {
                id: id_to_slug(id),
                user: sampler.user(&mut rng),
                story: id_to_slug(story),
                parent: Some(id_to_slug(parent)),
            }
        } else if pick(54) {
            LobstersRequest::CommentVote(
                sampler.user(&mut rng),
                id_to_slug(sampler.comment_for_vote(&mut rng)),
                Vote::Down,
            )
        } else if pick(53) {
            let id = rng.gen_range(nstories, MAX_SLUGGABLE_ID);
            // XXX: we're assuming that users who vote a lot also submit many stories
            LobstersRequest::Submit {
                id: id_to_slug(id),
                user: sampler.user(&mut rng),
                title: format!("benchmark {}", id),
            }
        } else if pick(21) {
            LobstersRequest::StoryVote(
                sampler.user(&mut rng),
                id_to_slug(sampler.story_for_vote(&mut rng)),
                Vote::Down,
            )
        } else {
            // ~.003%
            LobstersRequest::Logout(rng.gen_range(0, nusers))
        };

        let issued = next;
        pool = core.run(pool.send(WorkerCommand::Request(issued, req)))
            .unwrap();
        ops += 1;

        // schedule next delivery
        next += time::Duration::new(0, interarrival_ns.ind_sample(&mut rng) as u32);
    }

    ops
}
