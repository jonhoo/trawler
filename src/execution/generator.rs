use client::{LobstersClient, LobstersRequest, Vote};
use crossbeam_channel;
use execution::{self, id_to_slug, Sampler, MAX_SLUGGABLE_ID};
use rand::{self, Rng};
use std::sync::atomic;
use std::time;
use WorkerCommand;

pub(super) fn run<C>(
    load: execution::Workload,
    sampler: Sampler,
    pool: crossbeam_channel::Sender<WorkerCommand>,
    target: f64,
) -> f64
where
    C: LobstersClient + 'static,
{
    let warmup = load.warmup;
    let runtime = load.runtime;

    let start = time::Instant::now();
    let count_from = start + warmup;
    let end = start + warmup + runtime;

    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();

    let mut ops = 0;
    let mut rng = rand::thread_rng();
    let interarrival_ns = rand::distributions::exponential::Exp::new(target * 1e-9);

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

        // XXX: we're assuming that basically all page views happen as a user, and that the users
        // who are most active voters are also the ones that interact most with the site.
        // XXX: we're assuming that users who vote a lot also comment a lot
        // XXX: we're assuming that users who vote a lot also submit many stories
        let user = Some(sampler.user(&mut rng));
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
            LobstersRequest::CommentVote(id_to_slug(sampler.comment_for_vote(&mut rng)), Vote::Up)
        } else if pick(475) {
            LobstersRequest::StoryVote(id_to_slug(sampler.story_for_vote(&mut rng)), Vote::Up)
        } else if pick(316) {
            // comments without a parent
            LobstersRequest::Comment {
                id: id_to_slug(rng.gen_range(ncomments, MAX_SLUGGABLE_ID)),
                story: id_to_slug(sampler.story_for_comment(&mut rng)),
                parent: None,
            }
        } else if pick(87) {
            LobstersRequest::Login
        } else if pick(71) {
            // comments with a parent
            let id = rng.gen_range(ncomments, MAX_SLUGGABLE_ID);
            let story = sampler.story_for_comment(&mut rng);
            // we need to pick a comment that's on the chosen story
            // we know that every nth comment from prepopulation is to the same story
            let comments_per_story = ncomments / nstories;
            let parent = story + nstories * rng.gen_range(0, comments_per_story);
            LobstersRequest::Comment {
                id: id_to_slug(id),
                story: id_to_slug(story),
                parent: Some(id_to_slug(parent)),
            }
        } else if pick(54) {
            LobstersRequest::CommentVote(id_to_slug(sampler.comment_for_vote(&mut rng)), Vote::Down)
        } else if pick(53) {
            let id = rng.gen_range(nstories, MAX_SLUGGABLE_ID);
            LobstersRequest::Submit {
                id: id_to_slug(id),
                title: format!("benchmark {}", id),
            }
        } else if pick(21) {
            LobstersRequest::StoryVote(id_to_slug(sampler.story_for_vote(&mut rng)), Vote::Down)
        } else {
            // ~.003%
            LobstersRequest::Logout
        };

        if now > count_from {
            ops += 1;
        }

        let issued = next;
        pool.send(WorkerCommand::Request(issued, user, req))
            .unwrap();

        // schedule next delivery
        next += time::Duration::new(0, interarrival_ns.ind_sample(&mut rng) as u32);
    }

    let mut per_second = 0.0;
    let now = time::Instant::now();
    if now > count_from {
        let took = now.duration_since(count_from);
        if took != time::Duration::new(0, 0) {
            per_second =
                ops as f64 / (took.as_secs() as f64 + took.subsec_nanos() as f64 / 1_000_000_000f64)
        }
    }

    per_second
}
