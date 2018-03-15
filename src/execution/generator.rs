use execution::{self, id_to_slug};
use {BASE_COMMENTS, BASE_STORIES, BASE_USERS};
use client::{LobstersClient, LobstersRequest, Vote};
use std::time;
use WorkerCommand;
use futures::Sink;
use rand::{self, Rng};
use tokio_core;
use multiqueue;
use std::sync::atomic;

pub(super) fn run<C>(
    load: execution::Workload,
    mut pool: multiqueue::MPMCFutSender<WorkerCommand>,
    target: f64,
) -> usize
where
    C: LobstersClient + 'static,
{
    let warmup = load.warmup;
    let runtime = load.runtime;

    let nusers = (load.mem_scale * BASE_USERS as f64) as u32;
    let nstories = (load.mem_scale * BASE_STORIES as f64) as u32;
    let ncomments = (load.mem_scale * BASE_COMMENTS as f64) as u32;

    let start = time::Instant::now();
    let end = start + warmup + runtime;

    let mut ops = 0;
    let mut rng = rand::thread_rng();
    let interarrival = rand::distributions::exponential::Exp::new(target * 1e-9);

    let mut core = tokio_core::reactor::Core::new().unwrap();
    let mut next = time::Instant::now();
    while next < end {
        let now = time::Instant::now();

        if next > now {
            atomic::spin_loop_hint();
            continue;
        }

        // randomly pick next request type based on relative frequency
        // TODO: id distributions
        let seed = rng.gen_range(0, 100);
        let req = if seed < 30 {
            LobstersRequest::Frontpage
        } else if seed < 40 {
            LobstersRequest::Recent
        } else if seed < 80 {
            LobstersRequest::Story(id_to_slug(rng.gen_range(0, nstories)))
        } else if seed < 81 {
            LobstersRequest::Login(rng.gen_range(0, nusers))
        } else if seed < 82 {
            LobstersRequest::Logout(rng.gen_range(0, nusers))
        } else if seed < 90 {
            LobstersRequest::StoryVote(
                rng.gen_range(0, nusers),
                id_to_slug(rng.gen_range(0, nstories)),
                if rng.gen_weighted_bool(2) {
                    Vote::Up
                } else {
                    Vote::Down
                },
            )
        } else if seed < 95 {
            LobstersRequest::CommentVote(
                rng.gen_range(0, nusers),
                id_to_slug(rng.gen_range(0, ncomments)),
                if rng.gen_weighted_bool(2) {
                    Vote::Up
                } else {
                    Vote::Down
                },
            )
        } else if seed < 97 {
            // TODO: how do we pick a unique ID here?
            let id = rng.gen_range(nstories, nstories + u16::max_value() as u32);
            LobstersRequest::Submit {
                id: id_to_slug(id),
                user: rng.gen_range(0, nusers),
                title: format!("benchmark {}", id),
            }
        } else {
            // TODO: how do we pick a unique ID here?
            let id = rng.gen_range(ncomments, ncomments + u16::max_value() as u32);
            // TODO: sometimes pick a parent comment
            LobstersRequest::Comment {
                id: id_to_slug(id),
                user: rng.gen_range(0, nusers),
                story: id_to_slug(rng.gen_range(0, nstories)),
                parent: None,
            }
        };

        let issued = next;
        pool = core.run(pool.send(WorkerCommand::Request(issued, req)))
            .unwrap();
        ops += 1;

        // schedule next delivery
        use rand::distributions::IndependentSample;
        next += time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);
    }

    ops
}
