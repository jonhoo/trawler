use client::{LobstersClient, LobstersRequest};
use crossbeam_channel;
use execution::{self, id_to_slug, Sampler};
use rand::{self, Rng};
use std::sync::{Arc, Barrier, Mutex};
use std::{thread, time};
use tokio::prelude::*;
use WorkerCommand;
use BASE_OPS_PER_MIN;

pub(crate) fn run<C>(
    load: execution::Workload,
    in_flight: usize,
    client: C,
    prime: bool,
) -> (f64, Vec<(f64, execution::Stats, execution::Stats)>, usize)
where
    C: LobstersClient + 'static,
{
    let mut target = BASE_OPS_PER_MIN as f64 * load.req_scale / 60.0;
    let warmup = load.warmup;
    let runtime = load.runtime;

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    // TODO: all the stuff from
    //execution::issuer::run(warmup, runtime, in_flight, core, c, jobs)

    let now = time::Instant::now();

    // compute how many of each thing there will be in the database after scaling by mem_scale
    let sampler = Sampler::new(load.mem_scale);
    let nstories = sampler.nstories();

    // then, log in all the users
    let mut setup = futures::stream::FuturesOrdered::new();
    for u in 0..sampler.nusers() {
        setup.push(client.handle(Some(u), LobstersRequest::Login));
    }
    rt.block_on(setup.fold(0, |_, _| Ok(0))).unwrap();

    if prime {
        println!("--> priming database");
        let mut rng = rand::thread_rng();

        // first, we need to prime the database stories!
        let mut setup = futures::stream::FuturesOrdered::new();
        for id in 0..nstories {
            // NOTE: we're assuming that users who vote much also submit many stories
            let req = LobstersRequest::Submit {
                id: id_to_slug(id),
                title: format!("Base article {}", id),
            };
            setup.push(client.handle(Some(sampler.user(&mut rng)), req));
        }
        rt.block_on(setup.fold(0, |_, _| Ok(0))).unwrap();

        // and as many comments
        let mut setup = futures::stream::FuturesOrdered::new();
        for id in 0..sampler.ncomments() {
            let story = id % nstories; // TODO: distribution

            // synchronize occasionally to ensure that we can safely generate parent comments
            if story == 0 {
                rt.block_on(setup.fold(0, |_, _| Ok(0))).unwrap();
                setup = futures::stream::FuturesOrdered::new();
            }

            let parent = if rng.gen_bool(0.5) {
                // we need to pick a parent in the same story
                let generated_comments = id - story;
                // how many stories to we know there are per story?
                let generated_comments_per_story = generated_comments / nstories;
                // pick the nth comment to chosen story
                if generated_comments_per_story != 0 {
                    let story_comment = rng.gen_range(0, generated_comments_per_story);
                    Some(story + nstories * story_comment)
                } else {
                    None
                }
            } else {
                None
            };

            let req = LobstersRequest::Comment {
                id: id_to_slug(id),
                story: id_to_slug(story),
                parent: parent.map(id_to_slug),
            };

            // NOTE: we're assuming that users who vote much also submit many stories
            setup.push(client.handle(Some(sampler.user(&mut rng)), req));
        }

        // wait for all threads to finish priming comments
        rt.block_on(setup.fold(0, |_, _| Ok(0))).unwrap();
        println!("--> finished priming database");
    }

    // TODO: execution::generator::run::<C>(load, sampler, pool, target))

    drop(pool);
    let gps = generators.into_iter().map(|gen| gen.join().unwrap()).sum();

    // how many operations were left in the queue at the end?
    let dropped = jobs.iter().count();

    (gps, workers, dropped)
}
