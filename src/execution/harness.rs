use execution::{self, id_to_slug, Sampler};
use BASE_OPS_PER_MIN;
use client::{LobstersClient, LobstersRequest};
use std::{thread, time};
use WorkerCommand;
use futures::{self, Sink};
use rand::{self, Rng};
use std::sync::{Arc, Barrier, Mutex};
use tokio_core;
use multiqueue;

pub(crate) fn run<C, I>(
    load: execution::Workload,
    factory: I,
    prime: bool,
) -> (
    Vec<thread::JoinHandle<(execution::Stats, execution::Stats)>>,
    usize,
)
where
    I: Send + 'static,
    C: LobstersClient<Factory = I> + 'static,
{
    // generating a request takes a while because we have to generate random numbers (including
    // zipfs). so, depending on the target load, we may need more than one load generation
    // thread. we'll make them all share the pool of issuers though.
    let mut target = BASE_OPS_PER_MIN as f64 * load.req_scale / 60.0;
    let generator_capacity = 100_000.0; // req/s == 10 µs to generate a request
    let ngen = (target / generator_capacity).ceil() as usize;
    target /= ngen as f64;

    let nthreads = load.threads;
    let warmup = load.warmup;

    let factory = Arc::new(Mutex::new(factory));
    let (mut pool, jobs) = multiqueue::mpmc_fut_queue(0);
    let workers: Vec<_> = (0..nthreads)
        .map(|i| {
            let jobs = jobs.clone();
            let factory = factory.clone();
            thread::Builder::new()
                .name(format!("issuer-{}", i))
                .spawn(move || {
                    let core = tokio_core::reactor::Core::new().unwrap();
                    let c = C::spawn(&mut *factory.lock().unwrap(), &core.handle());

                    execution::issuer::run(warmup, core, c, jobs)
                    // NOTE: there may still be a bunch of requests in the queue here,
                    // but core.run() will return when the stream is closed.
                })
                .unwrap()
        })
        .collect();

    let mut core = tokio_core::reactor::Core::new().unwrap();
    let barrier = Arc::new(Barrier::new(nthreads + 1));
    let now = time::Instant::now();

    // compute how many of each thing there will be in the database after scaling by mem_scale
    let sampler = Sampler::new(load.mem_scale);
    let nstories = sampler.nstories();

    // then, log in all the users
    pool = core.run(
        pool.send_all(futures::stream::iter_ok(
            (0..sampler.nusers())
                .map(LobstersRequest::Login)
                .map(|l| WorkerCommand::Request(now, l)),
        )),
    ).unwrap()
        .0;

    // wait for all threads to be ready (and set their start time correctly)
    // we don't normally know which worker thread will receive any given command (it's mpmc
    // after all), but since a ::Wait causes the receiving thread to *block*, we know that once
    // it receives one, it can't receive another until the barrier has been passed! Therefore,
    // sending `nthreads` barriers should ensure that every thread gets one
    pool = core.run(pool.send_all(futures::stream::iter_ok(
        (0..nthreads).map(|_| WorkerCommand::Wait(barrier.clone())),
    ))).unwrap()
        .0;
    barrier.wait();

    if prime {
        println!("--> priming database");
        let mut rng = rand::thread_rng();

        // first, we need to prime the database stories!
        pool = core.run(
            pool.send_all(futures::stream::iter_ok(
                (0..nstories)
                    .map(|id| {
                        // NOTE: we're assuming that users who vote much also submit many stories
                        LobstersRequest::Submit {
                            id: id_to_slug(id),
                            user: sampler.user(&mut rng),
                            title: format!("Base article {}", id),
                        }
                    })
                    .map(|req| WorkerCommand::Request(now, req))
                    .chain((0..nthreads).map(|_| WorkerCommand::Wait(barrier.clone()))),
            )),
        ).unwrap()
            .0;

        // wait for all threads to finish priming stories
        barrier.wait();

        // and as many comments
        pool = core.run(
            pool.send_all(futures::stream::iter_ok(
                (0..sampler.ncomments())
                    .map(|id| {
                        let story = id % nstories; // TODO: distribution
                        let parent = if rng.gen_weighted_bool(2) {
                            // we need to pick a parent in the same story
                            let last_safe_comment_id = id.saturating_sub(nthreads as u32) / 2;
                            // how many stories to we know there are per story?
                            let safe_comments_per_story = last_safe_comment_id / nstories;
                            // pick the nth comment to chosen story
                            if safe_comments_per_story != 0 {
                                let story_comment = rng.gen_range(0, safe_comments_per_story);
                                Some(story + nstories * story_comment)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        // NOTE: we're assuming that users who vote much also submit many stories
                        LobstersRequest::Comment {
                            id: id_to_slug(id),
                            story: id_to_slug(story),
                            user: sampler.user(&mut rng),
                            parent: parent.map(id_to_slug),
                        }
                    })
                    .map(|req| WorkerCommand::Request(now, req))
                    .chain((0..nthreads).map(|_| WorkerCommand::Wait(barrier.clone()))),
            )),
        ).unwrap()
            .0;

        // wait for all threads to finish priming comments
        // the addition of the ::Wait barrier will also ensure that start is (re)set
        barrier.wait();
        println!("--> finished priming database");
    }

    let generators: Vec<_> = (0..ngen)
        .map(|geni| {
            let pool = pool.clone();
            let load = load.clone();
            let sampler = sampler.clone();

            thread::Builder::new()
                .name(format!("load-gen{}", geni))
                .spawn(move || execution::generator::run::<C>(load, sampler, pool, target))
                .unwrap()
        })
        .collect();

    drop(pool);
    let ops = generators.into_iter().map(|gen| gen.join().unwrap()).sum();
    (workers, ops)
}
