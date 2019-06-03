use crate::client::{LobstersClient, LobstersRequest, Vote};
use crate::execution::Stats;
use crate::execution::{self, id_to_slug, Sampler, MAX_SLUGGABLE_ID};
use crate::BASE_OPS_PER_MIN;
use futures::{future, Future, Stream};
use hdrhistogram::Histogram;
use rand::distributions::Distribution;
use rand::{self, Rng};
use std::cell::RefCell;
use std::sync::{atomic, Arc, Mutex};
use std::{mem, time};

thread_local! {
    static RMT_STATS: RefCell<Stats> = RefCell::new(Stats::default());
    static SJRN_STATS: RefCell<Stats> = RefCell::new(Stats::default());
}

pub(crate) fn run<C>(
    load: execution::Workload,
    in_flight: usize,
    mut client: C,
    prime: bool,
) -> (f64, execution::Stats, execution::Stats, usize)
where
    C: LobstersClient,
{
    let target = BASE_OPS_PER_MIN as f64 * load.req_scale / 60.0;
    /*
    // generating a request takes a while because we have to generate random numbers (including
    // zipfs). so, depending on the target load, we may need more than one load generation
    // thread. we'll make them all share the pool of issuers though.
    let generator_capacity = 100_000.0; // req/s == 10 µs to generate a request
    let ngen = (target / generator_capacity).ceil() as usize;
    target /= ngen as f64;

    let nthreads = load.threads;
    */
    let warmup = load.warmup;
    let runtime = load.runtime;

    let rmt_stats: Arc<Mutex<Stats>> = Arc::default();
    let sjrn_stats: Arc<Mutex<Stats>> = Arc::default();
    let mut rt = {
        let rmt_stats = rmt_stats.clone();
        let sjrn_stats = sjrn_stats.clone();
        tokio::runtime::Builder::new()
            .before_stop(move || {
                let _ = RMT_STATS.with(|my_stats| {
                    let mut stats = rmt_stats.lock().unwrap();
                    for (rtype, my_h) in my_stats.borrow_mut().drain() {
                        use std::collections::hash_map::Entry;
                        match stats.entry(rtype) {
                            Entry::Vacant(v) => {
                                v.insert(my_h);
                            }
                            Entry::Occupied(mut h) => h.get_mut().add(&my_h).unwrap(),
                        }
                    }
                });
                let _ = SJRN_STATS.with(|my_stats| {
                    let mut stats = sjrn_stats.lock().unwrap();
                    for (rtype, my_h) in my_stats.borrow_mut().drain() {
                        use std::collections::hash_map::Entry;
                        match stats.entry(rtype) {
                            Entry::Vacant(v) => {
                                v.insert(my_h);
                            }
                            Entry::Occupied(mut h) => h.get_mut().add(&my_h).unwrap(),
                        }
                    }
                });
            })
            .build()
            .unwrap()
    };

    if prime {
        if let Err(e) = rt.block_on(client.setup()) {
            panic!("client setup failed: {:?}", e);
        }
    }

    let start = time::Instant::now();

    // compute how many of each thing there will be in the database after scaling by mem_scale
    let sampler = Sampler::new(load.mem_scale);
    let nstories = sampler.nstories();

    // then, log in all the users
    rt.block_on(
        futures::stream::futures_unordered(
            (0..sampler.nusers()).map(|u| client.handle(Some(u), LobstersRequest::Login)),
        )
        .fold(0, |_, _| Ok(0)),
    )
    .expect("tokio runtime failed");

    if prime {
        println!("--> priming database");
        let mut rng = rand::thread_rng();

        // first, we need to prime the database stories!
        let mut futs = futures::stream::futures_unordered::FuturesUnordered::new();
        for id in 0..nstories {
            // NOTE: we're assuming that users who vote much also submit many stories
            let req = LobstersRequest::Submit {
                id: id_to_slug(id),
                title: format!("Base article {}", id),
            };
            futs.push(client.handle(Some(sampler.user(&mut rng)), req));
        }

        // and as many comments
        for id in 0..sampler.ncomments() {
            let story = id % nstories; // TODO: distribution

            // synchronize occasionally to ensure that we can safely generate parent comments
            if story == 0 {
                let futs = std::mem::replace(
                    &mut futs,
                    futures::stream::futures_unordered::FuturesUnordered::new(),
                );
                rt.block_on(futs.fold(0, |_, _| Ok(0)))
                    .expect("tokio runtime failed");
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
            futs.push(client.handle(Some(sampler.user(&mut rng)), req));
        }

        // wait for all threads to finish priming comments
        // the addition of the ::Wait barrier will also ensure that start is (re)set
        rt.block_on(futs.fold(0, |_, _| Ok(0)))
            .expect("tokio runtime failed");
        println!("--> finished priming database in {:?}", start.elapsed());
    }

    let start = time::Instant::now();
    let count_from = start + warmup;
    let end = start + warmup + runtime;

    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();

    let mut ops = 0;
    let mut _nissued = 0;
    let npending = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));
    let mut rng = rand::thread_rng();
    let interarrival_ns = rand::distributions::Exp::new(target * 1e-9);

    let mut next = time::Instant::now();
    while next < end {
        let now = time::Instant::now();

        // TODO: early exit at some point?

        if next > now || npending.load(atomic::Ordering::Acquire) > in_flight {
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

        _nissued += 1;
        if now > count_from {
            ops += 1;
        }

        let issued = next;
        let rtype = mem::discriminant(&req);
        let fut = client.handle(user, req);
        npending.fetch_add(1, atomic::Ordering::AcqRel);
        tokio::spawn(future::lazy(move || {
            let _ = issued.elapsed();
            let start = time::Instant::now();
            fut.then(move |r| {
                let rmt_time = start.elapsed();
                let sjrn_time = issued.elapsed();
                npending.fetch_sub(1, atomic::Ordering::AcqRel);
                match r {
                    Ok(_) => {
                        if now <= count_from {
                            // in warmup -- do nothing
                            return Ok(());
                        }
                        RMT_STATS.with(|stats| {
                            stats
                                .borrow_mut()
                                .entry(rtype)
                                .or_insert_with(|| {
                                    Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap()
                                })
                                .saturating_record(
                                    rmt_time.as_secs() * 1_000
                                        + rmt_time.subsec_nanos() as u64 / 1_000_000,
                                );
                        });
                        SJRN_STATS.with(|stats| {
                            stats
                                .borrow_mut()
                                .entry(rtype)
                                .or_insert_with(|| {
                                    Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap()
                                })
                                .saturating_record(
                                    sjrn_time.as_secs() * 1_000
                                        + sjrn_time.subsec_nanos() as u64 / 1_000_000,
                                );
                        });
                        Ok(())
                    }
                    Err(e) => unimplemented!("got error from future: {:?}", e),
                }
            })
        }));

        // schedule next delivery
        next += time::Duration::new(0, interarrival_ns.sample(&mut rng) as u32);
    }

    rt.shutdown_on_idle().wait().unwrap();
    assert_eq!(npending.load(atomic::Ordering::Acquire), 0);

    let mut per_second = 0.0;
    let now = time::Instant::now();
    if now > count_from {
        let took = now.duration_since(count_from);
        if took != time::Duration::new(0, 0) {
            per_second =
                ops as f64 / (took.as_secs() as f64 + took.subsec_nanos() as f64 / 1_000_000_000f64)
        }
    }

    // gather stats
    let mut rmt_stats = rmt_stats.lock().unwrap();
    let mut sjrn_stats = sjrn_stats.lock().unwrap();
    (
        per_second,
        std::mem::replace(&mut *rmt_stats, Default::default()),
        std::mem::replace(&mut *sjrn_stats, Default::default()),
        0,
    )
}
