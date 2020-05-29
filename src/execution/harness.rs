use crate::client::{AsyncShutdown, LobstersRequest, TrawlerRequest, Vote};
use crate::execution::Stats;
use crate::execution::{self, id_to_slug, Sampler, MAX_SLUGGABLE_ID};
use crate::BASE_OPS_PER_MIN;
use futures_util::stream::futures_unordered::FuturesUnordered;
use futures_util::stream::StreamExt;
use hdrhistogram::Histogram;
use rand::distributions::Distribution;
use rand::{self, Rng};
use std::cell::RefCell;
use std::sync::{atomic, Arc, Mutex};
use std::{mem, time};
use tower_make::MakeService;
use tower_service::Service;

thread_local! {
    static RMT_STATS: RefCell<Stats> = RefCell::new(Stats::default());
    static SJRN_STATS: RefCell<Stats> = RefCell::new(Stats::default());
}

macro_rules! await_ready {
    ($rt:ident, $client:ident) => {
        $rt.block_on(futures_util::future::poll_fn(|cx| $client.poll_ready(cx)))
            .expect("service failed to become ready");
    };
}
macro_rules! await_all {
    ($rt:ident, $fu:ident) => {
        if !$fu.is_empty() {
            let mut futs = std::mem::replace(&mut $fu, FuturesUnordered::new());
            $rt.block_on(async move {
                while let Some(r) = futs.next().await {
                    r.unwrap().unwrap();
                }
            });
        }
    };
}
macro_rules! maybe_await_all {
    ($rt:ident, $fu:ident, $cap:expr) => {
        if $fu.len() >= $cap {
            await_all!($rt, $fu);
        }
    };
}
macro_rules! call {
    ($rt:ident, $client:ident, $req:expr) => {{
        await_ready!($rt, $client);
        $client.call($req)
    }};
}
macro_rules! spawn_call {
    ($rt:ident, $client:ident, $req:expr) => {{
        let fut = call!($rt, $client, $req);
        $rt.spawn(fut)
    }};
}

pub(crate) fn run<MS>(
    load: execution::Workload,
    in_flight: usize,
    mut client: MS,
    prime: bool,
) -> (f64, execution::Stats, execution::Stats, usize)
where
    MS: MakeService<bool, TrawlerRequest>,
    MS::MakeError: std::fmt::Debug,
    <MS::Service as Service<TrawlerRequest>>::Future: Send + 'static,
    MS::Error: std::fmt::Debug + Send + 'static,
    MS::Response: Send + 'static,
    MS::Service: AsyncShutdown,
{
    let target = BASE_OPS_PER_MIN as f64 * load.scale / 60.0;

    // generating a request takes a while because we have to generate random numbers (including
    // zipfs). so, depending on the target load, we may need more than one load generation
    // thread. we'll make them all share the pool of issuers though.
    let generator_capacity = 100_000.0; // req/s == 10 µs to generate a request
    assert!(
        target < generator_capacity,
        "one generator thread cannot generate that much load"
    );

    let warmup = load.warmup;
    let runtime = load.runtime;

    let rmt_stats: Arc<Mutex<Stats>> = Arc::default();
    let sjrn_stats: Arc<Mutex<Stats>> = Arc::default();
    let mut rt = {
        let rmt_stats = rmt_stats.clone();
        let sjrn_stats = sjrn_stats.clone();
        tokio::runtime::Builder::new()
            .enable_all()
            .threaded_scheduler()
            .on_thread_stop(move || {
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

    let mut client = rt
        .block_on(client.make_service(prime))
        .expect("client setup failed");

    let start = time::Instant::now();

    // compute how many of each thing there will be in the database after scaling by mem_scale
    let sampler = Sampler::new(load.scale);
    let nstories = sampler.nstories();

    if prime {
        eprintln!("--> priming database");
        let mut rng = rand::thread_rng();

        // then, log in all the users
        let mut futs = FuturesUnordered::new();
        for uid in 0..sampler.nusers() {
            futs.push(spawn_call!(
                rt,
                client,
                TrawlerRequest {
                    user: Some(uid),
                    page: LobstersRequest::Login,
                    is_priming: true,
                }
            ));
            maybe_await_all!(rt, futs, in_flight);
        }
        await_all!(rt, futs);

        // first, we need to prime the database stories!
        let mut futs = FuturesUnordered::new();
        for id in 0..nstories {
            // NOTE: we're assuming that users who vote much also submit many stories
            futs.push(spawn_call!(
                rt,
                client,
                TrawlerRequest {
                    user: Some(sampler.user(&mut rng)),
                    page: LobstersRequest::Submit {
                        id: id_to_slug(id),
                        title: format!("Base article {}", id),
                    },
                    is_priming: true
                }
            ));
            maybe_await_all!(rt, futs, in_flight);
        }
        await_all!(rt, futs);

        // and as many comments
        for id in 0..sampler.ncomments() {
            let story = id % nstories; // TODO: distribution

            // synchronize occasionally to ensure that we can safely generate parent comments
            if story == 0 {
                await_all!(rt, futs);
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

            // NOTE: we're assuming that users who vote much also submit many stories
            futs.push(spawn_call!(
                rt,
                client,
                TrawlerRequest {
                    user: Some(sampler.user(&mut rng)),
                    page: LobstersRequest::Comment {
                        id: id_to_slug(id),
                        story: id_to_slug(story),
                        parent: parent.map(id_to_slug),
                    },
                    is_priming: true
                }
            ));
            maybe_await_all!(rt, futs, in_flight);
        }

        // wait for all priming comments
        await_all!(rt, futs);
        eprintln!("--> finished priming database in {:?}", start.elapsed());
    }

    let start = time::Instant::now();
    let mut count_from = start + warmup;
    let mut end = start + warmup + runtime;

    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();

    let mut ops = 0;
    let mut _nissued = 0;
    let npending = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));
    let mut rng = rand::thread_rng();
    let interarrival_ns = rand_distr::Exp::new(target * 1e-9).unwrap();

    let mut waited_after_warmup = false;
    let mut next = time::Instant::now();
    while next < end {
        let mut now = time::Instant::now();

        // TODO: early exit at some point?

        if next > now || npending.load(atomic::Ordering::Acquire) > in_flight {
            if now > end {
                // don't spin after we need to be done
                break;
            }
            atomic::spin_loop_hint();
            continue;
        }
        if !waited_after_warmup && next > count_from {
            // we want to make sure we don't "pollute" the main run with time spent in warmup.
            // for example,if warmup has built up a queue, we want that queue to drain before we
            // start to measure runtime.
            eprintln!("--> warmup finished; flushing queues @ {:?}", now);
            while npending.load(atomic::Ordering::Acquire) != 0 {
                std::thread::yield_now();
            }
            let waited = now.elapsed();
            count_from += waited;
            end += waited;

            waited_after_warmup = true;
            now = time::Instant::now();
            next = now;
            eprintln!("--> queues flushed after warmup in {:?}", waited);
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
        let fut = call!(
            rt,
            client,
            TrawlerRequest {
                user,
                page: req,
                is_priming: false
            }
        );
        npending.fetch_add(1, atomic::Ordering::AcqRel);
        rt.spawn(async move {
            let _ = issued.elapsed();
            let start = time::Instant::now();
            let r = fut.await;
            let rmt_time = start.elapsed();
            let sjrn_time = issued.elapsed();
            npending.fetch_sub(1, atomic::Ordering::AcqRel);

            r.unwrap();
            if now <= count_from {
                // in warmup -- do nothing
                return;
            }
            RMT_STATS.with(|stats| {
                stats
                    .borrow_mut()
                    .entry(rtype)
                    .or_insert_with(|| Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap())
                    .saturating_record(rmt_time.as_micros() as u64);
            });
            SJRN_STATS.with(|stats| {
                stats
                    .borrow_mut()
                    .entry(rtype)
                    .or_insert_with(|| Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap())
                    .saturating_record(sjrn_time.as_micros() as u64);
            });
        });

        // schedule next delivery
        next += time::Duration::from_nanos(interarrival_ns.sample(&mut rng) as u64);
    }

    rt.block_on(client.shutdown());
    drop(rt);
    let unfinished = npending.load(atomic::Ordering::Acquire);
    ops -= unfinished;

    let mut per_second = 0.0;
    let now = time::Instant::now();
    if now > count_from {
        let took = now.duration_since(count_from);
        if took != time::Duration::new(0, 0) {
            per_second = ops as f64 / took.as_secs_f64()
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
