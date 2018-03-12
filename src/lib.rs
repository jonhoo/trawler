//#![deny(missing_docs)]

extern crate futures;
extern crate hdrhistogram;
extern crate libc;
extern crate multiqueue;
extern crate rand;
extern crate tokio_core;
extern crate zipf;

mod issuer;
pub use issuer::{LobstersClient, LobstersRequest, Vote};

use hdrhistogram::Histogram;
use rand::Rng;
use futures::{Future, Sink, Stream};

use std::collections::HashMap;
use std::fs;
use std::time;
use std::thread;
use std::sync::{atomic, Arc, Mutex};
use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

const BASE_OPS_PER_SEC: usize = 10;

// at time of writing, https://lobste.rs/recent/page/1601 is last page
const BASE_STORIES: u32 = 25 * 1601;

// wild guess
const BASE_COMMENTS: u32 = BASE_STORIES * 10;

// document.querySelectorAll(".user_tree > li").length on https://lobste.rs/u
const BASE_USERS: u32 = 9000;

thread_local! {
    static SJRN: RefCell<HashMap<mem::Discriminant<LobstersRequest>, Histogram<u64>>> = RefCell::default();
    static RMT: RefCell<HashMap<mem::Discriminant<LobstersRequest>, Histogram<u64>>> = RefCell::default();
}

type Request = (time::Instant, LobstersRequest);

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

pub struct WorkloadBuilder<'a> {
    scale: f64,

    threads: usize,

    warmup: time::Duration,
    runtime: time::Duration,

    histogram_file: Option<&'a str>,
}

impl<'a> Default for WorkloadBuilder<'a> {
    fn default() -> Self {
        WorkloadBuilder {
            scale: 1.0,

            threads: 1,

            warmup: time::Duration::from_secs(10),
            runtime: time::Duration::from_secs(30),

            histogram_file: None,
        }
    }
}

impl<'a> WorkloadBuilder<'a> {
    pub fn scale(&mut self, factor: f64) -> &mut Self {
        self.scale = factor;
        self
    }

    pub fn issuers(&mut self, n: usize) -> &mut Self {
        self.threads = n;
        self
    }

    pub fn time(&mut self, warmup: time::Duration, runtime: time::Duration) -> &mut Self {
        self.warmup = warmup;
        self.runtime = runtime;
        self
    }

    pub fn with_histogram<'this>(&'this mut self, path: &'a str) -> &'this mut Self {
        self.histogram_file = Some(path);
        self
    }
}

impl<'a> WorkloadBuilder<'a> {
    fn to_static(&self) -> WorkloadBuilder<'static> {
        WorkloadBuilder {
            scale: self.scale,
            threads: self.threads,
            warmup: self.warmup,
            runtime: self.runtime,
            histogram_file: None,
        }
    }
}

impl<'a> WorkloadBuilder<'a> {
    pub fn run<C, I>(&self, factory: I)
    where
        I: Send + 'static,
        C: LobstersClient<Factory = I> + 'static,
    {
        // generating a request takes a while because we have to generate random numbers (including
        // zipfs). so, depending on the target load, we may need more than one load generation
        // thread. we'll make them all share the pool of issuers though.
        let mut target = BASE_OPS_PER_SEC as f64 * self.scale;
        let per_generator = 10;
        let ngen = (target as usize + per_generator - 1) / per_generator; // rounded up
        target /= ngen as f64;

        let nthreads = self.threads;

        let hists: (HashMap<_, _>, HashMap<_, _>) =
            if let Some(mut f) = self.histogram_file.and_then(|h| fs::File::open(h).ok()) {
                use hdrhistogram::serialization::Deserializer;
                let mut deserializer = Deserializer::new();
                let sjrn = LobstersRequest::all()
                    .map(|variant| (variant, deserializer.deserialize(&mut f).unwrap()))
                    .collect();
                let rmt = LobstersRequest::all()
                    .map(|variant| (variant, deserializer.deserialize(&mut f).unwrap()))
                    .collect();
                (sjrn, rmt)
            } else {
                let sjrn = LobstersRequest::all()
                    .map(|variant| {
                        (
                            variant,
                            Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap(),
                        )
                    })
                    .collect();
                let rmt = LobstersRequest::all()
                    .map(|variant| {
                        (
                            variant,
                            Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap(),
                        )
                    })
                    .collect();
                (sjrn, rmt)
            };
        let (mut sjrn_t, mut rmt_t) = hists;

        let start = time::Instant::now();
        let warmup = self.warmup;

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

                        Self::run_client(start, warmup, core, c, jobs);

                        // NOTE: there may still be a bunch of requests in the queue here,
                        // but core.run() will return when the stream is closed.

                        let sjrn = SJRN.with(|sjrn| {
                            let mut sjrn = sjrn.borrow_mut();
                            mem::replace(&mut *sjrn, HashMap::default())
                        });
                        let rmt = RMT.with(|rmt| {
                            let mut rmt = rmt.borrow_mut();
                            mem::replace(&mut *rmt, HashMap::default())
                        });
                        (sjrn, rmt)
                    })
                    .unwrap()
            })
            .collect();

        // TODO: detect if already primed!
        let mut core = tokio_core::reactor::Core::new().unwrap();
        if true {
            // first, we need to prime the database with BASE_STORIES stories!
            let now = time::Instant::now();
            let mut rng = rand::thread_rng();
            pool = core.run(
                pool.send_all(futures::stream::iter_ok(
                    (0..BASE_STORIES)
                        .map(|id| {
                            // TODO: distribution
                            let user = rng.gen_range(0, BASE_USERS);
                            LobstersRequest::Submit {
                                id: id_to_slug(id),
                                user: user,
                                title: format!("Base article {}", id),
                            }
                        })
                        .map(|req| (now, req)),
                )),
            ).unwrap()
                .0;

            // and as many comments
            pool = core.run(
                pool.send_all(futures::stream::iter_ok(
                    (0..BASE_COMMENTS)
                        .map(|id| {
                            let user = rng.gen_range(0, BASE_USERS); // TODO: distribution
                            let story = id % BASE_STORIES; // TODO: distribution
                            let parent = if rng.gen_weighted_bool(2) {
                                // we need to pick a parent in the same story
                                let last_safe_comment_id = id.saturating_sub(nthreads as u32);
                                // how many stories to we know there are per story?
                                let safe_comments_per_story = last_safe_comment_id / BASE_STORIES;
                                // pick the nth comment to chosen story
                                if safe_comments_per_story != 0 {
                                    let story_comment = rng.gen_range(0, safe_comments_per_story);
                                    Some(story + BASE_STORIES * story_comment)
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            LobstersRequest::Comment {
                                id: id_to_slug(id),
                                story: id_to_slug(story),
                                user: user,
                                parent: parent.map(id_to_slug),
                            }
                        })
                        .map(|req| (now, req)),
                )),
            ).unwrap()
                .0;
        }

        let start = time::Instant::now();
        let generators: Vec<_> = (0..ngen)
            .map(|geni| {
                let pool = pool.clone();
                let wl = self.to_static();

                thread::Builder::new()
                    .name(format!("load-gen{}", geni))
                    .spawn(move || wl.run_generator::<C>(pool, target))
                    .unwrap()
            })
            .collect();

        drop(pool);
        let ops: usize = generators.into_iter().map(|gen| gen.join().unwrap()).sum();

        for w in workers {
            let (sjrn, rmt) = w.join().unwrap();
            for (variant, h) in sjrn {
                sjrn_t
                    .get_mut(&variant)
                    .expect("missing entry for variant")
                    .add(h)
                    .unwrap();
            }
            for (variant, h) in rmt {
                rmt_t
                    .get_mut(&variant)
                    .expect("missing entry for variant")
                    .add(h)
                    .unwrap();
            }
        }

        // all done!
        let took = start.elapsed();
        println!(
            "# actual ops/s: {:.2}",
            ops as f64 / (took.as_secs() as f64 + took.subsec_nanos() as f64 / 1_000_000_000f64)
        );

        if let Some(h) = self.histogram_file {
            match fs::File::create(h) {
                Ok(mut f) => {
                    use hdrhistogram::serialization::Serializer;
                    use hdrhistogram::serialization::V2Serializer;
                    let mut s = V2Serializer::new();
                    for variant in LobstersRequest::all() {
                        s.serialize(&sjrn_t[&variant], &mut f).unwrap();
                    }
                    for variant in LobstersRequest::all() {
                        s.serialize(&rmt_t[&variant], &mut f).unwrap();
                    }
                }
                Err(e) => {
                    eprintln!("failed to open histogram file for writing: {:?}", e);
                }
            }
        }

        println!("{:<12}\t{:<12}\tpct\tms", "# op", "metric");
        for (variant, h) in &sjrn_t {
            for &pct in &[50, 95, 99] {
                println!(
                    "{:<12}\t{:<12}\t{}\t{:.2}",
                    LobstersRequest::variant_name(variant),
                    "sojourn",
                    pct,
                    h.value_at_quantile(pct as f64 / 100.0),
                );
            }
            println!(
                "{:<12}\t{:<12}\t100\t{:.2}",
                LobstersRequest::variant_name(variant),
                "sojourn",
                h.max()
            );
        }
        for (variant, h) in &rmt_t {
            for &pct in &[50, 95, 99] {
                println!(
                    "{:<12}\t{:<12}\t{}\t{:.2}",
                    LobstersRequest::variant_name(variant),
                    "processing",
                    pct,
                    h.value_at_quantile(pct as f64 / 100.0),
                );
            }
            println!(
                "{:<12}\t{:<12}\t100\t{:.2}",
                LobstersRequest::variant_name(variant),
                "processing",
                h.max()
            );
        }
    }

    fn run_client<C>(
        start: time::Instant,
        warmup: time::Duration,
        mut core: tokio_core::reactor::Core,
        client: C,
        jobs: multiqueue::MPMCFutReceiver<Request>,
    ) where
        C: LobstersClient,
    {
        let client = Rc::new(client);
        let handle = core.handle();
        core.run(jobs.for_each(move |(issued, request)| {
            let variant = mem::discriminant(&request);
            handle.spawn(C::handle(client.clone(), request).map(move |remote_t| {
                if issued.duration_since(start) > warmup {
                    let sjrn_t = issued.elapsed();

                    RMT.with(|h| {
                        let mut h = h.borrow_mut();
                        h.entry(variant)
                            .or_insert_with(|| {
                                Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap()
                            })
                            .saturating_record(
                                remote_t.as_secs() * 1_000
                                    + remote_t.subsec_nanos() as u64 / 1_000_000,
                            );
                    });
                    SJRN.with(|h| {
                        let mut h = h.borrow_mut();
                        h.entry(variant)
                            .or_insert_with(|| {
                                Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap()
                            })
                            .saturating_record(
                                sjrn_t.as_secs() * 1_000 + sjrn_t.subsec_nanos() as u64 / 1_000_000,
                            );
                    });
                }
            }));
            futures::future::finished(())
        })).unwrap()
    }

    fn run_generator<C>(self, mut pool: multiqueue::MPMCFutSender<Request>, target: f64) -> usize
    where
        C: LobstersClient + 'static,
    {
        let warmup = self.warmup;
        let runtime = self.runtime;

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
                LobstersRequest::Story(id_to_slug(rng.gen_range(0, BASE_STORIES)))
            } else if seed < 81 {
                LobstersRequest::Login(rng.gen_range(0, BASE_USERS))
            } else if seed < 82 {
                LobstersRequest::Logout(rng.gen_range(0, BASE_USERS))
            } else if seed < 90 {
                LobstersRequest::StoryVote(
                    rng.gen_range(0, BASE_USERS),
                    id_to_slug(rng.gen_range(0, BASE_STORIES)),
                    if rng.gen_weighted_bool(2) {
                        Vote::Up
                    } else {
                        Vote::Down
                    },
                )
            } else if seed < 95 {
                LobstersRequest::CommentVote(
                    rng.gen_range(0, BASE_USERS),
                    id_to_slug(rng.gen_range(0, BASE_COMMENTS)),
                    if rng.gen_weighted_bool(2) {
                        Vote::Up
                    } else {
                        Vote::Down
                    },
                )
            } else if seed < 97 {
                // TODO: how do we pick a unique ID here?
                let id = rng.gen_range(BASE_STORIES, BASE_STORIES + u16::max_value() as u32);
                LobstersRequest::Submit {
                    id: id_to_slug(id),
                    user: rng.gen_range(0, BASE_USERS),
                    title: format!("benchmark {}", id),
                }
            } else {
                // TODO: how do we pick a unique ID here?
                let id = rng.gen_range(BASE_COMMENTS, BASE_COMMENTS + u16::max_value() as u32);
                // TODO: sometimes pick a parent comment
                LobstersRequest::Comment {
                    id: id_to_slug(id),
                    user: rng.gen_range(0, BASE_USERS),
                    story: id_to_slug(rng.gen_range(0, BASE_STORIES)),
                    parent: None,
                }
            };

            let issued = next;
            pool = core.run(pool.send((issued, req))).unwrap();
            ops += 1;

            // schedule next delivery
            use rand::distributions::IndependentSample;
            next += time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);
        }

        ops
    }
}
