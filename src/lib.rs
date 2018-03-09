//#![deny(missing_docs)]

extern crate hdrhistogram;
extern crate libc;
extern crate rand;
extern crate rayon;
extern crate zipf;

mod issuer;
pub use issuer::{Issuer, LobstersClient, LobstersRequest, Vote};

use hdrhistogram::Histogram;
use rand::Rng;
use std::collections::HashMap;
use std::fs;
use std::time;
use std::thread;
use std::sync::{atomic, Arc, Barrier, Mutex};
use std::cell::RefCell;
use std::any::Any;
use std::mem;

const BASE_OPS_PER_SEC: usize = 10;

// at time of writing, https://lobste.rs/recent/page/1601 is last page
const BASE_STORIES: u32 = 25 * 1601;

// wild guess
const BASE_COMMENTS: u32 = BASE_STORIES * 10;

// document.querySelectorAll(".user_tree > li").length on https://lobste.rs/u
const BASE_USERS: u32 = 9000;

thread_local! {
    static CLIENT: RefCell<Option<Box<Any>>> = RefCell::new(None);
    static SJRN: RefCell<HashMap<mem::Discriminant<LobstersRequest>, Histogram<u64>>> = RefCell::default();
    static RMT: RefCell<HashMap<mem::Discriminant<LobstersRequest>, Histogram<u64>>> = RefCell::default();
}

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
    pub fn run<I>(&self, issuer: I)
    where
        I: Issuer + Send,
        I::Instance: 'static,
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

        let sjrn_t = Arc::new(Mutex::new(hists.0));
        let rmt_t = Arc::new(Mutex::new(hists.1));
        let finished = Arc::new(Barrier::new(nthreads + ngen));
        let ts = (sjrn_t.clone(), rmt_t.clone(), finished.clone());

        // okay, so here's a bit of an unfortunate situation:
        // we don't want to make I: 'static, as that's pretty restrictive. in particular because
        // *we* know that I won't outlive this function because we wait for the ThreadPool (which
        // is the only thing that has handles to I (through the Arc)). but convining the compiler
        // of that is tricky given that ThreadPool generally requires things to be 'static.
        //
        // we work around this by forcing a box of `issuer` and erasing its lifetime (by setting it
        // to 'static). again this is *only* safe because we wait for the threadpool.
        let issuer = Box::new(issuer) as Box<Issuer<Instance = I::Instance> + Send>;
        let issuer: Box<Issuer<Instance = I::Instance> + Send + 'static> =
            unsafe { mem::transmute(issuer) };
        let issuer = Arc::new(Mutex::new(issuer));

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|i| format!("issuer-{}", i))
            .num_threads(nthreads)
            .start_handler(move |_| {
                CLIENT.with(|c| {
                    *c.borrow_mut() = Some(Box::new(issuer.lock().unwrap().spawn()) as Box<Any>);
                })
            })
            .exit_handler(move |_| {
                SJRN.with(|sjrn| {
                    let mut sjrn_master = ts.0.lock().unwrap();
                    let sjrn = sjrn.borrow();
                    for (variant, h) in &*sjrn {
                        sjrn_master
                            .get_mut(variant)
                            .expect("missing master entry for variant")
                            .add(h)
                            .unwrap();
                    }
                });
                RMT.with(|rmt| {
                    let mut rmt_master = ts.1.lock().unwrap();
                    let rmt = rmt.borrow();
                    for (variant, h) in &*rmt {
                        rmt_master
                            .get_mut(variant)
                            .expect("missing master entry for variant")
                            .add(h)
                            .unwrap();
                    }
                });
                ts.2.wait();
            })
            .build()
            .map(Arc::new)
            .unwrap();

        let start = time::Instant::now();
        let generators: Vec<_> = (0..ngen)
            .map(|geni| {
                let pool = pool.clone();
                let finished = finished.clone();
                let wl = self.to_static();

                thread::Builder::new()
                    .name(format!("load-gen{}", geni))
                    .spawn(move || {
                        let ops = wl.run_generator::<I::Instance>(pool, target);
                        finished.wait();
                        ops
                    })
                    .unwrap()
            })
            .collect();

        drop(pool);
        let ops: usize = generators.into_iter().map(|gen| gen.join().unwrap()).sum();

        // all done!
        let took = start.elapsed();
        println!(
            "# actual ops/s: {:.2}",
            ops as f64 / (took.as_secs() as f64 + took.subsec_nanos() as f64 / 1_000_000_000f64)
        );

        let sjrn_t = sjrn_t.lock().unwrap();
        let rmt_t = rmt_t.lock().unwrap();

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

        println!("# op\tmetric\tpct\tms");
        for (variant, h) in &*sjrn_t {
            for &pct in &[50, 95, 99] {
                println!(
                    "{:?}\tsojourn\t{}\t{:.2}",
                    variant,
                    pct,
                    h.value_at_quantile(pct as f64 / 100.0),
                );
            }
            println!("{:?}\tsojourn\t100\t{:.2}", variant, h.max());
        }
        for (variant, h) in &*rmt_t {
            for &pct in &[50, 95, 99] {
                println!(
                    "{:?}\tprocessing\t{}\t{:.2}",
                    variant,
                    pct,
                    h.value_at_quantile(pct as f64 / 100.0),
                );
            }
            println!("{:?}\tprocessing\t100\t{:.2}", variant, h.max());
        }
    }

    fn run_generator<C>(self, pool: Arc<rayon::ThreadPool>, target: f64) -> usize
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
                LobstersRequest::Submit {
                    id: [0; 6],
                    user: rng.gen_range(0, BASE_USERS),
                    title: String::new(),
                }
            } else {
                // TODO: how do we pick a unique ID here?
                // TODO: sometimes pick a parent comment
                LobstersRequest::Comment {
                    id: id_to_slug(0),
                    user: rng.gen_range(0, BASE_USERS),
                    story: id_to_slug(rng.gen_range(0, BASE_STORIES)),
                    parent: None,
                }
            };

            let variant = mem::discriminant(&req);
            let issued = next;
            pool.spawn(move || {
                CLIENT.with(|c| {
                    // force to C so we get specialization
                    // see https://stackoverflow.com/a/33687996/472927
                    let mut issuer = c.borrow_mut();
                    let issuer = issuer.as_mut().unwrap();
                    let issuer = issuer.downcast_mut::<C>().unwrap();

                    let sent = time::Instant::now();
                    issuer.handle(req);
                    let done = time::Instant::now();

                    if sent.duration_since(start) > warmup {
                        let remote_t = done.duration_since(sent);
                        let sjrn_t = done.duration_since(issued);

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
                                    sjrn_t.as_secs() * 1_000
                                        + sjrn_t.subsec_nanos() as u64 / 1_000_000,
                                );
                        });
                    }
                });
            });
            ops += 1;

            // schedule next delivery
            use rand::distributions::IndependentSample;
            next += time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);
        }

        ops
    }
}
