//#![deny(missing_docs)]

extern crate futures;
extern crate hdrhistogram;
extern crate libc;
extern crate multiqueue;
extern crate rand;
extern crate tokio_core;
extern crate zipf;

mod client;
pub use client::{LobstersClient, LobstersRequest, Vote};

mod execution;

use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::fs;
use std::time;
use std::sync::{Arc, Barrier};

const BASE_OPS_PER_SEC: usize = 10;

// at time of writing, https://lobste.rs/recent/page/1601 is last page
const BASE_STORIES: u32 = 25 * 1601;

// wild guess
const BASE_COMMENTS: u32 = BASE_STORIES * 7;

// document.querySelectorAll(".user_tree > li").length on https://lobste.rs/u
const BASE_USERS: u32 = 9000;

#[derive(Debug, Clone)]
enum WorkerCommand {
    Request(time::Instant, LobstersRequest),
    Wait(Arc<Barrier>),
}

/// Set the parameters for a new Lobsters-like workload.
pub struct WorkloadBuilder<'a> {
    load: execution::Workload,
    histogram_file: Option<&'a str>,
}

impl<'a> Default for WorkloadBuilder<'a> {
    fn default() -> Self {
        WorkloadBuilder {
            load: execution::Workload {
                mem_scale: 1.0,
                req_scale: 1.0,

                threads: 1,

                warmup: time::Duration::from_secs(10),
                runtime: time::Duration::from_secs(30),
            },
            histogram_file: None,
        }
    }
}

impl<'a> WorkloadBuilder<'a> {
    /// Set the memory and request scale factor for the workload.
    ///
    /// A factor of 1 generates a workload commensurate with what the [real lobste.rs
    /// sees](https://lobste.rs/s/cqnzl5/). At memory scale 1, the site has ~40k stories with a
    /// total of ~300k comments and ~570k votes spread across 9k users. At request factor 1, the
    /// generated load is on average 44 requests/minute, with a request distribution set according
    /// to the one observed on lobste.rs (see `data/` for details).
    pub fn scale(&mut self, mem_factor: f64, req_factor: f64) -> &mut Self {
        self.load.mem_scale = mem_factor;
        self.load.req_scale = req_factor;
        self
    }

    pub fn issuers(&mut self, n: usize) -> &mut Self {
        self.load.threads = n;
        self
    }

    pub fn time(&mut self, warmup: time::Duration, runtime: time::Duration) -> &mut Self {
        self.load.warmup = warmup;
        self.load.runtime = runtime;
        self
    }

    pub fn with_histogram<'this>(&'this mut self, path: &'a str) -> &'this mut Self {
        self.histogram_file = Some(path);
        self
    }
}

impl<'a> WorkloadBuilder<'a> {
    pub fn run<C, I>(&self, factory: I, prime: bool)
    where
        I: Send + 'static,
        C: LobstersClient<Factory = I> + 'static,
    {
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

        // actually run the workload
        let start = time::Instant::now();
        let (workers, ops) = execution::harness::run::<C, _>(self.load.clone(), factory, prime);

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
}
