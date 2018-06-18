use client::LobstersClient;
use crossbeam_channel;
use execution::Stats;
use futures::Future;
use hdrhistogram::Histogram;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::{mem, time};
use tokio_core;
use WorkerCommand;

pub(super) fn run<C>(
    warmup: time::Duration,
    runtime: time::Duration,
    max_in_flight: usize,
    mut core: tokio_core::reactor::Core,
    client: C,
    jobs: crossbeam_channel::Receiver<WorkerCommand>,
) -> (f64, Stats, Stats)
where
    C: LobstersClient,
{
    let mut start = None;
    let mut nissued = 0;
    let client = Rc::new(client);
    let in_flight = Rc::new(RefCell::new(0));
    let handle = core.handle();
    let end = warmup + runtime;

    let sjrn = Rc::new(RefCell::new(HashMap::default()));
    let rmt = Rc::new(RefCell::new(HashMap::default()));

    loop {
        let x = if *in_flight.borrow() == 0 {
            // nothing in flight, so no sense in spinning
            jobs.recv()
                .map_err(|_| crossbeam_channel::TryRecvError::Disconnected)
        } else {
            // TODO
            // we need to split our attention between getting jobs and turning the core
            // once we have a futures-aware mpmc channel, we won't have to hack like this
            // track https://github.com/crossbeam-rs/crossbeam-channel/issues/22
            jobs.try_recv()
        };

        match x {
            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
            Err(crossbeam_channel::TryRecvError::Empty) => {
                // spend a little bit of time waiting for in-flight things to progress
                // we *could* give a timeout of 0.0 here, but then we could easily start spinning
                // it's a little sad that we only turn once between every time we try_recv though
                core.turn(Some(time::Duration::new(0, 100_000)));
            }
            Ok(WorkerCommand::Wait(barrier)) => {
                // when we get a barrier, wait for all pending requests to complete
                {
                    while *in_flight.borrow_mut() > 0 {
                        core.turn(None);
                    }
                }

                barrier.wait();
            }
            Ok(WorkerCommand::Start(barrier)) => {
                {
                    while *in_flight.borrow_mut() > 0 {
                        core.turn(None);
                    }
                }

                barrier.wait();
                // start should be set to the first time after priming has finished
                start = Some(time::Instant::now());
            }
            Ok(WorkerCommand::Request(issued, user, request)) => {
                // ensure we don't have too many requests in flight at the same time
                {
                    while *in_flight.borrow_mut() >= max_in_flight {
                        core.turn(None);
                    }
                    *in_flight.borrow_mut() += 1;
                }

                if let Some(start) = start {
                    if issued.duration_since(start) > warmup {
                        nissued += 1;
                    }
                }

                let in_flight = in_flight.clone();
                let sjrn = sjrn.clone();
                let rmt = rmt.clone();
                let variant = mem::discriminant(&request);
                handle.spawn(
                    C::handle(client.clone(), user, request).then(move |remote_t| {
                        *in_flight.borrow_mut() -= 1;
                        if start.is_none() {
                            return Ok(());
                        }

                        let start = start.unwrap();
                        if remote_t.is_ok() && issued.duration_since(start) > warmup {
                            let remote_t = remote_t.unwrap();
                            let sjrn_t = issued.elapsed();

                            rmt.borrow_mut()
                                .entry(variant)
                                .or_insert_with(|| {
                                    Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap()
                                })
                                .saturating_record(
                                    remote_t.as_secs() * 1_000
                                        + remote_t.subsec_nanos() as u64 / 1_000_000,
                                );
                            sjrn.borrow_mut()
                                .entry(variant)
                                .or_insert_with(|| {
                                    Histogram::<u64>::new_with_bounds(1, 10_000, 4).unwrap()
                                })
                                .saturating_record(
                                    sjrn_t.as_secs() * 1_000
                                        + sjrn_t.subsec_nanos() as u64 / 1_000_000,
                                );
                        }

                        Ok(())
                    }),
                );
            }
        }

        if let Some(start) = start {
            if start.elapsed() > end {
                // we're past the end of the experiments and should exit cleanly.
                // ignore anything left in the queue, and just finish up our current work.
                while *in_flight.borrow_mut() > 0 {
                    core.turn(None);
                }
                break;
            }
        }
    }

    let count_from = start.unwrap() + warmup;
    let mut per_second = 0.0;
    let now = time::Instant::now();
    if now > count_from {
        let took = now.duration_since(count_from);
        if took != time::Duration::new(0, 0) {
            per_second = nissued as f64
                / (took.as_secs() as f64 + took.subsec_nanos() as f64 / 1_000_000_000f64)
        }
    }

    let mut sjrn = sjrn.borrow_mut();
    let mut rmt = rmt.borrow_mut();
    (
        per_second,
        mem::replace(&mut *sjrn, HashMap::default()),
        mem::replace(&mut *rmt, HashMap::default()),
    )
}
