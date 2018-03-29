use WorkerCommand;
use client::LobstersClient;
use execution::Stats;
use futures::{Future, Stream};
use hdrhistogram::Histogram;
use multiqueue;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::{mem, time};
use tokio_core;

pub(super) fn run<C>(
    warmup: time::Duration,
    runtime: time::Duration,
    max_in_flight: usize,
    mut core: tokio_core::reactor::Core,
    client: C,
    mut jobs: multiqueue::MPMCFutReceiver<WorkerCommand>,
) -> (Stats, Stats)
where
    C: LobstersClient,
{
    let mut start = None;
    let client = Rc::new(client);
    let in_flight = Rc::new(RefCell::new(0));
    let handle = core.handle();
    let end = warmup + runtime;

    let sjrn = Rc::new(RefCell::new(HashMap::default()));
    let rmt = Rc::new(RefCell::new(HashMap::default()));

    while let Ok((Some(cmd), stream)) = core.run(jobs.into_future()) {
        jobs = stream;

        match cmd {
            WorkerCommand::Wait(barrier) => {
                // when we get a barrier, wait for all pending requests to complete
                {
                    while *in_flight.borrow_mut() > 0 {
                        core.turn(None);
                    }
                }

                barrier.wait();
            }
            WorkerCommand::Start(barrier) => {
                {
                    while *in_flight.borrow_mut() > 0 {
                        core.turn(None);
                    }
                }

                barrier.wait();
                // start should be set to the first time after priming has finished
                start = Some(time::Instant::now());
            }
            WorkerCommand::Request(issued, user, request) => {
                // ensure we don't have too many requests in flight at the same time
                {
                    while *in_flight.borrow_mut() >= max_in_flight {
                        core.turn(None);
                    }
                    *in_flight.borrow_mut() += 1;
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
                // drain the channel so we don't block the load generators
                core.run(jobs.filter(|_| false).collect()).unwrap();
                break;
            }
        }
    }

    let mut sjrn = sjrn.borrow_mut();
    let mut rmt = rmt.borrow_mut();
    (
        mem::replace(&mut *sjrn, HashMap::default()),
        mem::replace(&mut *rmt, HashMap::default()),
    )
}
