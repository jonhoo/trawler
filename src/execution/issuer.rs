use execution::Stats;
use client::LobstersClient;
use futures::{Future, Stream};
use std::collections::HashMap;
use WorkerCommand;
use hdrhistogram::Histogram;
use std::{mem, time};
use tokio_core;
use std::cell::RefCell;
use std::rc::Rc;
use multiqueue;

pub(super) fn run<C>(
    warmup: time::Duration,
    max_in_flight: usize,
    mut core: tokio_core::reactor::Core,
    client: C,
    mut jobs: multiqueue::MPMCFutReceiver<WorkerCommand>,
) -> (Stats, Stats)
where
    C: LobstersClient,
{
    let mut start = time::Instant::now();
    let client = Rc::new(client);
    let in_flight = Rc::new(RefCell::new(0));
    let handle = core.handle();

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
                // start should be set to the first time after priming has finished
                start = time::Instant::now();
            }
            WorkerCommand::Request(issued, request) => {
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
                handle.spawn(C::handle(client.clone(), request).map(move |remote_t| {
                    *in_flight.borrow_mut() -= 1;
                    // start < issued for priming (Login in particular)
                    if issued >= start && issued.duration_since(start) > warmup {
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
                                sjrn_t.as_secs() * 1_000 + sjrn_t.subsec_nanos() as u64 / 1_000_000,
                            );
                    }
                }));
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
