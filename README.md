# trawler-rs

[![Crates.io](https://img.shields.io/crates/v/trawler.svg)](https://crates.io/crates/trawler)
[![Documentation](https://docs.rs/trawler/badge.svg)](https://docs.rs/trawler/)
[![Build Status](https://travis-ci.org/jonhoo/trawler.svg?branch=master)](https://travis-ci.org/jonhoo/trawler)

This crate provides a workload generator that emulates the traffic to
[lobste.rs](https://lobste.rs). It is a mostly open-loop benchmark similar to
[TailBench](https://people.csail.mit.edu/sanchez/papers/2016.tailbench.iiswc.pdf), but it also
approximates the partly open-loop designed outlined in [*Open Versus Closed: A Cautionary
Tale*](https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/schroeder/schroeder.pdf) by
having clients potentially issue more than one query per request.

The benchmarker main component is the "load generator". It generates requests according to
actual lobste.rs traffic patterns as reported in [here](https://lobste.rs/s/cqnzl5/), records
the request time, and sends the request description to an implementor of
`Service<TrawlerRequest>`. When the resulting future resolves, the generator logs how long the
request took to process, *and* how long the request took from when it was generated until it
was satisfied (this is called the *sojourn time*).

Trawler is written so that it can *either* be run against an instance of
the [lobsters Rails app](https://github.com/lobsters/lobsters) *or*
directly against a backend by issuing queries. The former is done using the provided binary,
whereas the latter is done by linking against this crate as a library an implementing the
`Service` trait. The latter allows benchmarking a data storage backend without also incurring
the overhead of the Rails frontend. Note that if you want to benchmark against the Rails
application, you must apply the patches in `lobsters.diff` first.
