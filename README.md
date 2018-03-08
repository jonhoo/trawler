Trawler is a workload generator that emulates the traffic to
[lobste.rs](https://lobste.rs). It is a mostly open-loop benchmark
similar to
[TailBench](https://people.csail.mit.edu/sanchez/papers/2016.tailbench.iiswc.pdf),
but it also approximates the partly open-loop designed outlined in
[*Open Versus Closed: A Cautionary
Tale*](https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/schroeder/schroeder.pdf)
by having clients potentially issue more than one query per request.
Specifically, the benchmarker has two main components: load generators
and issuers. Load generators generate requests according to actual
lobste.rs traffic patterns as reported in
[here](https://lobste.rs/s/cqnzl5/), records the request time, and puts
the request description into a queue. Issuers take requests from the
queue and issues that request to the backend. When the backend responds,
the issuer logs how long the request took to process, *and* how long the
request took from when it was generated until it was satisfied (this is
called the *sojourn time*).

Trawler is written so that it can *either* be run against an instance of
the [lobsters Rails app](https://github.com/lobsters/lobsters) *or*
directly against a backend by issuing queries. The latter allows
benchmarking a data storage backend without also incurring the overhead
of the Rails frontend.
