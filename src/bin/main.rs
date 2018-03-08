#[macro_use]
extern crate clap;
extern crate hyper;
extern crate tokio_core;
extern crate trawler;
extern crate url;

use clap::{App, Arg};
use std::time;

struct WebClientSpawner {
    prefix: url::Url,
}
impl WebClientSpawner {
    fn new(prefix: &str) -> Self {
        WebClientSpawner {
            prefix: url::Url::parse(prefix).unwrap(),
        }
    }
}
impl trawler::Issuer for WebClientSpawner {
    type Instance = WebClient;

    fn spawn(&mut self) -> Self::Instance {
        WebClient::new(&self.prefix)
    }
}

struct WebClient {
    prefix: url::Url,
    core: tokio_core::reactor::Core,
    client: hyper::Client<hyper::client::HttpConnector>,
}
impl WebClient {
    fn new(prefix: &url::Url) -> Self {
        let core = tokio_core::reactor::Core::new().unwrap();
        let client = hyper::Client::new(&core.handle());
        WebClient {
            prefix: prefix.clone(),
            core: core,
            client: client,
        }
    }
}
impl trawler::LobstersClient for WebClient {
    fn handle(&mut self, req: trawler::LobstersRequest) {
        use trawler::LobstersRequest;
        match req {
            LobstersRequest::Frontpage => {
                use std::str::FromStr;
                let url = hyper::Uri::from_str(self.prefix.as_ref()).unwrap();
                self.core.run(self.client.get(url)).unwrap();
            }
            req => {
                println!("{:?}", req);
                //unimplemented!();
            }
        }
    }
}

fn main() {
    let args = App::new("trawler")
        .version("0.1")
        .about("Benchmark a lobste.rs Rails installation")
        .arg(
            Arg::with_name("scale")
                .short("s")
                .long("scale")
                .takes_value(true)
                .default_value("1.0")
                .help("Scale factor for workload"),
        )
        .arg(
            Arg::with_name("issuers")
                .short("i")
                .long("issuers")
                .takes_value(true)
                .default_value("4")
                .help("Number of issuers to run"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .takes_value(true)
                .default_value("30")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .takes_value(true)
                .default_value("10")
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("histogram")
                .long("histogram")
                .help("Use file-based serialized HdrHistograms")
                .takes_value(true)
                .long_help(
                    "If the file already exists, the existing histogram is extended.\
                     There are two histograms, written out in order: \
                     sojourn and remote.",
                ),
        )
        .arg(
            Arg::with_name("prefix")
                .value_name("URL-PREFIX")
                .takes_value(true)
                .default_value("http://localhost:3000")
                .index(1),
        )
        .get_matches();

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(value_t_or_exit!(args, "scale", f64))
        .issuers(value_t_or_exit!(args, "issuers", usize))
        .time(
            time::Duration::from_secs(value_t_or_exit!(args, "warmup", u64)),
            time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64)),
        );

    if let Some(h) = args.value_of("histogram") {
        wl.with_histogram(h);
    }

    wl.run(WebClientSpawner::new(args.value_of("prefix").unwrap()));
}
