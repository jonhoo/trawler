#[macro_use]
extern crate clap;
extern crate futures;
extern crate hyper;
extern crate regex;
extern crate tokio_core;
extern crate trawler;
extern crate url;

use clap::{App, Arg};
use std::time;
use std::collections::HashMap;
use futures::stream::Stream;

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
    cookies: HashMap<u32, hyper::header::Cookie>,
}
impl WebClient {
    fn new(prefix: &url::Url) -> Self {
        let core = tokio_core::reactor::Core::new().unwrap();
        let client = hyper::Client::new(&core.handle());
        WebClient {
            prefix: prefix.clone(),
            core: core,
            client: client,
            cookies: Default::default(),
        }
    }
}
impl trawler::LobstersClient for WebClient {
    fn handle(&mut self, req: trawler::LobstersRequest) {
        use std::str::FromStr;
        use trawler::{LobstersRequest, Vote};

        let mut uid = None;
        let mut req = match req {
            LobstersRequest::Frontpage => {
                let url = hyper::Uri::from_str(self.prefix.as_ref()).unwrap();
                hyper::Request::new(hyper::Method::Get, url)
            }
            LobstersRequest::Recent => {
                let url =
                    hyper::Uri::from_str(self.prefix.join("recent").unwrap().as_ref()).unwrap();
                hyper::Request::new(hyper::Method::Get, url)
            }
            LobstersRequest::Login(..) => {
                // XXX: do we want to pick randomly between logged in users when making requests?
                return;
            }
            LobstersRequest::Logout(..) => {
                /*
                let url =
                    hyper::Uri::from_str(self.prefix.join("logout").unwrap().as_ref()).unwrap();
                hyper::Request::new(hyper::Method::Post, url)
                */
                return;
            }
            LobstersRequest::Story(id) => {
                let url = hyper::Uri::from_str(
                    self.prefix
                        .join("s")
                        .unwrap()
                        .join(::std::str::from_utf8(&id[..]).unwrap())
                        .unwrap()
                        .as_ref(),
                ).unwrap();
                hyper::Request::new(hyper::Method::Get, url)
            }
            LobstersRequest::StoryVote(user, story, v) => {
                let url = hyper::Uri::from_str(
                    self.prefix
                        .join("stories")
                        .unwrap()
                        .join(::std::str::from_utf8(&story[..]).unwrap())
                        .unwrap()
                        .join(match v {
                            Vote::Up => "upvote",
                            Vote::Down => "unvote",
                        })
                        .unwrap()
                        .as_ref(),
                ).unwrap();
                uid = Some(user);
                hyper::Request::new(hyper::Method::Post, url)
            }
            LobstersRequest::CommentVote(user, comment, v) => {
                let url = hyper::Uri::from_str(
                    self.prefix
                        .join("comments")
                        .unwrap()
                        .join(::std::str::from_utf8(&comment[..]).unwrap())
                        .unwrap()
                        .join(match v {
                            Vote::Up => "upvote",
                            Vote::Down => "unvote",
                        })
                        .unwrap()
                        .as_ref(),
                ).unwrap();
                uid = Some(user);
                hyper::Request::new(hyper::Method::Post, url)
            }
            req => {
                println!("{:?}", req);
                return;
                //unimplemented!();
            }
        };

        if let Some(uid) = uid {
            let prefix = &self.prefix;
            let client = &mut self.client;
            let core = &mut self.core;
            let cookie = self.cookies
                .entry(uid)
                .or_insert_with(|| {
                    use hyper::header::{Cookie, Header, Raw, SetCookie};

                    // we need a cookie for the session
                    // which means we need to log in
                    // which means we need the csrf token for the log in form...
                    let url = hyper::Uri::from_str(prefix.join("login").unwrap().as_ref()).unwrap();
                    let res = core.run(client.get(url.clone())).unwrap();

                    // also, we *must* send the cookie that the login page sets for our subsequent
                    // login POST; presumably so that it can *check* the access token.
                    let mut cookie = Cookie::new();
                    if let Some(&SetCookie(ref content)) = res.headers().get() {
                        for c in content {
                            let c = Cookie::parse_header(&Raw::from(&**c)).unwrap();
                            for (k, v) in c.iter() {
                                cookie.append(k.to_string(), v.to_string());
                            }
                        }
                    }

                    // now extract the token
                    let b = core.run(res.body().concat2()).unwrap();
                    let re = regex::bytes::Regex::new(
                        "name=\"authenticity_token\" value=\"([^\"]+)\"",
                    ).unwrap();
                    let at = ::std::str::from_utf8(
                        re.captures_iter(&b)
                            .next()
                            .unwrap()
                            .get(1)
                            .unwrap()
                            .as_bytes(),
                    ).unwrap();

                    let mut req = hyper::Request::new(hyper::Method::Post, url);
                    let mut s = url::form_urlencoded::Serializer::new(String::new());
                    s.append_pair("utf8", "✓");
                    s.append_pair("authenticity_token", at);
                    s.append_pair("email", &format!("user{}", uid));
                    //s.append_pair("email", "test");
                    s.append_pair("password", "test");
                    s.append_pair("commit", "Login");
                    s.append_pair("referer", prefix.as_ref());
                    req.set_body(s.finish());
                    req.headers_mut()
                        .set(hyper::header::ContentType::form_url_encoded());
                    req.headers_mut().set(cookie);
                    let res = core.run(client.request(req)).unwrap();

                    if res.status() != hyper::StatusCode::Found {
                        panic!(
                            "Failed to log in as user{}/test. Make sure to create all the test users!",
                            uid
                        );
                    }

                    let mut cookie = Cookie::new();
                    if let Some(&SetCookie(ref content)) = res.headers().get() {
                        for c in content {
                            let c = Cookie::parse_header(&Raw::from(&**c)).unwrap();
                            for (k, v) in c.iter() {
                                cookie.append(k.to_string(), v.to_string());
                            }
                        }

                        cookie
                    } else {
                        unreachable!()
                    }
                })
                .clone();
            req.headers_mut().set(cookie);
        };

        let res = self.core.run(self.client.request(req)).unwrap();
        if res.status() != hyper::StatusCode::Ok {
            panic!(
                "{:?} status response. You probably forgot to prime.",
                res.status()
            );
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
