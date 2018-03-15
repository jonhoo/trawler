#[macro_use]
extern crate clap;
extern crate futures;
extern crate hyper;
extern crate regex;
extern crate tokio_core;
extern crate trawler;
extern crate url;
#[macro_use]
extern crate lazy_static;

use clap::{App, Arg};
use futures::Future;
use trawler::{LobstersRequest, Vote};

use std::time;
use std::collections::HashMap;
use std::str::FromStr;
use std::rc::Rc;
use std::sync::RwLock;

lazy_static! {
    static ref SESSION_COOKIES: RwLock<HashMap<u32, hyper::header::Cookie>> = RwLock::default();
}

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

struct WebClient {
    prefix: url::Url,
    client: hyper::Client<hyper::client::HttpConnector>,
}
impl WebClient {
    fn new(handle: &tokio_core::reactor::Handle, prefix: &url::Url) -> Self {
        let client = hyper::Client::new(handle);
        WebClient {
            prefix: prefix.clone(),
            client: client,
        }
    }

    fn get_cookie_for(
        this: Rc<Self>,
        uid: u32,
    ) -> Box<futures::Future<Item = hyper::header::Cookie, Error = hyper::Error>> {
        {
            let cookies = SESSION_COOKIES.read().unwrap();
            if let Some(cookie) = cookies.get(&uid) {
                return Box::new(futures::finished(cookie.clone()));
            }
        }

        use hyper::header::{Cookie, Header, Raw, SetCookie};

        let url = hyper::Uri::from_str(this.prefix.join("login").unwrap().as_ref()).unwrap();
        let mut req = hyper::Request::new(hyper::Method::Post, url);
        let mut s = url::form_urlencoded::Serializer::new(String::new());
        s.append_pair("utf8", "✓");
        s.append_pair("email", &format!("user{}", uid));
        //s.append_pair("email", "test");
        s.append_pair("password", "test");
        s.append_pair("commit", "Login");
        s.append_pair("referer", this.prefix.as_ref());
        req.set_body(s.finish());
        req.headers_mut()
            .set(hyper::header::ContentType::form_url_encoded());

        Box::new(this.client.request(req).and_then(move |res| {
            if res.status() != hyper::StatusCode::Found {
                use futures::Stream;
                futures::future::Either::A(res.body().concat2().map(move |body| {
                    panic!(
                        "Failed to log in as user{}/test. Make sure to apply the patches!\n{}",
                        uid,
                        ::std::str::from_utf8(&*body).unwrap(),
                    );
                }))
            } else {
                let mut cookie = Cookie::new();
                if let Some(&SetCookie(ref content)) = res.headers().get() {
                    for c in content {
                        let c = Cookie::parse_header(&Raw::from(&**c)).unwrap();
                        for (k, v) in c.iter() {
                            cookie.append(k.to_string(), v.to_string());
                        }
                    }
                } else {
                    unreachable!()
                }

                SESSION_COOKIES.write().unwrap().insert(uid, cookie.clone());
                futures::future::Either::B(futures::finished(cookie))
            }
        }))
    }
}
impl trawler::LobstersClient for WebClient {
    type Factory = WebClientSpawner;

    fn spawn(spawner: &mut Self::Factory, handle: &tokio_core::reactor::Handle) -> Self {
        WebClient::new(handle, &spawner.prefix)
    }

    fn handle(
        this: Rc<Self>,
        req: trawler::LobstersRequest,
    ) -> Box<futures::Future<Item = time::Duration, Error = ()>> {
        let mut uid = None;
        let sent = time::Instant::now();
        let mut expected = hyper::StatusCode::Ok;
        let mut req = match req {
            LobstersRequest::Frontpage => {
                // XXX: do we want to pick randomly between logged in users when making requests?
                let url = hyper::Uri::from_str(this.prefix.as_ref()).unwrap();
                hyper::Request::new(hyper::Method::Get, url)
            }
            LobstersRequest::Recent => {
                let url =
                    hyper::Uri::from_str(this.prefix.join("recent").unwrap().as_ref()).unwrap();
                hyper::Request::new(hyper::Method::Get, url)
            }
            LobstersRequest::Login(uid) => {
                return Box::new(
                    WebClient::get_cookie_for(this.clone(), uid)
                        .map(move |_| sent.elapsed())
                        .map_err(|_| ()),
                );
            }
            LobstersRequest::Logout(..) => {
                /*
                let url =
                    hyper::Uri::from_str(this.prefix.join("logout").unwrap().as_ref()).unwrap();
                hyper::Request::new(hyper::Method::Post, url)
                */
                return Box::new(futures::failed(()));
            }
            LobstersRequest::Story(id) => {
                let url = hyper::Uri::from_str(
                    this.prefix
                        .join("s/")
                        .unwrap()
                        .join(::std::str::from_utf8(&id[..]).unwrap())
                        .unwrap()
                        .as_ref(),
                ).unwrap();
                hyper::Request::new(hyper::Method::Get, url)
            }
            LobstersRequest::StoryVote(user, story, v) => {
                let url = hyper::Uri::from_str(
                    this.prefix
                        .join(&format!(
                            "stories/{}/{}",
                            ::std::str::from_utf8(&story[..]).unwrap(),
                            match v {
                                Vote::Up => "upvote",
                                Vote::Down => "unvote",
                            }
                        ))
                        .unwrap()
                        .as_ref(),
                ).unwrap();
                uid = Some(user);
                hyper::Request::new(hyper::Method::Post, url)
            }
            LobstersRequest::CommentVote(user, comment, v) => {
                let url = hyper::Uri::from_str(
                    this.prefix
                        .join(&format!(
                            "comments/{}/{}",
                            ::std::str::from_utf8(&comment[..]).unwrap(),
                            match v {
                                Vote::Up => "upvote",
                                Vote::Down => "unvote",
                            }
                        ))
                        .unwrap()
                        .as_ref(),
                ).unwrap();
                uid = Some(user);
                hyper::Request::new(hyper::Method::Post, url)
            }
            LobstersRequest::Submit { id, user, title } => {
                uid = Some(user);
                expected = hyper::StatusCode::Found;

                let url =
                    hyper::Uri::from_str(this.prefix.join("stories").unwrap().as_ref()).unwrap();
                let mut req = hyper::Request::new(hyper::Method::Post, url);
                let mut s = url::form_urlencoded::Serializer::new(String::new());
                s.append_pair("commit", "Submit");
                s.append_pair("story[short_id]", ::std::str::from_utf8(&id[..]).unwrap());
                s.append_pair("story[tags_a][]", "benchmark");
                s.append_pair("story[title]", &title);
                s.append_pair("story[description]", "to infinity");
                s.append_pair("utf8", "✓");
                req.set_body(s.finish());
                req.headers_mut()
                    .set(hyper::header::ContentType::form_url_encoded());
                req
            }
            LobstersRequest::Comment {
                id,
                user,
                story,
                parent,
            } => {
                uid = Some(user);

                let url =
                    hyper::Uri::from_str(this.prefix.join("comments").unwrap().as_ref()).unwrap();
                let mut req = hyper::Request::new(hyper::Method::Post, url);
                let mut s = url::form_urlencoded::Serializer::new(String::new());
                s.append_pair("short_id", ::std::str::from_utf8(&id[..]).unwrap());
                s.append_pair("comment", "moar benchmarking");
                if let Some(parent) = parent {
                    s.append_pair(
                        "parent_comment_short_id",
                        ::std::str::from_utf8(&parent[..]).unwrap(),
                    );
                }
                s.append_pair("story_id", ::std::str::from_utf8(&story[..]).unwrap());
                s.append_pair("utf8", "✓");
                req.set_body(s.finish());
                req.headers_mut()
                    .set(hyper::header::ContentType::form_url_encoded());
                req
            }
        };

        let req = if let Some(uid) = uid {
            futures::future::Either::A(WebClient::get_cookie_for(this.clone(), uid).map(
                move |cookie| {
                    req.headers_mut().set(cookie);
                    req
                },
            ))
        } else {
            futures::future::Either::B(futures::finished(req))
        };

        Box::new(req.and_then(move |req| {
            this.client.request(req).and_then(move |res| {
                if res.status() != expected {
                    use futures::Stream;

                    let status = res.status();
                    futures::future::Either::A(res.body().concat2().map(move |body| {
                        panic!(
                            "{:?} status response. You probably forgot to prime.\n{}",
                            status,
                            ::std::str::from_utf8(&*body).unwrap(),
                        );
                    }))
                } else {
                    futures::future::Either::B(futures::finished(sent.elapsed()))
                }
            })
        }).map_err(|e| {
            eprintln!("hyper: {:?}", e);
        }))
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
                .default_value("1")
                .help("Number of issuers to run"),
        )
        .arg(
            Arg::with_name("prime")
                .long("prime")
                .help("Set if the backend must be primed with initial stories and comments."),
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

    let scale = value_t_or_exit!(args, "scale", f64);

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(scale, scale)
        .issuers(value_t_or_exit!(args, "issuers", usize))
        .time(
            time::Duration::from_secs(value_t_or_exit!(args, "warmup", u64)),
            time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64)),
        );

    if let Some(h) = args.value_of("histogram") {
        wl.with_histogram(h);
    }

    wl.run::<WebClient, _>(
        WebClientSpawner::new(args.value_of("prefix").unwrap()),
        args.is_present("prime"),
    );
}
