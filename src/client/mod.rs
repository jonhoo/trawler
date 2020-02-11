use std::future::Future;

/// Implementors of this trait handle [`LobstersRequest`] that correspond to "real" lobste.rs
/// website requests queued up by the workload generator.
///
/// Note that the workload generator does not check that the implementor correctly perform the
/// queries corresponding to each request; this must be verified with manual inspection of the
/// Rails application or its query logs.
pub trait LobstersClient {
    /// Errors produced by the client.
    ///
    /// If any errors are produced, they are generally printed rather than returned.
    type Error: std::fmt::Debug + Send + 'static;

    /// A future that will resolve once setup has finished.
    // NOTE: these should be IntoFuture, but then we can't give the Send bound
    type SetupFuture: Future<Output = Result<(), Self::Error>> + Send + 'static;

    /// A future that will resolve once a request has finished processing.
    type RequestFuture: Future<Output = Result<(), Self::Error>> + Send + 'static;

    /// A future that will resolve once client shutdown has finished.
    type ShutdownFuture: Future<Output = Result<(), Self::Error>> + 'static;

    /// Set up a fresh instance of the backend before priming.
    ///
    /// Implementing this allows benchmarking a backend without ever running the lobste.rs
    /// application. Normally, a backend would need to run the lobsters setup routine:
    ///
    /// ```console
    /// $ rails db:drop
    /// $ rails db:create
    /// $ rails db:schema:load
    /// $ rails db:seed
    /// ```
    ///
    /// The default implementation of this method just prints an informational message saying that
    /// the backend was not re-created.
    fn setup(&mut self) -> Self::SetupFuture;

    /// Handle the given lobste.rs request, made on behalf of the given user,
    /// returning a future that resolves when the request has been satisfied.
    ///
    /// The `priming` argument is set to true if the request is being issued just to populate the
    /// database. In this case, the backend need only issue writes and not do any other processing
    /// normally associated with the given `request`.
    fn handle(
        &mut self,
        user: Option<UserId>,
        request: LobstersRequest,
        priming: bool,
    ) -> Self::RequestFuture;

    /// Initiate shutdown of the client.
    ///
    /// The tokio runtime will not be shut down until the returned future resolves.
    fn shutdown(self) -> Self::ShutdownFuture;
}

/// A unique lobste.rs six-character story id.
pub type StoryId = [u8; 6];

/// A unique lobste.rs six-character comment id.
pub type CommentId = [u8; 6];

/// A unique lobste.rs user id.
///
/// Implementors should have a reliable mapping betwen user id and username in both directions.
/// This type is used both in the context of a username (e.g., /u/<user>) and in the context of who
/// performed an action (e.g., POST /s/ as <user>). In the former case, <user> is a username, and
/// the database will have to do a lookup based on username. In the latter, the user id is
/// associated with some session, and the backend does not need to do a lookup.
///
/// In the future, it is likely that this type will be split into two types: one for "session key" and
/// one for "username", both of which will require lookups, but on different keys.
pub type UserId = u32;

/// An up or down vote.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Vote {
    /// Upvote
    Up,
    /// Downvote
    Down,
}

/// A single lobste.rs client request.
///
/// Note that one request may end up issuing multiple backend queries. To see which queries are
/// executed by the real lobste.rs, see the [lobste.rs source
/// code](https://github.com/lobsters/lobsters).
///
/// Any request type that mentions an "acting" user is guaranteed to have the `user` argument to
/// `handle` be `Some`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LobstersRequest {
    /// Render [the frontpage](https://lobste.rs/).
    Frontpage,

    /// Render [recently submitted stories](https://lobste.rs/recent).
    Recent,

    /// Render [recently submitted comments](https://lobste.rs/comments).
    Comments,

    /// Render a [user's profile](https://lobste.rs/u/jonhoo).
    ///
    /// Note that the id here should be treated as a username.
    User(UserId),

    /// Render a [particular story](https://lobste.rs/s/cqnzl5/).
    Story(StoryId),

    /// Log in the acting user.
    ///
    /// Note that a user need not be logged in by a `LobstersRequest::Login` in order for a
    /// user-action (like `LobstersRequest::Submit`) to be issued for that user. The id here should
    /// be considered *both* a username *and* an id. The user with the username derived from this
    /// id should have the given id.
    Login,

    /// Log out the acting user.
    Logout,

    /// Have the acting user issue an up or down vote for the given story.
    ///
    /// Note that the load generator does not guarantee that a given user will only issue a single
    /// vote for a given story, nor that they will issue an equivalent number of upvotes and
    /// downvotes for a given story.
    StoryVote(StoryId, Vote),

    /// Have the acting user issue an up or down vote for the given comment.
    ///
    /// Note that the load generator does not guarantee that a given user will only issue a single
    /// vote for a given comment, nor that they will issue an equivalent number of upvotes and
    /// downvotes for a given comment.
    CommentVote(CommentId, Vote),

    /// Have the acting user submit a new story to the site.
    ///
    /// Note that the *generator* dictates the ids of new stories so that it can more easily keep
    /// track of which stories exist, and thus which stories can be voted for or commented on.
    Submit {
        /// The new story's id.
        id: StoryId,
        /// The story's title.
        title: String,
    },

    /// Have the acting user submit a new comment to the given story.
    ///
    /// Note that the *generator* dictates the ids of new comments so that it can more easily keep
    /// track of which comments exist for the purposes of generating comment votes and deeper
    /// threads.
    Comment {
        /// The new comment's id.
        id: CommentId,
        /// The story the comment is for.
        story: StoryId,
        /// The id of the comment's parent comment, if any.
        parent: Option<CommentId>,
    },
}

use std::mem;
use std::vec;
impl LobstersRequest {
    /// Enumerate all possible request types in a deterministic order.
    pub fn all() -> vec::IntoIter<mem::Discriminant<Self>> {
        vec![
            mem::discriminant(&LobstersRequest::Story([0; 6])),
            mem::discriminant(&LobstersRequest::Frontpage),
            mem::discriminant(&LobstersRequest::User(0)),
            mem::discriminant(&LobstersRequest::Comments),
            mem::discriminant(&LobstersRequest::Recent),
            mem::discriminant(&LobstersRequest::CommentVote([0; 6], Vote::Up)),
            mem::discriminant(&LobstersRequest::StoryVote([0; 6], Vote::Up)),
            mem::discriminant(&LobstersRequest::Comment {
                id: [0; 6],
                story: [0; 6],
                parent: None,
            }),
            mem::discriminant(&LobstersRequest::Login),
            mem::discriminant(&LobstersRequest::Submit {
                id: [0; 6],
                title: String::new(),
            }),
            mem::discriminant(&LobstersRequest::Logout),
        ]
        .into_iter()
    }

    /// Give a textual representation of the given `LobstersRequest` discriminant.
    ///
    /// Useful for printing the keys of the maps of histograms returned by `run`.
    pub fn variant_name(v: &mem::Discriminant<Self>) -> &'static str {
        match *v {
            d if d == mem::discriminant(&LobstersRequest::Frontpage) => "Frontpage",
            d if d == mem::discriminant(&LobstersRequest::Recent) => "Recent",
            d if d == mem::discriminant(&LobstersRequest::Comments) => "Comments",
            d if d == mem::discriminant(&LobstersRequest::User(0)) => "User",
            d if d == mem::discriminant(&LobstersRequest::Story([0; 6])) => "Story",
            d if d == mem::discriminant(&LobstersRequest::Login) => "Login",
            d if d == mem::discriminant(&LobstersRequest::Logout) => "Logout",
            d if d == mem::discriminant(&LobstersRequest::StoryVote([0; 6], Vote::Up)) => {
                "StoryVote"
            }
            d if d == mem::discriminant(&LobstersRequest::CommentVote([0; 6], Vote::Up)) => {
                "CommentVote"
            }
            d if d
                == mem::discriminant(&LobstersRequest::Submit {
                    id: [0; 6],
                    title: String::new(),
                }) =>
            {
                "Submit"
            }
            d if d
                == mem::discriminant(&LobstersRequest::Comment {
                    id: [0; 6],
                    story: [0; 6],
                    parent: None,
                }) =>
            {
                "Comment"
            }
            _ => unreachable!(),
        }
    }

    /// Produce a textual representation of this request.
    ///
    /// These are on the form:
    ///
    /// ```text
    /// METHOD /path [params] <user>
    /// ```
    ///
    /// Where:
    ///
    ///  - `METHOD` is `GET` or `POST`.
    ///  - `/path` is the approximate lobste.rs URL endpoint for the request.
    ///  - `[params]` are any additional params to the request such as id to assign or associate a
    ///    new resource with with.
    pub fn describe(&self) -> String {
        match *self {
            LobstersRequest::Frontpage => String::from("GET /"),
            LobstersRequest::Recent => String::from("GET /recent"),
            LobstersRequest::Comments => String::from("GET /comments"),
            LobstersRequest::User(uid) => format!("GET /u/#{}", uid),
            LobstersRequest::Story(ref slug) => {
                format!("GET /s/{}", ::std::str::from_utf8(&slug[..]).unwrap())
            }
            LobstersRequest::Login => String::from("POST /login"),
            LobstersRequest::Logout => String::from("POST /logout"),
            LobstersRequest::StoryVote(ref story, v) => format!(
                "POST /stories/{}/{}",
                ::std::str::from_utf8(&story[..]).unwrap(),
                match v {
                    Vote::Up => "upvote",
                    Vote::Down => "downvote",
                },
            ),
            LobstersRequest::CommentVote(ref comment, v) => format!(
                "POST /comments/{}/{}",
                ::std::str::from_utf8(&comment[..]).unwrap(),
                match v {
                    Vote::Up => "upvote",
                    Vote::Down => "downvote",
                },
            ),
            LobstersRequest::Submit { ref id, .. } => format!(
                "POST /stories [{}]",
                ::std::str::from_utf8(&id[..]).unwrap(),
            ),
            LobstersRequest::Comment {
                ref id,
                ref story,
                ref parent,
            } => match *parent {
                Some(ref parent) => format!(
                    "POST /comments/{} [{}; {}]",
                    ::std::str::from_utf8(&parent[..]).unwrap(),
                    ::std::str::from_utf8(&id[..]).unwrap(),
                    ::std::str::from_utf8(&story[..]).unwrap(),
                ),
                None => format!(
                    "POST /comments [{}; {}]",
                    ::std::str::from_utf8(&id[..]).unwrap(),
                    ::std::str::from_utf8(&story[..]).unwrap(),
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn textual_requests() {
        assert_eq!(LobstersRequest::Frontpage.describe(), "GET /");
        assert_eq!(LobstersRequest::Recent.describe(), "GET /recent");
        assert_eq!(LobstersRequest::Comments.describe(), "GET /comments");
        assert_eq!(LobstersRequest::User(3).describe(), "GET /u/#3");
        assert_eq!(
            LobstersRequest::Story([48, 48, 48, 48, 57, 97]).describe(),
            "GET /s/00009a"
        );
        assert_eq!(LobstersRequest::Login.describe(), "POST /login");
        assert_eq!(LobstersRequest::Logout.describe(), "POST /logout");
        assert_eq!(
            LobstersRequest::StoryVote([48, 48, 48, 98, 57, 97], Vote::Up).describe(),
            "POST /stories/000b9a/upvote"
        );
        assert_eq!(
            LobstersRequest::StoryVote([48, 48, 48, 98, 57, 97], Vote::Down).describe(),
            "POST /stories/000b9a/downvote"
        );
        assert_eq!(
            LobstersRequest::CommentVote([48, 48, 48, 98, 57, 97], Vote::Up).describe(),
            "POST /comments/000b9a/upvote"
        );
        assert_eq!(
            LobstersRequest::CommentVote([48, 48, 48, 98, 57, 97], Vote::Down).describe(),
            "POST /comments/000b9a/downvote"
        );
        assert_eq!(
            LobstersRequest::Submit {
                id: [48, 48, 48, 48, 57, 97],
                title: String::from("foo"),
            }
            .describe(),
            "POST /stories [00009a]"
        );
        assert_eq!(
            LobstersRequest::Comment {
                id: [48, 48, 48, 48, 57, 97],
                story: [48, 48, 48, 48, 57, 98],
                parent: Some([48, 48, 48, 48, 57, 99]),
            }
            .describe(),
            "POST /comments/00009c [00009a; 00009b]"
        );
        assert_eq!(
            LobstersRequest::Comment {
                id: [48, 48, 48, 48, 57, 97],
                story: [48, 48, 48, 48, 57, 98],
                parent: None,
            }
            .describe(),
            "POST /comments [00009a; 00009b]"
        );
    }
}
