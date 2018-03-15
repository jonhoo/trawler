use futures;
use tokio_core;
use std::time;
use std::rc::Rc;

/// Implementors of this trait handle [`LobstersRequest`] that correspond to "real" lobste.rs
/// website requests queued up by the workload generator.
///
/// Note that the workload generator does not check that the implementor correctly perform the
/// queries corresponding to each request; this must be verified with manual inspection of the
/// Rails application or its query logs.
///
/// In order to allow generating more load, the load generator spins up multiple "issuers" that
/// each have their own `LobstersClient`. The associated `Factory` type tells the generator how to
/// spawn more clients.
pub trait LobstersClient {
    /// The type used to spawn more clients of this type.
    type Factory;

    /// Spawn a new client for an issuer running the given tokio reactor.
    fn spawn(&mut Self::Factory, &tokio_core::reactor::Handle) -> Self;

    /// Handle the given lobste.rs request, returning a future that resolves when the request has
    /// been satisfied.
    fn handle(Rc<Self>, LobstersRequest)
        -> Box<futures::Future<Item = time::Duration, Error = ()>>;
}

/// A unique lobste.rs six-character story id.
pub type StoryId = [u8; 6];

/// A unique lobste.rs six-character comment id.
pub type CommentId = [u8; 6];

/// A unique lobste.rs user id.
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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LobstersRequest {
    /// Render [the frontpage](https://lobste.rs/).
    Frontpage,

    /// Render [recently submitted stories](https://lobste.rs/recent).
    Recent,

    /// Render a [particular story](https://lobste.rs/s/cqnzl5/).
    Story(StoryId),

    /// Log in the given user.
    ///
    /// Note that a user need not be logged in by a `LobstersRequest` in order for a user-action
    /// (like `LobstersRequest::Submit`) to be issued for that user.
    Login(UserId),

    /// Log out the given user.
    ///
    /// Note that a user need not be logged in by a `LobstersRequest` in order for a user-action
    /// (like `LobstersRequest::Submit`) to be issued for that user.
    Logout(UserId),

    /// Have the given user issue an up or down vote for the given story.
    ///
    /// Note that the load generator does not guarantee that a given user will only issue a single
    /// vote for a given story, nor that they will issue an equivalent number of upvotes and
    /// downvotes for a given story.
    StoryVote(UserId, StoryId, Vote),

    /// Have the given user issue an up or down vote for the given comment.
    ///
    /// Note that the load generator does not guarantee that a given user will only issue a single
    /// vote for a given comment, nor that they will issue an equivalent number of upvotes and
    /// downvotes for a given comment.
    CommentVote(UserId, CommentId, Vote),

    /// Have the given user submit a new story to the site.
    ///
    /// Note that the *generator* dictates the ids of new stories so that it can more easily keep
    /// track of which stories exist, and thus which stories can be voted for or commented on.
    Submit {
        /// The new story's id.
        id: StoryId,
        /// The story's submitter.
        user: UserId,
        /// The story's title.
        title: String,
    },

    /// Have the given user submit a new comment to the given story.
    ///
    /// Note that the *generator* dictates the ids of new comments so that it can more easily keep
    /// track of which comments exist for the purposes of generating comment votes and deeper
    /// threads.
    Comment {
        /// The new comment's id.
        id: CommentId,
        /// The comment's author.
        user: UserId,
        /// The story the comment is for.
        story: StoryId,
        /// The id of the comment's parent comment, if any.
        parent: Option<CommentId>,
    },
}

use std::mem;
use std::vec;
impl LobstersRequest {
    pub(crate) fn all() -> vec::IntoIter<mem::Discriminant<Self>> {
        vec![
            mem::discriminant(&LobstersRequest::Frontpage),
            mem::discriminant(&LobstersRequest::Recent),
            mem::discriminant(&LobstersRequest::Story([0; 6])),
            mem::discriminant(&LobstersRequest::Login(0)),
            mem::discriminant(&LobstersRequest::Logout(0)),
            mem::discriminant(&LobstersRequest::StoryVote(0, [0; 6], Vote::Up)),
            mem::discriminant(&LobstersRequest::CommentVote(0, [0; 6], Vote::Up)),
            mem::discriminant(&LobstersRequest::Submit {
                id: [0; 6],
                user: 0,
                title: String::new(),
            }),
            mem::discriminant(&LobstersRequest::Comment {
                id: [0; 6],
                user: 0,
                story: [0; 6],
                parent: None,
            }),
        ].into_iter()
    }

    /// Give a textual representation of the given `LobstersRequest` discriminant.
    ///
    /// Useful for printing the keys of the maps of histograms returned by `run`.
    pub(crate) fn variant_name(v: &mem::Discriminant<Self>) -> &'static str {
        match *v {
            d if d == mem::discriminant(&LobstersRequest::Frontpage) => "Frontpage",
            d if d == mem::discriminant(&LobstersRequest::Recent) => "Recent",
            d if d == mem::discriminant(&LobstersRequest::Story([0; 6])) => "Story",
            d if d == mem::discriminant(&LobstersRequest::Login(0)) => "Login",
            d if d == mem::discriminant(&LobstersRequest::Logout(0)) => "Logout",
            d if d == mem::discriminant(&LobstersRequest::StoryVote(0, [0; 6], Vote::Up)) => {
                "StoryVote"
            }
            d if d == mem::discriminant(&LobstersRequest::CommentVote(0, [0; 6], Vote::Up)) => {
                "CommentVote"
            }
            d if d == mem::discriminant(&LobstersRequest::Submit {
                id: [0; 6],
                user: 0,
                title: String::new(),
            }) =>
            {
                "Submit"
            }
            d if d == mem::discriminant(&LobstersRequest::Comment {
                id: [0; 6],
                user: 0,
                story: [0; 6],
                parent: None,
            }) =>
            {
                "Comment"
            }
            _ => unreachable!(),
        }
    }
}
