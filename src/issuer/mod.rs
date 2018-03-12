use futures;
use tokio_core;
use std::time;
use std::rc::Rc;

pub trait LobstersClient {
    type Factory;

    fn spawn(&mut Self::Factory, &tokio_core::reactor::Handle) -> Self;
    fn handle(Rc<Self>, LobstersRequest)
        -> Box<futures::Future<Item = time::Duration, Error = ()>>;
}

pub type StoryId = [u8; 6];
pub type CommentId = [u8; 6];
pub type UserId = u32;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Vote {
    Up,
    Down,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LobstersRequest {
    Frontpage,
    Recent,
    Story(StoryId),
    Login(UserId),
    Logout(UserId),
    StoryVote(UserId, StoryId, Vote),
    CommentVote(UserId, CommentId, Vote),
    Submit {
        id: StoryId,
        user: UserId,
        title: String,
    },
    Comment {
        id: CommentId,
        user: UserId,
        story: StoryId,
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

    pub fn variant_name(v: &mem::Discriminant<Self>) -> &'static str {
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
