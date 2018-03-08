pub trait Issuer {
    type Instance: LobstersClient;

    fn spawn(&mut self) -> Self::Instance;
}

pub type StoryId = u32;
pub type UserId = u32;
pub type CommentId = u32;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Vote {
    Up,
    Down,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LobstersRequest {
    Frontpage,
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

pub trait LobstersClient {
    fn handle(&mut self, LobstersRequest);
}
