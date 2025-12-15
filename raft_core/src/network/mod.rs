use crate::message::RaftMessage;
use crate::state::NodeId;
use async_trait::async_trait;

#[async_trait]
pub trait Network: Send + Sync {
    async fn send(
        &self,
        sender_id: NodeId,
        target_id: NodeId,
        message: RaftMessage,
    ) -> anyhow::Result<()>;
    async fn run_listener(&self) -> anyhow::Result<()>;
}

pub mod mem;
pub mod tcp;
