//! [`FakeCoordinatorStubFactory`]: in-memory [`CoordinatorStubFactory`] for tests — delivers
//! directly into a neighbor's [`Handle::handle_execute_prepare_migration`] &c. with no wire
//! encoding, the fake half of the outbound substitutability seam (mirrors `ntk_rpc::FakeRpcClient`
//! for [`ntk_rpc::RpcClient`]).

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_peerservices::StubCallError;

use crate::actor::Handle;
use crate::domain::PropagationArgs;
use crate::traits::{CoordinatorStub, CoordinatorStubFactory};

/// Delivers directly into one neighbor's [`Handle`].
struct DirectStub(Handle);

/// Wraps `handle` as a [`CoordinatorStub`] that delivers directly into its own
/// `handle_execute_*` counterparts — the same building block [`FakeCoordinatorStubFactory`] is
/// built from, exposed so a test needing a custom [`CoordinatorStubFactory`] (e.g. one whose
/// neighbor set grows over time, or that mixes in a deliberately failing stub) can compose one
/// without reimplementing wire-free delivery.
#[must_use]
pub fn direct_stub(handle: Handle) -> Arc<dyn CoordinatorStub> {
    Arc::new(DirectStub(handle))
}

impl CoordinatorStub for DirectStub {
    fn execute_prepare_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handle = self.0.clone();
        Box::pin(async move {
            handle.handle_execute_prepare_migration(args).await;
            Ok(())
        })
    }

    fn execute_finish_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handle = self.0.clone();
        Box::pin(async move {
            handle.handle_execute_finish_migration(args).await;
            Ok(())
        })
    }

    fn execute_prepare_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handle = self.0.clone();
        Box::pin(async move {
            handle.handle_execute_prepare_enter(args).await;
            Ok(())
        })
    }

    fn execute_finish_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handle = self.0.clone();
        Box::pin(async move {
            handle.handle_execute_finish_enter(args).await;
            Ok(())
        })
    }

    fn execute_we_have_splitted(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handle = self.0.clone();
        Box::pin(async move {
            handle.handle_execute_we_have_splitted(args).await;
            Ok(())
        })
    }
}

/// Delivers to every neighbor at once — the fake analogue of a reliable-broadcast group
/// (`get_stub_for_all_neighbors`).
struct BroadcastStub(Vec<Handle>);

impl CoordinatorStub for BroadcastStub {
    fn execute_prepare_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handles = self.0.clone();
        Box::pin(async move {
            for h in handles {
                h.handle_execute_prepare_migration(args.clone()).await;
            }
            Ok(())
        })
    }

    fn execute_finish_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handles = self.0.clone();
        Box::pin(async move {
            for h in handles {
                h.handle_execute_finish_migration(args.clone()).await;
            }
            Ok(())
        })
    }

    fn execute_prepare_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handles = self.0.clone();
        Box::pin(async move {
            for h in handles {
                h.handle_execute_prepare_enter(args.clone()).await;
            }
            Ok(())
        })
    }

    fn execute_finish_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handles = self.0.clone();
        Box::pin(async move {
            for h in handles {
                h.handle_execute_finish_enter(args.clone()).await;
            }
            Ok(())
        })
    }

    fn execute_we_have_splitted(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let handles = self.0.clone();
        Box::pin(async move {
            for h in handles {
                h.handle_execute_we_have_splitted(args.clone()).await;
            }
            Ok(())
        })
    }
}

/// In-memory [`CoordinatorStubFactory`]: `neighbors` are the other simulated nodes'
/// Coordinator [`Handle`]s.
#[derive(Debug)]
pub struct FakeCoordinatorStubFactory {
    neighbors: Vec<Handle>,
}

impl FakeCoordinatorStubFactory {
    #[must_use]
    pub fn new(neighbors: Vec<Handle>) -> Self {
        Self { neighbors }
    }
}

impl CoordinatorStubFactory for FakeCoordinatorStubFactory {
    fn stub_for_each_neighbor(&self) -> Vec<Arc<dyn CoordinatorStub>> {
        self.neighbors
            .iter()
            .cloned()
            .map(|h| Arc::new(DirectStub(h)) as Arc<dyn CoordinatorStub>)
            .collect()
    }

    fn stub_for_all_neighbors(&self) -> Arc<dyn CoordinatorStub> {
        Arc::new(BroadcastStub(self.neighbors.clone()))
    }
}
