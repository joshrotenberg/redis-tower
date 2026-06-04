//! Compose Tower layers on top of a Redis FrameService.
//!
//! FrameService implements Service<Frame, Response=Frame>, so standard
//! Tower middleware works. CommandAdapter bridges typed commands back
//! onto the Frame-level stack.

use std::time::Duration;

use redis_tower::commands::*;
use redis_tower::metrics_layer::{ErrorKind, MetricsLayer, MetricsRecorder};
use redis_tower::{CommandAdapter, FrameService, TracingLayer};
use tower::ServiceBuilder;
use tower_service::Service;

/// A simple recorder that prints metrics to stdout.
struct PrintRecorder;

impl MetricsRecorder for PrintRecorder {
    fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
        match error {
            None => println!("  {command} took {duration:?} (ok)"),
            Some(kind) => println!("  {command} took {duration:?} (error: {kind:?})"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let frame_svc = FrameService::connect("127.0.0.1:6379").await?;

    // Stack TracingLayer and MetricsLayer onto the raw Frame service.
    let layered = ServiceBuilder::new()
        .layer(TracingLayer::new())
        .layer(MetricsLayer::new(PrintRecorder))
        .service(frame_svc);

    // Wrap with CommandAdapter so we can send typed commands.
    let mut svc = CommandAdapter::new(layered);

    // Use the composed service.
    let _: Option<bytes::Bytes> = svc.call(Set::new("mw:key", "layered")).await?;
    let val: Option<bytes::Bytes> = svc.call(Get::new("mw:key")).await?;
    println!("Got: {val:?}");
    let _: i64 = svc.call(Del::new("mw:key")).await?;

    Ok(())
}
