//! Verification-only gRPC peer with the real request broker and no domain data or providers.
use nakode_protocol::{ErrorCode, ServiceCapabilities, ServiceError};
use nakode_sdk::telemetry::{self, opentelemetry::trace::FutureExt};
use nakode_server::{ServerEndpoint, ServerRequest};
use std::{collections::HashMap, env, error::Error};
use tokio_stream::wrappers::UnixListenerStream;

fn main() -> Result<(), Box<dyn Error>> {
    let provider = telemetry::init(
        "nakode",
        env::var("FSTACK_TRACE_ENDPOINT")?,
        HashMap::from([
            (
                "authorization".to_owned(),
                env::var("FSTACK_TRACE_AUTHORIZATION")?,
            ),
            ("x-fstack-relay-telemetry".to_owned(), "1".to_owned()),
            ("x-fstack-relay-service".to_owned(), "nakode".to_owned()),
        ]),
    )?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let socket = env::var("FSTACK_TRACE_FIXTURE_SOCKET")?;
            let listener = tokio::net::UnixListener::bind(socket)?;
            let (endpoint, mut requests) =
                ServerEndpoint::channel("tracing-verification", ServiceCapabilities::default(), 32);
            tokio::spawn(async move {
                while let Some(request) = requests.recv().await {
                    let context = request.trace_context();
                    telemetry::operation("nakode.execute", async move {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        let error = ServiceError {
                            code: ErrorCode::NotFound,
                            message: "Verification session does not exist".into(),
                            retryable: false,
                        };
                        match request {
                            ServerRequest::Query { respond, .. } => {
                                let _ = respond.send(Err(error));
                            }
                            ServerRequest::Command { respond, .. } => {
                                let _ = respond.send(Err(error));
                            }
                            ServerRequest::Subscribe { respond, .. } => {
                                let _ = respond.send(Err(error));
                            }
                        }
                    })
                    .with_context(context)
                    .await;
                }
            });
            tonic::transport::Server::builder()
                .layer(telemetry::RpcLayer(false))
                .add_service(nakode_server::grpc::GrpcService::new(endpoint).into_server())
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await?;
            Ok::<_, Box<dyn Error>>(())
        })?;
    provider.shutdown()?;
    Ok(())
}
