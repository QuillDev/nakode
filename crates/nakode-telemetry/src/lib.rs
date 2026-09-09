//! Bounded OpenTelemetry export and request context shared by native clients and services.
use http::{HeaderMap, Request, Response};
pub use opentelemetry;
use opentelemetry::{
    Context, KeyValue, global,
    propagation::{Extractor, Injector},
    trace::{FutureExt, SpanKind, TraceContextExt, Tracer},
};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider},
};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tower::{Layer, Service};

pub struct Headers<'a>(pub &'a HeaderMap);
impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}
struct Inject<'a>(&'a mut HeaderMap);
impl Injector for Inject<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (http::HeaderName::from_bytes(key.as_bytes()), value.parse())
        {
            self.0.insert(key, value);
        }
    }
}

/// Keeps an SDK span alive through cancellation; dropping the owner always ends it.
#[derive(Debug)]
pub struct Scope {
    context: Context,
}
impl Scope {
    #[must_use]
    pub fn new(
        name: impl Into<std::borrow::Cow<'static, str>>,
        kind: SpanKind,
        parent: &Context,
    ) -> Self {
        let tracer = global::tracer("nakode-requests");
        let span = tracer
            .span_builder(name)
            .with_kind(kind)
            .start_with_context(&tracer, parent);
        Self {
            context: parent.with_span(span),
        }
    }
    #[must_use]
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    pub fn attribute(&self, key: &'static str, value: impl Into<opentelemetry::Value>) {
        self.context.span().set_attribute(KeyValue::new(key, value));
    }
    pub fn error(&self) {
        self.context
            .span()
            .set_status(opentelemetry::trace::Status::error("request failed"));
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        self.context.span().end();
    }
}

pub async fn operation<T>(name: &'static str, future: impl Future<Output = T>) -> T {
    let scope = Scope::new(name, SpanKind::Internal, &Context::current());
    future.with_context(scope.context()).await
}

/// No exporter is installed until the supervisor explicitly configures one.
///
/// # Errors
/// Returns an error when the HTTP exporter cannot be configured.
pub fn init<S: std::hash::BuildHasher>(
    service: &'static str,
    endpoint: String,
    headers: HashMap<String, String, S>,
) -> Result<SdkTracerProvider, Box<dyn std::error::Error>> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(endpoint)
        .with_headers(headers.into_iter().collect())
        .with_timeout(Duration::from_secs(2))
        .build()?;
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(512)
                .with_max_export_batch_size(64)
                .with_scheduled_delay(Duration::from_secs(1))
                .build(),
        )
        .build();
    let provider = SdkTracerProvider::builder()
        .with_resource(
            Resource::builder_empty()
                .with_attribute(KeyValue::new("service.name", service))
                .build(),
        )
        .with_span_processor(processor)
        .build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());
    Ok(provider)
}

#[derive(Clone, Copy)]
pub struct RpcLayer(pub bool);
impl<S> Layer<S> for RpcLayer {
    type Service = RpcService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RpcService {
            inner,
            client: self.0,
        }
    }
}
#[derive(Clone)]
pub struct RpcService<S> {
    inner: S,
    client: bool,
}
impl<S, B> Service<Request<B>> for RpcService<S>
where
    S: Service<Request<B>, Response = Response<tonic::body::Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut request: Request<B>) -> Self::Future {
        let path = request.uri().path();
        // gRPC paths are fixed service/method names; never export arbitrary URI components.
        let name = if include_str!("rpc-methods.txt")
            .lines()
            .any(|known| known == path)
        {
            format!("rpc {path}")
        } else {
            "rpc request".to_owned()
        };
        let parent = if self.client {
            Context::current()
        } else {
            global::get_text_map_propagator(|p| p.extract(&Headers(request.headers())))
        };
        let scope = Scope::new(
            name,
            if self.client {
                SpanKind::Client
            } else {
                SpanKind::Server
            },
            &parent,
        );
        scope.attribute(
            "request.long_poll",
            path.rsplit('/')
                .next()
                .is_some_and(|method| method.starts_with("Watch")),
        );
        let context = scope.context();
        if self.client {
            global::get_text_map_propagator(|p| {
                p.inject_context(&context, &mut Inject(request.headers_mut()));
            });
        }
        let future = {
            let _guard = context.clone().attach();
            self.inner.call(request)
        };
        Box::pin(async move {
            let response = match future.with_context(context).await {
                Ok(response) => response,
                Err(error) => {
                    scope.error();
                    return Err(error);
                }
            };
            record_status(&scope, response.headers());
            Ok(response.map(|body| {
                tonic::body::Body::new(TracedBody {
                    inner: body,
                    scope: Some(scope),
                })
            }))
        })
    }
}
fn record_status(scope: &Scope, headers: &HeaderMap) {
    if let Some(code) = headers
        .get("grpc-status")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
    {
        scope.attribute("rpc.grpc.status_code", code);
        if code != 0 {
            scope.error();
        }
    }
}
pin_project_lite::pin_project! {
    struct TracedBody { #[pin] inner: tonic::body::Body, scope: Option<Scope> }
}
impl http_body::Body for TracedBody {
    type Data = <tonic::body::Body as http_body::Body>::Data;
    type Error = <tonic::body::Body as http_body::Body>::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let _guard = this.scope.as_ref().map(|scope| scope.context().attach());
        let frame = this.inner.poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(scope), Some(headers)) = (this.scope.as_ref(), frame.trailers_ref()) {
                    record_status(scope, headers);
                }
            }
            Poll::Ready(Some(Err(_))) => {
                if let Some(scope) = this.scope.as_ref() {
                    scope.error();
                }
            }
            Poll::Ready(None) => {
                this.scope.take();
            }
            Poll::Pending => {}
        }
        frame
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn rpc_name_allowlist_matches_the_public_proto() {
        let mut service = "";
        let mut methods = Vec::new();
        for line in include_str!("../../../proto/nakode/v1/nakode.proto").lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("service ") {
                service = rest.split_whitespace().next().expect("service name");
            } else if let Some(rest) = line.strip_prefix("rpc ") {
                let method = rest.split('(').next().expect("method name").trim();
                methods.push(format!("/nakode.v1.{service}/{method}"));
            }
        }
        assert_eq!(
            methods,
            include_str!("rpc-methods.txt").lines().collect::<Vec<_>>()
        );
    }
}
