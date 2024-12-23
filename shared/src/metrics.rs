use {
    crate::config::ConfigPrometheus,
    http_body_util::{combinators::BoxBody, BodyExt, Empty as BodyEmpty, Full as BodyFull},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        service::service_fn,
        Request, Response, StatusCode,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    prometheus::{proto::MetricFamily, TextEncoder},
    std::{convert::Infallible, future::Future},
    tokio::{net::TcpListener, task::JoinHandle},
    tracing::{error, info},
};

pub async fn spawn_server(
    ConfigPrometheus { endpoint }: ConfigPrometheus,
    gather_metrics: impl Fn() -> Vec<MetricFamily> + Clone + Send + 'static,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(endpoint).await?;
    info!("start server at: {endpoint}");

    Ok(tokio::spawn(async move {
        tokio::pin!(shutdown);
        loop {
            let stream = tokio::select! {
                maybe_conn = listener.accept() => {
                    match maybe_conn {
                        Ok((stream, _addr)) => stream,
                        Err(error) => {
                            error!("failed to accept new connection: {error}");
                            break;
                        }
                    }
                }
                () = &mut shutdown => {
                    info!("shutdown");
                    break
                },
            };
            let gather_metrics = gather_metrics.clone();
            tokio::spawn(async move {
                if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |req: Request<BodyIncoming>| {
                            let gather_metrics = gather_metrics.clone();
                            async move {
                                match req.uri().path() {
                                    "/metrics" => metrics_handler(&gather_metrics()),
                                    _ => not_found_handler(),
                                }
                            }
                        }),
                    )
                    .await
                {
                    error!("failed to handle request: {error}");
                }
            });
        }
    }))
}

fn metrics_handler(
    metric_families: &[MetricFamily],
) -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    let metrics = TextEncoder::new()
        .encode_to_string(metric_families)
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder()
        .status(StatusCode::OK)
        .body(BodyFull::new(Bytes::from(metrics)).boxed())
}

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
}
