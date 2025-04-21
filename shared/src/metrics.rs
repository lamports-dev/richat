use {
    crate::config::ConfigMetrics,
    http_body_util::{BodyExt, Full as BodyFull},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        service::service_fn,
        Request, Response, StatusCode,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    std::future::Future,
    tokio::{net::TcpListener, task::JoinError, time::Duration},
    tracing::{error, info},
};

#[inline]
pub fn duration_to_seconds(d: Duration) -> f64 {
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 1e9
}

pub async fn spawn_server(
    ConfigMetrics { endpoint }: ConfigMetrics,
    gather_metrics: impl Fn() -> Bytes + Clone + Send + 'static,
    is_health_check: impl Fn() -> bool + Clone + Send + 'static,
    is_ready_check: impl Fn() -> bool + Clone + Send + 'static,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<impl Future<Output = Result<(), JoinError>>> {
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
            let is_health_check = is_health_check.clone();
            let is_ready_check = is_ready_check.clone();
            tokio::spawn(async move {
                if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |req: Request<BodyIncoming>| {
                            let gather_metrics = gather_metrics.clone();
                            let is_health_check = is_health_check.clone();
                            let is_ready_check = is_ready_check.clone();
                            async move {
                                let (status, bytes) = match req.uri().path() {
                                    "/health" => {
                                        if is_health_check() {
                                            (StatusCode::OK, Bytes::from("OK"))
                                        } else {
                                            (
                                                StatusCode::INTERNAL_SERVER_ERROR,
                                                Bytes::from("Service is unhealthy"),
                                            )
                                        }
                                    }
                                    "/metrics" => (StatusCode::OK, gather_metrics()),
                                    "/ready" => {
                                        if is_ready_check() {
                                            (StatusCode::OK, Bytes::from("OK"))
                                        } else {
                                            (
                                                StatusCode::INTERNAL_SERVER_ERROR,
                                                Bytes::from("Service is not ready"),
                                            )
                                        }
                                    }
                                    _ => (StatusCode::NOT_FOUND, Bytes::new()),
                                };

                                Response::builder()
                                    .status(status)
                                    .body(BodyFull::new(bytes).boxed())
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
