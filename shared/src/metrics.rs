use {
    crate::config::ConfigPrometheus,
    http_body_util::{combinators::BoxBody, BodyExt, Empty as BodyEmpty},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        service::service_fn,
        Request, Response, StatusCode,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    std::{convert::Infallible, future::Future},
    tokio::{net::TcpListener, task::JoinHandle},
    tracing::{error, info},
};

pub async fn spawn_server(
    ConfigPrometheus { endpoint }: ConfigPrometheus,
    metrics_handler: impl Fn() -> http::Result<Response<BoxBody<Bytes, Infallible>>>
        + Clone
        + Send
        + 'static,
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
            let metrics_handler = metrics_handler.clone();
            tokio::spawn(async move {
                if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |req: Request<BodyIncoming>| {
                            let metrics_handler = metrics_handler.clone();
                            async move {
                                match req.uri().path() {
                                    "/metrics" => metrics_handler(),
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

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
}
