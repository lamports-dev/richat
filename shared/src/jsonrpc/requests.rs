use {
    crate::jsonrpc::{
        helpers::{
            get_x_bigtable_disabled, get_x_subscription_id, response_200, response_400,
            response_500, to_vec, RpcResponse,
        },
        metrics::{
            RPC_REQUESTS_DURATION_SECONDS, RPC_REQUESTS_GENERATED_BYTES_TOTAL, RPC_REQUESTS_TOTAL,
        },
    },
    futures::{
        future::BoxFuture,
        stream::{FuturesOrdered, StreamExt},
    },
    http_body_util::{BodyExt, Limited},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        http::Result as HttpResult,
        HeaderMap,
    },
    jsonrpsee_types::{error::ErrorCode, Request, Response, ResponsePayload, TwoPointZero},
    metrics::{counter, histogram},
    quanta::Instant,
    richat_metrics::duration_to_seconds,
    std::{collections::HashMap, fmt, sync::Arc},
};

pub type RpcRequestResult = anyhow::Result<Vec<u8>>;

pub type RpcRequestHandler<S> =
    Box<dyn Fn(S, Arc<str>, bool, Request<'_>) -> BoxFuture<'_, RpcRequestResult> + Send + Sync>;

#[derive(Debug)]
enum RpcRequests<'a> {
    Single(Request<'a>),
    Batch(Vec<Request<'a>>),
}

impl<'a> RpcRequests<'a> {
    fn parse(bytes: &'a Bytes) -> serde_json::Result<Self> {
        for i in 0..bytes.len() {
            if bytes[i] == b'[' {
                return serde_json::from_slice::<Vec<Request<'_>>>(bytes).map(Self::Batch);
            } else if bytes[i] == b'{' {
                break;
            }
        }
        serde_json::from_slice::<Request<'_>>(bytes).map(Self::Single)
    }
}

pub struct RpcRequestsProcessor<S> {
    body_limit: usize,
    state: S,
    extra_headers: HeaderMap,
    methods: HashMap<&'static str, RpcRequestHandler<S>>,
}

impl<S> fmt::Debug for RpcRequestsProcessor<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcRequestsProcessor").finish()
    }
}

impl<S: Clone> RpcRequestsProcessor<S> {
    pub fn new(body_limit: usize, state: S, extra_headers: HeaderMap) -> Self {
        Self {
            body_limit,
            state,
            extra_headers,
            methods: HashMap::new(),
        }
    }

    pub fn add_handler(
        &mut self,
        method: &'static str,
        handler: RpcRequestHandler<S>,
    ) -> &mut Self {
        self.methods.insert(method, handler);
        self
    }

    pub async fn on_request(&self, req: hyper::Request<BodyIncoming>) -> HttpResult<RpcResponse> {
        let (parts, body) = req.into_parts();

        let x_subscription_id = get_x_subscription_id(&parts.headers);
        let upstream_disabled = get_x_bigtable_disabled(&parts.headers);

        let bytes = match Limited::new(body, self.body_limit).collect().await {
            Ok(body) => body.to_bytes(),
            Err(error) => return response_400(error),
        };
        let requests = match RpcRequests::parse(&bytes) {
            Ok(requests) => requests,
            Err(error) => return response_400(error),
        };

        let mut buffer = match requests {
            RpcRequests::Single(request) => {
                match self
                    .process(Arc::clone(&x_subscription_id), upstream_disabled, request)
                    .await
                {
                    Ok(response) => response,
                    Err(error) => return response_500(error),
                }
            }
            RpcRequests::Batch(requests) => {
                let mut futures = FuturesOrdered::new();
                for request in requests {
                    let x_subscription_id = Arc::clone(&x_subscription_id);
                    futures.push_back(self.process(
                        Arc::clone(&x_subscription_id),
                        upstream_disabled,
                        request,
                    ));
                }

                let mut buffer = Vec::new();
                buffer.push(b'[');
                while let Some(result) = futures.next().await {
                    match result {
                        Ok(mut response) => {
                            buffer.append(&mut response);
                        }
                        Err(error) => return response_500(error),
                    }
                    if !futures.is_empty() {
                        buffer.push(b',');
                    }
                }
                buffer.push(b']');
                buffer
            }
        };
        buffer.push(b'\n');
        counter!(
            RPC_REQUESTS_GENERATED_BYTES_TOTAL,
            "x_subscription_id" => x_subscription_id,
        )
        .increment(buffer.len() as u64);
        response_200(buffer, &self.extra_headers)
    }

    async fn process<'a>(
        &'a self,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'a>,
    ) -> anyhow::Result<Vec<u8>> {
        let Some((method, handle)) = self.methods.get_key_value(request.method.as_ref()) else {
            return Ok(to_vec(&Response {
                jsonrpc: Some(TwoPointZero),
                payload: ResponsePayload::<()>::error(ErrorCode::MethodNotFound),
                id: request.id.into_owned(),
            }));
        };

        let ts = Instant::now();
        let result = handle(
            self.state.clone(),
            Arc::clone(&x_subscription_id),
            upstream_disabled,
            request,
        )
        .await;
        counter!(
            RPC_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => *method,
        )
        .increment(1);
        histogram!(
            RPC_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => x_subscription_id,
            "method" => *method,
        )
        .record(duration_to_seconds(ts.elapsed()));
        result
    }
}
