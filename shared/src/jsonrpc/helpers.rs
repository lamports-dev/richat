use {
    http_body_util::{combinators::BoxBody, BodyExt, Full as BodyFull},
    hyper::{body::Bytes, header::CONTENT_TYPE, http::Result as HttpResult, HeaderMap, StatusCode},
    jsonrpsee_types::{
        ErrorCode, ErrorObject, ErrorObjectOwned, Id, Response, ResponsePayload, TwoPointZero,
    },
    serde::Serialize,
    solana_rpc_client_api::custom_error::RpcCustomError,
    std::{fmt, sync::Arc},
};

pub const X_SUBSCRIPTION_ID: &str = "x-subscription-id";
pub const X_BIGTABLE: &str = "x-bigtable"; // https://github.com/anza-xyz/agave/blob/v2.2.10/rpc/src/rpc_service.rs#L554

pub type RpcResponse = hyper::Response<BoxBody<Bytes, std::convert::Infallible>>;

pub fn get_x_subscription_id(headers: &HeaderMap) -> Arc<str> {
    headers
        .get(X_SUBSCRIPTION_ID)
        .and_then(|value| value.to_str().ok().map(ToOwned::to_owned))
        .unwrap_or_default()
        .into()
}

pub fn get_x_bigtable_disabled(headers: &HeaderMap) -> bool {
    headers.get(X_BIGTABLE).is_some_and(|v| v == "disabled")
}

pub fn response_200<D: Into<Bytes>>(data: D, extra_headers: &HeaderMap) -> HttpResult<RpcResponse> {
    let mut builder =
        hyper::Response::builder().header(CONTENT_TYPE, "application/json; charset=utf-8");
    for (key, value) in extra_headers {
        builder = builder.header(key, value);
    }
    builder.body(BodyFull::from(data.into()).boxed())
}

pub fn response_400<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(format!("{error}\n").boxed())
}

pub fn response_500<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(format!("{error}\n").boxed())
}

pub fn jsonrpc_response_success<T: Clone + Serialize>(id: Id<'_>, payload: T) -> Vec<u8> {
    to_vec(&Response {
        jsonrpc: Some(TwoPointZero),
        payload: ResponsePayload::success(payload),
        id,
    })
}

pub fn jsonrpc_response_error(id: Id<'_>, error: ErrorObjectOwned) -> Vec<u8> {
    to_vec(&Response {
        jsonrpc: Some(TwoPointZero),
        payload: ResponsePayload::<()>::error(error),
        id,
    })
}

pub fn jsonrpc_response_error_custom(id: Id<'_>, error: RpcCustomError) -> Vec<u8> {
    let error = jsonrpc_core::Error::from(error);
    jsonrpc_response_error(
        id,
        ErrorObject::owned(error.code.code() as i32, error.message, error.data),
    )
}

pub fn jsonrpc_error_invalid_params<S: Serialize>(
    message: impl Into<String>,
    data: Option<S>,
) -> ErrorObjectOwned {
    ErrorObject::owned(ErrorCode::InvalidParams.code(), message, data)
}

pub fn to_vec<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("json serialization never fail")
}
