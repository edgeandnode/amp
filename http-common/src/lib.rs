use std::fmt::Display;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

pub type BoxRequestError = Box<dyn RequestError>;

pub trait RequestError: Display + Send + Sync + 'static {
    fn error_code(&self) -> &'static str;
    fn status_code(&self) -> StatusCode;
}

impl IntoResponse for BoxRequestError {
    fn into_response(self) -> axum::response::Response {
        let res = json!({
            "error_code": self.error_code(),
            "error_message": self.to_string(),
        });

        (self.status_code(), res.to_string()).into_response()
    }
}

impl<E: RequestError> From<E> for BoxRequestError {
    fn from(e: E) -> Self {
        Box::new(e)
    }
}
