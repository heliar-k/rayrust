//! Error types for rayrust.

use std::ffi::NulError;
use std::str::Utf8Error;

use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RayError {
    #[error("Ray runtime error: {0}")]
    Runtime(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("FFI error: {0}")]
    Ffi(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Actor error: {0}")]
    Actor(String),

    #[error("Task error: {0}")]
    Task(String),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Unreconstructable object: {0}")]
    Unreconstructable(String),

    #[error("Timed out: {0}")]
    Timeout(String),

    #[error("Null byte in string")]
    NullByte(#[from] NulError),

    #[error("UTF-8 conversion error")]
    Utf8(#[from] Utf8Error),
}

impl From<rmp_serde::encode::Error> for RayError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        RayError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for RayError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        RayError::Serialization(e.to_string())
    }
}
