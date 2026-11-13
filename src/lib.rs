//! Versioned-object lifetime coordination.
//!
//! V0 is under development. Primitive/model tests are not evidence of a complete
//! cross-store protocol or permission to enable collection over existing data.
#![forbid(unsafe_code)]

pub mod claim;
pub mod clock;
pub mod identity;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

#[cfg(feature = "s3")]
pub mod s3;
