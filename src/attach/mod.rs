//! The pieces `fed host` and `fed attach` share.
//!
//! - [`launch`]: the JSON `fed start` writes to a host's stdin, and the
//!   events the host writes back.
//! - [`protocol`]: the frames that travel over the attach socket.
//! - [`detach`]: the key sequence that ends an attach session.
//! - [`scrollback`]: the recent output a client receives when it connects.

pub mod detach;
pub mod launch;
pub mod protocol;
pub mod scrollback;
