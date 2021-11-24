mod average_delta_counter;
pub mod downloader;
mod downloader_linear;
mod downloader_preverified;
mod header_slice_status_watch;
pub mod header_slices;
pub mod stage;
mod stage_stream;

mod fetch_receive_stage;
mod fetch_request_stage;
mod header_slice_verifier;
mod penalize_stage;
mod preverified_hashes_config;
mod refill_stage;
mod retry_stage;
mod save_stage;
mod top_block_estimate_stage;
mod verify_stage_linear;
mod verify_stage_linear_link;
mod verify_stage_preverified;

#[cfg(feature = "crossterm")]
pub mod ui_crossterm;
#[cfg(feature = "crossterm")]
pub use ui_crossterm::HeaderSlicesView;

#[cfg(not(feature = "crossterm"))]
pub mod ui_tracing;

#[cfg(not(feature = "crossterm"))]
pub use ui_tracing::HeaderSlicesView;
