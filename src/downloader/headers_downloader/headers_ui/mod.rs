mod average_delta_counter;

use super::{super::ui::ui_view, headers};

#[cfg(feature = "crossterm")]
mod ui_crossterm;
#[cfg(feature = "crossterm")]
pub use ui_crossterm::HeaderSlicesView;

#[cfg(not(feature = "crossterm"))]
mod ui_tracing;
#[cfg(not(feature = "crossterm"))]
pub use ui_tracing::HeaderSlicesView;
