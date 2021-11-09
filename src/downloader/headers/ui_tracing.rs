use crate::downloader::{
    headers::header_slices::{HeaderSliceStatus, HeaderSlices},
    ui_view::UIView,
};
use std::sync::Arc;
use tracing::*;

pub struct HeaderSlicesView {
    header_slices: Arc<HeaderSlices>,
}

impl HeaderSlicesView {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self { header_slices }
    }
}

impl UIView for HeaderSlicesView {
    fn draw(&self) -> anyhow::Result<()> {
        let min_block_num = self.header_slices.min_block_num();
        let max_block_num = self.header_slices.max_block_num();
        let final_block_num = self.header_slices.final_block_num();
        let counters = self.header_slices.status_counters();

        // overall progress
        info!(
            "downloading headers {} - {} of {} ...",
            min_block_num.0, max_block_num.0, final_block_num.0,
        );

        // counters
        let counters_str = format_counters(counters);
        debug!("{}", counters_str);

        Ok(())
    }
}

fn format_counters(counters: Vec<(HeaderSliceStatus, usize)>) -> String {
    let mut line = String::new();
    for (status, count) in counters {
        line.push_str(std::format!("{}: {}; ", status, count).as_str());
    }
    line
}
