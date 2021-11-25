use super::{
    average_delta_counter::AverageDeltaCounter,
    header_slices::{HeaderSliceStatus, HeaderSlices},
};
use crate::downloader::ui_view::UIView;
use std::{cell::RefCell, sync::Arc};
use tracing::*;

pub struct HeaderSlicesView {
    header_slices: Arc<HeaderSlices>,
    phase_name: String,
    speed_counter: RefCell<AverageDeltaCounter>,
}

impl HeaderSlicesView {
    pub fn new(header_slices: Arc<HeaderSlices>, phase_name: &str) -> Self {
        Self {
            header_slices,
            phase_name: String::from(phase_name),
            speed_counter: RefCell::new(AverageDeltaCounter::new(60)),
        }
    }
}

impl UIView for HeaderSlicesView {
    fn draw(&self) -> anyhow::Result<()> {
        let phase_name = &self.phase_name;
        let min_block_num = self.header_slices.min_block_num();
        let max_block_num = self.header_slices.max_block_num();
        let final_block_num = self.header_slices.final_block_num();
        let counters = self.header_slices.status_counters();

        // speed
        let mut speed_counter = self.speed_counter.borrow_mut();
        speed_counter.update(min_block_num.0);
        let speed = speed_counter.average();

        // overall progress
        info!(
            "{} headers {} - {} of {} at {} blk/sec ...",
            phase_name, min_block_num.0, max_block_num.0, final_block_num.0, speed,
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
