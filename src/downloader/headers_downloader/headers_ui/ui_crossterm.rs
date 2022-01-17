use super::{
    average_delta_counter::AverageDeltaCounter,
    headers::header_slices::{HeaderSliceStatus, HeaderSlices},
    ui_view::UIView,
};
use crossterm::{cursor, style, terminal, QueueableCommand};
use std::{
    cell::RefCell,
    io::{stdout, Write},
    sync::Arc,
};

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
        let statuses = self.header_slices.clone_statuses();

        // speed
        let mut speed_counter = self.speed_counter.borrow_mut();
        speed_counter.update(min_block_num.0);
        let speed = speed_counter.average();

        let mut stdout = stdout();

        // save the logging position
        stdout.queue(cursor::SavePosition {})?;

        // draw at the top of the window
        stdout.queue(cursor::MoveTo(0, 0))?;
        stdout.queue(terminal::EnableLineWrap {})?;

        // overall progress
        let progress_desc = std::format!(
            "{} headers {} - {} of {} at {} blk/sec ...",
            phase_name,
            min_block_num.0,
            max_block_num.0,
            final_block_num.0,
            speed,
        );
        stdout.queue(style::Print(progress_desc))?;
        stdout.queue(terminal::Clear(terminal::ClearType::UntilNewLine))?;
        stdout.queue(cursor::MoveToNextLine(1))?;

        // slice statuses
        for status in statuses {
            let c = char::from(status);
            stdout.queue(style::Print(c))?;
        }
        stdout.queue(terminal::Clear(terminal::ClearType::UntilNewLine))?;
        stdout.queue(cursor::MoveToNextLine(1))?;

        // counters
        stdout.queue(style::Print(format_counters(counters)))?;
        stdout.queue(terminal::Clear(terminal::ClearType::UntilNewLine))?;
        stdout.queue(cursor::MoveToNextLine(1))?;

        // delimiter line
        stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine))?;
        stdout.queue(style::Print("\n"))?;

        stdout.queue(cursor::RestorePosition {})?;
        stdout.flush()?;

        Ok(())
    }
}

fn format_counters(counters: Vec<(HeaderSliceStatus, usize)>) -> String {
    let mut line = String::new();
    for (status, count) in counters {
        line.push_str(std::format!("({}): {}; ", char::from(status), count).as_str());
    }
    line
}
