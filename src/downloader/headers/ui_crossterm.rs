use crate::downloader::{
    headers::header_slices::{HeaderSliceStatus, HeaderSlices},
    ui_view::UIView,
};
use crossterm::{cursor, style, terminal, QueueableCommand};
use std::{
    io::{stdout, Write},
    sync::Arc,
};

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
        let statuses = self.header_slices.clone_statuses();
        let counters = self.header_slices.status_counters();
        let min_block_num = self.header_slices.min_block_num();
        let max_block_num = self.header_slices.max_block_num();
        let final_block_num = self.header_slices.final_block_num();

        let mut stdout = stdout();

        // save the logging position
        stdout.queue(cursor::SavePosition {})?;

        // draw at the top of the window
        stdout.queue(cursor::MoveTo(0, 0))?;
        stdout.queue(terminal::EnableLineWrap {})?;

        // overall progress
        let progress_desc = std::format!(
            "downloading headers {} - {} of {} ...",
            min_block_num.0,
            max_block_num.0,
            final_block_num.0,
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

impl From<HeaderSliceStatus> for char {
    fn from(status: HeaderSliceStatus) -> Self {
        match status {
            HeaderSliceStatus::Empty => '-',
            HeaderSliceStatus::Waiting => '<',
            HeaderSliceStatus::Downloaded => '.',
            HeaderSliceStatus::Verified => '#',
            HeaderSliceStatus::Saved => '+',
        }
    }
}
