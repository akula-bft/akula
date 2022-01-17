use std::collections::VecDeque;

pub struct AverageDeltaCounter {
    window: VecDeque<u64>,
    current_value: Option<u64>,
    sum: u64,
}

impl AverageDeltaCounter {
    pub fn new(window_size: usize) -> Self {
        Self {
            window: VecDeque::<u64>::with_capacity(window_size),
            current_value: None,
            sum: 0,
        }
    }

    pub fn update(&mut self, value: u64) {
        let previous_value = self.current_value.unwrap_or(value);
        self.current_value = Some(value);

        if self.window.len() == self.window.capacity() {
            self.sum -= self.window.pop_front().unwrap();
        }

        let delta = value - previous_value;
        self.window.push_back(delta);
        self.sum += delta;
    }

    pub fn average(&self) -> u64 {
        let len = self.window.len() as u64;
        if len == 0 {
            0
        } else {
            self.sum / len
        }
    }
}
