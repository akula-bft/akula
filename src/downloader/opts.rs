use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(
        long = "downloader.headers-mem-limit",
        help = "How much memory in Mb to allocate for the active parallel download window.",
        default_value = "50"
    )]
    pub headers_mem_limit_mb: u32,
    #[structopt(
        long = "downloader.headers-batch-size",
        help = "How many headers to download per stage run.",
        default_value = "100000"
    )]
    pub headers_batch_size: usize,
}

impl Opts {
    pub fn headers_mem_limit(&self) -> usize {
        byte_unit::n_mib_bytes!(self.headers_mem_limit_mb as u128)
            .try_into()
            .unwrap_or(usize::MAX)
    }
}
