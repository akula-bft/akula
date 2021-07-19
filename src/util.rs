#[cfg(test)]
pub mod test_util {
    use std::future::Future;
    use tokio::runtime::Builder;

    pub fn run_test<F: Future<Output = ()> + Send + 'static>(f: F) {
        Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(32 * 1024 * 1024)
            .build()
            .unwrap()
            .block_on(async move { tokio::spawn(f).await.unwrap() })
    }
}
