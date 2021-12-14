use tokio::task::JoinHandle;

unsafe fn extend_lifetime<'f, T, R>(
    func: Box<dyn Fn(T) -> R + Send + 'f>,
) -> Box<dyn Fn(T) -> R + Send + 'static> {
    std::mem::transmute::<_, Box<dyn Fn(T) -> R + Send + 'static>>(func)
}

pub async fn map_parallel<'f, T, R, F>(items: Vec<T>, func: F) -> Vec<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Copy + 'f,
{
    let handles = Vec::<JoinHandle<R>>::from_iter(items.into_iter().map(|item| {
        // SAFETY: task_func is only used within the task,
        // and the task is joined at the end of this async function,
        // at which point func is still alive.
        let task_func = unsafe { extend_lifetime(Box::new(func)) };
        tokio::spawn(async move { task_func(item) })
    }));

    let mut results = Vec::<R>::new();
    for handle in handles {
        results.push(handle.await.unwrap())
    }
    results
}
