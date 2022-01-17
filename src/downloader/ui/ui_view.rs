pub trait UIView: Send {
    fn draw(&self) -> anyhow::Result<()>;
}
