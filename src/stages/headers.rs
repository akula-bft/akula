use crate::{
    consensus::Consensus,
    models::BlockNumber,
    p2p::{collections::Graph, node::Node},
};
use std::{sync::Arc, time::Duration};

const HEADERS_UPPER_BOUND: usize = 1 << 10;
const STAGE_UPPER_BOUND: usize = 3 << 15;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

pub struct HeaderDownload {
    pub node: Arc<Node>,
    pub consensus: Arc<dyn Consensus>,
    pub max_block: BlockNumber,
    pub graph: Graph,
}

impl HeaderDownload {}
