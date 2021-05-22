use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct AutoDupSortConfig {
    pub from: usize,
    pub to: usize,
}

#[derive(Deserialize)]
pub struct DupSortConfig {
    pub auto: Option<AutoDupSortConfig>,
}

#[derive(Deserialize)]
pub struct TableInfo {
    pub dup_sort: Option<DupSortConfig>,
}

pub static TABLES: Lazy<HashMap<String, TableInfo>> =
    Lazy::new(|| toml::from_str(include_str!("../db_tables.toml")).unwrap());
