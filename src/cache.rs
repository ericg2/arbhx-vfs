use arbhx::{DataMode, Operator};
use moka::future::Cache;
use std::io;
use std::time::Duration;

#[derive(Debug)]
pub struct DataCache {
    pub cache: Cache<DataMode, Operator>,
}

impl DataCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(1000)
                .time_to_idle(Duration::from_mins(5))
                .build(),
        }
    }
    pub async fn get_data(&self, mode: DataMode) -> io::Result<Operator> {
        match self.cache.get(&mode).await {
            Some(x) => Ok(x),
            None => {
                let op = Operator::with_info(mode.clone())?;
                self.cache.insert(mode, op.clone()).await;
                Ok(op)
            }
        }
    }
}
