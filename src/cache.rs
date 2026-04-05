use arbhx::{DataMode, Operator};
use std::collections::HashMap;
use std::io;
use std::sync::Mutex;

#[derive(Debug)]
pub struct DataCache {
    cache: Mutex<HashMap<DataMode, Operator>>,
}

impl DataCache {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_data(&mut self, mode: DataMode) -> io::Result<Operator> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(op) = cache.get(&mode) {
                return Ok(op.clone());
            }
        }

        let op = Operator::with_info(mode.clone())?;
        {
            let mut cache = self.cache.lock().unwrap();
            cache.insert(mode, op.clone());
        }
        Ok(op)
    }
}
