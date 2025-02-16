
use std::sync::Arc;
use std::sync::Mutex;

use env_logger;
use lazy_static::lazy_static;

lazy_static! {
    static ref LOG_SET_UP: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

/// Initialize logging once.
pub fn log_setup() {
    let mut log_setup = LOG_SET_UP.lock().unwrap();
    if !*log_setup {
        env_logger::builder()
            .format_timestamp_millis()
            .filter_level(log::LevelFilter::Info)
            .init();
        *log_setup = true;
    }
}
