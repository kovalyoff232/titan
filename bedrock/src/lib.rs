pub mod arc_buffer_pool;

pub mod btree;

pub mod buffer_pool;

pub mod failpoint;

pub mod lock_manager;

pub mod page;

pub mod pager;

pub mod transaction;

pub mod wal;

pub const PAGE_SIZE: usize = 8192;

pub type PageId = u32;

pub type TupleId = (PageId, u16);

pub use buffer_pool::PageGuard;
pub use page::Page;

pub fn debug_logs_enabled() -> bool {
    std::env::var_os("TITAN_DEBUG_LOG").is_some()
}

#[macro_export]
macro_rules! bedrock_debug_log {
    ($($arg:tt)*) => {
        if $crate::debug_logs_enabled() {
            println!($($arg)*);
        }
    };
}
