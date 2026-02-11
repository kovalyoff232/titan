pub mod arc_buffer_pool;

pub mod btree;

pub mod buffer_pool;

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
