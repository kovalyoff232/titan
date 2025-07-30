//! # Bedrock Storage Engine
//! The Storage Engine for the Titan Database.
//! This crate is responsible for managing the on-disk and in-memory representation of data.

/// The B-Tree implementation.
pub mod btree;
/// The buffer pool manager.
pub mod buffer_pool;
/// The lock manager for concurrency control.
pub mod lock_manager;
/// The page layout and data structures.
pub mod page;
/// The pager for reading and writing pages to disk.
pub mod pager;
/// The transaction manager.
pub mod transaction;
/// The Write-Ahead Log for recovery.
pub mod wal;

/// The size of a single page in bytes.
pub const PAGE_SIZE: usize = 8192;

/// A unique identifier for a page in the database file.
/// The first page is page 0.
pub type PageId = u32;

/// A unique identifier for a tuple on a page.
pub type TupleId = (PageId, u16); // (page_id, item_id)

// The Page struct is now defined in the page module.
pub use page::Page;
pub use buffer_pool::PageGuard;
