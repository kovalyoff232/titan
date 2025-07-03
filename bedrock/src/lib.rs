//! The Storage Engine for the Titan Database.
//!
//! This crate is responsible for managing the on-disk and in-memory representation of data.

pub mod btree;
pub mod buffer_pool;
pub mod page;
pub mod pager;
pub mod transaction;
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
