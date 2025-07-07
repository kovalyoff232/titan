//! The Pager is responsible for reading and writing pages to the database file.
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::{Page, PageId, PAGE_SIZE};

/// The Pager is responsible for reading and writing pages to the database file.
pub struct Pager {
    file: File,
    pub num_pages: u32,
}

impl Pager {
    /// Opens the database file, creating it and its parent directories if they don't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_ref = path.as_ref();
        println!("[Pager::open] Opening database file at: {:?}", path_ref);
        if let Some(parent) = path_ref.parent() {
            create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path_ref)?;

        let file_size = file.metadata()?.len();
        let num_pages = (file_size / PAGE_SIZE as u64) as u32;
        println!(
            "[Pager::open] File size: {}, initial num_pages: {}",
            file_size, num_pages
        );

        Ok(Self { file, num_pages })
    }

    /// Reads a page from the database file. If the page is beyond the end of the file,
    /// it returns a new, empty page.
    pub fn read_page(&mut self, page_id: PageId) -> io::Result<Page> {
        println!("[Pager::read_page] Reading page_id: {}", page_id);
        let mut page = Page::new(page_id); // It already initializes the header
        if page_id >= self.num_pages {
            println!(
                "[Pager::read_page] Page {} is new, returning initialized page.",
                page_id
            );
            return Ok(page);
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let bytes_read = self.file.read(&mut page.data)?;
        if bytes_read == 0 {
            // This case can happen if the file was extended but not written to yet.
            // The page is already initialized by Page::new, so we are good.
            println!(
                "[Pager::read_page] Read 0 bytes for page {}, using fresh initialized page.",
                page_id
            );
        } else if bytes_read < PAGE_SIZE {
            println!(
                "[Pager::read_page] Read {} bytes (less than page size), zeroing rest.",
                bytes_read
            );
            for i in bytes_read..PAGE_SIZE {
                page.data[i] = 0;
            }
        } else {
            println!("[Pager::read_page] Successfully read full page {}", page_id);
        }

        Ok(page)
    }

    /// Writes a page to the database file.
    pub fn write_page(&mut self, page: &Page) -> io::Result<()> {
        println!("[Pager::write_page] Writing page_id: {}", page.id);
        let offset = page.id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.sync_all()?;
        if page.id >= self.num_pages {
            self.num_pages = page.id + 1;
            println!(
                "[Pager::write_page] Increased num_pages to {}",
                self.num_pages
            );
        }
        Ok(())
    }

    /// Allocates a new page in the database file and returns its PageId.
    pub fn allocate_page(&mut self) -> io::Result<PageId> {
        let page_id = self.num_pages;
        self.num_pages += 1;
        println!(
            "[Pager::allocate_page] Allocating new page_id: {}. New num_pages: {}",
            page_id, self.num_pages
        );
        Ok(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_pager() {
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let mut pager = Pager::open(temp_path.to_str().unwrap()).unwrap();

        let page_id = pager.allocate_page().unwrap();
        assert_eq!(page_id, 0);

        let mut page = pager.read_page(page_id).unwrap();
        let mut header = page.read_header();
        header.lsn = 123;
        page.write_header(&header);
        pager.write_page(&page).unwrap();

        // Re-open the pager and check if the page is there
        drop(pager);
        let mut pager = Pager::open(temp_path.to_str().unwrap()).unwrap();
        let page = pager.read_page(page_id).unwrap();
        let header = page.read_header();
        assert_eq!(header.lsn, 123);
    }
}
