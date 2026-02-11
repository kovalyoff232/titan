use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::{PAGE_SIZE, Page, PageId};

pub struct Pager {
    file: File,
    pub num_pages: u32,
}

impl Pager {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_ref = path.as_ref();
        crate::bedrock_debug_log!("[Pager::open] Opening database file at: {path_ref:?}");
        if let Some(parent) = path_ref.parent() {
            create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_ref)?;

        let file_size = file.metadata()?.len();
        let num_pages = (file_size / PAGE_SIZE as u64) as u32;
        crate::bedrock_debug_log!(
            "[Pager::open] File size: {file_size}, initial num_pages: {num_pages}"
        );

        Ok(Self { file, num_pages })
    }

    pub fn read_page(&mut self, page_id: PageId) -> io::Result<Page> {
        crate::bedrock_debug_log!("[Pager::read_page] Reading page_id: {page_id}");
        let mut page = Page::new(page_id);
        if page_id >= self.num_pages {
            crate::bedrock_debug_log!(
                "[Pager::read_page] Page {page_id} is new, returning initialized page."
            );
            return Ok(page);
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let bytes_read = self.file.read(&mut page.data)?;
        if bytes_read == 0 {
            crate::bedrock_debug_log!(
                "[Pager::read_page] Read 0 bytes for page {page_id}, using fresh initialized page."
            );
        } else if bytes_read < PAGE_SIZE {
            crate::bedrock_debug_log!(
                "[Pager::read_page] Read {bytes_read} bytes (less than page size), zeroing rest."
            );
            for i in bytes_read..PAGE_SIZE {
                page.data[i] = 0;
            }
        } else {
            crate::bedrock_debug_log!("[Pager::read_page] Successfully read full page {page_id}");
        }

        Ok(page)
    }

    pub fn write_page(&mut self, page: &Page) -> io::Result<()> {
        crate::bedrock_debug_log!("[Pager::write_page] Writing page_id: {}", page.id);
        let offset = page.id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.sync_all()?;
        if page.id >= self.num_pages {
            self.num_pages = page.id + 1;
            crate::bedrock_debug_log!(
                "[Pager::write_page] Increased num_pages to {}",
                self.num_pages
            );
        }
        Ok(())
    }

    pub fn allocate_page(&mut self) -> io::Result<PageId> {
        let page_id = self.num_pages;
        self.num_pages += 1;
        crate::bedrock_debug_log!(
            "[Pager::allocate_page] Allocating new page_id: {page_id}. New num_pages: {}",
            self.num_pages
        );
        Ok(page_id)
    }

    pub fn deallocate_page(&mut self, _page_id: PageId) {}
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

        drop(pager);
        let mut pager = Pager::open(temp_path.to_str().unwrap()).unwrap();
        let page = pager.read_page(page_id).unwrap();
        let header = page.read_header();
        assert_eq!(header.lsn, 123);
    }
}
