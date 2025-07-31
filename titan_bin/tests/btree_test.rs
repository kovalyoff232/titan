use bedrock::btree::{btree_delete, btree_insert, btree_search};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::pager::Pager;
use bedrock::transaction::TransactionManager;
use bedrock::wal::WalManager;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[test]
fn test_btree_page_deallocation() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let wal_path = temp_dir.path().join("test.wal");

    let pager = Pager::open(&db_path).unwrap();
    let bpm = Arc::new(BufferPoolManager::new(pager));
    let tm = Arc::new(TransactionManager::new(1));
    let wm = Arc::new(Mutex::new(WalManager::open(&wal_path).unwrap()));

    let tx_id = tm.begin();

    // Create a new B-Tree
    let root_page_guard = bpm.new_page().unwrap();
    let mut root_page = root_page_guard.write();
    root_page.as_btree_leaf_page();
    let root_page_id = root_page.id;
    drop(root_page);

    // Insert enough keys to cause a split
    let mut current_root_id = root_page_id;
    for i in 0..100 {
        current_root_id =
            btree_insert(&bpm, &tm, &wm, tx_id, current_root_id, i, (i as u32, i as u16)).unwrap();
    }

    // Delete all keys, which should cause merges and deallocations
    for i in 0..100 {
        current_root_id = btree_delete(&bpm, &tm, &wm, tx_id, current_root_id, i).unwrap();
    }

    // After all deletions, the tree should be empty, and all pages except the root should be deallocated.
    let final_page_count = bpm.pager.lock().unwrap().num_pages;
    assert_eq!(
        final_page_count, 1,
        "All pages except the root should be deallocated"
    );
}