use bearr::{
    btree::BTree,
    file_system::{FileId, FileSystem},
};

fn main() {
    BTree::pretty_print_pages(
        FileId {
            lsm_level: 0,
            sst_number: 0,
        },
        &FileSystem::new("test_files/database/basic", 16, 16).unwrap(),
    )
    .unwrap();
}
