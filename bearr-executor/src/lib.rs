mod executor;
mod io;
mod sync;

// TODO: these should actually be db operations and not include something as raw as a file read
// The read function that we use internally should return the values themselves not wrapped in this enum
// Only the main Db API should return these
pub enum DbRequest {
    Read {
        file: io_uring::types::Fd,
        offset: u64,
        num_bytes: u32,
        buffer: Box<[u8]>,
    },
}

pub enum DbResponse {
    ReadResult(Result<(Box<[u8]>, usize), (Box<[u8]>, std::io::Error)>),
}
