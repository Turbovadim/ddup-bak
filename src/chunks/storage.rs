use super::ChunkHash;
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

pub trait ChunkStorage: Send + Sync {
    #[inline]
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let hex = super::hex(chunk);
        let mut path = String::with_capacity(hex.len() + 8);
        path.push_str(&hex[..2]);
        path.push('/');
        path.push_str(&hex[2..4]);
        path.push('/');
        path.push_str(&hex[4..]);
        path.push_str(".chunk");
        PathBuf::from(path)
    }

    fn read_chunk_content(&self, chunk: &ChunkHash)
    -> std::io::Result<Box<dyn Read + Send + Sync>>;
    fn write_chunk_content(&self, chunk: &ChunkHash, content: &[u8]) -> std::io::Result<()>;
    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()>;
    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>>;

    fn has_chunk(&self, chunk: &ChunkHash) -> std::io::Result<bool> {
        let mut content = match self.read_chunk_content(chunk) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
        // A directory opens but can't be read; a chunk has at least its format byte.
        match content.read_exact(&mut [0u8; 1]) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Removes leftovers of interrupted writes. `clean` calls it while nothing writes chunks.
    fn remove_leftovers(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// Makes every chunk written so far durable. Backups call it before saving the index that
    /// counts them, so `write_chunk_content` need not sync each chunk. Required so a wrapper
    /// around another storage can't silently drop the inner one's sync.
    fn sync(&self) -> std::io::Result<()>;

    /// Makes one written chunk durable. Backups call it for a chunk the saved index already
    /// counts, which can't wait for `sync`. A wrapper should forward it: the default syncs
    /// everything.
    fn sync_chunk(&self, _chunk: &ChunkHash) -> std::io::Result<()> {
        self.sync()
    }
}

pub struct ChunkStorageLocal {
    path: PathBuf,
    durable: durable::State,
}

impl ChunkStorageLocal {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            durable: Default::default(),
        }
    }
}

impl ChunkStorage for ChunkStorageLocal {
    fn read_chunk_content(
        &self,
        chunk: &ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        Ok(Box::new(File::open(
            self.path.join(self.path_from_chunk(chunk)),
        )?))
    }

    fn write_chunk_content(&self, chunk: &ChunkHash, content: &[u8]) -> std::io::Result<()> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        // Replaces an existing file: one the index doesn't count may be torn by a crash before
        // `sync`.
        let path = self.path.join(self.path_from_chunk(chunk));
        let parent = path.parent().unwrap();
        std::fs::create_dir_all(parent)?;
        durable::begin(&self.path, &self.durable)?;

        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("{}.{unique}.tmp", std::process::id()));
        let mut file = match File::create_new(&tmp_path) {
            // Left by a dead process with the same pid.
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(&tmp_path)?;
                File::create_new(&tmp_path)?
            }
            file => file?,
        };

        if let Err(err) = file
            .write_all(content)
            .and_then(|()| durable::chunk(&self.durable, &file))
        {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        if let Err(err) = std::fs::rename(&tmp_path, &path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
        durable::wrote(&self.durable, path);
        Ok(())
    }

    fn sync(&self) -> std::io::Result<()> {
        durable::all(&self.path, &self.durable)
    }

    fn sync_chunk(&self, chunk: &ChunkHash) -> std::io::Result<()> {
        durable::one(
            &self.path,
            &self.durable,
            &self.path.join(self.path_from_chunk(chunk)),
        )
    }

    fn has_chunk(&self, chunk: &ChunkHash) -> std::io::Result<bool> {
        Ok(is_chunk_file(&self.path.join(self.path_from_chunk(chunk))))
    }

    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()> {
        let path = self.path.join(self.path_from_chunk(chunk));
        std::fs::remove_file(&path)?;

        for parent in path.ancestors().skip(1).take(2) {
            if std::fs::read_dir(parent)?.next().is_some() {
                break;
            }
            std::fs::remove_dir(parent)?;
        }

        Ok(())
    }

    fn remove_leftovers(&self) -> std::io::Result<()> {
        // Only our own names in hex-named shard directories, never through symlinks.
        let is_shard = |entry: &std::fs::DirEntry| -> std::io::Result<bool> {
            Ok(entry.file_type()?.is_dir()
                && entry.file_name().to_str().is_some_and(|name| {
                    name.len() == 2 && name.bytes().all(|b| b.is_ascii_hexdigit())
                }))
        };
        for level in std::fs::read_dir(&self.path)? {
            let level = level?;
            if !is_shard(&level)? {
                continue;
            }
            for dir in std::fs::read_dir(level.path())? {
                let dir = dir?;
                if !is_shard(&dir)? {
                    continue;
                }
                for file in std::fs::read_dir(dir.path())? {
                    let file = file?;
                    if file.file_type()?.is_file()
                        && file.file_name().to_str().is_some_and(is_chunk_temporary)
                    {
                        std::fs::remove_file(file.path())?;
                    }
                }
            }
        }
        Ok(())
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>> {
        let mut hashes = Vec::new();

        let entries = |path: PathBuf, dirs: bool| -> std::io::Result<Vec<std::fs::DirEntry>> {
            let entries = match std::fs::read_dir(path) {
                Ok(entries) => entries.collect::<Result<Vec<_>, _>>()?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(err) => return Err(err),
            };
            entries
                .into_iter()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir() == dirs))
                .map(Ok)
                .collect()
        };

        for first in entries(self.path.clone(), true)? {
            for second in entries(first.path(), true)? {
                for file in entries(second.path(), false)? {
                    let name = format!(
                        "{}{}{}",
                        first.file_name().to_string_lossy(),
                        second.file_name().to_string_lossy(),
                        file.file_name().to_string_lossy()
                    );
                    if let Some(hash) = parse_chunk_name(&name) {
                        hashes.push(hash);
                    }
                }
            }
        }

        Ok(hashes)
    }
}

fn parse_chunk_name(name: &str) -> Option<ChunkHash> {
    let hex = name.strip_suffix(".chunk")?;
    if hex.len() != 64 {
        return None;
    }

    let mut hash = [0; 32];
    for (byte, pair) in hash.iter_mut().zip(hex.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }

    Some(hash)
}

/// Chunk durability in four steps: `begin` before a chunk is written, `chunk` and `wrote` after,
/// and `all` once per backup. `one` does for a single chunk at `path` what `all` does for every
/// chunk.
#[cfg(unix)]
mod durable {
    use parking_lot::Mutex;
    use std::{
        collections::HashSet,
        fs::File,
        path::{Path, PathBuf},
    };

    #[derive(Default)]
    pub struct State {
        /// Directories that chunks were renamed into since the last `all`.
        dirs: Mutex<HashSet<PathBuf>>,
        #[cfg(target_os = "linux")]
        batch: batch::Batch,
    }

    pub fn begin(_dir: &Path, _state: &State) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        _state.batch.begin(_dir)?;
        Ok(())
    }

    pub fn chunk(_state: &State, file: &File) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        if _state.batch.active() {
            return Ok(());
        }
        sync(file)
    }

    pub fn wrote(state: &State, path: PathBuf) {
        let parent = path.parent().unwrap();
        let mut dirs = state.dirs.lock();
        if !dirs.contains(parent) {
            dirs.insert(parent.to_path_buf());
        }
        drop(dirs);
        #[cfg(target_os = "linux")]
        state.batch.wrote(path);
    }

    pub fn all(dir: &Path, state: &State) -> std::io::Result<()> {
        let dirs = std::mem::take(&mut *state.dirs.lock());
        #[cfg(target_os = "linux")]
        if state.batch.all()? {
            return Ok(());
        }
        sync_dirs(dirs.iter().map(PathBuf::as_path))?;
        flush(dir)
    }

    pub fn one(dir: &Path, _state: &State, path: &Path) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        if _state.batch.active() {
            sync_path(path)?;
        }
        sync_dirs(path.parent())?;
        flush(dir)
    }

    /// Syncs the directories and the two levels above each, which `write_chunk_content` may have
    /// created.
    fn sync_dirs<'a>(dirs: impl IntoIterator<Item = &'a Path>) -> std::io::Result<()> {
        let dirs: HashSet<&Path> = dirs
            .into_iter()
            .flat_map(|dir| dir.ancestors().take(3))
            .collect();
        dirs.into_iter().try_for_each(sync_path)
    }

    fn sync_path(path: &Path) -> std::io::Result<()> {
        match File::open(path) {
            Ok(file) => sync(&file),
            // `clean` may have deleted a chunk that a failed backup wrote.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Plain `fsync` on Apple hands the data to the drive without flushing its cache, which
    /// `sync_all` would do per call; `flush` does that once.
    fn sync(file: &File) -> std::io::Result<()> {
        #[cfg(target_vendor = "apple")]
        if unsafe { libc::fsync(std::os::fd::AsRawFd::as_raw_fd(file)) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        #[cfg(not(target_vendor = "apple"))]
        file.sync_all()?;
        Ok(())
    }

    /// F_FULLFSYNC flushes the drive's cache, which holds everything `sync` handed it.
    fn flush(_dir: &Path) -> std::io::Result<()> {
        #[cfg(target_vendor = "apple")]
        File::open(_dir)?.sync_all()?;
        Ok(())
    }

    /// One `syncfs` for a backup's chunks instead of a sync per chunk, where that is safe.
    #[cfg(target_os = "linux")]
    mod batch {
        use super::sync_path;
        use parking_lot::Mutex;
        use std::{
            fs::File,
            os::fd::AsRawFd,
            path::{Path, PathBuf},
            sync::{LazyLock, OnceLock},
        };

        /// Most chunks `all` syncs one by one. Past that a single `syncfs` is cheaper, though it
        /// also flushes everything else on the filesystem.
        const FSYNC_LIMIT: usize = 64;

        /// Filesystems whose `syncfs` makes the data durable and reports failed writes: ext,
        /// XFS, Btrfs, F2FS and ZFS. FUSE doesn't pass it to its daemon and overlayfs may drop
        /// the errors, so anything else syncs each chunk.
        const SYNCFS_FILESYSTEMS: [u32; 5] =
            [0xEF53, 0x5846_5342, 0x9123_683E, 0xF2F5_2010, 0x2FC1_2FC1];

        /// `syncfs` reports failed writes since Linux 5.8. Older kernels sync each chunk, as does
        /// a kernel whose version can't be read.
        static SYNCFS_REPORTS_ERRORS: LazyLock<bool> = LazyLock::new(|| {
            std::fs::read_to_string("/proc/sys/kernel/osrelease").is_ok_and(|release| {
                let mut parts = release
                    .split(|c: char| !c.is_ascii_digit())
                    .map(|part| part.parse::<u32>().unwrap_or(0));
                (parts.next(), parts.next()) >= (Some(5), Some(8))
            })
        });

        #[derive(Default)]
        pub struct Batch {
            /// The chunk directory, if `syncfs` is used there. `syncfs` misses a failed write
            /// that another caller saw before its descriptor was opened, so this one is opened
            /// ahead of the chunk writes.
            dir: OnceLock<Option<File>>,
            /// Chunks written since the last `all`. Stops growing one past `FSYNC_LIMIT`.
            written: Mutex<Vec<PathBuf>>,
        }

        impl Batch {
            pub fn begin(&self, dir: &Path) -> std::io::Result<()> {
                if self.dir.get().is_none() {
                    let dir = File::open(dir)?;
                    let batch = *SYNCFS_REPORTS_ERRORS && syncfs_is_durable(&dir)?;
                    let _ = self.dir.set(batch.then_some(dir));
                }
                Ok(())
            }

            /// Whether chunks are left unsynced for `all`.
            pub fn active(&self) -> bool {
                matches!(self.dir.get(), Some(Some(_)))
            }

            pub fn wrote(&self, path: PathBuf) {
                if !self.active() {
                    return;
                }
                let mut written = self.written.lock();
                if written.len() <= FSYNC_LIMIT {
                    written.push(path);
                }
            }

            /// Syncs the chunks written since the last call. `true` if it flushed the whole
            /// filesystem, directories included; a few chunks are synced on their own instead.
            pub fn all(&self) -> std::io::Result<bool> {
                let Some(Some(dir)) = self.dir.get() else {
                    return Ok(false);
                };
                let written = std::mem::take(&mut *self.written.lock());
                if written.len() <= FSYNC_LIMIT {
                    written.iter().try_for_each(|path| sync_path(path))?;
                    return Ok(false);
                }
                if unsafe { libc::syncfs(dir.as_raw_fd()) } != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(true)
            }
        }

        fn syncfs_is_durable(dir: &File) -> std::io::Result<bool> {
            let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
            if unsafe { libc::fstatfs(dir.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let kind = unsafe { stat.assume_init() }.f_type as u32;
            Ok(SYNCFS_FILESYSTEMS.contains(&kind))
        }
    }
}

/// Directories can't be synced here, so only the chunks are.
#[cfg(not(unix))]
mod durable {
    use std::{
        fs::File,
        path::{Path, PathBuf},
    };

    #[derive(Default)]
    pub struct State;

    pub fn begin(_dir: &Path, _state: &State) -> std::io::Result<()> {
        Ok(())
    }

    pub fn chunk(_state: &State, file: &File) -> std::io::Result<()> {
        file.sync_all()
    }

    pub fn wrote(_state: &State, _path: PathBuf) {}

    pub fn all(_dir: &Path, _state: &State) -> std::io::Result<()> {
        Ok(())
    }

    pub fn one(_dir: &Path, _state: &State, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }
}

/// A regular file with at least the format byte. Empty ones, left by a crash, get rewritten.
fn is_chunk_file(path: &Path) -> bool {
    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
}

/// Matches `write_chunk_content` temp names: `<hex>.<pid>.<counter>.tmp`.
fn is_chunk_temporary(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    parts.len() == 4
        && parts[3] == "tmp"
        && parts[0].len() == 60
        && parts[0].bytes().all(|b| b.is_ascii_hexdigit())
        && parts[1..3]
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}
