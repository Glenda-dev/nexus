use alloc::vec::Vec;
use glenda::cap::Endpoint;
use glenda::error::Error;
use glenda::interface::fs::{FileHandleService, FileSystemService};
use glenda::ipc::{Badge, MsgFlags, MsgTag, UTCB};
use glenda::protocol;
use glenda::protocol::fs::{DEntry, OpenFlags, Stat};
use glenda::set_mrs;

pub struct FileSystemProxy(pub Endpoint);
pub struct FileHandleProxy(pub Endpoint);

impl FileSystemService for FileSystemProxy {
    fn open(
        &mut self,
        badge: Badge,
        path: &str,
        flags: OpenFlags,
        mode: u32,
    ) -> Result<usize, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.write(path.as_bytes());
        utcb.set_badge(badge);
        set_mrs!(utcb, flags.bits(), mode);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::OPEN, MsgFlags::HAS_BUFFER));
        self.0.proxy(utcb)?;
        Err(Error::Success) // Signal that we have proxied and don't need to reply
    }

    fn mkdir(&mut self, badge: Badge, path: &str, mode: u32) -> Result<(), Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.write(path.as_bytes());
        utcb.set_badge(badge);
        set_mrs!(utcb, mode);
        utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::MKDIR,
            MsgFlags::HAS_BUFFER,
        ));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn unlink(&mut self, badge: Badge, path: &str) -> Result<(), Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.write(path.as_bytes());
        utcb.set_badge(badge);
        utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::UNLINK,
            MsgFlags::HAS_BUFFER,
        ));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn rename(&mut self, badge: Badge, old_path: &str, new_path: &str) -> Result<(), Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        unsafe { utcb.write_postcard(&(old_path, new_path))? };
        utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::RENAME,
            MsgFlags::HAS_BUFFER,
        ));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn stat_path(&mut self, badge: Badge, path: &str) -> Result<Stat, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.write(path.as_bytes());
        utcb.set_badge(badge);
        utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::STAT_PATH,
            MsgFlags::HAS_BUFFER,
        ));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }
}

impl FileHandleService for FileHandleProxy {
    fn close(&mut self, badge: Badge) -> Result<(), Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::CLOSE, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn stat(&self, badge: Badge) -> Result<Stat, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::STAT, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn read(&mut self, badge: Badge, offset: usize, buf: &mut [u8]) -> Result<usize, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        set_mrs!(utcb, buf.len(), offset as usize);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::READ_SYNC, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn write(&mut self, badge: Badge, offset: usize, buf: &[u8]) -> Result<usize, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        set_mrs!(utcb, offset as usize);
        utcb.write(buf);
        utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::WRITE_SYNC,
            MsgFlags::HAS_BUFFER,
        ));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn getdents(&mut self, badge: Badge, count: usize) -> Result<Vec<DEntry>, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        set_mrs!(utcb, count);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::GETDENTS, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn seek(&mut self, badge: Badge, offset: i64, whence: usize) -> Result<usize, Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        set_mrs!(utcb, offset as usize, whence);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::SEEK, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn sync(&mut self, badge: Badge) -> Result<(), Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::SYNC, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }

    fn truncate(&mut self, badge: Badge, size: usize) -> Result<(), Error> {
        let utcb = unsafe { UTCB::new() };
        utcb.clear();
        utcb.set_badge(badge);
        set_mrs!(utcb, size as usize);
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::TRUNCATE, MsgFlags::NONE));
        self.0.proxy(utcb)?;
        Err(Error::Success)
    }
}
