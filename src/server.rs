use alloc::collections::BTreeMap;
use alloc::string::String;
use glenda::cap::{CSPACE_CAP, CapPtr, Endpoint, Reply};
use glenda::client::{InitClient, ResourceClient};
use glenda::error::Error;
use glenda::interface::InitService;
use glenda::interface::fs::{FileSystemService, VirtualFileSystemService};
use glenda::interface::system::SystemService;
use glenda::ipc::{Badge, MsgFlags, MsgTag, UTCB};
use glenda::protocol;
use glenda::protocol::fs::{OpenFlags, Stat};
use glenda::utils::manager::{CSpaceManager, CSpaceService};

pub struct NexusManager<'a> {
    res_client: &'a mut ResourceClient,
    init_client: &'a mut InitClient,

    // CSpace Management
    cspace: CSpaceManager,

    // Namespace Management (Path -> Target FS Endpoint)
    mounts: BTreeMap<String, Endpoint>,

    // Lifecycle
    endpoint: Option<Endpoint>,
    reply: CapPtr,
    recv: CapPtr,
}

impl<'a> NexusManager<'a> {
    pub fn new(res_client: &'a mut ResourceClient, init_client: &'a mut InitClient) -> Self {
        Self {
            res_client,
            init_client,
            cspace: CSpaceManager::new(CSPACE_CAP, 100),
            mounts: BTreeMap::new(),
            endpoint: None,
            reply: glenda::cap::REPLY_SLOT,
            recv: glenda::cap::RECV_SLOT,
        }
    }

    fn find_mount(&self, path: &str) -> Option<(Endpoint, String)> {
        let mut best_match: Option<(&String, &Endpoint)> = None;
        for (m_path, target) in &self.mounts {
            if path.starts_with(m_path) {
                if best_match.is_none() || m_path.len() > best_match.unwrap().0.len() {
                    best_match = Some((m_path, target));
                }
            }
        }

        best_match.map(|(m_path, target)| {
            let mut sub_path = &path[m_path.len()..];
            if sub_path.is_empty() {
                sub_path = "/";
            }
            (*target, String::from(sub_path))
        })
    }
}

impl<'a> SystemService for NexusManager<'a> {
    fn init(&mut self) -> Result<(), Error> {
        log!("Init Routing VFS ...");
        self.init_client.report_service(Badge::null(), protocol::init::ServiceState::Starting)?;

        Ok(())
    }

    fn listen(&mut self, ep: Endpoint, reply: CapPtr, recv: CapPtr) -> Result<(), Error> {
        self.endpoint = Some(ep);
        self.reply = reply;
        self.recv = recv;
        Ok(())
    }

    fn run(&mut self) -> Result<(), Error> {
        log!("Running server loop...");
        self.init_client.report_service(Badge::null(), protocol::init::ServiceState::Running)?;

        let ep = self.endpoint.ok_or(Error::NotInitialized)?;

        loop {
            let mut utcb = unsafe { UTCB::new() };
            utcb.clear();

            // Clear receive slot to avoid AlreadyExists error
            let _ = self.cspace.root().delete(self.recv);

            utcb.set_reply_window(self.reply);
            utcb.set_recv_window(self.recv);

            if let Err(_) = ep.recv(&mut utcb) {
                continue;
            }

            let b = utcb.get_badge();
            if b.bits() != 0 {
                continue;
            }

            if let Err(e) = self.dispatch(&mut utcb) {
                log!("Err handling FS request: {:?}", e);
                utcb.set_msg_tag(MsgTag::err());
                utcb.set_mr(0, e as usize);
            }

            let _ = self.reply(&mut utcb);
        }
    }

    fn dispatch(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let tag = utcb.get_msg_tag();
        match (tag.proto(), tag.label()) {
            (protocol::FS_PROTO, protocol::fs::OPEN) => {
                let path = unsafe { utcb.read_str()? };
                let flags = OpenFlags::from_bits_truncate(utcb.get_mr(0));
                let mode = utcb.get_mr(1) as u32;
                log!("Open: {} (flags={:?})", path, flags);
                let handle_cap = self.open(&path, flags, mode)?;
                utcb.set_msg_tag(MsgTag::new(
                    protocol::FS_PROTO,
                    protocol::fs::OPEN,
                    MsgFlags::NONE,
                ));
                utcb.set_cap_transfer(CapPtr::from(handle_cap));
                Ok(())
            }
            (protocol::FS_PROTO, protocol::fs::MKDIR) => {
                let path = unsafe { utcb.read_str()? };
                let mode = utcb.get_mr(0) as u32;
                self.mkdir(&path, mode)?;
                utcb.set_msg_tag(MsgTag::new(
                    protocol::FS_PROTO,
                    protocol::fs::MKDIR,
                    MsgFlags::NONE,
                ));
                Ok(())
            }
            (protocol::FS_PROTO, protocol::fs::UNLINK) => {
                let path = unsafe { utcb.read_str()? };
                self.unlink(&path)?;
                utcb.set_msg_tag(MsgTag::new(
                    protocol::FS_PROTO,
                    protocol::fs::UNLINK,
                    MsgFlags::NONE,
                ));
                Ok(())
            }
            (protocol::FS_PROTO, protocol::fs::STAT_PATH) => {
                let path = unsafe { utcb.read_str()? };
                let stat = self.stat_path(&path)?;
                utcb.set_msg_tag(MsgTag::new(
                    protocol::FS_PROTO,
                    protocol::fs::STAT_PATH,
                    MsgFlags::NONE,
                ));
                utcb.set_mr(0, stat.ino as usize);
                utcb.set_mr(1, stat.mode as usize);
                utcb.set_mr(2, stat.nlink as usize);
                utcb.set_mr(3, stat.uid as usize);
                utcb.set_mr(4, stat.gid as usize);
                utcb.set_mr(5, stat.size as usize);
                utcb.set_mr(6, stat.blksize as usize);
                utcb.set_mr(7, stat.blocks as usize);
                utcb.set_mr(8, stat.atime as usize);
                utcb.set_mr(9, stat.mtime as usize);
                utcb.set_mr(10, stat.ctime as usize);
                Ok(())
            }
            (protocol::FS_PROTO, protocol::fs::MOUNT) => {
                let path = unsafe { utcb.read_str()? };
                let target_ep_cap = utcb.get_cap_transfer();
                if target_ep_cap.is_null() {
                    return Err(Error::InvalidArgs);
                }
                self.mount(&path, Endpoint::from(target_ep_cap))?;
                utcb.set_msg_tag(MsgTag::new(
                    protocol::FS_PROTO,
                    protocol::fs::MOUNT,
                    MsgFlags::NONE,
                ));
                Ok(())
            }
            _ => Err(Error::NotSupported),
        }
    }

    fn reply(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        Reply::from(self.reply).reply(utcb)
    }

    fn stop(&mut self) {
        log!("Stopping Nexus server...");
    }
}

impl<'a> FileSystemService for NexusManager<'a> {
    fn open(&mut self, path: &str, flags: OpenFlags, mode: u32) -> Result<usize, Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_mr(0, flags.bits());
        sub_utcb.set_mr(1, mode as usize);

        let recv_slot = self.cspace.alloc(self.res_client)?;
        sub_utcb.set_recv_window(recv_slot);
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::OPEN,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;
        Ok(recv_slot.bits())
    }

    fn mkdir(&mut self, path: &str, mode: u32) -> Result<(), Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_mr(0, mode as usize);
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::MKDIR,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;
        Ok(())
    }

    fn unlink(&mut self, path: &str) -> Result<(), Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::UNLINK,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;
        Ok(())
    }

    fn rename(&mut self, _old_path: &str, _new_path: &str) -> Result<(), Error> {
        Err(Error::NotSupported)
    }

    fn stat_path(&mut self, path: &str) -> Result<Stat, Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::STAT_PATH,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;

        let mut stat = Stat::default();
        stat.ino = sub_utcb.get_mr(0) as u64;
        stat.mode = sub_utcb.get_mr(1) as u32;
        stat.nlink = sub_utcb.get_mr(2) as u32;
        stat.uid = sub_utcb.get_mr(3) as u32;
        stat.gid = sub_utcb.get_mr(4) as u32;
        stat.size = sub_utcb.get_mr(5) as u64;
        stat.blksize = sub_utcb.get_mr(6) as u32;
        stat.blocks = sub_utcb.get_mr(7) as u64;
        stat.atime = sub_utcb.get_mr(8) as u64;
        stat.mtime = sub_utcb.get_mr(9) as u64;
        stat.ctime = sub_utcb.get_mr(10) as u64;
        Ok(stat)
    }
}

impl<'a> VirtualFileSystemService for NexusManager<'a> {
    fn mount(&mut self, path: &str, target: Endpoint) -> Result<(), Error> {
        log!("Mounting target FS at: {}", path);
        let slot = self.cspace.alloc(self.res_client)?;
        self.cspace.root().move_cap(target.cap(), slot)?;
        self.mounts.insert(String::from(path), Endpoint::from(slot));
        Ok(())
    }

    fn unmount(&mut self, path: &str) -> Result<(), Error> {
        if let Some(target) = self.mounts.remove(path) {
            let _ = self.cspace.root().delete(target.cap());
        }
        Ok(())
    }
}
