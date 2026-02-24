use alloc::collections::BTreeMap;
use alloc::string::String;
use glenda::cap::{CSPACE_CAP, Endpoint};
use glenda::client::ResourceClient;
use glenda::error::Error;
use glenda::ipc::{MsgFlags, MsgTag, UTCB};
use glenda::protocol;
use glenda::protocol::fs::OpenFlags;
use glenda::utils::manager::{CSpaceManager, CSpaceService};

pub struct NexusManager {
    res_client: ResourceClient,

    // CSpace Management
    cspace: CSpaceManager,

    // Namespace Management (Path -> Target FS Endpoint)
    mounts: BTreeMap<String, Endpoint>,
}

impl NexusManager {
    pub fn new(
        _proc_client: glenda::client::ProcessClient,
        res_client: ResourceClient,
        _dev_endpoint: Endpoint,
    ) -> Self {
        Self { res_client, cspace: CSpaceManager::new(CSPACE_CAP, 100), mounts: BTreeMap::new() }
    }

    pub fn init(&mut self) -> Result<(), Error> {
        log!("Init Routing VFS ...");
        Ok(())
    }

    // Add a mount method
    pub fn mount(&mut self, path: &str, target: Endpoint) {
        log!("Mounting target FS at: {}", path);
        self.mounts.insert(String::from(path), target);
    }

    pub fn run(&mut self) -> Result<(), Error> {
        log!("Running server loop...");

        loop {
            let mut utcb = unsafe { UTCB::new() };
            utcb.clear();

            // Clear receive slot to avoid AlreadyExists error
            let _ = self.cspace.root().delete(glenda::cap::RECV_SLOT);

            utcb.set_reply_window(glenda::cap::REPLY_CAP.cap());
            utcb.set_recv_window(glenda::cap::RECV_SLOT);

            if let Err(_) = glenda::cap::ENDPOINT_CAP.recv(&mut utcb) {
                continue;
            }

            let b = utcb.get_badge();
            if b.bits() != 0 {
                log!(
                    "Nexus received message with badge={}, ignoring (should go to target FS)",
                    b.bits()
                );
                continue;
            }

            let res = glenda::ipc_dispatch! {
                self, utcb,
                (protocol::FS_PROTO, protocol::fs::OPEN) => |s: &mut Self, u: &mut UTCB| {
                    s.handle_open(u)
                },
                (protocol::FS_PROTO, protocol::fs::MKDIR) => |s: &mut Self, u: &mut UTCB| {
                    s.handle_mkdir(u)
                },
                (protocol::FS_PROTO, protocol::fs::UNLINK) => |s: &mut Self, u: &mut UTCB| {
                    s.handle_unlink(u)
                },
                (protocol::FS_PROTO, protocol::fs::STAT_PATH) => |s: &mut Self, u: &mut UTCB| {
                    s.handle_stat_path(u)
                },
                (protocol::FS_PROTO, protocol::fs::MOUNT) => |s: &mut Self, u: &mut UTCB| {
                    s.handle_mount(u)
                }
            };

            if let Err(e) = res {
                log!("Err handling FS request: {:?}", e);
                utcb.set_msg_tag(MsgTag::err());
                utcb.set_mr(0, e as usize);
            }

            let _ = glenda::cap::REPLY_CAP.reply(&mut utcb);
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

    fn handle_open(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let path = unsafe { utcb.read_str()? };
        let flags = OpenFlags::from_bits_truncate(utcb.get_mr(0));
        let mode = utcb.get_mr(1) as u32;

        log!("Open: {} (flags={:?})", path, flags);
        let (target, sub_path) = self.find_mount(&path).ok_or(Error::NotFound)?;

        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_mr(0, flags.bits());
        sub_utcb.set_mr(1, mode as usize);

        let recv_slot = self.cspace.alloc(&mut self.res_client)?;
        sub_utcb.set_recv_window(recv_slot);
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::OPEN,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;

        utcb.set_msg_tag(sub_utcb.get_msg_tag());
        utcb.set_cap_transfer(recv_slot);
        Ok(())
    }

    fn handle_stat_path(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let path = unsafe { utcb.read_str()? };
        let (target, sub_path) = self.find_mount(&path).ok_or(Error::NotFound)?;

        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::STAT_PATH,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;

        utcb.set_msg_tag(sub_utcb.get_msg_tag());
        for i in 0..16 {
            utcb.set_mr(i, sub_utcb.get_mr(i));
        }
        Ok(())
    }

    fn handle_mkdir(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let path = unsafe { utcb.read_str()? };
        let mode = utcb.get_mr(0);
        let (target, sub_path) = self.find_mount(&path).ok_or(Error::NotFound)?;

        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_mr(0, mode);
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::MKDIR,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;

        utcb.set_msg_tag(sub_utcb.get_msg_tag());
        Ok(())
    }

    fn handle_unlink(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let path = unsafe { utcb.read_str()? };
        let (target, sub_path) = self.find_mount(&path).ok_or(Error::NotFound)?;

        let mut sub_utcb = unsafe { UTCB::new() };
        sub_utcb.clear();
        unsafe { sub_utcb.write_str(&sub_path)? };
        sub_utcb.set_msg_tag(MsgTag::new(
            protocol::FS_PROTO,
            protocol::fs::UNLINK,
            MsgFlags::HAS_BUFFER,
        ));
        target.call(&mut sub_utcb)?;

        utcb.set_msg_tag(sub_utcb.get_msg_tag());
        Ok(())
    }

    fn handle_mount(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let path = unsafe { utcb.read_str()? };
        let target_ep_cap = utcb.get_cap_transfer();
        if target_ep_cap.is_null() {
            return Err(Error::InvalidArgs);
        }

        let slot = self.cspace.alloc(&mut self.res_client)?;
        self.cspace.root().move_cap(target_ep_cap, slot)?;

        self.mount(&path, Endpoint::from(slot));
        utcb.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::MOUNT, MsgFlags::NONE));
        Ok(())
    }
}
