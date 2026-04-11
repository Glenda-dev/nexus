use alloc::collections::BTreeMap;
use alloc::string::String;
use glenda::cap::{CSPACE_CAP, CapPtr, Endpoint, Reply, Rights};
use glenda::client::{FsClient, InitClient, ResourceClient};
use glenda::error::Error;
use glenda::interface::CSpaceService;
use glenda::interface::InitService;
use glenda::interface::fs::{FileHandleService, FileSystemService, VirtualFileSystemService};
use glenda::interface::system::SystemService;
use glenda::ipc::server::handle_call;
use glenda::ipc::{Badge, MsgFlags, MsgTag, UTCB};
use glenda::protocol;
use glenda::protocol::fs::{OpenFlags, Stat};
use glenda::utils::manager::CSpaceManager;

pub struct NexusIpc {
    endpoint: Option<Endpoint>,
    reply: CapPtr,
    recv: CapPtr,
}

pub struct NexusManager<'a> {
    res_client: &'a mut ResourceClient,
    init_client: &'a mut InitClient,

    // CSpace Management
    cspace: CSpaceManager,

    // Namespace Management (Path -> Target FS Endpoint)
    mounts: BTreeMap<String, Endpoint>,
    // File-handle route (caller badge -> target FS endpoint)
    open_routes: BTreeMap<usize, Endpoint>,

    // Lifecycle
    ipc: NexusIpc,
}

impl<'a> NexusManager<'a> {
    pub fn new(res_client: &'a mut ResourceClient, init_client: &'a mut InitClient) -> Self {
        Self {
            res_client,
            init_client,
            cspace: CSpaceManager::new(CSPACE_CAP, 16),
            mounts: BTreeMap::new(),
            open_routes: BTreeMap::new(),
            ipc: NexusIpc {
                endpoint: None,
                reply: glenda::cap::REPLY_SLOT,
                recv: glenda::cap::RECV_SLOT,
            },
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

    fn mint_badged_endpoint(&mut self, target: Endpoint, badge: Badge) -> Result<Endpoint, Error> {
        let slot = self.cspace.alloc(self.res_client)?;
        CSPACE_CAP.mint_self(target.cap(), slot, badge, Rights::ALL)?;
        Ok(Endpoint::from(slot))
    }
}

impl<'a> SystemService for NexusManager<'a> {
    fn init(&mut self) -> Result<(), Error> {
        log!("Init Routing VFS ...");
        self.init_client.report_service(Badge::null(), protocol::init::ServiceState::Starting)?;

        Ok(())
    }

    fn listen(&mut self, ep: Endpoint, reply: CapPtr, recv: CapPtr) -> Result<(), Error> {
        self.ipc.endpoint = Some(ep);
        self.ipc.reply = reply;
        self.ipc.recv = recv;
        Ok(())
    }

    fn run(&mut self) -> Result<(), Error> {
        log!("Running server loop...");
        self.init_client.report_service(Badge::null(), protocol::init::ServiceState::Running)?;

        let ep = self.ipc.endpoint.ok_or(Error::NotInitialized)?;

        loop {
            let mut utcb = unsafe { UTCB::new() };
            utcb.clear();

            utcb.set_reply_window(self.ipc.reply);
            utcb.set_recv_window(self.ipc.recv);

            if let Err(e) = ep.recv(&mut utcb) {
                error!("Recv error: {:?}", e);
                continue;
            }

            match self.dispatch(&mut utcb) {
                Ok(()) => {
                    let _ = self.reply(&mut utcb);
                }
                Err(Error::Success) => {
                    // Proxied, no need to reply
                    let _ = CSPACE_CAP.delete(self.ipc.reply);
                }
                Err(e) => {
                    log!("Err handling FS request: {:?}", e);
                    utcb.set_msg_tag(MsgTag::err());
                    utcb.set_mr(0, e as usize);
                    let _ = self.reply(&mut utcb);
                }
            }
        }
    }

    fn dispatch(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        let badge = utcb.get_badge();
        ipc_dispatch! {
            self, utcb,
            (protocol::FS_PROTO, protocol::fs::OPEN) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let flags = OpenFlags::from_bits_truncate(u.get_mr(0));
                    let mode = u.get_mr(1) as u32;
                    let fd = s.open(badge, &path, flags, mode)?;
                    Ok(fd)
                })
            },
            (protocol::FS_PROTO, protocol::fs::MKDIR) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let mode = u.get_mr(0) as u32;
                    s.mkdir(badge, &path, mode)?;
                    Ok(())
                })
            },
            (protocol::FS_PROTO, protocol::fs::UNLINK) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    s.unlink(badge, &path)?;
                    Ok(())
                })
            },
            (protocol::FS_PROTO, protocol::fs::STAT_PATH) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let stat = s.stat_path(badge, &path)?;
                    unsafe { u.write_obj(&stat)? };
                    Ok(())
                })
            },
            (protocol::FS_PROTO, protocol::fs::MOUNT) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let target_ep_cap = s.ipc.recv;
                    s.mount(badge, &path, Endpoint::from(target_ep_cap))?;
                    Ok(())
                })
            },
            (protocol::FS_PROTO, protocol::fs::UNMOUNT) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    s.unmount(badge, &path)?;
                    Ok(())
                })
            },
            (protocol::FS_PROTO, protocol::fs::READ_SYNC) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let len = u_inner.get_mr(0);
                    let offset = u_inner.get_mr(1);
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut client = FsClient::new(target);
                    let mut buf = alloc::vec![0u8; len];
                    let read_len = client.read(Badge::null(), offset as usize, &mut buf)?;
                    u_inner.write(&buf[..read_len]);
                    Ok(read_len)
                })
            },
            (protocol::FS_PROTO, protocol::fs::WRITE_SYNC) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let offset = u_inner.get_mr(0);
                    let payload = alloc::vec::Vec::from(u_inner.buffer());
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut client = FsClient::new(target);
                    let written = client.write(Badge::null(), offset as usize, &payload)?;
                    Ok(written)
                })
            },
            (protocol::FS_PROTO, protocol::fs::SETUP_IOURING) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut fwd = unsafe { UTCB::new() };
                    fwd.clear();

                    let mut flags = MsgFlags::NONE;
                    if u_inner.get_msg_tag().flags().contains(MsgFlags::HAS_CAP) {
                        flags |= MsgFlags::HAS_CAP;
                        fwd.set_cap_transfer(s.ipc.recv);
                    }

                    fwd.set_mr(0, u_inner.get_mr(0));
                    fwd.set_mr(1, u_inner.get_mr(1));
                    fwd.set_mr(2, u_inner.get_mr(2));
                    fwd.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::SETUP_IOURING, flags));
                    target.call(&mut fwd)?;
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::PROCESS_IOURING) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |_u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut fwd = unsafe { UTCB::new() };
                    fwd.clear();
                    fwd.set_msg_tag(MsgTag::new(protocol::FS_PROTO, protocol::fs::PROCESS_IOURING, MsgFlags::NONE));
                    target.call(&mut fwd)?;
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::CLOSE) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |_u_inner| {
                    let target = s.open_routes.remove(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut client = FsClient::new(target);
                    client.close(Badge::null())?;
                    let _ = CSPACE_CAP.delete(target.cap());
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::STAT) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let client = FsClient::new(target);
                    let stat = client.stat(Badge::null())?;
                    unsafe { u_inner.write_obj(&stat)? };
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::GETDENTS) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let count = u_inner.get_mr(0);
                    let mut client = FsClient::new(target);
                    let entries = client.getdents(Badge::null(), count)?;
                    unsafe { u_inner.write_vec(&entries)?; }
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::SEEK) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let offset = u_inner.get_mr(0) as i64;
                    let whence = u_inner.get_mr(1);
                    let mut client = FsClient::new(target);
                    let new_off = client.seek(Badge::null(), offset, whence)?;
                    Ok(new_off)
                })
            },
            (protocol::FS_PROTO, protocol::fs::SYNC) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |_u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut client = FsClient::new(target);
                    client.sync(Badge::null())?;
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::TRUNCATE) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let size = u_inner.get_mr(0);
                    let mut client = FsClient::new(target);
                    client.truncate(Badge::null(), size)?;
                    Ok(0usize)
                })
            },
            (_, _) => |_, u: &mut UTCB| {
                error!(
                    "Unknown request: badge={}, proto={:#x}, label={:#x}",
                    badge,
                    u.get_msg_tag().proto(),
                    u.get_msg_tag().label()
                );
                Err(Error::NotSupported)
            }
        }
    }

    fn reply(&mut self, utcb: &mut UTCB) -> Result<(), Error> {
        Reply::from(self.ipc.reply).reply(utcb)
    }

    fn stop(&mut self) {
        log!("Stopping Nexus server...");
    }
}

impl<'a> FileSystemService for NexusManager<'a> {
    fn open(
        &mut self,
        badge: Badge,
        path: &str,
        flags: OpenFlags,
        mode: u32,
    ) -> Result<usize, Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        if let Some(old) = self.open_routes.insert(badge.bits(), badged_target) {
            let _ = CSPACE_CAP.delete(old.cap());
        }
        let mut client = FsClient::new(badged_target);
        match client.open(Badge::null(), &sub_path, flags, mode) {
            Ok(fd) => Ok(fd),
            Err(e) => {
                self.open_routes.remove(&badge.bits());
                let _ = CSPACE_CAP.delete(badged_target.cap());
                Err(e)
            }
        }
    }

    fn mkdir(&mut self, badge: Badge, path: &str, mode: u32) -> Result<(), Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.mkdir(Badge::null(), &sub_path, mode);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn unlink(&mut self, badge: Badge, path: &str) -> Result<(), Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.unlink(Badge::null(), &sub_path);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn rename(&mut self, _badge: Badge, _old_path: &str, _new_path: &str) -> Result<(), Error> {
        Err(Error::NotSupported)
    }

    fn stat_path(&mut self, badge: Badge, path: &str) -> Result<Stat, Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.stat_path(Badge::null(), &sub_path);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }
}

impl<'a> VirtualFileSystemService for NexusManager<'a> {
    fn mount(&mut self, _badge: Badge, path: &str, target: Endpoint) -> Result<(), Error> {
        log!("Mounting target FS at: {}", path);
        let slot = self.cspace.alloc(self.res_client)?;
        CSPACE_CAP.transfer_self(target.cap(), slot)?;
        self.mounts.insert(String::from(path), Endpoint::from(slot));
        Ok(())
    }

    fn unmount(&mut self, _badge: Badge, path: &str) -> Result<(), Error> {
        log!("Unmounting FS at: {}", path);
        if let Some(target) = self.mounts.remove(path) {
            let _ = CSPACE_CAP.delete(target.cap());
        }
        Ok(())
    }
}
