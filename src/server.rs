use crate::proxy::FileSystemProxy;
use alloc::collections::BTreeMap;
use alloc::string::String;
use glenda::cap::{CSPACE_CAP, CapPtr, Endpoint, Reply};
use glenda::client::{InitClient, ResourceClient};
use glenda::error::Error;
use glenda::interface::InitService;
use glenda::interface::fs::{FileSystemService, VirtualFileSystemService};
use glenda::interface::system::SystemService;
use glenda::ipc::server::{handle_call, handle_cap_call};
use glenda::ipc::{Badge, MsgTag, UTCB};
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

            if let Err(e) = ep.recv(&mut utcb) {
                log!("Recv error: {:?}", e);
                continue;
            }

            match self.dispatch(&mut utcb) {
                Ok(()) => {
                    let _ = self.reply(&mut utcb);
                }
                Err(Error::Success) => {
                    // Proxied, no need to reply
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
                handle_cap_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let flags = OpenFlags::from_bits_truncate(u.get_mr(0));
                    let mode = u.get_mr(1) as u32;
                    let handle_cap = s.open(badge, &path, flags, mode)?;
                    Ok(CapPtr::from(handle_cap))
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
                    let target_ep_cap = s.recv;
                    s.mount(badge, &path, Endpoint::from(target_ep_cap))?;
                    Ok(())
                })
            },
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
    fn open(
        &mut self,
        badge: Badge,
        path: &str,
        flags: OpenFlags,
        mode: u32,
    ) -> Result<usize, Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        FileSystemProxy(target).open(badge, &sub_path, flags, mode)
    }

    fn mkdir(&mut self, badge: Badge, path: &str, mode: u32) -> Result<(), Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        FileSystemProxy(target).mkdir(badge, &sub_path, mode)
    }

    fn unlink(&mut self, badge: Badge, path: &str) -> Result<(), Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        FileSystemProxy(target).unlink(badge, &sub_path)
    }

    fn rename(&mut self, _badge: Badge, _old_path: &str, _new_path: &str) -> Result<(), Error> {
        Err(Error::NotSupported)
    }

    fn stat_path(&mut self, badge: Badge, path: &str) -> Result<Stat, Error> {
        let (target, sub_path) = self.find_mount(path).ok_or(Error::NotFound)?;
        FileSystemProxy(target).stat_path(badge, &sub_path)
    }
}

impl<'a> VirtualFileSystemService for NexusManager<'a> {
    fn mount(&mut self, _badge: Badge, path: &str, target: Endpoint) -> Result<(), Error> {
        log!("Mounting target FS at: {}", path);
        let slot = self.cspace.alloc(self.res_client)?;
        self.cspace.root().move_cap(target.cap(), slot)?;
        self.mounts.insert(String::from(path), Endpoint::from(slot));
        Ok(())
    }

    fn unmount(&mut self, _badge: Badge, path: &str) -> Result<(), Error> {
        if let Some(target) = self.mounts.remove(path) {
            let _ = self.cspace.root().delete(target.cap());
        }
        Ok(())
    }
}
