use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use crate::view::View;
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

    // View Management
    views: BTreeMap<usize, View>,
    pid_view_map: BTreeMap<usize, usize>,
    next_view_id: usize,
    // File-handle route (caller badge -> target FS endpoint)
    open_routes: BTreeMap<usize, Endpoint>,

    // Lifecycle
    ipc: NexusIpc,
}

impl<'a> NexusManager<'a> {
    const DEFAULT_VIEW_ID: usize = 0;
    const S_IFMT: u32 = 0o170000;
    const S_IFLNK: u32 = 0o120000;
    const MAX_SYMLINK_DEPTH: usize = 40;

    pub fn new(res_client: &'a mut ResourceClient, init_client: &'a mut InitClient) -> Self {
        let mut views = BTreeMap::new();
        views.insert(Self::DEFAULT_VIEW_ID, View::new("/"));

        Self {
            res_client,
            init_client,
            cspace: CSpaceManager::new(CSPACE_CAP, 16),
            views,
            pid_view_map: BTreeMap::new(),
            next_view_id: Self::DEFAULT_VIEW_ID + 1,
            open_routes: BTreeMap::new(),
            ipc: NexusIpc {
                endpoint: None,
                reply: glenda::cap::REPLY_SLOT,
                recv: glenda::cap::RECV_SLOT,
            },
        }
    }

    fn pid_from_badge(badge: Badge) -> usize {
        badge.bits()
    }

    fn view_id_for_pid(&self, pid: usize) -> usize {
        self.pid_view_map.get(&pid).copied().unwrap_or(Self::DEFAULT_VIEW_ID)
    }

    fn view_for_badge(&self, badge: Badge) -> Option<&View> {
        let pid = Self::pid_from_badge(badge);
        let view_id = self.view_id_for_pid(pid);
        self.views.get(&view_id).or_else(|| self.views.get(&Self::DEFAULT_VIEW_ID))
    }

    fn view_for_badge_mut(&mut self, badge: Badge) -> Result<&mut View, Error> {
        let pid = Self::pid_from_badge(badge);
        let view_id = self.view_id_for_pid(pid);
        let target_view_id =
            if self.views.contains_key(&view_id) { view_id } else { Self::DEFAULT_VIEW_ID };
        self.pid_view_map.insert(pid, target_view_id);
        self.views.get_mut(&target_view_id).ok_or(Error::NotFound)
    }

    fn normalize_absolute_path(path: &str) -> String {
        View::normalize_absolute_path(path)
    }

    fn view_path_to_global(&self, badge: Badge, path: &str) -> Result<String, Error> {
        let view = self.view_for_badge(badge).ok_or(Error::NotFound)?;
        Ok(view.map_path_into_view_root(path))
    }

    fn join_components(parts: &[&str]) -> String {
        let mut out = String::new();
        for (idx, part) in parts.iter().enumerate() {
            if idx > 0 {
                out.push('/');
            }
            out.push_str(part);
        }
        out
    }

    fn parent_dir(path: &str) -> String {
        let normalized = Self::normalize_absolute_path(path);
        if normalized == "/" {
            return normalized;
        }
        if let Some(pos) = normalized.rfind('/') {
            if pos == 0 {
                return String::from("/");
            }
            return String::from(&normalized[..pos]);
        }
        String::from("/")
    }

    fn join_paths(base: &str, tail: &str) -> String {
        if tail.starts_with('/') {
            return Self::normalize_absolute_path(tail);
        }
        let mut out = String::from(base);
        if !out.ends_with('/') {
            out.push('/');
        }
        out.push_str(tail);
        Self::normalize_absolute_path(&out)
    }

    fn split_parent_name(path: &str) -> Result<(String, String), Error> {
        let normalized = Self::normalize_absolute_path(path);
        if normalized == "/" {
            return Err(Error::InvalidArgs);
        }
        let slash = normalized.rfind('/').ok_or(Error::InvalidArgs)?;
        let parent =
            if slash == 0 { String::from("/") } else { String::from(&normalized[..slash]) };
        let name = String::from(&normalized[slash + 1..]);
        if name.is_empty() {
            return Err(Error::InvalidArgs);
        }
        Ok((parent, name))
    }

    fn is_symlink_mode(mode: u32) -> bool {
        (mode & Self::S_IFMT) == Self::S_IFLNK
    }

    fn find_mount_with_root(&self, badge: Badge, path: &str) -> Option<(String, Endpoint, String)> {
        let view = self.view_for_badge(badge)?;
        view.find_mount_with_root(path).map(|(m_path, target, sub_path)| {
            (String::from(m_path), target, sub_path)
        })
    }

    fn find_mount(&self, badge: Badge, path: &str) -> Option<(Endpoint, String)> {
        self.find_mount_with_root(badge, path).map(|(_, target, sub_path)| (target, sub_path))
    }

    fn mint_badged_endpoint(&mut self, target: Endpoint, badge: Badge) -> Result<Endpoint, Error> {
        let slot = self.cspace.alloc(self.res_client)?;
        CSPACE_CAP.mint_self(target.cap(), slot, badge, Rights::ALL)?;
        Ok(Endpoint::from(slot))
    }

    fn lstat_global_path(&mut self, badge: Badge, path: &str) -> Result<Stat, Error> {
        let normalized = Self::normalize_absolute_path(path);
        let (target, sub_path) = self.find_mount(badge, &normalized).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = match client.lstat_path(Badge::null(), &sub_path) {
            Ok(stat) => Ok(stat),
            Err(Error::NotSupported) => client.stat_path(Badge::null(), &sub_path),
            Err(e) => Err(e),
        };
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn readlink_global_path(&mut self, badge: Badge, path: &str) -> Result<String, Error> {
        let normalized = Self::normalize_absolute_path(path);
        let (target, sub_path) = self.find_mount(badge, &normalized).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.readlink_path(Badge::null(), &sub_path);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn resolve_global_path(
        &mut self,
        badge: Badge,
        path: &str,
        follow_final: bool,
    ) -> Result<String, Error> {
        let mut current = self.view_path_to_global(badge, path)?;
        let mut followed = 0usize;

        loop {
            let components: Vec<&str> =
                current.split('/').filter(|part| !part.is_empty() && *part != ".").collect();

            if components.is_empty() {
                return Ok(String::from("/"));
            }

            let mut prefix = String::from("/");
            let mut replaced = false;

            for (idx, part) in components.iter().enumerate() {
                if prefix.len() > 1 {
                    prefix.push('/');
                }
                prefix.push_str(part);

                let is_last = idx + 1 == components.len();
                if is_last && !follow_final {
                    continue;
                }

                let st = self.lstat_global_path(badge, &prefix)?;
                if !Self::is_symlink_mode(st.mode) {
                    continue;
                }

                followed += 1;
                if followed > Self::MAX_SYMLINK_DEPTH {
                    return Err(Error::ResourceBusy);
                }

                let target = self.readlink_global_path(badge, &prefix)?;
                let source_mount = self
                    .find_mount_with_root(badge, &prefix)
                    .map(|(mount_path, _, _)| mount_path);
                let parent = Self::parent_dir(&prefix);
                let mut merged = if target.starts_with('/') {
                    let normalized_target = Self::normalize_absolute_path(&target);
                    if self.find_mount(badge, &normalized_target).is_some() {
                        normalized_target
                    } else if let Some(mount_path) = source_mount.as_deref() {
                        if mount_path != "/" {
                            let mut remapped = String::from(mount_path);
                            if normalized_target != "/" {
                                remapped.push_str(&normalized_target);
                            }
                            Self::normalize_absolute_path(&remapped)
                        } else {
                            normalized_target
                        }
                    } else {
                        normalized_target
                    }
                } else {
                    Self::join_paths(&parent, &target)
                };

                if !is_last {
                    let rest = Self::join_components(&components[idx + 1..]);
                    merged = Self::join_paths(&merged, &rest);
                }

                current = merged;
                replaced = true;
                break;
            }

            if !replaced {
                return Ok(current);
            }
        }
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
            (protocol::FS_PROTO, protocol::fs::CREATE_VIEW) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let root = unsafe { u.read_str()? };
                    let view_id = s.create_view(badge, &root)?;
                    Ok(view_id)
                })
            },
            (protocol::FS_PROTO, protocol::fs::SET_VIEW) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let view_id = u.get_mr(0);
                    s.set_view(badge, view_id)?;
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::READ_SYNC) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let req_len = u_inner.get_mr(0);
                    let offset = u_inner.get_mr(1);
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut client = FsClient::new(target);
                    let max_len = core::cmp::min(req_len, u_inner.buffer_mut().len());
                    let read_len = {
                        let buf = u_inner.buffer_mut();
                        client.read(Badge::null(), offset as usize, &mut buf[..max_len])?
                    };
                    u_inner.set_size(read_len);
                    Ok(read_len)
                })
            },
            (protocol::FS_PROTO, protocol::fs::WRITE_SYNC) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u_inner| {
                    let offset = u_inner.get_mr(0);
                    let target = *s.open_routes.get(&badge.bits()).ok_or(Error::NotFound)?;
                    let mut client = FsClient::new(target);
                    let written = client.write(Badge::null(), offset as usize, u_inner.buffer())?;
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
            (protocol::FS_PROTO, protocol::fs::LSTAT_PATH) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let stat = s.lstat_path(badge, &path)?;
                    unsafe { u.write_obj(&stat)? };
                    Ok(0usize)
                })
            },
            (protocol::FS_PROTO, protocol::fs::READLINK_PATH) => |s: &mut Self, u: &mut UTCB| {
                handle_call(u, |u| {
                    let path = unsafe { u.read_str()? };
                    let target = s.readlink_path(badge, &path)?;
                    unsafe { u.write_str(&target)? };
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
        let resolved = self.resolve_global_path(badge, path, true)?;
        let (target, sub_path) = self.find_mount(badge, &resolved).ok_or(Error::NotFound)?;
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
        let normalized = Self::normalize_absolute_path(path);
        let (parent, name) = Self::split_parent_name(&normalized)?;
        let resolved_parent = self.resolve_global_path(badge, &parent, true)?;
        let target_path = Self::join_paths(&resolved_parent, &name);

        let (target, sub_path) = self.find_mount(badge, &target_path).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.mkdir(Badge::null(), &sub_path, mode);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn unlink(&mut self, badge: Badge, path: &str) -> Result<(), Error> {
        let resolved = self.resolve_global_path(badge, path, false)?;
        let (target, sub_path) = self.find_mount(badge, &resolved).ok_or(Error::NotFound)?;
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
        let resolved = self.resolve_global_path(badge, path, true)?;
        let (target, sub_path) = self.find_mount(badge, &resolved).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.stat_path(Badge::null(), &sub_path);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn lstat_path(&mut self, badge: Badge, path: &str) -> Result<Stat, Error> {
        let resolved = self.resolve_global_path(badge, path, false)?;
        let (target, sub_path) = self.find_mount(badge, &resolved).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.lstat_path(Badge::null(), &sub_path);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }

    fn readlink_path(&mut self, badge: Badge, path: &str) -> Result<String, Error> {
        let resolved = self.resolve_global_path(badge, path, false)?;
        let (target, sub_path) = self.find_mount(badge, &resolved).ok_or(Error::NotFound)?;
        let badged_target = self.mint_badged_endpoint(target, badge)?;
        let mut client = FsClient::new(badged_target);
        let ret = client.readlink_path(Badge::null(), &sub_path);
        let _ = CSPACE_CAP.delete(badged_target.cap());
        ret
    }
}

impl<'a> VirtualFileSystemService for NexusManager<'a> {
    fn mount(&mut self, badge: Badge, path: &str, target: Endpoint) -> Result<(), Error> {
        let normalized = Self::normalize_absolute_path(path);
        log!("Mounting target FS at: {}", normalized);
        let slot = self.cspace.alloc(self.res_client)?;
        CSPACE_CAP.transfer_self(target.cap(), slot)?;
        self.view_for_badge_mut(badge)?.mounts.insert(normalized, Endpoint::from(slot));
        Ok(())
    }

    fn unmount(&mut self, badge: Badge, path: &str) -> Result<(), Error> {
        let normalized = Self::normalize_absolute_path(path);
        log!("Unmounting FS at: {}", normalized);
        if let Some(target) = self.view_for_badge_mut(badge)?.mounts.remove(&normalized) {
            let _ = CSPACE_CAP.delete(target.cap());
        }
        Ok(())
    }

    fn create_view(&mut self, badge: Badge, root: &str) -> Result<usize, Error> {
        let pid = Self::pid_from_badge(badge);
        let source_view_id = self.view_id_for_pid(pid);
        let source_view = self
            .views
            .get(&source_view_id)
            .or_else(|| self.views.get(&Self::DEFAULT_VIEW_ID))
            .ok_or(Error::NotFound)?;
        let view_id = self.next_view_id;
        self.next_view_id = self.next_view_id.wrapping_add(1);
        let new_view = source_view.clone_with_root(root);
        self.views.insert(view_id, new_view);
        Ok(view_id)
    }

    fn set_view(&mut self, badge: Badge, view_id: usize) -> Result<(), Error> {
        if !self.views.contains_key(&view_id) {
            return Err(Error::NotFound);
        }
        let pid = Self::pid_from_badge(badge);
        self.pid_view_map.insert(pid, view_id);
        Ok(())
    }
}
