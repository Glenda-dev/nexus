#![no_std]
#![no_main]

#[macro_use]
extern crate glenda;
extern crate alloc;

use glenda::cap::{CapType, ENDPOINT_SLOT, Endpoint, MONITOR_CAP};
use glenda::client::{ProcessClient, ResourceClient};
use glenda::interface::ResourceService;
use glenda::ipc::Badge;

mod layout;
mod server;
use crate::layout::{DEVICE_CAP, DEVICE_SLOT};
pub use server::NexusManager;

#[unsafe(no_mangle)]
fn main() -> usize {
    glenda::console::init_logging("Nexus");
    log!("VFS Service starting...");

    let proc_client = ProcessClient::new(MONITOR_CAP);
    let mut res_client = ResourceClient::new(MONITOR_CAP);

    // Register Nexus endpoint
    if let Err(e) = res_client.alloc(Badge::null(), CapType::Endpoint, 0, ENDPOINT_SLOT) {
        log!("Failed to allocate endpoint: {:?}", e);
        return 1;
    }

    // Allocate device endpoint slot
    if let Err(e) = res_client.get_cap(
        Badge::null(),
        glenda::protocol::resource::ResourceType::Endpoint,
        glenda::protocol::resource::DEVICE_ENDPOINT,
        DEVICE_SLOT,
    ) {
        log!("Failed to get device endpoint: {:?}", e);
        return 1;
    }

    // Attempt to fetch InitrdFS endpoint from Resource Manager
    // We retry because initrdfs might be spawned lazily by fossil after probing ramdisk
    // TODO: Fix this
    log!("Attempting to find InitrdFS root...");
    let initrd_ep = res_client
        .get_cap(
            Badge::null(),
            glenda::protocol::resource::ResourceType::Endpoint,
            glenda::protocol::resource::INITRD_ENDPOINT,
            layout::INIT_SLOT,
        )
        .expect("Failed to get InitrdFS endpoint");

    let mut server = NexusManager::new(proc_client, res_client, DEVICE_CAP);

    server.mount("/", Endpoint::from(initrd_ep));
    log!("Mounted InitrdFS to /");

    if let Err(e) = server.init() {
        log!("Failed to init: {:?}", e);
        return 1;
    }

    if let Err(e) = server.run() {
        log!("Exited with error: {:?}", e);
        return 1;
    }
    0
}
