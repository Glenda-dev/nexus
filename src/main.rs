#![no_std]
#![no_main]
#![allow(dead_code)]

#[macro_use]
extern crate glenda;
extern crate alloc;

use crate::layout::INIT_SLOT;
use glenda::cap::{CapType, ENDPOINT_SLOT, Endpoint, MONITOR_CAP};
use glenda::client::{InitClient, ResourceClient};
use glenda::interface::ResourceService;
use glenda::interface::system::SystemService;
use glenda::ipc::Badge;

mod layout;
mod proxy;
mod server;
pub use server::NexusManager;

#[unsafe(no_mangle)]
fn main() -> usize {
    glenda::console::init_logging("Nexus");
    log!("VFS Service starting...");

    let mut res_client = ResourceClient::new(MONITOR_CAP);

    // Register Nexus endpoint
    if let Err(e) = res_client.alloc(Badge::null(), CapType::Endpoint, 0, ENDPOINT_SLOT) {
        log!("Failed to allocate endpoint: {:?}", e);
        return 1;
    }

    log!("Registering Nexus FS Service...");
    res_client
        .register_cap(
            Badge::null(),
            glenda::protocol::resource::ResourceType::Endpoint,
            glenda::protocol::resource::FS_ENDPOINT,
            ENDPOINT_SLOT,
        )
        .ok();

    if let Err(e) = res_client.get_cap(
        Badge::null(),
        glenda::protocol::resource::ResourceType::Endpoint,
        glenda::protocol::resource::INIT_ENDPOINT,
        INIT_SLOT,
    ) {
        log!("Failed to get init endpoint: {:?}", e);
        return 1;
    }

    let mut init_client = InitClient::new(Endpoint::from(INIT_SLOT));

    let mut server = NexusManager::new(&mut res_client, &mut init_client);

    if let Err(e) = server.init() {
        log!("Failed to init: {:?}", e);
        return 1;
    }

    if let Err(e) =
        server.listen(glenda::cap::ENDPOINT_CAP, glenda::cap::REPLY_SLOT, glenda::cap::RECV_SLOT)
    {
        log!("Failed to listen: {:?}", e);
        return 1;
    }

    if let Err(e) = server.run() {
        log!("Exited with error: {:?}", e);
        return 1;
    }
    0
}
