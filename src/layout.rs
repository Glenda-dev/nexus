use glenda::cap::{CapPtr, Endpoint};

pub const DEVICE_SLOT: CapPtr = CapPtr::from(10);
pub const DEVICE_CAP: Endpoint = Endpoint::from(DEVICE_SLOT);
pub const INIT_SLOT: CapPtr = CapPtr::from(11);
