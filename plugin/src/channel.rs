use {
    crate::protobuf::ProtobufMessage,
    solana_sdk::clock::Slot,
    std::{cell::UnsafeCell, thread_local},
};

#[derive(Debug)]
pub struct GeyserMessages {
    //
}

impl GeyserMessages {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        todo!()
    }

    pub fn push(&self, _slot: Slot, message: ProtobufMessage) {
        thread_local! {
            // 16MiB should be enough for any message
            // except blockinfo with rewards list (what doesn't make sense after partition reward, starts from epoch 706)
            static BUFFER: UnsafeCell<Vec<u8>> = UnsafeCell::new(Vec::with_capacity(16 * 1024 * 1024));
        }

        let _message = BUFFER.with(|buffer| message.encode(unsafe { &mut *buffer.get() }));
    }
}
