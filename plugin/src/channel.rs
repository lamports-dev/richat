use {crate::protobuf::ProtobufMessage, std::cell::RefCell};

/// 16 MiB, `should be` enough for any message
const BUFFER_CAPACITY: usize = 16 * 1024 * 1024;

thread_local! {
    // except blockinfo with rewards list (what doesn't make sense after partition reward, starts from epoch 706)
    static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUFFER_CAPACITY));
}

#[derive(Debug)]
pub struct GeyserMessages {
    //
}

impl GeyserMessages {
    pub fn new() -> Self {
        todo!()
    }

    pub fn push(&self, message: ProtobufMessage) -> Vec<u8> {
        BUFFER.with(|cell| {
            let mut buffer = cell.borrow_mut();
            let message = message.encode(&mut buffer);
            drop(buffer);
            message
        })
        // FIXME: do we need to return encoded message??
    }
}
