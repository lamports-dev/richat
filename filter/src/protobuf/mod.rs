use {
    prost::{
        bytes::{Buf, BufMut},
        encoding::{DecodeContext, WireType},
        DecodeError, Message,
    },
    prost_types::Timestamp,
    yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof,
};

#[derive(Debug, Clone)]
pub struct SubscribeUpdateMessage<'a> {
    // #[prost(message, repeated, tag = "1")]
    pub filters: &'a [&'a str],
    pub update_oneof: Option<UpdateOneof>,
    // #[prost(message, optional, tag = "11")]
    pub created_at: Option<Timestamp>,
}

impl<'a> Message for SubscribeUpdateMessage<'a> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        todo!()
    }

    fn encoded_len(&self) -> usize {
        todo!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}
