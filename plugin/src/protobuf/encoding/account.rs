use {
    super::{bytes_encode, bytes_encoded_len, field_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    prost::encoding::{self, encode_key, encode_varint, WireType},
    solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub struct Account<'a> {
    slot: Slot,
    account: &'a ReplicaAccountInfoV3<'a>,
}

impl<'a> prost::Message for Account<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        encode_key(1, WireType::LengthDelimited, buf);
        encode_varint(self.account_encoded_len() as u64, buf);
        bytes_encode(1, self.account.pubkey, buf);
        encoding::uint64::encode(2, &self.account.lamports, buf);
        bytes_encode(3, self.account.owner, buf);
        encoding::bool::encode(4, &self.account.executable, buf);
        encoding::uint64::encode(5, &self.account.rent_epoch, buf);
        encoding::uint64::encode(6, &self.account.write_version, buf);
        if let Some(txn) = self.account.txn {
            bytes_encode(7, txn.signature().as_ref(), buf);
        }

        encoding::uint64::encode(2, &self.slot, buf)
    }

    fn encoded_len(&self) -> usize {
        field_encoded_len(1, self.account_encoded_len())
            + encoding::uint64::encoded_len(2, &self.slot)
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl hyper::body::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

impl<'a> Account<'a> {
    pub const fn new(slot: Slot, account: &'a ReplicaAccountInfoV3<'a>) -> Self {
        Self { slot, account }
    }

    fn account_encoded_len(&self) -> usize {
        bytes_encoded_len(1u32, self.account.pubkey)
            + encoding::uint64::encoded_len(2u32, &self.account.lamports)
            + bytes_encoded_len(3u32, self.account.owner)
            + encoding::bool::encoded_len(4u32, &self.account.executable)
            + encoding::uint64::encoded_len(5u32, &self.account.rent_epoch)
            + encoding::uint64::encoded_len(6u32, &self.account.write_version)
            + self
                .account
                .txn
                .map_or(0, |txn| bytes_encoded_len(7u32, txn.signature().as_ref()))
    }
}
