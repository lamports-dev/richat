use {
    super::{bytes_encode, bytes_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    prost::encoding::{self, WireType},
    solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub struct Account<'a> {
    account: &'a ReplicaAccountInfoV3<'a>,
    slot: Slot,
}

impl<'a> prost::Message for Account<'a> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        let wrapper = ReplicaAccountInfoWrapper(self.account);
        encoding::message::encode(1, &wrapper, buf);
        if self.slot != 0 {
            encoding::uint64::encode(2, &self.slot, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let wrapper = ReplicaAccountInfoWrapper(self.account);
        encoding::message::encoded_len(1, &wrapper)
            + if self.slot != 0 {
                encoding::uint64::encoded_len(2, &self.slot)
            } else {
                0
            }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl bytes::Buf,
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
}

#[derive(Debug)]
struct ReplicaAccountInfoWrapper<'a>(&'a ReplicaAccountInfoV3<'a>);

impl<'a> prost::Message for ReplicaAccountInfoWrapper<'a> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        bytes_encode(1, self.0.pubkey, buf);
        if self.0.lamports != 0 {
            encoding::uint64::encode(2, &self.0.lamports, buf);
        };
        bytes_encode(3, self.0.owner, buf);
        if self.0.executable {
            encoding::bool::encode(4, &self.0.executable, buf);
        }
        if self.0.rent_epoch != 0 {
            encoding::uint64::encode(5, &self.0.rent_epoch, buf);
        }
        bytes_encode(6, self.0.data, buf);
        if self.0.write_version != 0 {
            encoding::uint64::encode(7, &self.0.write_version, buf);
        }
        if let Some(txn) = self.0.txn {
            bytes_encode(8, txn.signature().as_ref(), buf);
        }
    }
    fn encoded_len(&self) -> usize {
        bytes_encoded_len(1, self.0.pubkey)
            + if self.0.lamports != 0 {
                encoding::uint64::encoded_len(2, &self.0.lamports)
            } else {
                0
            }
            + bytes_encoded_len(3, self.0.owner)
            + if self.0.executable {
                encoding::bool::encoded_len(4, &self.0.executable)
            } else {
                0
            }
            + if self.0.rent_epoch != 0 {
                encoding::uint64::encoded_len(5, &self.0.rent_epoch)
            } else {
                0
            }
            + bytes_encoded_len(6, self.0.data)
            + if self.0.write_version != 0 {
                encoding::uint64::encoded_len(7, &self.0.write_version)
            } else {
                0
            }
            + self
                .0
                .txn
                .map_or(0, |txn| bytes_encoded_len(8, txn.signature().as_ref()))
    }
    fn clear(&mut self) {
        unimplemented!()
    }
    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}
