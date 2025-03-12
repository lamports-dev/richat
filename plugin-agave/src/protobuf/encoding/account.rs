use {
    super::{bytes_encode, bytes_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    prost::encoding::{self, WireType},
    solana_sdk::clock::Slot,
    std::ops::Deref,
};

#[derive(Debug)]
struct ReplicaWrapper<'a>(&'a ReplicaAccountInfoV3<'a>);

impl<'a> Deref for ReplicaWrapper<'a> {
    type Target = &'a ReplicaAccountInfoV3<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for ReplicaWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        bytes_encode(1, self.pubkey, buf);
        if self.lamports != 0 {
            encoding::uint64::encode(2, &self.lamports, buf);
        };
        bytes_encode(3, self.owner, buf);
        if self.executable {
            encoding::bool::encode(4, &self.executable, buf);
        }
        if self.rent_epoch != 0 {
            encoding::uint64::encode(5, &self.rent_epoch, buf);
        }
        if !self.data.is_empty() {
            bytes_encode(6, self.data, buf);
        }
        if self.write_version != 0 {
            encoding::uint64::encode(7, &self.write_version, buf);
        }
        if let Some(txn) = self.txn {
            bytes_encode(8, txn.signature().as_ref(), buf);
        }
    }

    fn encoded_len(&self) -> usize {
        bytes_encoded_len(1, self.pubkey)
            + if self.lamports != 0 {
                encoding::uint64::encoded_len(2, &self.lamports)
            } else {
                0
            }
            + bytes_encoded_len(3, self.owner)
            + if self.executable {
                encoding::bool::encoded_len(4, &self.executable)
            } else {
                0
            }
            + if self.rent_epoch != 0 {
                encoding::uint64::encoded_len(5, &self.rent_epoch)
            } else {
                0
            }
            + if !self.data.is_empty() {
                bytes_encoded_len(6, self.data)
            } else {
                0
            }
            + if self.write_version != 0 {
                encoding::uint64::encoded_len(7, &self.write_version)
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

#[derive(Debug)]
pub struct Account<'a> {
    account: &'a ReplicaAccountInfoV3<'a>,
    slot: Slot,
}

impl<'a> Account<'a> {
    pub const fn new(slot: Slot, account: &'a ReplicaAccountInfoV3<'a>) -> Self {
        Self { slot, account }
    }
}

impl prost::Message for Account<'_> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        let wrapper = ReplicaWrapper(self.account);
        encoding::message::encode(1, &wrapper, buf);
        if self.slot != 0 {
            encoding::uint64::encode(2, &self.slot, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let wrapper = ReplicaWrapper(self.account);
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
