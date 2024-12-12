use {
    super::{bytes_encode, bytes_encoded_len, field_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    prost::encoding::{self, encode_key, encode_varint, message, WireType},
    solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub struct Transaction {
    slot: Slot,
    transaction: proto::TransactionInfo,
}

impl super::super::Message for Transaction {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        let index = self.transaction.index as u64;

        encode_key(1, WireType::LengthDelimited, buf);
        encode_varint(self.transaction_encoded_len() as u64, buf);

        bytes_encode(1, self.transaction.signature.as_ref(), buf);
        encoding::bool::encode(2, &self.transaction.is_vote, buf);
        message::encode(3, &self.transaction.transaction, buf);
        message::encode(4, &self.transaction.transaction_status_meta, buf);
        encoding::uint64::encode(5, &index, buf);

        encoding::uint64::encode(2, &self.slot, buf)
    }
    fn encoded_len(&self) -> usize {
        field_encoded_len(1, self.transaction_encoded_len())
            + encoding::uint64::encoded_len(2, &self.slot)
    }
}

impl Transaction {
    pub fn new(slot: Slot, transaction: &ReplicaTransactionInfoV2<'_>) -> Self {
        Self {
            slot,
            transaction: proto::TransactionInfo::from(transaction),
        }
    }
    fn transaction_encoded_len(&self) -> usize {
        let index = self.transaction.index as u64;

        bytes_encoded_len(1u32, self.transaction.signature.as_ref())
            + encoding::bool::encoded_len(2u32, &self.transaction.is_vote)
            + message::encoded_len(3u32, &self.transaction.transaction)
            + message::encoded_len(4u32, &self.transaction.transaction_status_meta)
            + encoding::uint64::encoded_len(5u32, &index)
    }
}

mod proto {
    mod convert_to {
        use {
            crate::protobuf::encoding::proto::convert_to,
            solana_sdk::{
                instruction::CompiledInstruction,
                message::{
                    v0::{LoadedMessage, MessageAddressTableLookup},
                    LegacyMessage, MessageHeader, SanitizedMessage,
                },
                pubkey::Pubkey,
                signature::Signature,
                transaction::{SanitizedTransaction, TransactionError},
                transaction_context::TransactionReturnData,
            },
            solana_transaction_status::{
                InnerInstruction, InnerInstructions, TransactionStatusMeta, TransactionTokenBalance,
            },
        };

        pub fn create_transaction(tx: &SanitizedTransaction) -> super::Transaction {
            super::Transaction {
                signatures: tx
                    .signatures()
                    .iter()
                    .map(|signature| <Signature as AsRef<[u8]>>::as_ref(signature).into())
                    .collect(), // TODO: try to remove allocation
                message: Some(create_message(tx.message())),
            }
        }

        pub fn create_message(message: &SanitizedMessage) -> super::Message {
            match message {
                SanitizedMessage::Legacy(LegacyMessage { message, .. }) => super::Message {
                    header: Some(create_header(&message.header)),
                    account_keys: create_pubkeys(&message.account_keys),
                    recent_blockhash: message.recent_blockhash.to_bytes().into(),
                    instructions: create_instructions(&message.instructions),
                    versioned: false,
                    address_table_lookups: Vec::new(),
                },
                SanitizedMessage::V0(LoadedMessage { message, .. }) => super::Message {
                    header: Some(create_header(&message.header)),
                    account_keys: create_pubkeys(&message.account_keys),
                    recent_blockhash: message.recent_blockhash.to_bytes().into(),
                    instructions: create_instructions(&message.instructions),
                    versioned: true,
                    address_table_lookups: create_lookups(&message.address_table_lookups),
                },
            }
        }

        pub const fn create_header(header: &MessageHeader) -> super::MessageHeader {
            super::MessageHeader {
                num_required_signatures: header.num_required_signatures as u32,
                num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
                num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
            }
        }

        pub fn create_pubkeys(pubkeys: &[Pubkey]) -> Vec<Vec<u8>> {
            pubkeys
                .iter()
                .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
                .collect() // TODO: try to remove allocation
        }

        pub fn create_instructions(ixs: &[CompiledInstruction]) -> Vec<super::CompiledInstruction> {
            ixs.iter().map(create_instruction).collect() // TODO: try to remove allocation
        }

        pub fn create_instruction(ix: &CompiledInstruction) -> super::CompiledInstruction {
            super::CompiledInstruction {
                program_id_index: ix.program_id_index as u32,
                accounts: ix.accounts.clone(), // TODO: try to remove allocation
                data: ix.data.clone(),         // TODO: try to remove allocation
            }
        }

        pub fn create_lookups(
            lookups: &[MessageAddressTableLookup],
        ) -> Vec<super::MessageAddressTableLookup> {
            lookups.iter().map(create_lookup).collect() // TODO: try to remove allocation
        }

        pub fn create_lookup(
            lookup: &MessageAddressTableLookup,
        ) -> super::MessageAddressTableLookup {
            super::MessageAddressTableLookup {
                account_key: <Pubkey as AsRef<[u8]>>::as_ref(&lookup.account_key).into(),
                writable_indexes: lookup.writable_indexes.clone(), // TODO: try to remove allocation
                readonly_indexes: lookup.readonly_indexes.clone(), // TODO: try to remove allocation
            }
        }

        pub fn create_transaction_meta(
            meta: &TransactionStatusMeta,
        ) -> super::TransactionStatusMeta {
            let TransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_addresses,
                return_data,
                compute_units_consumed,
            } = meta;
            let err = create_transaction_error(status);
            let inner_instructions_none = inner_instructions.is_none();
            let inner_instructions = inner_instructions
                .as_deref()
                .map(create_inner_instructions_vec)
                .unwrap_or_default();
            let log_messages_none = log_messages.is_none();
            let log_messages = log_messages.clone().unwrap_or_default(); // TODO: try to remove allocation
            let pre_token_balances = pre_token_balances
                .as_deref()
                .map(create_token_balances)
                .unwrap_or_default();
            let post_token_balances = post_token_balances
                .as_deref()
                .map(create_token_balances)
                .unwrap_or_default();
            let rewards = rewards
                .as_deref()
                .map(convert_to::create_rewards)
                .unwrap_or_default();
            let loaded_writable_addresses = create_pubkeys(&loaded_addresses.writable);
            let loaded_readonly_addresses = create_pubkeys(&loaded_addresses.readonly);

            super::TransactionStatusMeta {
                err,
                fee: *fee,
                pre_balances: pre_balances.clone(),
                post_balances: post_balances.clone(),
                inner_instructions,
                inner_instructions_none,
                log_messages,
                log_messages_none,
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_writable_addresses,
                loaded_readonly_addresses,
                return_data: return_data.as_ref().map(create_return_data),
                return_data_none: return_data.is_none(),
                compute_units_consumed: *compute_units_consumed,
            }
        }

        pub fn create_transaction_error(
            status: &Result<(), TransactionError>,
        ) -> Option<super::TransactionError> {
            match status {
                Ok(()) => None,
                Err(err) => Some(super::TransactionError {
                    err: bincode::serialize(&err).expect("transaction error to serialize to bytes"), // TODO: try to remove allocation
                }),
            }
        }

        pub fn create_inner_instructions_vec(
            ixs: &[InnerInstructions],
        ) -> Vec<super::InnerInstructions> {
            ixs.iter().map(create_inner_instructions).collect() // TODO: try to remove allocation
        }

        pub fn create_inner_instructions(
            instructions: &InnerInstructions,
        ) -> super::InnerInstructions {
            super::InnerInstructions {
                index: instructions.index as u32,
                instructions: create_inner_instruction_vec(&instructions.instructions),
            }
        }

        pub fn create_inner_instruction_vec(
            ixs: &[InnerInstruction],
        ) -> Vec<super::InnerInstruction> {
            ixs.iter().map(create_inner_instruction).collect() // TODO: try to remove allocation
        }

        pub fn create_inner_instruction(instruction: &InnerInstruction) -> super::InnerInstruction {
            super::InnerInstruction {
                program_id_index: instruction.instruction.program_id_index as u32,
                accounts: instruction.instruction.accounts.clone(), // TODO: try to remove allocation
                data: instruction.instruction.data.clone(), // TODO: try to remove allocation
                stack_height: instruction.stack_height,
            }
        }

        pub fn create_token_balances(
            balances: &[TransactionTokenBalance],
        ) -> Vec<super::TokenBalance> {
            balances.iter().map(create_token_balance).collect() // TODO: try to remove allocation
        }

        pub fn create_token_balance(balance: &TransactionTokenBalance) -> super::TokenBalance {
            super::TokenBalance {
                account_index: balance.account_index as u32,
                mint: balance.mint.clone(), // TODO: try to remove allocation
                ui_token_amount: Some(super::UiTokenAmount {
                    ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
                    decimals: balance.ui_token_amount.decimals as u32,
                    amount: balance.ui_token_amount.amount.clone(), // TODO: try to remove allocation
                    ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(), // TODO: try to remove allocation
                }),
                owner: balance.owner.clone(), // TODO: try to remove allocation
                program_id: balance.program_id.clone(), // TODO: try to remove allocation
            }
        }

        pub fn create_return_data(return_data: &TransactionReturnData) -> super::ReturnData {
            super::ReturnData {
                program_id: return_data.program_id.to_bytes().into(),
                data: return_data.data.clone(),
            }
        }
    }

    use {
        crate::protobuf::encoding::proto,
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
        solana_sdk::{pubkey::Pubkey, signature::Signature},
        std::collections::HashSet,
    };

    #[derive(Debug)]
    pub struct TransactionInfo {
        pub signature: Signature,
        pub is_vote: bool,
        pub transaction: Transaction,
        pub transaction_status_meta: TransactionStatusMeta,
        pub index: usize,
        pub account_keys: HashSet<Pubkey>,
    }

    impl From<&ReplicaTransactionInfoV2<'_>> for TransactionInfo {
        fn from(value: &ReplicaTransactionInfoV2<'_>) -> Self {
            let account_keys = value
                .transaction
                .message()
                .account_keys()
                .iter()
                .copied()
                .collect(); // TODO: try to remove allocation

            Self {
                signature: *value.signature,
                is_vote: value.is_vote,
                transaction: convert_to::create_transaction(value.transaction),
                transaction_status_meta: convert_to::create_transaction_meta(
                    value.transaction_status_meta,
                ),
                index: value.index,
                account_keys,
            }
        }
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct Transaction {
        #[prost(bytes = "vec", repeated, tag = "1")]
        pub signatures: Vec<Vec<u8>>,
        #[prost(message, optional, tag = "2")]
        pub message: Option<Message>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct Message {
        #[prost(message, optional, tag = "1")]
        pub header: Option<MessageHeader>,
        #[prost(bytes = "vec", repeated, tag = "2")]
        pub account_keys: Vec<Vec<u8>>,
        #[prost(bytes = "vec", tag = "3")]
        pub recent_blockhash: Vec<u8>,
        #[prost(message, repeated, tag = "4")]
        pub instructions: Vec<CompiledInstruction>,
        #[prost(bool, tag = "5")]
        pub versioned: bool,
        #[prost(message, repeated, tag = "6")]
        pub address_table_lookups: Vec<MessageAddressTableLookup>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct MessageHeader {
        #[prost(uint32, tag = "1")]
        pub num_required_signatures: u32,
        #[prost(uint32, tag = "2")]
        pub num_readonly_signed_accounts: u32,
        #[prost(uint32, tag = "3")]
        pub num_readonly_unsigned_accounts: u32,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct MessageAddressTableLookup {
        #[prost(bytes = "vec", tag = "1")]
        pub account_key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub writable_indexes: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        pub readonly_indexes: Vec<u8>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct TransactionStatusMeta {
        #[prost(message, optional, tag = "1")]
        pub err: Option<TransactionError>,
        #[prost(uint64, tag = "2")]
        pub fee: u64,
        #[prost(uint64, repeated, tag = "3")]
        pub pre_balances: Vec<u64>,
        #[prost(uint64, repeated, tag = "4")]
        pub post_balances: Vec<u64>,
        #[prost(message, repeated, tag = "5")]
        pub inner_instructions: Vec<InnerInstructions>,
        #[prost(bool, tag = "10")]
        pub inner_instructions_none: bool,
        #[prost(string, repeated, tag = "6")]
        pub log_messages: Vec<String>,
        #[prost(bool, tag = "11")]
        pub log_messages_none: bool,
        #[prost(message, repeated, tag = "7")]
        pub pre_token_balances: Vec<TokenBalance>,
        #[prost(message, repeated, tag = "8")]
        pub post_token_balances: Vec<TokenBalance>,
        #[prost(message, repeated, tag = "9")]
        pub rewards: Vec<proto::Reward>,
        #[prost(bytes = "vec", repeated, tag = "12")]
        pub loaded_writable_addresses: Vec<Vec<u8>>,
        #[prost(bytes = "vec", repeated, tag = "13")]
        pub loaded_readonly_addresses: Vec<Vec<u8>>,
        #[prost(message, optional, tag = "14")]
        pub return_data: Option<ReturnData>,
        #[prost(bool, tag = "15")]
        pub return_data_none: bool,
        #[prost(uint64, optional, tag = "16")]
        pub compute_units_consumed: Option<u64>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct TransactionError {
        #[prost(bytes = "vec", tag = "1")]
        pub err: Vec<u8>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct InnerInstructions {
        #[prost(uint32, tag = "1")]
        pub index: u32,
        #[prost(message, repeated, tag = "2")]
        pub instructions: Vec<InnerInstruction>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct InnerInstruction {
        #[prost(uint32, tag = "1")]
        pub program_id_index: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub accounts: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        pub data: Vec<u8>,
        #[prost(uint32, optional, tag = "4")]
        pub stack_height: Option<u32>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct CompiledInstruction {
        #[prost(uint32, tag = "1")]
        pub program_id_index: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub accounts: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        pub data: Vec<u8>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct TokenBalance {
        #[prost(uint32, tag = "1")]
        pub account_index: u32,
        #[prost(string, tag = "2")]
        pub mint: String,
        #[prost(message, optional, tag = "3")]
        pub ui_token_amount: Option<UiTokenAmount>,
        #[prost(string, tag = "4")]
        pub owner: String,
        #[prost(string, tag = "5")]
        pub program_id: String,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct UiTokenAmount {
        #[prost(double, tag = "1")]
        pub ui_amount: f64,
        #[prost(uint32, tag = "2")]
        pub decimals: u32,
        #[prost(string, tag = "3")]
        pub amount: String,
        #[prost(string, tag = "4")]
        pub ui_amount_string: String,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct ReturnData {
        #[prost(bytes = "vec", tag = "1")]
        pub program_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub data: Vec<u8>,
    }
}
