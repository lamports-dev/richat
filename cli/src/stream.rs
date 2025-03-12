use {
    anyhow::Context,
    futures::stream::{BoxStream, StreamExt},
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    prost::Message,
    richat_client::error::ReceiveError,
    richat_proto::{
        convert_from,
        geyser::{
            subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlock, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
            SubscribeUpdateTransactionInfo,
        },
    },
    serde_json::{json, Value},
    solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::UiTransactionEncoding,
    std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
    tracing::{error, info},
};

pub async fn handle_stream(
    stream: BoxStream<'static, Result<SubscribeUpdate, ReceiveError>>,
    pb_multi: Arc<MultiProgress>,
    stats: bool,
) -> anyhow::Result<()> {
    let mut pb_accounts_c = 0;
    let pb_accounts = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("accounts"))?;
    let mut pb_slots_c = 0;
    let pb_slots = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("slots"))?;
    let mut pb_txs_c = 0;
    let pb_txs = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("transactions"))?;
    let mut pb_txs_st_c = 0;
    let pb_txs_st = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("transactions statuses"))?;
    let mut pb_entries_c = 0;
    let pb_entries = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("entries"))?;
    let mut pb_blocks_mt_c = 0;
    let pb_blocks_mt = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("blocks meta"))?;
    let mut pb_blocks_c = 0;
    let pb_blocks = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("blocks"))?;
    let mut pb_pp_c = 0;
    let pb_pp = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("ping/pong"))?;
    let mut pb_total_c = 0;
    let pb_total = crate_progress_bar(&pb_multi, ProgressBarTpl::Total)?;

    tokio::pin!(stream);
    while let Some(message) = stream.next().await {
        let msg = match message {
            Ok(msg) => msg,
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        };

        if stats {
            let encoded_len = msg.encoded_len() as u64;
            let (pb_c, pb) = match msg.update_oneof {
                Some(UpdateOneof::Account(_)) => (&mut pb_accounts_c, &pb_accounts),
                Some(UpdateOneof::Slot(_)) => (&mut pb_slots_c, &pb_slots),
                Some(UpdateOneof::Transaction(_)) => (&mut pb_txs_c, &pb_txs),
                Some(UpdateOneof::TransactionStatus(_)) => (&mut pb_txs_st_c, &pb_txs_st),
                Some(UpdateOneof::Entry(_)) => (&mut pb_entries_c, &pb_entries),
                Some(UpdateOneof::BlockMeta(_)) => (&mut pb_blocks_mt_c, &pb_blocks_mt),
                Some(UpdateOneof::Block(_)) => (&mut pb_blocks_c, &pb_blocks),
                Some(UpdateOneof::Ping(_)) => (&mut pb_pp_c, &pb_pp),
                Some(UpdateOneof::Pong(_)) => (&mut pb_pp_c, &pb_pp),
                None => {
                    pb_multi.println("update not found in the message")?;
                    break;
                }
            };
            *pb_c += 1;
            pb.set_message(format_thousands(*pb_c));
            pb.inc(encoded_len);
            pb_total_c += 1;
            pb_total.set_message(format_thousands(pb_total_c));
            pb_total.inc(encoded_len);

            continue;
        }

        let filters = msg.filters;
        let created_at: SystemTime = msg
            .created_at
            .ok_or(anyhow::anyhow!("no created_at in the message"))?
            .try_into()
            .context("failed to parse created_at")?;
        match msg.update_oneof {
            Some(UpdateOneof::Account(msg)) => {
                let account = msg
                    .account
                    .ok_or(anyhow::anyhow!("no account in the message"))?;
                let mut value = create_pretty_account(account)?;
                value["isStartup"] = json!(msg.is_startup);
                value["slot"] = json!(msg.slot);
                print_update("account", created_at, &filters, value);
            }
            Some(UpdateOneof::Slot(msg)) => {
                let status =
                    SlotStatus::try_from(msg.status).context("failed to decode commitment")?;
                print_update(
                    "slot",
                    created_at,
                    &filters,
                    json!({
                        "slot": msg.slot,
                        "parent": msg.parent,
                        "status": status.as_str_name(),
                        "deadError": msg.dead_error,
                    }),
                );
            }
            Some(UpdateOneof::Transaction(msg)) => {
                let tx = msg
                    .transaction
                    .ok_or(anyhow::anyhow!("no transaction in the message"))?;
                let mut value = create_pretty_transaction(tx)?;
                value["slot"] = json!(msg.slot);
                print_update("transaction", created_at, &filters, value);
            }
            Some(UpdateOneof::TransactionStatus(msg)) => {
                print_update(
                    "transactionStatus",
                    created_at,
                    &filters,
                    json!({
                        "slot": msg.slot,
                        "signature": Signature::try_from(msg.signature.as_slice()).context("invalid signature")?.to_string(),
                        "isVote": msg.is_vote,
                        "index": msg.index,
                        "err": convert_from::create_tx_error(msg.err.as_ref())
                            .map_err(|error| anyhow::anyhow!(error))
                            .context("invalid error")?,
                    }),
                );
            }
            Some(UpdateOneof::Entry(msg)) => {
                print_update("entry", created_at, &filters, create_pretty_entry(msg)?);
            }
            Some(UpdateOneof::BlockMeta(msg)) => {
                print_update(
                    "blockmeta",
                    created_at,
                    &filters,
                    create_pretty_blockmeta(msg)?,
                );
            }
            Some(UpdateOneof::Block(msg)) => {
                print_update("block", created_at, &filters, create_pretty_block(msg)?);
            }
            Some(UpdateOneof::Ping(_)) => {}
            Some(UpdateOneof::Pong(_)) => {}
            None => {
                error!("update not found in the message");
                break;
            }
        }
    }
    info!("stream closed");
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressBarTpl {
    Msg(&'static str),
    Total,
}

fn crate_progress_bar(
    pb: &MultiProgress,
    pb_t: ProgressBarTpl,
) -> Result<ProgressBar, indicatif::style::TemplateError> {
    let pb = pb.add(ProgressBar::no_length());
    let tpl = match pb_t {
        ProgressBarTpl::Msg(kind) => {
            format!("{{spinner}} {kind}: {{msg}} / ~{{bytes}} (~{{bytes_per_sec}})")
        }
        ProgressBarTpl::Total => {
            "{spinner} total: {msg} / ~{bytes} (~{bytes_per_sec}) in {elapsed_precise}".to_owned()
        }
    };
    pb.set_style(ProgressStyle::with_template(&tpl)?);
    Ok(pb)
}

fn format_thousands(value: u64) -> String {
    value
        .to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .expect("invalid number")
        .join(",")
}

fn create_pretty_account(account: SubscribeUpdateAccountInfo) -> anyhow::Result<Value> {
    Ok(json!({
        "pubkey": Pubkey::try_from(account.pubkey).map_err(|_| anyhow::anyhow!("invalid account pubkey"))?.to_string(),
        "lamports": account.lamports,
        "owner": Pubkey::try_from(account.owner).map_err(|_| anyhow::anyhow!("invalid account owner"))?.to_string(),
        "executable": account.executable,
        "rentEpoch": account.rent_epoch,
        "data": const_hex::encode(account.data),
        "writeVersion": account.write_version,
        "txnSignature": account
            .txn_signature
            .map(|sig| Signature::try_from(sig).map_err(|_| anyhow::anyhow!("invalid txn signature")))
            .transpose()?
            .map(|sig| sig.to_string()),
    }))
}

fn create_pretty_transaction(tx: SubscribeUpdateTransactionInfo) -> anyhow::Result<Value> {
    Ok(json!({
        "signature": Signature::try_from(tx.signature.as_slice()).context("invalid signature")?.to_string(),
        "isVote": tx.is_vote,
        "tx": convert_from::create_tx_with_meta(tx)
            .map_err(|error| anyhow::anyhow!(error))
            .context("invalid tx with meta")?
            .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
            .context("failed to encode transaction")?,
    }))
}

fn create_pretty_entry(msg: SubscribeUpdateEntry) -> anyhow::Result<Value> {
    Ok(json!({
        "slot": msg.slot,
        "index": msg.index,
        "numHashes": msg.num_hashes,
        "hash": Hash::new_from_array(<[u8; 32]>::try_from(msg.hash.as_slice()).context("invalid entry hash")?).to_string(),
        "executedTransactionCount": msg.executed_transaction_count,
        "startingTransactionIndex": msg.starting_transaction_index,
    }))
}

fn create_pretty_blockmeta(msg: SubscribeUpdateBlockMeta) -> anyhow::Result<Value> {
    Ok(json!({
        "slot": msg.slot,
        "blockhash": msg.blockhash,
        "rewards": if let Some(rewards) = msg.rewards {
            Some(convert_from::create_rewards_obj(rewards).map_err(|error| anyhow::anyhow!(error))?)
        } else {
            None
        },
        "blockTime": msg.block_time.map(|obj| obj.timestamp),
        "blockHeight": msg.block_height.map(|obj| obj.block_height),
        "parentSlot": msg.parent_slot,
        "parentBlockhash": msg.parent_blockhash,
        "executedTransactionCount": msg.executed_transaction_count,
        "entriesCount": msg.entries_count,
    }))
}

fn create_pretty_block(msg: SubscribeUpdateBlock) -> anyhow::Result<Value> {
    Ok(json!({
        "slot": msg.slot,
        "blockhash": msg.blockhash,
        "rewards": if let Some(rewards) = msg.rewards {
            Some(convert_from::create_rewards_obj(rewards).map_err(|error| anyhow::anyhow!(error))?)
        } else {
            None
        },
        "blockTime": msg.block_time.map(|obj| obj.timestamp),
        "blockHeight": msg.block_height.map(|obj| obj.block_height),
        "parentSlot": msg.parent_slot,
        "parentBlockhash": msg.parent_blockhash,
        "executedTransactionCount": msg.executed_transaction_count,
        "transactions": msg.transactions.into_iter().map(create_pretty_transaction).collect::<Result<Value, _>>()?,
        "updatedAccountCount": msg.updated_account_count,
        "accounts": msg.accounts.into_iter().map(create_pretty_account).collect::<Result<Value, _>>()?,
        "entriesCount": msg.entries_count,
        "entries": msg.entries.into_iter().map(create_pretty_entry).collect::<Result<Value, _>>()?,
    }))
}

fn print_update(kind: &str, created_at: SystemTime, filters: &[String], value: Value) {
    let unix_since = created_at
        .duration_since(UNIX_EPOCH)
        .expect("valid system time");
    info!(
        "{kind} ({}) at {}.{:0>6}: {}",
        filters.join(","),
        unix_since.as_secs(),
        unix_since.subsec_micros(),
        serde_json::to_string(&value).expect("json serialization failed")
    );
}
