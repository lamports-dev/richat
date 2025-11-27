use {
    criterion::{criterion_group, criterion_main, BatchSize, Criterion},
    prost_types::Timestamp,
    richat::channel::{ParsedMessage, SenderShared, SharedChannel},
    richat_filter::message::MessageSlot,
    richat_proto::geyser::SlotStatus,
    solana_sdk::clock::Slot,
    std::{hint::black_box, sync::Arc, time::Duration},
};

fn make_slot_message(slot: Slot) -> ParsedMessage {
    ParsedMessage::Slot(Arc::new(MessageSlot::Prost {
        slot,
        parent: None,
        status: SlotStatus::SlotProcessed,
        dead_error: None,
        created_at: Timestamp {
            seconds: 0,
            nanos: 0,
        },
        size: 128,
    }))
}

fn bench_channel_push(c: &mut Criterion) {
    let count: usize = 8_192;
    let messages: Vec<ParsedMessage> = (0..count as u64)
        .map(|slot| make_slot_message(slot))
        .collect();

    let mut group = c.benchmark_group("channel_ring");
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("push_only", |b| {
        b.iter_batched(
            || {
                let shared = Arc::new(SharedChannel::new(1 << 16, false));
                let sender = SenderShared::new(&shared, 1 << 16, usize::MAX);
                (shared, sender)
            },
            |(shared, mut sender)| {
                for msg in messages.iter() {
                    sender.push(msg.slot(), msg.clone(), None);
                }
                black_box(shared.tail())
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, bench_channel_push);
criterion_main!(benches);
