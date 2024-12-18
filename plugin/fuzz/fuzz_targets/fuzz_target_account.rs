#![no_main]

use libfuzzer_sys::fuzz_target;

#[derive(arbitrary::Arbitrary, Debug)]
pub struct Account<'a> {
    pubkey: &'a [u8],
    lamports: u64,
    owner: &'a [u8],
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
    write_version: u64,
}

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
});
