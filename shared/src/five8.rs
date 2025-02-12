use {
    five8::DecodeError,
    solana_sdk::{
        pubkey::{ParsePubkeyError, Pubkey},
        signature::{ParseSignatureError, Signature},
    },
};

pub fn pubkey_decode<I: AsRef<[u8]>>(encoded: I) -> Result<Pubkey, ParsePubkeyError> {
    let mut out = [0; 32];
    match five8::decode_32(encoded, &mut out) {
        Ok(()) => Ok(Pubkey::new_from_array(out)),
        Err(DecodeError::InvalidChar(_)) => Err(ParsePubkeyError::Invalid),
        Err(DecodeError::TooLong) => Err(ParsePubkeyError::WrongSize),
        Err(DecodeError::TooShort) => Err(ParsePubkeyError::WrongSize),
        Err(DecodeError::LargestTermTooHigh) => Err(ParsePubkeyError::WrongSize),
        Err(DecodeError::OutputTooLong) => Err(ParsePubkeyError::WrongSize),
    }
}

pub fn pubkey_encode(bytes: &[u8; 32]) -> String {
    let mut out = [0; 44];
    let len = five8::encode_32(bytes, &mut out) as usize;
    out[0..len].iter().copied().map(char::from).collect()
}

pub fn signature_decode<I: AsRef<[u8]>>(encoded: I) -> Result<Signature, ParseSignatureError> {
    let mut out = [0; 64];
    match five8::decode_64(encoded, &mut out) {
        Ok(()) => Ok(Signature::from(out)),
        Err(DecodeError::InvalidChar(_)) => Err(ParseSignatureError::Invalid),
        Err(DecodeError::TooLong) => Err(ParseSignatureError::WrongSize),
        Err(DecodeError::TooShort) => Err(ParseSignatureError::WrongSize),
        Err(DecodeError::LargestTermTooHigh) => Err(ParseSignatureError::WrongSize),
        Err(DecodeError::OutputTooLong) => Err(ParseSignatureError::WrongSize),
    }
}

pub fn signature_encode(bytes: &[u8; 64]) -> String {
    let mut out = [0; 88];
    let len = five8::encode_64(bytes, &mut out) as usize;
    out[0..len].iter().copied().map(char::from).collect()
}
