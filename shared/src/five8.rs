use {
    five8::DecodeError,
    solana_sdk::{
        pubkey::{ParsePubkeyError, Pubkey},
        signature::{ParseSignatureError, Signature},
    },
};

pub fn pubkey_parse<I: AsRef<[u8]>>(encoded: I) -> Result<Pubkey, ParsePubkeyError> {
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

pub fn signature_parse<I: AsRef<[u8]>>(encoded: I) -> Result<Signature, ParseSignatureError> {
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
