use {
    richat_filter::message::MessageTransaction,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{
        collections::HashSet,
        hash::{Hash, Hasher},
    },
};

#[derive(Debug)]
pub struct TransactionFilter {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    pub signature: Option<Signature>,
    pub account_include: HashSet<Pubkey>,
    pub account_exclude: HashSet<Pubkey>,
    pub account_required: HashSet<Pubkey>,
}

impl Hash for TransactionFilter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vote.hash(state);
        self.failed.hash(state);
        self.signature.hash(state);
        for pubkeys in &[
            &self.account_include,
            &self.account_exclude,
            &self.account_required,
        ] {
            let mut pubkeys = pubkeys.iter().copied().collect::<Vec<_>>();
            pubkeys.sort_unstable();
            pubkeys.hash(state)
        }
    }
}

impl TransactionFilter {
    pub fn matches(&self, message: &MessageTransaction) -> bool {
        if let Some(vote) = self.vote {
            if vote != message.vote() {
                return false;
            }
        }

        if let Some(failed) = self.failed {
            if failed != message.failed() {
                return false;
            }
        }

        if let Some(filter_signature) = &self.signature {
            if filter_signature != message.signature() {
                return false;
            }
        }

        if !self.account_include.is_empty()
            && self
                .account_include
                .intersection(message.account_keys())
                .next()
                .is_none()
        {
            return false;
        }

        if !self.account_exclude.is_empty()
            && self
                .account_exclude
                .intersection(message.account_keys())
                .next()
                .is_some()
        {
            return false;
        }

        if !self.account_required.is_empty()
            && !self.account_required.is_subset(message.account_keys())
        {
            return false;
        }

        true
    }
}
