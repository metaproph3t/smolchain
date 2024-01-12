//! We need to store accounts ina way that allows us to query the state of any
//! given fork. To do this, we have 1 `VersionedAccount` per account ID that
//! keeps track of any per-fork updates. `AccountsDb` stores these
//! `VersionedAccount`s in a `DashMap` so that we can access them in parallel.
//!
//! Anytime a fork makes an update to an account, we add the update to the
//! `VersionedAccount`'s `inflight_updates` queue. When a fork is rooted
//! (i.e., reaches economic finality) `AccountsDb` flushes its
//! `inflight_updates` to the `VersionedAccount`'s `finalized_acc` field
//! and deletes any updates that are older than the rooted slot but aren't
//! ancestors of it.

use super::*;

use std::collections::VecDeque;

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::mapref::one::{Ref, RefMut};
use dashmap::try_result::TryResult;
use dashmap::DashMap;

#[derive(Debug)]
pub enum LoadError {
    OneOrMoreAccountsLocked,
}

#[derive(Default, Debug, Clone)]
pub struct VersionedAccount {
    finalized_acc: Option<Account>,
    inflight_updates: VecDeque<(Slot, Account)>,
}

pub struct AccountsDb {
    finalized_slot: AtomicU64,
    accounts: DashMap<AccountId, VersionedAccount>,
}

impl VersionedAccount {
    pub fn get_account(&self, slots_to_include: &[Slot]) -> Option<&Account> {
        for (slot, account) in self.inflight_updates.iter().rev() {
            if slots_to_include.contains(&slot) {
                return Some(&account);
            }
        }

        self.finalized_acc.as_ref()
    }

    pub fn load_account(&mut self, slots_to_include: &[Slot]) -> &mut Account {
        let current_slot = *slots_to_include.last().unwrap();

        if self.inflight_updates.len() > 0
            && self
                .inflight_updates
                .get(self.inflight_updates.len() - 1)
                .unwrap()
                .0
                == current_slot
        {
        } else if let Some((_, account)) = self
            .inflight_updates
            .iter()
            .rev()
            .find(|(slot, _)| slots_to_include.contains(slot))
        {
            self.inflight_updates
                .push_back((current_slot, account.clone()));
        } else if let Some(finalized_acc) = &self.finalized_acc {
            self.inflight_updates
                .push_back((current_slot, finalized_acc.clone()));
        } else {
            self.inflight_updates
                .push_back((current_slot, Account::default()));
        }

        &mut self.inflight_updates.back_mut().unwrap().1
    }

    pub fn set_account(&mut self, account: Account, slot: Slot) {
        if self.inflight_updates.len() > 0 {
            let last_inflight_update = self.inflight_updates.back_mut().unwrap();
            if last_inflight_update.0 == slot {
                last_inflight_update.1 = account;
                return;
            }
        }
        self.inflight_updates.push_back((slot, account));
    }
}

impl AccountsDb {
    pub fn genesis_database() -> Self {
        let accounts_db = AccountsDb {
            finalized_slot: AtomicU64::new(0),
            accounts: DashMap::new(),
        };
        accounts_db.accounts.insert(
            0,
            VersionedAccount {
                finalized_acc: Some(Account {
                    balance: GENESIS_SUPPLY,
                }),
                inflight_updates: VecDeque::new(),
            },
        );

        accounts_db
    }

    pub fn initialize_empty_versioned_account(&self, account_id: AccountId) {
        self.accounts.insert(
            account_id,
            VersionedAccount {
                finalized_acc: None,
                inflight_updates: VecDeque::new(),
            },
        );
    }

    pub fn get_versioned_account(
        &self,
        account_id: AccountId,
    ) -> Option<Ref<AccountId, VersionedAccount>> {
        self.accounts.get(&account_id)
    }

    pub fn load_versioned_accounts(
        &self,
        read_account_ids: &[AccountId],
        write_account_ids: &[AccountId],
    ) -> Result<
        (
            Vec<Ref<AccountId, VersionedAccount>>,
            Vec<RefMut<AccountId, VersionedAccount>>,
        ),
        LoadError,
    > {
        let mut read_accounts = Vec::new();
        let mut write_accounts = Vec::new();

        for account_id in [read_account_ids, write_account_ids].concat() {
            if !self.accounts.contains_key(&account_id) {
                self.initialize_empty_versioned_account(account_id);
            }
        }

        for account_id in read_account_ids {
            let try_result = self.accounts.try_get(account_id);

            match try_result {
                TryResult::Locked => return Err(LoadError::OneOrMoreAccountsLocked),
                TryResult::Absent => unreachable!(),
                TryResult::Present(account) => {
                    read_accounts.push(account);
                }
            }
        }

        for account_id in write_account_ids {
            let try_result = self.accounts.try_get_mut(account_id);

            match try_result {
                TryResult::Locked => return Err(LoadError::OneOrMoreAccountsLocked),
                TryResult::Absent => unreachable!(),
                TryResult::Present(account) => {
                    write_accounts.push(account);
                }
            }
        }

        Ok((read_accounts, write_accounts))
    }

    pub fn finalize(&self, slots: &[Slot]) {
        let tip = *slots.last().unwrap();
        let finalized_slot = self.finalized_slot.load(Ordering::Relaxed);

        if tip <= finalized_slot {
            return;
        }

        self.accounts.iter_mut().for_each(|mut versioned_account| {
            while let Some((update_slot, account)) = versioned_account.inflight_updates.pop_front()
            {
                if update_slot <= tip {
                    if slots.contains(&update_slot) {
                        versioned_account.finalized_acc = Some(account);
                    }
                } else {
                    versioned_account
                        .inflight_updates
                        .push_front((update_slot, account));
                    break;
                }
            }
        });

        self.finalized_slot.store(tip, Ordering::Relaxed);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_genesis_database() {
        let accounts_db = AccountsDb::genesis_database();
        assert_eq!(accounts_db.accounts.len(), 1);
        assert_eq!(
            accounts_db
                .accounts
                .get(&0)
                .unwrap()
                .finalized_acc
                .as_ref()
                .unwrap()
                .balance,
            GENESIS_SUPPLY
        );
    }

    #[test]
    fn test_initialize_empty_versioned_account() {
        let accounts_db = AccountsDb::genesis_database();
        accounts_db.initialize_empty_versioned_account(1);
        assert_eq!(accounts_db.accounts.len(), 2);
    }

    #[test]
    fn test_load_versioned_accounts() {
        let accounts_db = AccountsDb::genesis_database();
        accounts_db.initialize_empty_versioned_account(1);
        accounts_db.initialize_empty_versioned_account(2);
        accounts_db.initialize_empty_versioned_account(3);

        {
            let (read_accounts, mut write_accounts) = accounts_db
                .load_versioned_accounts(&[0, 1], &[2, 3])
                .expect("load");

            assert_eq!(read_accounts.len(), 2);
            assert_eq!(write_accounts.len(), 2);

            assert_eq!(
                read_accounts[0].get_account(&[0]).unwrap().balance,
                GENESIS_SUPPLY
            );
            assert_eq!(write_accounts[0].load_account(&[0]).balance, 0);

            let (from_slice, to_slice) = write_accounts.split_at_mut(1);

            let from = &mut from_slice[0];
            let to = &mut to_slice[0];

            let from = from.load_account(&[0]);
            let to = to.load_account(&[0]);

            from.balance = 10;
            to.balance = 15;
        }

        assert_eq!(
            accounts_db
                .accounts
                .get(&3)
                .unwrap()
                .get_account(&[0])
                .unwrap()
                .balance,
            15
        );
    }
}
