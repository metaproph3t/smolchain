use std::sync::Arc;

pub const GENESIS_SUPPLY: u64 = 1_000_000;

pub type AccountId = u64;
pub type Slot = u64;

pub mod accounts_db;
use accounts_db::AccountsDb;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Account {
    pub balance: u64,
}

struct Transaction {
    pub from: AccountId,
    pub to: AccountId,
    pub amount: u64,
}

struct Bank {
    pub slot: Slot,
    // the last ancestor is the slot of this bank
    pub ancestors: Vec<Slot>,
    pub accounts_db: Arc<AccountsDb>,
}

impl Bank {
    pub fn genesis_bank() -> Self {
        Self {
            slot: 0,
            ancestors: vec![0],
            accounts_db: Arc::new(AccountsDb::genesis_database()),
        }
    }

    pub fn get_account(&self, account_id: AccountId) -> Option<Account> {
        let stored_account = self.accounts_db.get_versioned_account(account_id)?;

        stored_account.get_account(&self.ancestors).cloned()
    }

    pub fn new_from_parent(&self, slot: Slot) -> Self {
        let mut ancestors = self.ancestors.clone();

        ancestors.push(slot);

        Self {
            slot,
            ancestors,
            accounts_db: self.accounts_db.clone(),
        }
    }

    pub fn finalize(&self) {
        self.accounts_db.finalize(&self.ancestors);
    }

    pub fn apply(&self, tx: &Transaction) {
        let (_, mut write_accounts) = self
            .accounts_db
            .load_versioned_accounts(&[], &[tx.from, tx.to])
            .expect("load accounts");

        // we need to do this because we need to borrow mutably twice
        let (from_slice, to_slice) = write_accounts.split_at_mut(1);
        let from = from_slice[0].load_account(&self.ancestors);
        let to = to_slice[0].load_account(&self.ancestors);

        from.balance -= tx.amount;
        to.balance += tx.amount;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_account() {
        let bank = Bank::genesis_bank();

        assert_eq!(bank.slot, 0);
        assert_eq!(bank.ancestors, vec![0]);

        assert_eq!(
            bank.get_account(0),
            Some(Account {
                balance: GENESIS_SUPPLY
            })
        );
        assert_eq!(bank.get_account(1), None);
    }

    #[test]
    fn test_apply() {
        let bank_0 = Bank::genesis_bank();

        let tx = Transaction {
            from: 0,
            to: 1,
            amount: 42,
        };

        bank_0.apply(&tx);

        assert_eq!(bank_0.get_account(0).unwrap().balance, GENESIS_SUPPLY - 42);
        assert_eq!(bank_0.get_account(1).unwrap().balance, 42);

        let bank_1 = bank_0.new_from_parent(1);

        assert_eq!(bank_1.get_account(0).unwrap().balance, GENESIS_SUPPLY - 42);
        assert_eq!(bank_1.get_account(1).unwrap().balance, 42);

        let tx = Transaction {
            from: 1,
            to: 0,
            amount: 10,
        };

        bank_1.apply(&tx);

        assert_eq!(bank_1.get_account(0).unwrap().balance, GENESIS_SUPPLY - 32);
        assert_eq!(bank_1.get_account(1).unwrap().balance, 32);

        assert_eq!(bank_0.get_account(0).unwrap().balance, GENESIS_SUPPLY - 42);
        assert_eq!(bank_0.get_account(1).unwrap().balance, 42);

        // a competing fork
        let bank_2 = bank_0.new_from_parent(2);

        // 0 is a double-spender :)
        let tx = Transaction {
            from: 0,
            to: 1,
            amount: 1,
        };

        bank_2.apply(&tx);

        assert_eq!(bank_2.get_account(0).unwrap().balance, GENESIS_SUPPLY - 43);
        assert_eq!(bank_2.get_account(1).unwrap().balance, 43);

        assert_eq!(bank_1.get_account(0).unwrap().balance, GENESIS_SUPPLY - 32);
        assert_eq!(bank_1.get_account(1).unwrap().balance, 32);

        bank_2.finalize();

        assert_eq!(bank_1.get_account(0).unwrap().balance, GENESIS_SUPPLY - 43);
        assert_eq!(bank_1.get_account(1).unwrap().balance, 43);
    }

    //#[test]
    //fn test_benchmark() {
    //    let bank = Bank::genesis_bank();

    //    let tx = Transaction {
    //        from: 0,
    //        to: 1,
    //        amount: 1,
    //    };

    //    let mut total = 0;

    //    let start = std::time::Instant::now();

    //    for _ in 0..1_000_000 {
    //        bank.apply(&tx);
    //    }

    //    println!("elapsed millis: {}", start.elapsed().as_millis());
    //}
}

fn main() {
    println!("Hello, world!");
}
