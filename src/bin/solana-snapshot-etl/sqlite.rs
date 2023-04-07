use anchor_lang::prelude::*;
use borsh::BorshDeserialize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::error;
use rusqlite::{params, Connection};
use solana_sdk::program_pack::Pack;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use solana_snapshot_etl::{append_vec_iter, AppendVecIterator};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use whirlpool::{
    math::tick_math::sqrt_price_from_tick_index,
    state::{Position, Whirlpool},
};

use crate::mpl_metadata;
use crate::state::Obligation;
pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(crate) struct SqliteIndexer {
    db: Connection,
    db_path: PathBuf,
    db_temp_guard: TempFileGuard,

    multi_progress: MultiProgress,
    progress: Arc<Progress>,
}

struct Progress {
    accounts_counter: ProgressCounter,
    token_accounts_counter: ProgressCounter,
    metaplex_accounts_counter: ProgressCounter,
}

pub(crate) struct IndexStats {
    pub(crate) accounts_total: u64,
    pub(crate) token_accounts_total: u64,
    pub(crate) metaplex_accounts_total: u64,
}

impl SqliteIndexer {
    pub(crate) fn new(db_path: PathBuf) -> Result<Self> {
        // Create temporary DB file, which gets promoted on success.
        let temp_file_name = format!("_{}.tmp", db_path.file_name().unwrap().to_string_lossy());
        let db_temp_path = db_path.with_file_name(&temp_file_name);
        let _ = std::fs::remove_file(&db_temp_path);
        let db_temp_guard = TempFileGuard::new(db_temp_path.clone());

        // Open database.
        let db = Self::create_db(&db_temp_path)?;

        // Create progress bars.
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>13.bold.dim} {spinner} rate={per_sec:>13} total={human_pos:>11}",
        )
        .unwrap();
        let multi_progress = MultiProgress::new();
        let accounts_counter = ProgressCounter::new(
            multi_progress.add(
                ProgressBar::new_spinner()
                    .with_style(spinner_style.clone())
                    .with_prefix("accs"),
            ),
        );
        let token_accounts_counter = ProgressCounter::new(
            multi_progress.add(
                ProgressBar::new_spinner()
                    .with_style(spinner_style.clone())
                    .with_prefix("token_accs"),
            ),
        );
        let metaplex_accounts_counter = ProgressCounter::new(
            multi_progress.add(
                ProgressBar::new_spinner()
                    .with_style(spinner_style.clone())
                    .with_prefix("metaplex_accs"),
            ),
        );

        Ok(Self {
            db,
            db_path,
            db_temp_guard,

            multi_progress,
            progress: Arc::new(Progress {
                accounts_counter,
                token_accounts_counter,
                metaplex_accounts_counter,
            }),
        })
    }

    fn create_db(path: &Path) -> Result<Connection> {
        let db = Connection::open(&path)?;
        db.pragma_update(None, "synchronous", false)?;
        db.pragma_update(None, "journal_mode", "off")?;
        db.pragma_update(None, "locking_mode", "exclusive")?;
        db.execute(
            "\
CREATE TABLE account  (
    pubkey TEXT NOT NULL PRIMARY KEY,
    data_len INTEGER(8) NOT NULL,
    owner TEXT NOT NULL,
    lamports INTEGER(8) NOT NULL,
    executable INTEGER(1) NOT NULL,
    rent_epoch INTEGER(8) NOT NULL
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE token_mint (
    pubkey TEXT NOT NULL PRIMARY KEY,
    mint_authority TEXT NULL,
    supply INTEGER(8) NOT NULL,
    decimals INTEGER(2) NOT NULL,
    is_initialized BOOL NOT NULL,
    freeze_authority TEXT NULL
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE token_account (
    pubkey TEXT NOT NULL PRIMARY KEY,
    mint TEXT NOT NULL,
    owner TEXT NOT NULL,
    amount INTEGER(8) NOT NULL,
    delegate TEXT,
    state INTEGER(1) NOT NULL,
    is_native INTEGER(8),
    delegated_amount INTEGER(8) NOT NULL,
    close_authority TEXT
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE solend (
    pubkey TEXT NOT NULL,
    owner TEXT NOT NULL,
    version TEXT NOT NULL,
    lending_market TEXT NOT NULL,
    deposit_reserve TEXT NOT NULL,
    deposit_amount Integer(8) NOT NULL
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE orca  (
    pubkey TEXT NOT NULL PRIMARY KEY,
    data_len INTEGER(8) NOT NULL,
    position_mint TEXT NOT NULL,
    pool TEXT NOT NULL,
    price_lower INTEGER(8) NOT NULL, 
    price_upper INTEGER(8) NOT NULL, 
    liquidity INTEGER(8) NOT NULL
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE whirlpool_pools  (
    pubkey TEXT NOT NULL PRIMARY KEY,
    data_len INTEGER(8) NOT NULL,
    token_a TEXT NOT NULL,
    token_b TEXT NOT NULL,
    sqrt_price INTEGER(8) NOT NULL
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE port (
    pubkey TEXT NOT NULL PRIMARY KEY,
    owner TEXT NOT NULL,
    data TEXT NOT NULL
);",
            [],
        )?;
        //         db.execute(
        //             "\
        // CREATE TABLE token_multisig (
        //     pubkey TEXT NOT NULL,
        //     signer TEXT NOT NULL,
        //     m INTEGER(2) NOT NULL,
        //     n INTEGER(2) NOT NULL,
        //     PRIMARY KEY (pubkey, signer)
        // );",
        //             [],
        //         )?;
        db.execute(
            "\
CREATE TABLE token_metadata (
    pubkey TEXT NOT NULL,
    mint TEXT NOT NULL,
    update_authority TEXT NOT NULL,
    name TEXT NOT NULL,
    symbol TEXT(10) NOT NULL,
    uri TEXT(200) NOT NULL,
    data_length INTEGER(8) NOT NULL,
    seller_fee_basis_points INTEGER(4) NOT NULL,
    primary_sale_happened INTEGER(1) NOT NULL,
    is_mutable INTEGER(1) NOT NULL,
    edition_nonce INTEGER(2) NULL,
    collection_verified INTEGER(1) NULL,
    collection_key TEXT NULL
);",
            [],
        )?;
        Ok(db)
    }

    pub(crate) fn set_cache_size(&mut self, size_mib: i64) -> Result<()> {
        let size = size_mib * 1024;
        self.db.pragma_update(None, "cache_size", -size)?;
        Ok(())
    }

    pub(crate) fn insert_all(mut self, iterator: AppendVecIterator) -> Result<IndexStats> {
        let mut worker = Worker {
            db: &self.db,
            progress: Arc::clone(&self.progress),
        };
        for append_vec in iterator {
            worker.on_append_vec(append_vec?)?;
        }
        self.db.pragma_update(None, "query_only", true)?;
        let stats = IndexStats {
            accounts_total: self.progress.accounts_counter.get(),
            token_accounts_total: self.progress.token_accounts_counter.get(),
            metaplex_accounts_total: self.progress.metaplex_accounts_counter.get(),
        };
        self.db_temp_guard.promote(self.db_path)?;
        let _ = &self.multi_progress;
        Ok(stats)
    }
}

struct Worker<'a> {
    db: &'a Connection,
    progress: Arc<Progress>,
}

impl<'a> AppendVecConsumer for Worker<'a> {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for acc in append_vec_iter(Rc::new(append_vec)) {
            self.insert_account(&acc.access().unwrap())?;
        }
        Ok(())
    }
}

impl<'a> Worker<'a> {
    fn insert_account(&mut self, account: &StoredAccountMeta) -> Result<()> {
        let filter_account_owners = "11111111111111111111111111111111";
        let filter_account_mints = "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So,3JFC4cB56Er45nWVe29Bhnn5GnwQzSmHVf6eUq9ac91h,6UA3yn28XecAHLTwoCtjfzy3WcyQj1x13bxnH8urUiKt,5ijRoAHVgd5T5CNtK5KDRUBZ7Bffb69nktMj5n6ks6m4,4xTpJ4p76bAeggXoYywpCCNKfJspbuRzZ79R7pRhbqSf,Afvh7TWfcT1E9eEEWJk17fPjnqk36hreTJJK5g3s4fm8,7iKG16aukdXXw43MowbfrGqXhAoYe51iVR9u2Nf2dCEY,8cn7JcYVjDZesLa3RTt3NXne4WcDw9PdUneQWuByehwW,7HqhfUqig7kekN8FbJCtQ36VgdXKriZWQ62rTve9ZmQ,SoLEao8wTzSfqhuou8rcYsVoLjthVmiXuEjzdNPMnCz";
        self.progress.accounts_counter.inc();
        if bs58::encode(account.account_meta.owner.as_ref())
            .into_string()
            .contains("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo")
            && account.data.len() == 1300
        {
            //Dump solend token accounts
            self.insert_metadata_solend(account)?;
        }
        if bs58::encode(account.account_meta.owner.as_ref())
            .into_string()
            .contains("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc")
            && account.data.len() == 216
        {
            //Dump only whirlpool positions accounts
            self.insert_orca(account)?;
        }
        if bs58::encode(account.meta.pubkey)
            .into_string()
            .contains("AiMZS5U3JMvpdvsr1KeaMiS354Z1DeSg5XjA4yYRxtFf")
        {
            //Whirlpool pool address
            self.insert_whirlpool_pool(account)?;
        }
        if filter_account_owners.contains(
            bs58::encode(account.account_meta.owner.as_ref())
                .into_string()
                .as_str(),
        ) {
            self.insert_account_meta(account)?;
        }
        if bs58::encode(account.account_meta.owner.as_ref())
            .into_string()
            .contains("Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR")
        {
            //Port finance accounts
            self.insert_port(account)?;
        }
        if account.account_meta.owner == mpl_metadata::id() {
            self.insert_token_metadata(account)?;
        }
        match account.meta.data_len as usize {
            spl_token::state::Account::LEN => {
                if let Ok(token_account) = spl_token::state::Account::unpack(account.data) {
                    let token_account_mint =
                        bs58::encode(token_account.mint.as_ref()).into_string();
                    // if "Dt1Cuau5m5CSmun8hZstjEh9RszxAmejnq7ZaHNcuXfA".contains(token_account_mint.as_str()) {
                    //     self.insert_port(account, &token_account)?;
                    // }
                    if !filter_account_mints.contains(token_account_mint.as_str())
                        && token_account.amount != 1
                    {
                        return Ok(());
                    }
                    self.insert_account_meta(account)?;
                    if account.account_meta.owner == spl_token::id() {
                        self.insert_token(account, &token_account)?;
                    }
                }
            }
            spl_token::state::Mint::LEN => {
                if let Ok(token_mint) = spl_token::state::Mint::unpack(account.data) {
                    let mint_pubkey = bs58::encode(account.meta.pubkey.as_ref()).into_string();
                    if !filter_account_mints.contains(mint_pubkey.as_str()) {
                        return Ok(());
                    }
                    self.insert_token_mint(account, &token_mint)?;
                }
            }
            // spl_token::state::Multisig::LEN => {
            //     if let Ok(token_multisig) = spl_token::state::Multisig::unpack(account.data) {
            //         self.insert_token_multisig(account, &token_multisig)?;
            //     }
            // }
            _ => {
                //     warn!(
                //         "Token program account {} has unexpected size {}",
                //         account.meta.pubkey, account.meta.data_len
                //     );
                return Ok(());
            }
        }

        Ok(())
    }

    fn insert_account_meta(&mut self, account: &StoredAccountMeta) -> Result<()> {
        let mut account_insert = self.db.prepare_cached(
            "\
INSERT OR REPLACE INTO account (pubkey, data_len, owner, lamports, executable, rent_epoch)
    VALUES (?, ?, ?, ?, ?, ?);",
        )?;
        account_insert.insert(params![
            bs58::encode(account.meta.pubkey.as_ref()).into_string(),
            account.meta.data_len as i64,
            bs58::encode(account.account_meta.owner.as_ref()).into_string(),
            account.account_meta.lamports as i64,
            account.account_meta.executable,
            account.account_meta.rent_epoch as i64,
        ])?;
        Ok(())
    }

    fn insert_token(
        &mut self,
        account: &StoredAccountMeta,
        token_account: &spl_token::state::Account,
    ) -> Result<()> {
        self.insert_token_account(account, token_account)?;
        self.progress.token_accounts_counter.inc();
        Ok(())
    }

    fn insert_token_account(
        &mut self,
        account: &StoredAccountMeta,
        token_account: &spl_token::state::Account,
    ) -> Result<()> {
        let mut token_account_insert = self.db.prepare_cached("\
INSERT OR REPLACE INTO token_account (pubkey, mint, owner, amount, delegate, state, is_native, delegated_amount, close_authority)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);")?;
        token_account_insert.insert(params![
            bs58::encode(account.meta.pubkey.as_ref()).into_string(),
            bs58::encode(token_account.mint.as_ref()).into_string(),
            bs58::encode(token_account.owner.as_ref()).into_string(),
            token_account.amount as i64,
            Option::<[u8; 32]>::from(token_account.delegate.map(|key| key.to_bytes())),
            token_account.state as u8,
            Option::<u64>::from(token_account.is_native),
            token_account.delegated_amount as i64,
            Option::<[u8; 32]>::from(token_account.close_authority.map(|key| key.to_bytes())),
        ])?;
        Ok(())
    }

    fn insert_token_mint(
        &mut self,
        account: &StoredAccountMeta,
        token_mint: &spl_token::state::Mint,
    ) -> Result<()> {
        let mut token_mint_insert = self.db.prepare_cached("\
INSERT OR REPLACE INTO token_mint (pubkey, mint_authority, supply, decimals, is_initialized, freeze_authority)
    VALUES (?, ?, ?, ?, ?, ?);")?;
        token_mint_insert.insert(params![
            bs58::encode(account.meta.pubkey.as_ref()).into_string(),
            Option::<[u8; 32]>::from(token_mint.mint_authority.map(|key| key.to_bytes()),),
            token_mint.supply as i64,
            token_mint.decimals,
            token_mint.is_initialized,
            Option::<[u8; 32]>::from(token_mint.freeze_authority.map(|key| key.to_bytes())),
        ])?;
        Ok(())
    }

    //     fn insert_token_multisig(
    //         &mut self,
    //         account: &StoredAccountMeta,
    //         token_multisig: &spl_token::state::Multisig,
    //     ) -> Result<()> {
    //         let mut token_multisig_insert = self.db.prepare_cached(
    //             "\
    // INSERT OR REPLACE INTO token_multisig (pubkey, signer, m, n)
    //     VALUES (?, ?, ?, ?);",
    //         )?;
    //         for signer in &token_multisig.signers[..token_multisig.n as usize] {
    //             token_multisig_insert.insert(params![
    //                 account.meta.pubkey.as_ref(),
    //                 signer.as_ref(),
    //                 token_multisig.m,
    //                 token_multisig.n
    //             ])?;
    //         }
    //         Ok(())
    //     }

    fn insert_token_metadata(&mut self, account: &StoredAccountMeta) -> Result<()> {
        if account.data.is_empty() {
            return Ok(());
        }
        let mut data_peek = account.data;
        let account_key = match mpl_metadata::AccountKey::deserialize(&mut data_peek) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        match account_key {
            mpl_metadata::AccountKey::MetadataV1 => {
                let meta_v1 = mpl_metadata::Metadata::deserialize(&mut data_peek).map_err(|e| {
                    format!(
                        "Invalid token-metadata v1 metadata acc {}: {}",
                        account.meta.pubkey, e
                    )
                })?;

                let meta_v1_1 = mpl_metadata::MetadataExt::deserialize(&mut data_peek).ok();
                let meta_v1_2 = meta_v1_1
                    .as_ref()
                    .and_then(|_| mpl_metadata::MetadataExtV1_2::deserialize(&mut data_peek).ok());

                self.insert_token_metadata_metadata(
                    account,
                    &meta_v1,
                    meta_v1_1.as_ref(),
                    meta_v1_2.as_ref(),
                )?;
            }
            _ => return Ok(()), // TODO
        }
        Ok(())
    }

    fn insert_token_metadata_metadata(
        &mut self,
        account: &StoredAccountMeta,
        meta_v1: &mpl_metadata::Metadata,
        meta_v1_1: Option<&mpl_metadata::MetadataExt>,
        meta_v1_2: Option<&mpl_metadata::MetadataExtV1_2>,
    ) -> Result<()> {
        let collection = meta_v1_2.as_ref().and_then(|m| m.collection.as_ref());
        if !meta_v1
            .data
            .name
            .as_str()
            .contains("Raydium Concentrated Liquidity")
        {
            return Ok(());
        }

        self.progress.metaplex_accounts_counter.inc();
        self.db
            .prepare_cached(
                "\
INSERT OR REPLACE INTO token_metadata (
    pubkey,
    mint,
    update_authority,
    name,
    symbol,
    uri,
    data_length,
    seller_fee_basis_points,
    primary_sale_happened,
    is_mutable,
    edition_nonce,
    collection_verified,
    collection_key
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            )?
            .insert(params![
                bs58::encode(account.meta.pubkey.as_ref()).into_string(),
                bs58::encode(meta_v1.mint.as_ref()).into_string(),
                bs58::encode(meta_v1.update_authority.as_ref()).into_string(),
                meta_v1.data.name,
                meta_v1.data.symbol,
                meta_v1.data.uri,
                account.data.len(),
                meta_v1.data.seller_fee_basis_points,
                meta_v1.primary_sale_happened,
                meta_v1.is_mutable,
                meta_v1_1.map(|c| c.edition_nonce),
                collection.map(|c| c.verified),
                collection.map(|c| c.key.as_ref()),
            ])?;
        Ok(())
    }

    fn insert_metadata_solend(&mut self, account: &StoredAccountMeta) -> Result<()> {
        if account.data.is_empty() && account.data.len() < 1300 {
            return Ok(());
        }
        let data = Obligation::unpack_unchecked(account.data)?;
        let deposits = data.deposits;
        let mut deposit_reserve = "";
        let mut msol_deposited = 0;
        if deposits.len() > 0 {
            for i in 0..deposits.len() {
                if bs58::encode(deposits[i].deposit_reserve).into_string()
                    == "CCpirWrgNuBVLdkP2haxLTbD6XqEgaYuVXixbbpxUB6"
                {
                    msol_deposited = deposits[i].deposited_amount;
                    deposit_reserve = "CCpirWrgNuBVLdkP2haxLTbD6XqEgaYuVXixbbpxUB6";
                    break;
                }
            }
        }

        if deposit_reserve == "" {
            return Ok(());
        }

        self.db
            .prepare_cached(
                "\
INSERT OR REPLACE INTO solend (
    pubkey,
    owner,
    version,
    lending_market,
    deposit_reserve,
    deposit_amount
) VALUES (?, ?, ?, ?, ?, ?);",
            )?
            .insert(params![
                bs58::encode(account.meta.pubkey.as_ref()).into_string(),
                bs58::encode(data.owner).into_string(),
                data.version,
                bs58::encode(data.lending_market).into_string(),
                deposit_reserve,
                msol_deposited,
            ])?;

        Ok(())
    }

    fn insert_orca(&mut self, account: &StoredAccountMeta) -> Result<()> {
        let mut account_data = account.data;
        let position = Position::try_deserialize(&mut account_data)?;
        if bs58::encode(position.whirlpool).into_string()
            != "AiMZS5U3JMvpdvsr1KeaMiS354Z1DeSg5XjA4yYRxtFf"
        {
            return Ok(());
        }
        let sqrt_price_lower = sqrt_price_from_tick_index(position.tick_lower_index);
        let sqrt_price_upper = sqrt_price_from_tick_index(position.tick_upper_index);

        let mut account_insert = self.db.prepare_cached(
            "\
INSERT OR REPLACE INTO orca (pubkey, data_len, position_mint, pool, price_lower, price_upper, liquidity)
    VALUES (?, ?, ?, ?, ?, ?, ?);",
        )?;
        account_insert.insert(params![
            bs58::encode(account.meta.pubkey).into_string(),
            account.meta.data_len as i64,
            bs58::encode(position.position_mint).into_string(),
            bs58::encode(position.whirlpool).into_string(),
            sqrt_price_lower as i64,
            sqrt_price_upper as i64,
            position.liquidity as i64,
        ])?;
        Ok(())
    }

    fn insert_whirlpool_pool(&mut self, account: &StoredAccountMeta) -> Result<()> {
        let mut account_data = account.data;
        let whirlpool = Whirlpool::try_deserialize(&mut account_data)?;

        let mut account_insert = self.db.prepare_cached(
            "\
INSERT OR REPLACE INTO whirlpool_pools (pubkey, data_len, token_a, token_b, sqrt_price)
    VALUES (?, ?, ?, ?, ?);",
        )?;
        account_insert.insert(params![
            bs58::encode(account.meta.pubkey).into_string(),
            account.meta.data_len as i64,
            whirlpool.token_mint_a.to_string(),
            whirlpool.token_mint_b.to_string(),
            whirlpool.sqrt_price as i64,
        ])?;
        Ok(())
    }

    fn insert_port(&mut self, account: &StoredAccountMeta) -> Result<()> {
        let mut account_insert = self.db.prepare_cached(
            "\
INSERT OR REPLACE INTO port (pubkey, owner, data)
    VALUES (?, ?, ?);",
        )?;
        account_insert.insert(params![
            bs58::encode(account.meta.pubkey).into_string(),
            bs58::encode(account.account_meta.owner.as_ref()).into_string(),
            account.data,
        ])?;
        Ok(())
    }
}

struct ProgressCounter {
    progress_bar: Mutex<ProgressBar>,
    counter: AtomicU64,
}

impl ProgressCounter {
    fn new(progress_bar: ProgressBar) -> Self {
        Self {
            progress_bar: Mutex::new(progress_bar),
            counter: AtomicU64::new(0),
        }
    }

    fn get(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn inc(&self) {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        if count % 1024 == 0 {
            self.progress_bar.lock().unwrap().set_position(count)
        }
    }
}

impl Drop for ProgressCounter {
    fn drop(&mut self) {
        let progress_bar = self.progress_bar.lock().unwrap();
        progress_bar.set_position(self.get());
        progress_bar.finish();
    }
}

struct TempFileGuard {
    pub path: Option<PathBuf>,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn promote<P: AsRef<Path>>(&mut self, new_name: P) -> std::io::Result<()> {
        std::fs::rename(
            self.path.take().expect("cannot promote non-existent file"),
            new_name,
        )
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            if let Err(e) = std::fs::remove_file(path) {
                error!("Failed to remove temp DB: {}", e);
            }
        }
    }
}
