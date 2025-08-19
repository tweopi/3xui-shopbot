import sqlite3
from datetime import datetime
import logging
from pathlib import Path
import json

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/app/project")
DB_FILE = PROJECT_ROOT / "users.db"

def initialize_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY, username TEXT, total_spent REAL DEFAULT 0,
                    total_months INTEGER DEFAULT 0, trial_used BOOLEAN DEFAULT 0,
                    agreed_to_terms BOOLEAN DEFAULT 0,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned BOOLEAN DEFAULT 0,
                    referred_by INTEGER,
                    referral_balance REAL DEFAULT 0,
                    referral_balance_all REAL DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS vpn_keys (
                    key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    host_name TEXT NOT NULL,
                    xui_client_uuid TEXT NOT NULL,
                    key_email TEXT NOT NULL UNIQUE,
                    expiry_date TIMESTAMP,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    username TEXT,
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    amount_rub REAL NOT NULL,
                    amount_currency REAL,
                    currency_name TEXT,
                    payment_method TEXT,
                    metadata TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS xui_hosts(
                    host_name TEXT NOT NULL,
                    host_url TEXT NOT NULL,
                    host_username TEXT NOT NULL,
                    host_pass TEXT NOT NULL,
                    host_inbound_id INTEGER NOT NULL
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    plan_name TEXT NOT NULL,
                    months INTEGER NOT NULL,
                    price REAL NOT NULL,
                    FOREIGN KEY (host_name) REFERENCES xui_hosts (host_name)
                )
            ''')            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    subject TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id INTEGER NOT NULL,
                    sender TEXT NOT NULL, -- 'user' | 'admin'
                    content TEXT NOT NULL,
                    media TEXT, -- JSON with Telegram file_id(s), type, caption, mime, size, etc.
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES support_tickets (ticket_id)
                )
            ''')
            default_settings = {
                "panel_login": "admin",
                "panel_password": "admin",
                "about_text": None,
                "terms_url": None,
                "privacy_url": None,
                "support_user": None,
                "support_text": None,
                "channel_url": None,
                "force_subscription": "true",
                "receipt_email": "example@example.com",
                "telegram_bot_token": None,
                "telegram_bot_username": None,
                "trial_enabled": "true",
                "trial_duration_days": "3",
                "enable_referrals": "true",
                "referral_percentage": "10",
                "referral_discount": "5",
                "minimum_withdrawal": "100",
                "admin_telegram_id": None,
                "yookassa_shop_id": None,
                "yookassa_secret_key": None,
                "sbp_enabled": "false",
                "cryptobot_token": None,
                "heleket_merchant_id": None,
                "heleket_api_key": None,
                "domain": None,
                "ton_wallet_address": None,
                "tonapi_key": None,
                "support_forum_chat_id": None,
            }
            run_migration()
            for key, value in default_settings.items():
                cursor.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
            logging.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database error on initialization: {e}")

def run_migration():
    if not DB_FILE.exists():
        logging.error("Users.db database file was not found. There is nothing to migrate.")
        return

    logging.info(f"Starting the migration of the database: {DB_FILE}")

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        logging.info("The migration of the table 'users' ...")
    
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'referred_by' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
            logging.info(" -> The column 'referred_by' is successfully added.")
        else:
            logging.info(" -> The column 'referred_by' already exists.")
            
        if 'referral_balance' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referral_balance REAL DEFAULT 0")
            logging.info(" -> The column 'referral_balance' is successfully added.")
        else:
            logging.info(" -> The column 'referral_balance' already exists.")
        
        if 'referral_balance_all' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referral_balance_all REAL DEFAULT 0")
            logging.info(" -> The column 'referral_balance_all' is successfully added.")
        else:
            logging.info(" -> The column 'referral_balance_all' already exists.")
        
        logging.info("The table 'users' has been successfully updated.")

        logging.info("The migration of the table 'Transactions' ...")

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
        table_exists = cursor.fetchone()

        if table_exists:
            cursor.execute("PRAGMA table_info(transactions)")
            trans_columns = [row[1] for row in cursor.fetchall()]
            
            if 'payment_id' in trans_columns and 'status' in trans_columns and 'username' in trans_columns:
                logging.info("The 'Transactions' table already has a new structure. Migration is not required.")
            else:
                backup_name = f"transactions_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                logging.warning(f"The old structure of the TRANSACTIONS table was discovered. I rename in '{backup_name}' ...")
                cursor.execute(f"ALTER TABLE transactions RENAME TO {backup_name}")
                
                logging.info("I create a new table 'Transactions' with the correct structure ...")
                create_new_transactions_table(cursor)
                logging.info("The new table 'Transactions' has been successfully created. The old data is saved.")
        else:
            logging.info("TRANSACTIONS table was not found. I create a new one ...")
            create_new_transactions_table(cursor)
            logging.info("The new table 'Transactions' has been successfully created.")

        # Migrate support_tickets: add forum/thread columns if missing
        logging.info("The migration of the table 'support_tickets' ...")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='support_tickets'")
        table_exists = cursor.fetchone()
        if table_exists:
            cursor.execute("PRAGMA table_info(support_tickets)")
            st_columns = [row[1] for row in cursor.fetchall()]
            if 'forum_chat_id' not in st_columns:
                cursor.execute("ALTER TABLE support_tickets ADD COLUMN forum_chat_id TEXT")
                logging.info(" -> The column 'forum_chat_id' is successfully added to 'support_tickets'.")
            else:
                logging.info(" -> The column 'forum_chat_id' already exists in 'support_tickets'.")
            if 'message_thread_id' not in st_columns:
                cursor.execute("ALTER TABLE support_tickets ADD COLUMN message_thread_id INTEGER")
                logging.info(" -> The column 'message_thread_id' is successfully added to 'support_tickets'.")
            else:
                logging.info(" -> The column 'message_thread_id' already exists in 'support_tickets'.")
        else:
            logging.warning("Table 'support_tickets' not found, skipping its migration.")

        conn.commit()

        # Migrate support_messages: add media column if missing
        logging.info("The migration of the table 'support_messages' ...")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='support_messages'")
        table_exists = cursor.fetchone()
        if table_exists:
            cursor.execute("PRAGMA table_info(support_messages)")
            sm_columns = [row[1] for row in cursor.fetchall()]
            if 'media' not in sm_columns:
                cursor.execute("ALTER TABLE support_messages ADD COLUMN media TEXT")
                logging.info(" -> The column 'media' is successfully added to 'support_messages'.")
            else:
                logging.info(" -> The column 'media' already exists in 'support_messages'.")
        else:
            logging.warning("Table 'support_messages' not found, skipping its migration.")
        conn.close()
        
        logging.info("--- The database is successfully completed! ---")

    except sqlite3.Error as e:
        logging.error(f"An error occurred during migration: {e}")

def create_new_transactions_table(cursor: sqlite3.Cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            username TEXT,
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            payment_id TEXT UNIQUE NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            amount_rub REAL NOT NULL,
            amount_currency REAL,
            currency_name TEXT,
            payment_method TEXT,
            metadata TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

def create_host(name: str, url: str, user: str, passwd: str, inbound: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id) VALUES (?, ?, ?, ?, ?)",
                (name, url, user, passwd, inbound)
            )
            conn.commit()
            logging.info(f"Successfully created a new host: {name}")
    except sqlite3.Error as e:
        logging.error(f"Error creating host '{name}': {e}")

def delete_host(host_name: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE host_name = ?", (host_name,))
            cursor.execute("DELETE FROM xui_hosts WHERE host_name = ?", (host_name,))
            conn.commit()
            logging.info(f"Successfully deleted host '{host_name}' and its plans.")
    except sqlite3.Error as e:
        logging.error(f"Error deleting host '{host_name}': {e}")

def get_host(host_name: str) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts WHERE host_name = ?", (host_name,))
            result = cursor.fetchone()
            return dict(result) if result else None
    except sqlite3.Error as e:
        logging.error(f"Error getting host '{host_name}': {e}")
        return None

def get_all_hosts() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts")
            hosts = cursor.fetchall()
            return [dict(row) for row in hosts]
    except sqlite3.Error as e:
        logging.error(f"Error getting list of all hosts: {e}")
        return []

def get_all_keys() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys")
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get all keys: {e}")
        return []

def get_setting(key: str) -> str | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result[0] if result else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get setting '{key}': {e}")
        return None
        
def get_all_settings() -> dict:
    settings = {}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT key, value FROM bot_settings")
            rows = cursor.fetchall()
            for row in rows:
                settings[row['key']] = row['value']
    except sqlite3.Error as e:
        logging.error(f"Failed to get all settings: {e}")
    return settings

def update_setting(key: str, value: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
            logging.info(f"Setting '{key}' updated.")
    except sqlite3.Error as e:
        logging.error(f"Failed to update setting '{key}': {e}")

def create_plan(host_name: str, plan_name: str, months: int, price: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO plans (host_name, plan_name, months, price) VALUES (?, ?, ?, ?)",
                (host_name, plan_name, months, price)
            )
            conn.commit()
            logging.info(f"Created new plan '{plan_name}' for host '{host_name}'.")
    except sqlite3.Error as e:
        logging.error(f"Failed to create plan for host '{host_name}': {e}")

def get_plans_for_host(host_name: str) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE host_name = ? ORDER BY months", (host_name,))
            plans = cursor.fetchall()
            return [dict(plan) for plan in plans]
    except sqlite3.Error as e:
        logging.error(f"Failed to get plans for host '{host_name}': {e}")
        return []

def get_plan_by_id(plan_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,))
            plan = cursor.fetchone()
            return dict(plan) if plan else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get plan by id '{plan_id}': {e}")
        return None

def delete_plan(plan_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
            conn.commit()
            logging.info(f"Deleted plan with id {plan_id}.")
    except sqlite3.Error as e:
        logging.error(f"Failed to delete plan with id {plan_id}: {e}")

def update_plan(plan_id: int, plan_name: str, months: int, price: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE plans SET plan_name = ?, months = ?, price = ? WHERE plan_id = ?",
                (plan_name, months, price, plan_id)
            )
            conn.commit()
            if cursor.rowcount == 0:
                logging.warning(f"No plan updated for id {plan_id} (not found).")
                return False
            logging.info(f"Updated plan {plan_id}: name='{plan_name}', months={months}, price={price}.")
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to update plan {plan_id}: {e}")
        return False

def register_user_if_not_exists(telegram_id: int, username: str, referrer_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT telegram_id FROM users WHERE telegram_id = ?", (telegram_id,))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO users (telegram_id, username, registration_date, referred_by) VALUES (?, ?, ?, ?)",
                    (telegram_id, username, datetime.now(), referrer_id)
                )
            else:
                cursor.execute("UPDATE users SET username = ? WHERE telegram_id = ?", (username, telegram_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to register user {telegram_id}: {e}")

def add_to_referral_balance(user_id: int, amount: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance = referral_balance + ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to add to referral balance for user {user_id}: {e}")

def set_referral_balance(user_id: int, value: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance = ? WHERE telegram_id = ?", (value, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to set referral balance for user {user_id}: {e}")

def set_referral_balance_all(user_id: int, value: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance_all = ? WHERE telegram_id = ?", (value, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to set total referral balance for user {user_id}: {e}")

def get_referral_balance(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_balance FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get referral balance for user {user_id}: {e}")
        return 0.0

def get_referral_count(user_id: int) -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,))
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get referral count for user {user_id}: {e}")
        return 0

def get_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            user_data = cursor.fetchone()
            return dict(user_data) if user_data else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get user {telegram_id}: {e}")
        return None

def set_terms_agreed(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET agreed_to_terms = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
            logging.info(f"User {telegram_id} has agreed to terms.")
    except sqlite3.Error as e:
        logging.error(f"Failed to set terms agreed for user {telegram_id}: {e}")

def update_user_stats(telegram_id: int, amount_spent: float, months_purchased: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET total_spent = total_spent + ?, total_months = total_months + ? WHERE telegram_id = ?", (amount_spent, months_purchased, telegram_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to update user stats for {telegram_id}: {e}")

def get_user_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get user count: {e}")
        return 0

def get_total_keys_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vpn_keys")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get total keys count: {e}")
        return 0

def get_total_spent_sum() -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(total_spent) FROM users")
            return cursor.fetchone()[0] or 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get total spent sum: {e}")
        return 0.0

def create_pending_transaction(payment_id: str, user_id: int, amount_rub: float, metadata: dict) -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO transactions (payment_id, user_id, status, amount_rub, metadata) VALUES (?, ?, ?, ?, ?)",
                (payment_id, user_id, 'pending', amount_rub, json.dumps(metadata))
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Failed to create pending transaction: {e}")
        return 0

def find_and_complete_ton_transaction(payment_id: str, amount_ton: float) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM transactions WHERE payment_id = ? AND status = 'pending'", (payment_id,))
            transaction = cursor.fetchone()
            if not transaction:
                logger.warning(f"TON Webhook: Received payment for unknown or completed payment_id: {payment_id}")
                return None
            
            
            cursor.execute(
                "UPDATE transactions SET status = 'paid', amount_currency = ?, currency_name = 'TON', payment_method = 'TON' WHERE payment_id = ?",
                (amount_ton, payment_id)
            )
            conn.commit()
            
            return json.loads(transaction['metadata'])
    except sqlite3.Error as e:
        logging.error(f"Failed to complete TON transaction {payment_id}: {e}")
        return None

def log_transaction(username: str, transaction_id: str | None, payment_id: str | None, user_id: int, status: str, amount_rub: float, amount_currency: float | None, currency_name: str | None, payment_method: str, metadata: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO transactions
                   (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, created_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, datetime.now())
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to log transaction for user {user_id}: {e}")

def get_paginated_transactions(page: int = 1, per_page: int = 15) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    transactions = []
    total = 0
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM transactions")
            total = cursor.fetchone()[0]

            query = "SELECT * FROM transactions ORDER BY created_date DESC LIMIT ? OFFSET ?"
            cursor.execute(query, (per_page, offset))
            
            for row in cursor.fetchall():
                transaction_dict = dict(row)
                
                metadata_str = transaction_dict.get('metadata')
                if metadata_str:
                    try:
                        metadata = json.loads(metadata_str)
                        transaction_dict['host_name'] = metadata.get('host_name', 'N/A')
                        transaction_dict['plan_name'] = metadata.get('plan_name', 'N/A')
                    except json.JSONDecodeError:
                        transaction_dict['host_name'] = 'Error'
                        transaction_dict['plan_name'] = 'Error'
                else:
                    transaction_dict['host_name'] = 'N/A'
                    transaction_dict['plan_name'] = 'N/A'
                
                transactions.append(transaction_dict)
            
    except sqlite3.Error as e:
        logging.error(f"Failed to get paginated transactions: {e}")
    
    return transactions, total

def set_trial_used(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET trial_used = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
            logging.info(f"Trial period marked as used for user {telegram_id}.")
    except sqlite3.Error as e:
        logging.error(f"Failed to set trial used for user {telegram_id}: {e}")

def add_new_key(user_id: int, host_name: str, xui_client_uuid: str, key_email: str, expiry_timestamp_ms: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            expiry_date = datetime.fromtimestamp(expiry_timestamp_ms / 1000)
            cursor.execute(
                "INSERT INTO vpn_keys (user_id, host_name, xui_client_uuid, key_email, expiry_date) VALUES (?, ?, ?, ?, ?)",
                (user_id, host_name, xui_client_uuid, key_email, expiry_date)
            )
            new_key_id = cursor.lastrowid
            conn.commit()
            return new_key_id
    except sqlite3.Error as e:
        logging.error(f"Failed to add new key for user {user_id}: {e}")
        return None

def delete_key_by_email(email: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE key_email = ?", (email,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to delete key '{email}': {e}")

def get_user_keys(user_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE user_id = ? ORDER BY key_id", (user_id,))
            keys = cursor.fetchall()
            return [dict(key) for key in keys]
    except sqlite3.Error as e:
        logging.error(f"Failed to get keys for user {user_id}: {e}")
        return []

def get_key_by_id(key_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_id = ?", (key_id,))
            key_data = cursor.fetchone()
            return dict(key_data) if key_data else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get key by ID {key_id}: {e}")
        return None

def get_key_by_email(key_email: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_email = ?", (key_email,))
            key_data = cursor.fetchone()
            return dict(key_data) if key_data else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get key by email {key_email}: {e}")
        return None

def update_key_info(key_id: int, new_xui_uuid: str, new_expiry_ms: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            expiry_date = datetime.fromtimestamp(new_expiry_ms / 1000)
            cursor.execute("UPDATE vpn_keys SET xui_client_uuid = ?, expiry_date = ? WHERE key_id = ?", (new_xui_uuid, expiry_date, key_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to update key {key_id}: {e}")

def get_next_key_number(user_id: int) -> int:
    keys = get_user_keys(user_id)
    return len(keys) + 1

def get_keys_for_host(host_name: str) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE host_name = ?", (host_name,))
            keys = cursor.fetchall()
            return [dict(key) for key in keys]
    except sqlite3.Error as e:
        logging.error(f"Failed to get keys for host '{host_name}': {e}")
        return []

def get_all_vpn_users():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT user_id FROM vpn_keys")
            users = cursor.fetchall()
            return [dict(user) for user in users]
    except sqlite3.Error as e:
        logging.error(f"Failed to get all vpn users: {e}")
        return []

def update_key_status_from_server(key_email: str, xui_client_data):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            if xui_client_data:
                expiry_date = datetime.fromtimestamp(xui_client_data.expiry_time / 1000)
                cursor.execute("UPDATE vpn_keys SET xui_client_uuid = ?, expiry_date = ? WHERE key_email = ?", (xui_client_data.id, expiry_date, key_email))
            else:
                cursor.execute("DELETE FROM vpn_keys WHERE key_email = ?", (key_email,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to update key status for {key_email}: {e}")

def get_daily_stats_for_charts(days: int = 30) -> dict:
    stats = {'users': {}, 'keys': {}}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            query_users = """
                SELECT date(registration_date) as day, COUNT(*)
                FROM users
                WHERE registration_date >= date('now', ?)
                GROUP BY day
                ORDER BY day;
            """
            cursor.execute(query_users, (f'-{days} days',))
            for row in cursor.fetchall():
                stats['users'][row[0]] = row[1]
            
            query_keys = """
                SELECT date(created_date) as day, COUNT(*)
                FROM vpn_keys
                WHERE created_date >= date('now', ?)
                GROUP BY day
                ORDER BY day;
            """
            cursor.execute(query_keys, (f'-{days} days',))
            for row in cursor.fetchall():
                stats['keys'][row[0]] = row[1]
    except sqlite3.Error as e:
        logging.error(f"Failed to get daily stats for charts: {e}")
    return stats


def get_recent_transactions(limit: int = 15) -> list[dict]:
    transactions = []
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            query = """
                SELECT
                    k.key_id,
                    k.host_name,
                    k.created_date,
                    u.telegram_id,
                    u.username
                FROM vpn_keys k
                JOIN users u ON k.user_id = u.telegram_id
                ORDER BY k.created_date DESC
                LIMIT ?;
            """
            cursor.execute(query, (limit,))
            transactions = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get recent transactions: {e}")
    return transactions

def get_all_users() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY registration_date DESC")
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get all users: {e}")
        return []

def ban_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to ban user {telegram_id}: {e}")

def unban_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to unban user {telegram_id}: {e}")

def delete_user_keys(user_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE user_id = ?", (user_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to delete keys for user {user_id}: {e}")

# --- Support tickets/messages CRUD ---
def create_support_ticket(user_id: int, subject: str | None = None) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO support_tickets (user_id, subject) VALUES (?, ?)",
                (user_id, subject)
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Failed to create support ticket for user {user_id}: {e}")
        return None

def add_support_message(ticket_id: int, sender: str, content: str) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO support_messages (ticket_id, sender, content) VALUES (?, ?, ?)",
                (ticket_id, sender, content)
            )
            cursor.execute(
                "UPDATE support_tickets SET updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (ticket_id,)
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Failed to add support message to ticket {ticket_id}: {e}")
        return None

def update_ticket_thread_info(ticket_id: int, forum_chat_id: str | None, message_thread_id: int | None) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE support_tickets SET forum_chat_id = ?, message_thread_id = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (forum_chat_id, message_thread_id, ticket_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to update thread info for ticket {ticket_id}: {e}")
        return False

def get_ticket(ticket_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM support_tickets WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get ticket {ticket_id}: {e}")
        return None

def get_ticket_by_thread(forum_chat_id: str, message_thread_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM support_tickets WHERE forum_chat_id = ? AND message_thread_id = ?",
                (str(forum_chat_id), int(message_thread_id))
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get ticket by thread {forum_chat_id}/{message_thread_id}: {e}")
        return None

def get_user_tickets(user_id: int, status: str | None = None) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE user_id = ? AND status = ? ORDER BY updated_at DESC",
                    (user_id, status)
                )
            else:
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC",
                    (user_id,)
                )
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get tickets for user {user_id}: {e}")
        return []

def get_ticket_messages(ticket_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM support_messages WHERE ticket_id = ? ORDER BY created_at ASC",
                (ticket_id,)
            )
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get messages for ticket {ticket_id}: {e}")
        return []

def set_ticket_status(ticket_id: int, status: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE support_tickets SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (status, ticket_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to set status '{status}' for ticket {ticket_id}: {e}")
        return False

def update_ticket_subject(ticket_id: int, subject: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE support_tickets SET subject = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (subject, ticket_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to update subject for ticket {ticket_id}: {e}")
        return False

def delete_ticket(ticket_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # First delete messages to maintain FK consistency
            cursor.execute(
                "DELETE FROM support_messages WHERE ticket_id = ?",
                (ticket_id,)
            )
            # Then delete the ticket itself
            cursor.execute(
                "DELETE FROM support_tickets WHERE ticket_id = ?",
                (ticket_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to delete ticket {ticket_id}: {e}")
        return False

def get_tickets_paginated(page: int = 1, per_page: int = 20, status: str | None = None) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = ?", (status,))
                total = cursor.fetchone()[0] or 0
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE status = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (status, per_page, offset)
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM support_tickets")
                total = cursor.fetchone()[0] or 0
                cursor.execute(
                    "SELECT * FROM support_tickets ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (per_page, offset)
                )
            return [dict(r) for r in cursor.fetchall()], total
    except sqlite3.Error as e:
        logging.error("Failed to get paginated support tickets: %s", e)
        return [], 0

def get_open_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Failed to get open tickets count: %s", e)
        return 0