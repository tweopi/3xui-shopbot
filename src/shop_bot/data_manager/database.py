import sqlite3
from datetime import datetime
import logging
from pathlib import Path
import json
import re

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/app/project")
DB_FILE = PROJECT_ROOT / "users.db"

def normalize_host_name(name: str | None) -> str:
    """Normalize host name by trimming and removing invisible/unicode spaces.
    Removes: NBSP(\u00A0), ZERO WIDTH SPACE(\u200B), ZWNJ(\u200C), ZWJ(\u200D), BOM(\uFEFF).
    """
    s = (name or "").strip()
    for ch in ("\u00A0", "\u200B", "\u200C", "\u200D", "\uFEFF"):
        s = s.replace(ch, "")
    return s

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
                    balance REAL DEFAULT 0,
                    referred_by INTEGER,
                    referral_balance REAL DEFAULT 0,
                    referral_balance_all REAL DEFAULT 0,
                    referral_start_bonus_received BOOLEAN DEFAULT 0
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
                    host_inbound_id INTEGER NOT NULL,
                    subscription_url TEXT,
                    ssh_host TEXT,
                    ssh_port INTEGER,
                    ssh_user TEXT,
                    ssh_password TEXT,
                    ssh_key_path TEXT
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
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS host_speedtests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    method TEXT NOT NULL, -- 'ssh' | 'net'
                    ping_ms REAL,
                    jitter_ms REAL,
                    download_mbps REAL,
                    upload_mbps REAL,
                    server_name TEXT,
                    server_id TEXT,
                    ok INTEGER NOT NULL DEFAULT 1,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_host_speedtests_host_time ON host_speedtests(host_name, created_at DESC)")
            default_settings = {
                "panel_login": "admin",
                "panel_password": "admin",
                "about_text": None,
                "terms_url": None,
                "privacy_url": None,
                "support_user": None,
                "support_text": None,
                # Editable content
                "main_menu_text": None,
                "howto_android_text": None,
                "howto_ios_text": None,
                "howto_windows_text": None,
                "howto_linux_text": None,
                # Button texts (customizable)
                "btn_try": "🎁 Попробовать бесплатно",
                "btn_profile": "👤 Мой профиль",
                "btn_my_keys": "🔑 Мои ключи ({count})",
                "btn_buy_key": "💳 Купить ключ",
                "btn_top_up": "➕ Пополнить баланс",
                "btn_referral": "🤝 Реферальная программа",
                "btn_support": "🆘 Поддержка",
                "btn_about": "ℹ️ О проекте",
                "btn_howto": "❓ Как использовать",
                "btn_admin": "⚙️ Админка",
                "btn_back_to_menu": "⬅️ Назад в меню",
                "btn_back": "⬅️ Назад",
                "btn_back_to_plans": "⬅️ Назад к тарифам",
                "btn_back_to_key": "⬅️ Назад к ключу",
                "btn_back_to_keys": "⬅️ Назад к списку ключей",
                "btn_extend_key": "➕ Продлить этот ключ",
                "btn_show_qr": "📱 Показать QR-код",
                "btn_instruction": "📖 Инструкция",
                "btn_switch_server": "🌍 Сменить сервер",
                "btn_skip_email": "➡️ Продолжить без почты",
                "btn_go_to_payment": "Перейти к оплате",
                "btn_check_payment": "✅ Проверить оплату",
                "btn_pay_with_balance": "💼 Оплатить с баланса",
                # About/links
                "btn_channel": "📰 Наш канал",
                "btn_terms": "📄 Условия использования",
                "btn_privacy": "🔒 Политика конфиденциальности",
                # Howto platform buttons
                "btn_howto_android": "📱 Android",
                "btn_howto_ios": "📱 iOS",
                "btn_howto_windows": "💻 Windows",
                "btn_howto_linux": "🐧 Linux",
                # Support menu
                "btn_support_open": "🆘 Открыть поддержку",
                "btn_support_new_ticket": "✍️ Новое обращение",
                "btn_support_my_tickets": "📨 Мои обращения",
                "btn_support_external": "🆘 Внешняя поддержка",
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
                "admin_telegram_ids": None,
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
                # Referral program advanced
                "enable_fixed_referral_bonus": "false",
                "fixed_referral_bonus_amount": "50",
                "referral_reward_type": "percent_purchase",  # percent_purchase | fixed_purchase | fixed_start_referrer
                "referral_on_start_referrer_amount": "20",
                # Backups
                "backup_interval_days": "1",
                # Telegram Stars payments
                "stars_enabled": "false",
                # Сколько звёзд списывать за 1 RUB (напр., 1.5 звезды за 1 рубль)
                "stars_per_rub": "1",
                # Заголовок/описание инвойсов Stars
                "stars_title": "VPN подписка",
                "stars_description": "Оплата в Telegram Stars",
                # YooMoney separate payments
                "yoomoney_enabled": "false",
                "yoomoney_wallet": None,
                "yoomoney_api_token": None,
            }
            run_migration()
            for key, value in default_settings.items():
                cursor.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
            logging.info("База данных успешно инициализирована.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка базы данных при инициализации: {e}")

def run_migration():
    if not DB_FILE.exists():
        logging.error("Файл базы данных users.db не найден. Мигрировать нечего.")
        return

    logging.info(f"Начинаю миграцию базы данных: {DB_FILE}")

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        logging.info("Миграция таблицы 'users' ...")
    
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'referred_by' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
            logging.info(" -> Столбец 'referred_by' успешно добавлен.")
        else:
            logging.info(" -> Столбец 'referred_by' уже существует.")
            
        if 'balance' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0")
            logging.info(" -> Столбец 'balance' успешно добавлен.")
        else:
            logging.info(" -> Столбец 'balance' уже существует.")
        
        if 'referral_balance' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referral_balance REAL DEFAULT 0")
            logging.info(" -> Столбец 'referral_balance' успешно добавлен.")
        else:
            logging.info(" -> Столбец 'referral_balance' уже существует.")
        
        if 'referral_balance_all' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referral_balance_all REAL DEFAULT 0")
            logging.info(" -> Столбец 'referral_balance_all' успешно добавлен.")
        else:
            logging.info(" -> Столбец 'referral_balance_all' уже существует.")

        if 'referral_start_bonus_received' not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN referral_start_bonus_received BOOLEAN DEFAULT 0")
            logging.info(" -> Столбец 'referral_start_bonus_received' успешно добавлен.")
        else:
            logging.info(" -> Столбец 'referral_start_bonus_received' уже существует.")
        
        logging.info("Таблица 'users' успешно обновлена.")

        logging.info("Миграция таблицы 'transactions' ...")

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
        table_exists = cursor.fetchone()

        if table_exists:
            cursor.execute("PRAGMA table_info(transactions)")
            trans_columns = [row[1] for row in cursor.fetchall()]
            
            if 'payment_id' in trans_columns and 'status' in trans_columns and 'username' in trans_columns:
                logging.info("Таблица 'transactions' уже имеет новую структуру. Миграция не требуется.")
            else:
                backup_name = f"transactions_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                logging.warning(f"Обнаружена старая структура таблицы 'transactions'. Переименовываю в '{backup_name}' ...")
                cursor.execute(f"ALTER TABLE transactions RENAME TO {backup_name}")
                
                logging.info("Создаю новую таблицу 'transactions' с корректной структурой ...")
                create_new_transactions_table(cursor)
                logging.info("Новая таблица 'transactions' успешно создана. Старые данные сохранены.")
        else:
            logging.info("Таблица 'transactions' не найдена. Создаю новую ...")
            create_new_transactions_table(cursor)
            logging.info("Новая таблица 'transactions' успешно создана.")

        logging.info("Миграция таблицы 'support_tickets' ...")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='support_tickets'")
        table_exists = cursor.fetchone()
        if table_exists:
            cursor.execute("PRAGMA table_info(support_tickets)")
            st_columns = [row[1] for row in cursor.fetchall()]
            if 'forum_chat_id' not in st_columns:
                cursor.execute("ALTER TABLE support_tickets ADD COLUMN forum_chat_id TEXT")
                logging.info(" -> Столбец 'forum_chat_id' успешно добавлен в 'support_tickets'.")
            else:
                logging.info(" -> Столбец 'forum_chat_id' уже существует в 'support_tickets'.")
            if 'message_thread_id' not in st_columns:
                cursor.execute("ALTER TABLE support_tickets ADD COLUMN message_thread_id INTEGER")
                logging.info(" -> Столбец 'message_thread_id' успешно добавлен в 'support_tickets'.")
            else:
                logging.info(" -> Столбец 'message_thread_id' уже существует в 'support_tickets'.")
        else:
            logging.warning("Таблица 'support_tickets' не найдена, пропускаю её миграцию.")

        conn.commit()
        
        logging.info("Миграция таблицы 'support_messages' ...")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='support_messages'")
        table_exists = cursor.fetchone()
        if table_exists:
            cursor.execute("PRAGMA table_info(support_messages)")
            sm_columns = [row[1] for row in cursor.fetchall()]
            if 'media' not in sm_columns:
                cursor.execute("ALTER TABLE support_messages ADD COLUMN media TEXT")
                logging.info(" -> Столбец 'media' успешно добавлен в 'support_messages'.")
            else:
                logging.info(" -> Столбец 'media' уже существует в 'support_messages'.")
        else:
            logging.warning("Таблица 'support_messages' не найдена, пропускаю её миграцию.")
        
        logging.info("Миграция таблицы 'xui_hosts' ...")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='xui_hosts'")
        table_exists = cursor.fetchone()
        if table_exists:
            cursor.execute("PRAGMA table_info(xui_hosts)")
            xh_columns = [row[1] for row in cursor.fetchall()]
            if 'subscription_url' not in xh_columns:
                cursor.execute("ALTER TABLE xui_hosts ADD COLUMN subscription_url TEXT")
                logging.info(" -> Столбец 'subscription_url' успешно добавлен в 'xui_hosts'.")
            else:
                logging.info(" -> Столбец 'subscription_url' уже существует в 'xui_hosts'.")
            # SSH settings for speedtests (optional)
            if 'ssh_host' not in xh_columns:
                cursor.execute("ALTER TABLE xui_hosts ADD COLUMN ssh_host TEXT")
                logging.info(" -> Столбец 'ssh_host' успешно добавлен в 'xui_hosts'.")
            if 'ssh_port' not in xh_columns:
                cursor.execute("ALTER TABLE xui_hosts ADD COLUMN ssh_port INTEGER")
                logging.info(" -> Столбец 'ssh_port' успешно добавлен в 'xui_hosts'.")
            if 'ssh_user' not in xh_columns:
                cursor.execute("ALTER TABLE xui_hosts ADD COLUMN ssh_user TEXT")
                logging.info(" -> Столбец 'ssh_user' успешно добавлен в 'xui_hosts'.")
            if 'ssh_password' not in xh_columns:
                cursor.execute("ALTER TABLE xui_hosts ADD COLUMN ssh_password TEXT")
                logging.info(" -> Столбец 'ssh_password' успешно добавлен в 'xui_hosts'.")
            if 'ssh_key_path' not in xh_columns:
                cursor.execute("ALTER TABLE xui_hosts ADD COLUMN ssh_key_path TEXT")
                logging.info(" -> Столбец 'ssh_key_path' успешно добавлен в 'xui_hosts'.")
            # Clean up host_name values from invisible spaces and trim
            try:
                cursor.execute(
                    """
                    UPDATE xui_hosts
                    SET host_name = TRIM(
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(host_name,
                            char(160), ''),      -- NBSP
                            char(8203), ''),     -- ZERO WIDTH SPACE
                            char(8204), ''),     -- ZWNJ
                            char(8205), ''),     -- ZWJ
                            char(65279), ''      -- BOM
                        )
                    )
                    """
                )
                conn.commit()
                logging.info(" -> Нормализованы существующие значения host_name в 'xui_hosts'.")
            except Exception as e:
                logging.warning(f" -> Не удалось нормализовать существующие значения host_name: {e}")
        else:
            logging.warning("Таблица 'xui_hosts' не найдена, пропускаю её миграцию.")
        # Create table for host speedtests
        try:
            cursor = conn.cursor()
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS host_speedtests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    method TEXT NOT NULL, -- 'ssh' | 'net'
                    ping_ms REAL,
                    jitter_ms REAL,
                    download_mbps REAL,
                    upload_mbps REAL,
                    server_name TEXT,
                    server_id TEXT,
                    ok INTEGER NOT NULL DEFAULT 1,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_host_speedtests_host_time ON host_speedtests(host_name, created_at DESC)")
            conn.commit()
            logging.info("Таблица 'host_speedtests' готова к использованию.")
        except sqlite3.Error as e:
            logging.error(f"Не удалось создать 'host_speedtests': {e}")

        conn.close()
        
        logging.info("--- Миграция базы данных успешно завершена! ---")

    except sqlite3.Error as e:
        logging.error(f"Ошибка во время миграции: {e}")

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

def create_host(name: str, url: str, user: str, passwd: str, inbound: int, subscription_url: str | None = None):
    try:
        name = normalize_host_name(name)
        url = (url or "").strip()
        user = (user or "").strip()
        passwd = passwd or ""
        try:
            inbound = int(inbound)
        except Exception:
            pass
        subscription_url = (subscription_url or None)

        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id, subscription_url) VALUES (?, ?, ?, ?, ?, ?)",
                    (name, url, user, passwd, inbound, subscription_url)
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id) VALUES (?, ?, ?, ?, ?)",
                    (name, url, user, passwd, inbound)
                )
            conn.commit()
            logging.info(f"Успешно создан новый хост: {name}")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при создании хоста '{name}': {e}")

def update_host_subscription_url(host_name: str, subscription_url: str | None) -> bool:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            exists = cursor.fetchone() is not None
            if not exists:
                logging.warning(f"update_host_subscription_url: хост с именем '{host_name}' не найден (после TRIM)")
                return False

            cursor.execute(
                "UPDATE xui_hosts SET subscription_url = ? WHERE TRIM(host_name) = TRIM(?)",
                (subscription_url, host_name)
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить subscription_url для хоста '{host_name}': {e}")
        return False

def set_referral_start_bonus_received(user_id: int) -> bool:
    """Пометить, что пользователь получил стартовый бонус за реферальную регистрацию."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET referral_start_bonus_received = 1 WHERE telegram_id = ?",
                (user_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось пометить получение стартового реферального бонуса для пользователя {user_id}: {e}")
        return False

def update_host_url(host_name: str, new_url: str) -> bool:
    """Обновить URL панели XUI для указанного хоста."""
    try:
        host_name = normalize_host_name(host_name)
        new_url = (new_url or "").strip()
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_url: хост с именем '{host_name}' не найден")
                return False

            cursor.execute(
                "UPDATE xui_hosts SET host_url = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_url, host_name)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить host_url для хоста '{host_name}': {e}")
        return False

def update_host_name(old_name: str, new_name: str) -> bool:
    """Переименовать хост во всех связанных таблицах (xui_hosts, plans, vpn_keys)."""
    try:
        old_name_n = normalize_host_name(old_name)
        new_name_n = normalize_host_name(new_name)
        if not new_name_n:
            logging.warning("update_host_name: new host name is empty after normalization")
            return False
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (old_name_n,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_name: исходный хост не найден '{old_name_n}'")
                return False
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (new_name_n,))
            exists_target = cursor.fetchone() is not None
            if exists_target and old_name_n.lower() != new_name_n.lower():
                logging.warning(f"update_host_name: целевое имя '{new_name_n}' уже используется")
                return False

            cursor.execute(
                "UPDATE xui_hosts SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)",
                (new_name_n, old_name_n)
            )
            cursor.execute(
                "UPDATE plans SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)",
                (new_name_n, old_name_n)
            )
            cursor.execute(
                "UPDATE vpn_keys SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)",
                (new_name_n, old_name_n)
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось переименовать хост с '{old_name}' на '{new_name}': {e}")
        return False

def delete_host(host_name: str):
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            cursor.execute("DELETE FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            conn.commit()
            logging.info(f"Хост '{host_name}' и его тарифы успешно удалены.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка удаления хоста '{host_name}': {e}")

def get_host(host_name: str) -> dict | None:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            result = cursor.fetchone()
            return dict(result) if result else None
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения хоста '{host_name}': {e}")
        return None

def update_host_ssh_settings(
    host_name: str,
    ssh_host: str | None = None,
    ssh_port: int | None = None,
    ssh_user: str | None = None,
    ssh_password: str | None = None,
    ssh_key_path: str | None = None,
) -> bool:
    """Обновить SSH-параметры для speedtest/maintenance по хосту.
    Переданные None значения очищают соответствующие поля (ставят NULL).
    """
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_ssh_settings: хост не найден '{host_name_n}'")
                return False

            cursor.execute(
                """
                UPDATE xui_hosts
                SET ssh_host = ?, ssh_port = ?, ssh_user = ?, ssh_password = ?, ssh_key_path = ?
                WHERE TRIM(host_name) = TRIM(?)
                """,
                (
                    (ssh_host or None),
                    (int(ssh_port) if ssh_port is not None else None),
                    (ssh_user or None),
                    (ssh_password if ssh_password is not None else None),
                    (ssh_key_path or None),
                    host_name_n,
                ),
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить SSH-настройки для хоста '{host_name}': {e}")
        return False

def delete_key_by_id(key_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE key_id = ?", (key_id,))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось удалить ключ по id {key_id}: {e}")
        return False

def update_key_comment(key_id: int, comment: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET comment = ? WHERE key_id = ?", (comment, key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить комментарий ключа для {key_id}: {e}")
        return False

def get_all_hosts() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts")
            hosts = cursor.fetchall()
            # Normalize host_name in returned dicts to avoid trailing/invisible chars in runtime
            result = []
            for row in hosts:
                d = dict(row)
                d['host_name'] = normalize_host_name(d.get('host_name'))
                result.append(d)
            return result
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения списка всех хостов: {e}")
        return []

def get_speedtests(host_name: str, limit: int = 20) -> list[dict]:
    """Получить последние результаты спидтестов по хосту (ssh/net), новые сверху."""
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                limit_int = int(limit)
            except Exception:
                limit_int = 20
            cursor.execute(
                """
                SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps,
                       server_name, server_id, ok, error, created_at
                FROM host_speedtests
                WHERE TRIM(host_name) = TRIM(?)
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """,
                (host_name_n, limit_int),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить speedtest-данные для хоста '{host_name}': {e}")
        return []

def get_latest_speedtest(host_name: str) -> dict | None:
    """Получить последний по времени спидтест для хоста."""
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps,
                       server_name, server_id, ok, error, created_at
                FROM host_speedtests
                WHERE TRIM(host_name) = TRIM(?)
                ORDER BY datetime(created_at) DESC
                LIMIT 1
                """,
                (host_name_n,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить последний speedtest для хоста '{host_name}': {e}")
        return None

def find_and_complete_pending_transaction(
    payment_id: str,
    amount_rub: float | None,
    payment_method: str,
    currency_name: str | None = None,
    amount_currency: float | None = None,
) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM transactions WHERE payment_id = ? AND status = 'pending'", (payment_id,))
            transaction = cursor.fetchone()
            if not transaction:
                logger.warning(f"Pending transaction not found for payment_id={payment_id}")
                return None

            cursor.execute(
                """
                UPDATE transactions
                SET status = 'paid',
                    amount_rub = COALESCE(?, amount_rub),
                    amount_currency = COALESCE(?, amount_currency),
                    currency_name = COALESCE(?, currency_name),
                    payment_method = COALESCE(?, payment_method)
                WHERE payment_id = ?
                """,
                (amount_rub, amount_currency, currency_name, payment_method, payment_id)
            )
            conn.commit()

            try:
                raw_md = None
                try:
                    raw_md = transaction['metadata']
                except Exception:
                    raw_md = None
                md = json.loads(raw_md) if raw_md else {}
            except Exception:
                md = {}
            return md
    except sqlite3.Error as e:
        logging.error(f"Failed to complete pending transaction {payment_id}: {e}")
        return None

def insert_host_speedtest(
    host_name: str,
    method: str,
    ping_ms: float | None = None,
    jitter_ms: float | None = None,
    download_mbps: float | None = None,
    upload_mbps: float | None = None,
    server_name: str | None = None,
    server_id: str | None = None,
    ok: bool = True,
    error: str | None = None,
) -> bool:
    """Сохранить результат спидтеста в таблицу host_speedtests."""
    try:
        host_name_n = normalize_host_name(host_name)
        method_s = (method or '').strip().lower()
        if method_s not in ('ssh', 'net'):
            method_s = 'ssh'
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO host_speedtests
                (host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, ok, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                , (
                    host_name_n,
                    method_s,
                    ping_ms,
                    jitter_ms,
                    download_mbps,
                    upload_mbps,
                    server_name,
                    server_id,
                    1 if ok else 0,
                    (error or None)
                )
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось сохранить запись speedtest для '{host_name}': {e}")
        return False

def get_admin_stats() -> dict:
    """Return aggregated statistics for the admin dashboard.
    Includes:
    - total_users: count of users
    - total_keys: count of all keys
    - active_keys: keys with expiry_date in the future
    - total_income: sum of amount_rub for successful transactions
    """
    stats = {
        "total_users": 0,
        "total_keys": 0,
        "active_keys": 0,
        "total_income": 0.0,
        # today's metrics
        "today_new_users": 0,
        "today_income": 0.0,
        "today_issued_keys": 0,
    }
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # users
            cursor.execute("SELECT COUNT(*) FROM users")
            row = cursor.fetchone()
            stats["total_users"] = (row[0] or 0) if row else 0

            # total keys
            cursor.execute("SELECT COUNT(*) FROM vpn_keys")
            row = cursor.fetchone()
            stats["total_keys"] = (row[0] or 0) if row else 0

            # active keys
            cursor.execute("SELECT COUNT(*) FROM vpn_keys WHERE expiry_date > CURRENT_TIMESTAMP")
            row = cursor.fetchone()
            stats["active_keys"] = (row[0] or 0) if row else 0

            # income: consider common success markers (total)
            cursor.execute(
                "SELECT COALESCE(SUM(amount_rub), 0) FROM transactions WHERE status IN ('paid','success','succeeded') AND LOWER(COALESCE(payment_method, '')) <> 'balance'"
            )
            row = cursor.fetchone()
            stats["total_income"] = float(row[0] or 0.0) if row else 0.0

            # today's metrics
            # new users today
            cursor.execute(
                "SELECT COUNT(*) FROM users WHERE date(registration_date) = date('now')"
            )
            row = cursor.fetchone()
            stats["today_new_users"] = (row[0] or 0) if row else 0

            # today's income
            cursor.execute(
                """
                SELECT COALESCE(SUM(amount_rub), 0)
                FROM transactions
                WHERE status IN ('paid','success','succeeded')
                  AND LOWER(COALESCE(payment_method, '')) <> 'balance'
                  AND date(created_date) = date('now')
                """
            )
            row = cursor.fetchone()
            stats["today_income"] = float(row[0] or 0.0) if row else 0.0

            # today's issued keys
            cursor.execute(
                "SELECT COUNT(*) FROM vpn_keys WHERE date(created_date) = date('now')"
            )
            row = cursor.fetchone()
            stats["today_issued_keys"] = (row[0] or 0) if row else 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get admin stats: {e}")
    return stats

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

def get_keys_for_user(user_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE user_id = ? ORDER BY created_date DESC", (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get keys for user {user_id}: {e}")
        return []

def get_key_by_id(key_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get key by id {key_id}: {e}")
        return None

def update_key_email(key_id: int, new_email: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET key_email = ? WHERE key_id = ?", (new_email, key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.IntegrityError as e:
        logging.error(f"Email uniqueness violation for key {key_id}: {e}")
        return False
    except sqlite3.Error as e:
        logging.error(f"Failed to update key email for {key_id}: {e}")
        return False

def update_key_host(key_id: int, new_host_name: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET host_name = ? WHERE key_id = ?", (normalize_host_name(new_host_name), key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to update key host for {key_id}: {e}")
        return False

def create_gift_key(user_id: int, host_name: str, key_email: str, months: int, xui_client_uuid: str | None = None) -> int | None:
    """Создать подарочный ключ: задаёт expiry_date = now + months, host_name нормализуется.
    Возвращает key_id или None при ошибке."""
    try:
        host_name = normalize_host_name(host_name)
        from datetime import timedelta
        expiry = datetime.now() + timedelta(days=30 * int(months or 1))
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO vpn_keys (user_id, host_name, xui_client_uuid, key_email, expiry_date) VALUES (?, ?, ?, ?, ?)",
                (user_id, host_name, xui_client_uuid or f"GIFT-{user_id}-{int(datetime.now().timestamp())}", key_email, expiry.isoformat())
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        logging.error(f"Failed to create gift key for user {user_id}: duplicate email {key_email}: {e}")
        return None
    except sqlite3.Error as e:
        logging.error(f"Failed to create gift key for user {user_id}: {e}")
        return None

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

def get_admin_ids() -> set[int]:
    """Возвращает множество ID администраторов из настроек.
    Поддерживает оба варианта: одиночный 'admin_telegram_id' и список 'admin_telegram_ids'
    через запятую/пробелы или JSON-массив.
    """
    ids: set[int] = set()
    try:
        single = get_setting("admin_telegram_id")
        if single:
            try:
                ids.add(int(single))
            except Exception:
                pass
        multi_raw = get_setting("admin_telegram_ids")
        if multi_raw:
            s = (multi_raw or "").strip()
            # Попробуем как JSON-массив
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    for v in arr:
                        try:
                            ids.add(int(v))
                        except Exception:
                            pass
                    return ids
            except Exception:
                pass
            # Иначе как строка с разделителями (запятая/пробел)
            parts = [p for p in re.split(r"[\s,]+", s) if p]
            for p in parts:
                try:
                    ids.add(int(p))
                except Exception:
                    pass
    except Exception as e:
        logging.warning(f"get_admin_ids failed: {e}")
    return ids

def is_admin(user_id: int) -> bool:
    """Проверка прав администратора по списку ID из настроек."""
    try:
        return int(user_id) in get_admin_ids()
    except Exception:
        return False
        
def get_referrals_for_user(user_id: int) -> list[dict]:
    """Возвращает список пользователей, которых пригласил данный user_id.
    Поля: telegram_id, username, registration_date, total_spent.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT telegram_id, username, registration_date, total_spent
                FROM users
                WHERE referred_by = ?
                ORDER BY registration_date DESC
                """,
                (user_id,)
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error(f"Failed to get referrals for user {user_id}: {e}")
        return []
        
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
        host_name = normalize_host_name(host_name)
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
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE TRIM(host_name) = TRIM(?) ORDER BY months", (host_name,))
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
            cursor.execute("SELECT referred_by FROM users WHERE telegram_id = ?", (telegram_id,))
            row = cursor.fetchone()
            if not row:
                # Новый пользователь — сразу сохраняем возможного реферера
                cursor.execute(
                    "INSERT INTO users (telegram_id, username, registration_date, referred_by) VALUES (?, ?, ?, ?)",
                    (telegram_id, username, datetime.now(), referrer_id)
                )
            else:
                # Пользователь уже есть — обновим username, и если есть реферер и поле пустое, допишем
                cursor.execute("UPDATE users SET username = ? WHERE telegram_id = ?", (username, telegram_id))
                current_ref = row[0]
                if referrer_id and (current_ref is None or str(current_ref).strip() == "") and int(referrer_id) != int(telegram_id):
                    try:
                        cursor.execute("UPDATE users SET referred_by = ? WHERE telegram_id = ?", (int(referrer_id), telegram_id))
                    except Exception:
                        # best-effort
                        pass
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

def add_to_referral_balance_all(user_id: int, amount: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET referral_balance_all = referral_balance_all + ? WHERE telegram_id = ?",
                (amount, user_id)
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to add to total referral balance for user {user_id}: {e}")

def get_referral_balance_all(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_balance_all FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            return row[0] if row else 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get total referral balance for user {user_id}: {e}")
        return 0.0

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

def get_balance(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get balance for user {user_id}: {e}")
        return 0.0

def adjust_user_balance(user_id: int, delta: float) -> bool:
    """Скорректировать баланс пользователя на указанную дельту (может быть отрицательной)."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE telegram_id = ?", (float(delta), user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to adjust balance for user {user_id}: {e}")
        return False

def set_balance(user_id: int, value: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = ? WHERE telegram_id = ?", (value, user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to set balance for user {user_id}: {e}")
        return False

def add_to_balance(user_id: int, amount: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = balance + ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to add to balance for user {user_id}: {e}")
        return False

def deduct_from_balance(user_id: int, amount: float) -> bool:
    """Атомарное списание с основного баланса при достаточности средств."""
    if amount <= 0:
        return True
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("SELECT balance FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            current = row[0] if row else 0.0
            if current < amount:
                conn.rollback()
                return False
            cursor.execute("UPDATE users SET balance = balance - ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to deduct from balance for user {user_id}: {e}")
        return False

def deduct_from_referral_balance(user_id: int, amount: float) -> bool:
    """Атомарное списание с реферального баланса при достаточности средств."""
    if amount <= 0:
        return True
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("SELECT referral_balance FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            current = row[0] if row else 0.0
            if current < amount:
                conn.rollback()
                return False
            cursor.execute("UPDATE users SET referral_balance = referral_balance - ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to deduct from referral balance for user {user_id}: {e}")
        return False

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
            logging.info(f"Пользователь {telegram_id} согласился с условиями.")
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
            # Consider only completed/paid transactions when summing total spent
            cursor.execute(
                """
                SELECT COALESCE(SUM(amount_rub), 0.0)
                FROM transactions
                WHERE LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success')
                  AND LOWER(COALESCE(payment_method, '')) <> 'balance'
                """
            )
            val = cursor.fetchone()
            return (val[0] if val else 0.0) or 0.0
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

def delete_key_by_email(email: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE key_email = ?", (email,))
            affected = cursor.rowcount
            conn.commit()
            logger.debug(f"delete_key_by_email('{email}') affected={affected}")
            return affected > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to delete key '{email}': {e}")
        return False

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
 
def update_key_host_and_info(key_id: int, new_host_name: str, new_xui_uuid: str, new_expiry_ms: int):
    """Update key's host, UUID and expiry in a single transaction."""
    try:
        new_host_name = normalize_host_name(new_host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            expiry_date = datetime.fromtimestamp(new_expiry_ms / 1000)
            cursor.execute(
                "UPDATE vpn_keys SET host_name = ?, xui_client_uuid = ?, expiry_date = ? WHERE key_id = ?",
                (new_host_name, new_xui_uuid, expiry_date, key_id)
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to update key {key_id} host and info: {e}")

def get_next_key_number(user_id: int) -> int:
    keys = get_user_keys(user_id)
    return len(keys) + 1

def get_keys_for_host(host_name: str) -> list[dict]:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE TRIM(host_name) = TRIM(?)", (host_name,))
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
            cursor.execute(
                "DELETE FROM support_messages WHERE ticket_id = ?",
                (ticket_id,)
            )
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

def get_closed_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'closed'")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Failed to get closed tickets count: %s", e)
        return 0

def get_all_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Failed to get all tickets count: %s", e)
        return 0