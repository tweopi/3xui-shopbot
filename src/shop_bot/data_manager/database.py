import sqlite3
from datetime import datetime, timezone
import logging
from pathlib import Path
import json
import re
from typing import Any

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/app/project")
DB_FILE = PROJECT_ROOT / "users.db"


def _now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _to_datetime_str(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _normalize_email(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _normalize_key_row(row: sqlite3.Row | dict | None) -> dict | None:
    if row is None:
        return None
    data = dict(row)
    email = _normalize_email(data.get("email") or data.get("key_email"))
    if email:
        data["email"] = email
        data["key_email"] = email
    rem_uuid = data.get("remnawave_user_uuid") or data.get("xui_client_uuid")
    if rem_uuid:
        data["remnawave_user_uuid"] = rem_uuid
        data["xui_client_uuid"] = rem_uuid
    expire_value = data.get("expire_at") or data.get("expiry_date")
    if expire_value:
        expire_str = expire_value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(expire_value, datetime) else str(expire_value)
        data["expire_at"] = expire_str
        data["expiry_date"] = expire_str
    created_value = data.get("created_at") or data.get("created_date")
    if created_value:
        created_str = created_value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(created_value, datetime) else str(created_value)
        data["created_at"] = created_str
        data["created_date"] = created_str
    subscription_url = data.get("subscription_url") or data.get("connection_string")
    if subscription_url:
        data["subscription_url"] = subscription_url
        data.setdefault("connection_string", subscription_url)
    return data


def _get_table_columns(cursor: sqlite3.Cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def _ensure_table_column(cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
    columns = _get_table_columns(cursor, table)
    if column not in columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _ensure_unique_index(cursor: sqlite3.Cursor, name: str, table: str, column: str) -> None:
    cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table}({column})")


def _ensure_index(cursor: sqlite3.Cursor, name: str, table: str, column: str) -> None:
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({column})")


def normalize_host_name(name: str | None) -> str:
    """Normalize host name by trimming and removing invisible/unicode spaces."""
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
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    total_spent REAL DEFAULT 0,
                    total_months INTEGER DEFAULT 0,
                    trial_used BOOLEAN DEFAULT 0,
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
                    host_name TEXT,
                    squad_uuid TEXT,
                    remnawave_user_uuid TEXT,
                    short_uuid TEXT,
                    email TEXT UNIQUE,
                    key_email TEXT UNIQUE,
                    subscription_url TEXT,
                    expire_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    traffic_limit_bytes INTEGER,
                    traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
                    tag TEXT,
                    description TEXT
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
                    host_name TEXT PRIMARY KEY,
                    squad_uuid TEXT UNIQUE,
                    description TEXT,
                    default_traffic_limit_bytes INTEGER,
                    default_traffic_strategy TEXT DEFAULT 'NO_RESET',
                    host_url TEXT,
                    host_username TEXT,
                    host_pass TEXT,
                    host_inbound_id INTEGER,
                    subscription_url TEXT,
                    ssh_host TEXT,
                    ssh_port INTEGER,
                    ssh_user TEXT,
                    ssh_password TEXT,
                    ssh_key_path TEXT,
                    is_active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    metadata TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT,
                    squad_uuid TEXT,
                    plan_name TEXT NOT NULL,
                    months INTEGER,
                    duration_days INTEGER,
                    price REAL NOT NULL,
                    traffic_limit_bytes INTEGER,
                    traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
                    is_active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    metadata TEXT,
                    FOREIGN KEY (host_name) REFERENCES xui_hosts (host_name)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT "open",
                    subject TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id INTEGER NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT NOT NULL,
                    media TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES support_tickets (ticket_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS host_speedtests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    method TEXT NOT NULL,
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
                "enable_fixed_referral_bonus": "false",
                "fixed_referral_bonus_amount": "50",
                "referral_reward_type": "percent_purchase",
                "referral_on_start_referrer_amount": "20",
                "backup_interval_days": "1",
                "remnawave_base_url": None,
                "remnawave_api_token": None,
                "remnawave_cookies": "{}",
                "remnawave_is_local_network": "false",
                "default_extension_days": "30",
            }
            run_migration()
            for key, value in default_settings.items():
                cursor.execute(
                    "INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)",
                    (key, value),
                )
            conn.commit()
            logging.info("Database initialised")
    except sqlite3.Error as e:
        logging.error("Failed to initialize database: %s", e)


def _ensure_users_columns(cursor: sqlite3.Cursor) -> None:
    mapping = {
        "referred_by": "INTEGER",
        "balance": "REAL DEFAULT 0",
        "referral_balance": "REAL DEFAULT 0",
        "referral_balance_all": "REAL DEFAULT 0",
        "referral_start_bonus_received": "BOOLEAN DEFAULT 0",
    }
    for column, definition in mapping.items():
        _ensure_table_column(cursor, "users", column, definition)


def _ensure_hosts_columns(cursor: sqlite3.Cursor) -> None:
    extras = {
        "squad_uuid": "TEXT",
        "description": "TEXT",
        "default_traffic_limit_bytes": "INTEGER",
        "default_traffic_strategy": "TEXT DEFAULT 'NO_RESET'",
        "is_active": "INTEGER DEFAULT 1",
        "sort_order": "INTEGER DEFAULT 0",
        "metadata": "TEXT",
        "subscription_url": "TEXT",
        "ssh_host": "TEXT",
        "ssh_port": "INTEGER",
        "ssh_user": "TEXT",
        "ssh_password": "TEXT",
        "ssh_key_path": "TEXT",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "xui_hosts", column, definition)


def _ensure_plans_columns(cursor: sqlite3.Cursor) -> None:
    extras = {
        "squad_uuid": "TEXT",
        "duration_days": "INTEGER",
        "traffic_limit_bytes": "INTEGER",
        "traffic_limit_strategy": "TEXT DEFAULT 'NO_RESET'",
        "is_active": "INTEGER DEFAULT 1",
        "sort_order": "INTEGER DEFAULT 0",
        "metadata": "TEXT",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "plans", column, definition)


def _finalize_vpn_key_indexes(cursor: sqlite3.Cursor) -> None:
    _ensure_unique_index(cursor, "uq_vpn_keys_email", "vpn_keys", "email")
    _ensure_unique_index(cursor, "uq_vpn_keys_key_email", "vpn_keys", "key_email")
    _ensure_index(cursor, "idx_vpn_keys_user_id", "vpn_keys", "user_id")
    _ensure_index(cursor, "idx_vpn_keys_rem_uuid", "vpn_keys", "remnawave_user_uuid")
    _ensure_index(cursor, "idx_vpn_keys_expire_at", "vpn_keys", "expire_at")


def _rebuild_vpn_keys_table(cursor: sqlite3.Cursor) -> None:
    columns = _get_table_columns(cursor, "vpn_keys")
    legacy_markers = {"xui_client_uuid", "expiry_date", "created_date", "connection_string"}
    required = {"remnawave_user_uuid", "email", "expire_at", "created_at", "updated_at"}
    if required.issubset(columns) and not (columns & legacy_markers):
        _finalize_vpn_key_indexes(cursor)
        return

    cursor.execute("ALTER TABLE vpn_keys RENAME TO vpn_keys_legacy")
    cursor.execute('''
        CREATE TABLE vpn_keys (
            key_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            host_name TEXT,
            squad_uuid TEXT,
            remnawave_user_uuid TEXT,
            short_uuid TEXT,
            email TEXT UNIQUE,
            key_email TEXT UNIQUE,
            subscription_url TEXT,
            expire_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            traffic_limit_bytes INTEGER,
            traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
            tag TEXT,
            description TEXT
        )
    ''')
    old_columns = _get_table_columns(cursor, "vpn_keys_legacy")

    def has(column: str) -> bool:
        return column in old_columns

    def col(column: str, default: str = "NULL") -> str:
        return column if has(column) else default

    rem_uuid_expr = "remnawave_user_uuid" if has("remnawave_user_uuid") else ("xui_client_uuid" if has("xui_client_uuid") else "NULL")
    email_expr = "LOWER(email)" if has("email") else ("LOWER(key_email)" if has("key_email") else "NULL")
    key_email_expr = "LOWER(key_email)" if has("key_email") else ("LOWER(email)" if has("email") else "NULL")
    subscription_expr = col("subscription_url", "connection_string" if has("connection_string") else "NULL")
    expire_expr = col("expire_at", "expiry_date" if has("expiry_date") else "NULL")
    created_expr = col("created_at", "created_date" if has("created_date") else "CURRENT_TIMESTAMP")
    updated_expr = col("updated_at", created_expr)
    traffic_strategy_expr = col("traffic_limit_strategy", "'NO_RESET'")

    select_clause = ",\n            ".join([
        f"{col('key_id')} AS key_id",
        f"{col('user_id')} AS user_id",
        f"{col('host_name')} AS host_name",
        f"{col('squad_uuid')} AS squad_uuid",
        f"{rem_uuid_expr} AS remnawave_user_uuid",
        f"{col('short_uuid')} AS short_uuid",
        f"{email_expr} AS email",
        f"{key_email_expr} AS key_email",
        f"{subscription_expr} AS subscription_url",
        f"{expire_expr} AS expire_at",
        f"{created_expr} AS created_at",
        f"{updated_expr} AS updated_at",
        f"{col('traffic_limit_bytes')} AS traffic_limit_bytes",
        f"{traffic_strategy_expr} AS traffic_limit_strategy",
        f"{col('tag')} AS tag",
        f"{col('description')} AS description",
    ])

    cursor.execute(
        f"""
        INSERT INTO vpn_keys (
            key_id,
            user_id,
            host_name,
            squad_uuid,
            remnawave_user_uuid,
            short_uuid,
            email,
            key_email,
            subscription_url,
            expire_at,
            created_at,
            updated_at,
            traffic_limit_bytes,
            traffic_limit_strategy,
            tag,
            description
        )
        SELECT
            {select_clause}
        FROM vpn_keys_legacy
        """
    )
    cursor.execute("DROP TABLE vpn_keys_legacy")
    cursor.execute("SELECT MAX(key_id) FROM vpn_keys")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute("INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES('vpn_keys', ?)", (max_id,))
    _finalize_vpn_key_indexes(cursor)


def _ensure_vpn_keys_schema(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vpn_keys'")
    if cursor.fetchone() is None:
        cursor.execute('''
            CREATE TABLE vpn_keys (
                key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                host_name TEXT,
                squad_uuid TEXT,
                remnawave_user_uuid TEXT,
                short_uuid TEXT,
                email TEXT UNIQUE,
                key_email TEXT UNIQUE,
                subscription_url TEXT,
                expire_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                traffic_limit_bytes INTEGER,
                traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
                tag TEXT,
                description TEXT
            )
        ''')
        _finalize_vpn_key_indexes(cursor)
        return
    _rebuild_vpn_keys_table(cursor)


def run_migration():
    if not DB_FILE.exists():
        logging.error("Database file missing, migration skipped.")
        return

    logging.info("Running database migrations: %s", DB_FILE)

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = OFF")
            _ensure_users_columns(cursor)
            _ensure_hosts_columns(cursor)
            _ensure_plans_columns(cursor)
            _ensure_vpn_keys_schema(cursor)
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
    except sqlite3.Error as e:
        logging.error("Database migration failed: %s", e)


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
    - active_keys: keys with expire_at in the future
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
            cursor.execute("SELECT COUNT(*) FROM vpn_keys WHERE expire_at IS NOT NULL AND datetime(expire_at) > CURRENT_TIMESTAMP")
            row = cursor.fetchone()
            stats["active_keys"] = (row[0] or 0) if row else 0

            # income: consider common success markers (total)
            cursor.execute(
                "SELECT COALESCE(SUM(amount_rub), 0) FROM transactions WHERE status IN ('paid','success','succeeded')"
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
                  AND date(created_date) = date('now')
                """
            )
            row = cursor.fetchone()
            stats["today_income"] = float(row[0] or 0.0) if row else 0.0

            # today's issued keys
            cursor.execute(
                "SELECT COUNT(*) FROM vpn_keys WHERE date(COALESCE(created_at, updated_at, CURRENT_TIMESTAMP)) = date('now')"
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
            return [_normalize_key_row(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get all keys: {e}")
        return []


def get_keys_for_user(user_id: int) -> list[dict]:
    return get_user_keys(user_id)

def update_key_email(key_id: int, new_email: str) -> bool:
    normalized = _normalize_email(new_email) or new_email.strip()
    return update_key_fields(key_id, email=normalized)

def update_key_host(key_id: int, new_host_name: str) -> bool:
    return update_key_fields(key_id, host_name=new_host_name)

def create_gift_key(user_id: int, host_name: str, key_email: str, months: int, remnawave_user_uuid: str | None = None) -> int | None:
    """Создать подарочный ключ: expiry = now + months."""
    try:
        from datetime import timedelta

        months_value = max(1, int(months or 1))
        expiry_dt = datetime.utcnow() + timedelta(days=30 * months_value)
        expiry_ms = int(expiry_dt.timestamp() * 1000)
        uuid_value = remnawave_user_uuid or f"GIFT-{user_id}-{int(datetime.utcnow().timestamp())}"
        return add_new_key(
            user_id=user_id,
            host_name=host_name,
            remnawave_user_uuid=uuid_value,
            key_email=key_email,
            expiry_timestamp_ms=expiry_ms,
        )
    except sqlite3.Error as e:
        logging.error(f"Failed to create gift key for user {user_id}: {e}")
        return None
    except Exception as e:
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

def add_new_key(
    user_id: int,
    host_name: str | None,
    remnawave_user_uuid: str,
    key_email: str,
    expiry_timestamp_ms: int,
    *,
    squad_uuid: str | None = None,
    short_uuid: str | None = None,
    subscription_url: str | None = None,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    description: str | None = None,
    tag: str | None = None,
) -> int | None:
    host_name_norm = normalize_host_name(host_name) if host_name else None
    email_normalized = _normalize_email(key_email) or key_email.strip()
    expire_str = _to_datetime_str(expiry_timestamp_ms) or _now_str()
    created_str = _now_str()
    strategy_value = traffic_limit_strategy or "NO_RESET"
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO vpn_keys (
                    user_id,
                    host_name,
                    squad_uuid,
                    remnawave_user_uuid,
                    short_uuid,
                    email,
                    key_email,
                    subscription_url,
                    expire_at,
                    created_at,
                    updated_at,
                    traffic_limit_bytes,
                    traffic_limit_strategy,
                    tag,
                    description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    host_name_norm,
                    squad_uuid,
                    remnawave_user_uuid,
                    short_uuid,
                    email_normalized,
                    email_normalized,
                    subscription_url,
                    expire_str,
                    created_str,
                    created_str,
                    traffic_limit_bytes,
                    strategy_value,
                    tag,
                    description,
                ),
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        logging.error(
            "Failed to add new key for user %s due to integrity error: %s",
            user_id,
            e,
        )
        return None
    except sqlite3.Error as e:
        logging.error("Failed to add new key for user %s: %s", user_id, e)
        return None


def _apply_key_updates(key_id: int, updates: dict[str, Any]) -> bool:
    if not updates:
        return False
    updates = dict(updates)
    updates["updated_at"] = _now_str()
    columns = ", ".join(f"{column} = ?" for column in updates)
    values = list(updates.values())
    values.append(key_id)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE vpn_keys SET {columns} WHERE key_id = ?",
                tuple(values),
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error("Failed to update key %s: %s", key_id, e)
        return False


def update_key_fields(
    key_id: int,
    *,
    host_name: str | None = None,
    squad_uuid: str | None = None,
    remnawave_user_uuid: str | None = None,
    short_uuid: str | None = None,
    email: str | None = None,
    subscription_url: str | None = None,
    expire_at_ms: int | None = None,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    tag: str | None = None,
    description: str | None = None,
) -> bool:
    updates: dict[str, Any] = {}
    if host_name is not None:
        updates["host_name"] = normalize_host_name(host_name)
    if squad_uuid is not None:
        updates["squad_uuid"] = squad_uuid
    if remnawave_user_uuid is not None:
        updates["remnawave_user_uuid"] = remnawave_user_uuid
    if short_uuid is not None:
        updates["short_uuid"] = short_uuid
    if email is not None:
        normalized = _normalize_email(email) or email.strip()
        updates["email"] = normalized
        updates["key_email"] = normalized
    if subscription_url is not None:
        updates["subscription_url"] = subscription_url
    if expire_at_ms is not None:
        expire_str = _to_datetime_str(expire_at_ms) or _now_str()
        updates["expire_at"] = expire_str
    if traffic_limit_bytes is not None:
        updates["traffic_limit_bytes"] = traffic_limit_bytes
    if traffic_limit_strategy is not None:
        updates["traffic_limit_strategy"] = traffic_limit_strategy or "NO_RESET"
    if tag is not None:
        updates["tag"] = tag
    if description is not None:
        updates["description"] = description
    return _apply_key_updates(key_id, updates)


def delete_key_by_email(email: str) -> bool:
    lookup = _normalize_email(email) or email.strip()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM vpn_keys WHERE email = ? OR key_email = ?",
                (lookup, lookup),
            )
            affected = cursor.rowcount
            conn.commit()
            logger.debug("delete_key_by_email('%s') affected=%s", email, affected)
            return affected > 0
    except sqlite3.Error as e:
        logging.error("Failed to delete key '%s': %s", email, e)
        return False


def get_user_keys(user_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE user_id = ? ORDER BY datetime(created_at) DESC, key_id DESC",
                (user_id,),
            )
            rows = cursor.fetchall()
            return [_normalize_key_row(row) for row in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get keys for user %s: %s", user_id, e)
        return []


def get_key_by_id(key_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            return _normalize_key_row(row)
    except sqlite3.Error as e:
        logging.error("Failed to get key by ID %s: %s", key_id, e)
        return None


def get_key_by_email(key_email: str) -> dict | None:
    lookup = _normalize_email(key_email) or key_email.strip()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE email = ? OR key_email = ?",
                (lookup, lookup),
            )
            row = cursor.fetchone()
            return _normalize_key_row(row)
    except sqlite3.Error as e:
        logging.error("Failed to get key by email %s: %s", key_email, e)
        return None


def get_key_by_remnawave_uuid(remnawave_uuid: str) -> dict | None:
    if not remnawave_uuid:
        return None
    try:
        normalized_uuid = remnawave_uuid.strip()
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE remnawave_user_uuid = ? LIMIT 1",
                (normalized_uuid,),
            )
            row = cursor.fetchone()
            return _normalize_key_row(row)
    except sqlite3.Error as e:
        logging.error("Failed to get key by remnawave uuid %s: %s", remnawave_uuid, e)
        return None


def update_key_info(key_id: int, new_remnawave_uuid: str, new_expiry_ms: int, **kwargs) -> bool:
    return update_key_fields(
        key_id,
        remnawave_user_uuid=new_remnawave_uuid,
        expire_at_ms=new_expiry_ms,
        **kwargs,
    )


def update_key_host_and_info(
    key_id: int,
    new_host_name: str,
    new_remnawave_uuid: str,
    new_expiry_ms: int,
    **kwargs,
) -> bool:
    return update_key_fields(
        key_id,
        host_name=new_host_name,
        remnawave_user_uuid=new_remnawave_uuid,
        expire_at_ms=new_expiry_ms,
        **kwargs,
    )


def get_next_key_number(user_id: int) -> int:
    return len(get_user_keys(user_id)) + 1


def get_keys_for_host(host_name: str) -> list[dict]:
    try:
        host_name_normalized = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE TRIM(host_name) = TRIM(?)",
                (host_name_normalized,),
            )
            rows = cursor.fetchall()
            return [_normalize_key_row(row) for row in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get keys for host '%s': %s", host_name, e)
        return []


def get_all_vpn_users() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT user_id FROM vpn_keys")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get all vpn users: %s", e)
        return []


def update_key_status_from_server(key_email: str, client_data) -> bool:
    try:
        normalized_email = _normalize_email(key_email) or key_email.strip()
        existing = get_key_by_email(normalized_email)
        if client_data:
            if isinstance(client_data, dict):
                remote_uuid = client_data.get('uuid') or client_data.get('id')
                expire_value = client_data.get('expireAt') or client_data.get('expiryDate')
                subscription_url = client_data.get('subscriptionUrl') or client_data.get('subscription_url')
                expiry_ms = None
                if expire_value:
                    try:
                        remote_dt = datetime.fromisoformat(str(expire_value).replace('Z', '+00:00'))
                        expiry_ms = int(remote_dt.timestamp() * 1000)
                    except Exception:
                        expiry_ms = None
            else:
                remote_uuid = getattr(client_data, 'id', None) or getattr(client_data, 'uuid', None)
                expiry_ms = getattr(client_data, 'expiry_time', None)
                subscription_url = getattr(client_data, 'subscription_url', None)
            if not existing:
                return False
            return update_key_fields(
                existing['key_id'],
                remnawave_user_uuid=remote_uuid,
                expire_at_ms=expiry_ms,
                subscription_url=subscription_url,
            )
        if existing:
            return delete_key_by_email(normalized_email)
        return True
    except sqlite3.Error as e:
        logging.error("Failed to update key status for %s: %s", key_email, e)
        return False


def get_daily_stats_for_charts(days: int = 30) -> dict:
    stats = {'users': {}, 'keys': {}}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT date(registration_date) AS day, COUNT(*)
                FROM users
                WHERE registration_date >= date('now', ?)
                GROUP BY day
                ORDER BY day
                """,
                (f'-{days} days',),
            )
            for day, count in cursor.fetchall():
                stats['users'][day] = count

            cursor.execute(
                """
                SELECT date(COALESCE(created_at, updated_at, CURRENT_TIMESTAMP)) AS day, COUNT(*)
                FROM vpn_keys
                WHERE COALESCE(created_at, updated_at, CURRENT_TIMESTAMP) >= date('now', ?)
                GROUP BY day
                ORDER BY day
                """,
                (f'-{days} days',),
            )
            for day, count in cursor.fetchall():
                stats['keys'][day] = count
    except sqlite3.Error as e:
        logging.error("Failed to get daily stats for charts: %s", e)
    return stats


def get_recent_transactions(limit: int = 15) -> list[dict]:
    transactions: list[dict] = []
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    k.key_id,
                    k.host_name,
                    k.created_at,
                    u.telegram_id,
                    u.username
                FROM vpn_keys k
                JOIN users u ON k.user_id = u.telegram_id
                ORDER BY datetime(k.created_at) DESC, k.key_id DESC
                LIMIT ?
                """,
                (limit,),
            )
            for row in cursor.fetchall():
                transactions.append(
                    {
                        "key_id": row["key_id"],
                        "host_name": row["host_name"],
                        "created_at": row["created_at"],
                        "telegram_id": row["telegram_id"],
                        "username": row["username"],
                    }
                )
    except sqlite3.Error as e:
        logging.error("Failed to get recent transactions: %s", e)
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



