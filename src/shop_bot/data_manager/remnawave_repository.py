import logging
import sqlite3
from datetime import datetime
from typing import Any

from shop_bot.data_manager import database

logger = logging.getLogger(__name__)

DB_FILE = database.DB_FILE
normalize_host_name = database.normalize_host_name


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _default_expire_at_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def list_squads(active_only: bool = False) -> list[dict[str, Any]]:
    query = "SELECT * FROM xui_hosts"
    params: list[Any] = []
    if active_only:
        query += " WHERE COALESCE(is_active, 1) = 1"
    query += " ORDER BY sort_order ASC, host_name ASC"
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_squad(identifier: str) -> dict[str, Any] | None:
    if not identifier:
        return None
    ident = identifier.strip()
    if not ident:
        return None
    normalized = normalize_host_name(ident)
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM xui_hosts
            WHERE TRIM(host_name) = TRIM(?)
               OR TRIM(host_name) = TRIM(?)
               OR TRIM(squad_uuid) = TRIM(?)
               OR TRIM(squad_uuid) = TRIM(?)
            LIMIT 1
            """,
            (ident, normalized, ident, normalized),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def get_key_by_id(key_id: int) -> dict | None:
    return database.get_key_by_id(key_id)


def get_key_by_email(email: str) -> dict | None:
    return database.get_key_by_email(email)


def get_key_by_remnawave_uuid(remnawave_uuid: str) -> dict | None:
    return database.get_key_by_remnawave_uuid(remnawave_uuid)


def record_key(
    user_id: int,
    squad_uuid: str,
    remnawave_user_uuid: str,
    email: str,
    *,
    host_name: str | None = None,
    expire_at_ms: int | None = None,
    short_uuid: str | None = None,
    subscription_url: str | None = None,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    tag: str | None = None,
    description: str | None = None,
) -> int | None:
    expire_ms = expire_at_ms if expire_at_ms is not None else _default_expire_at_ms()
    email_normalized = _normalize_email(email)
    host_name_norm = normalize_host_name(host_name) if host_name else None

    existing = None
    if email_normalized:
        existing = database.get_key_by_email(email_normalized)
    if not existing and remnawave_user_uuid:
        existing = database.get_key_by_remnawave_uuid(remnawave_user_uuid)

    try:
        if existing:
            key_id = existing['key_id']
            database.update_key_fields(
                key_id,
                host_name=host_name_norm or existing.get('host_name'),
                squad_uuid=squad_uuid or existing.get('squad_uuid'),
                remnawave_user_uuid=remnawave_user_uuid or existing.get('remnawave_user_uuid'),
                short_uuid=short_uuid or existing.get('short_uuid'),
                email=email_normalized or existing.get('email'),
                subscription_url=subscription_url,
                expire_at_ms=expire_ms,
                traffic_limit_bytes=traffic_limit_bytes,
                traffic_limit_strategy=traffic_limit_strategy,
                tag=tag,
                description=description,
            )
            return key_id

        return database.add_new_key(
            user_id=user_id,
            host_name=host_name_norm,
            remnawave_user_uuid=remnawave_user_uuid,
            key_email=email_normalized or email,
            expiry_timestamp_ms=expire_ms,
            squad_uuid=squad_uuid,
            short_uuid=short_uuid,
            subscription_url=subscription_url,
            traffic_limit_bytes=traffic_limit_bytes,
            traffic_limit_strategy=traffic_limit_strategy,
            description=description,
            tag=tag,
        )
    except Exception:
        logger.exception("Remnawave repository failed to record key for user %s", user_id)
        return None


def record_key_from_payload(
    user_id: int,
    payload: dict[str, Any],
    *,
    host_name: str | None = None,
    description: str | None = None,
    tag: str | None = None,
) -> int | None:
    if not payload:
        return None
    squad_uuid = (payload.get('squad_uuid') or payload.get('squadUuid') or '').strip()
    remnawave_user_uuid = (payload.get('client_uuid') or payload.get('uuid') or payload.get('id') or '').strip()
    email = payload.get('email') or payload.get('accountEmail') or ''
    expire_at_ms = payload.get('expiry_timestamp_ms')
    if expire_at_ms is None:
        expire_iso = payload.get('expireAt') or payload.get('expiryDate')
        if expire_iso:
            try:
                expire_at_ms = int(datetime.fromisoformat(str(expire_iso).replace('Z', '+00:00')).timestamp() * 1000)
            except Exception:
                expire_at_ms = None
    return record_key(
        user_id=user_id,
        squad_uuid=squad_uuid,
        remnawave_user_uuid=remnawave_user_uuid,
        email=email,
        host_name=host_name or payload.get('host_name'),
        expire_at_ms=expire_at_ms,
        short_uuid=payload.get('short_uuid') or payload.get('shortUuid'),
        subscription_url=payload.get('subscription_url')
            or payload.get('connection_string')
            or payload.get('subscriptionUrl'),
        traffic_limit_bytes=payload.get('traffic_limit_bytes') or payload.get('trafficLimitBytes'),
        traffic_limit_strategy=payload.get('traffic_limit_strategy') or payload.get('trafficLimitStrategy'),
        tag=tag or payload.get('tag'),
        description=description or payload.get('description'),
    )


def update_key(
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
    return database.update_key_fields(
        key_id,
        host_name=host_name,
        squad_uuid=squad_uuid,
        remnawave_user_uuid=remnawave_user_uuid,
        short_uuid=short_uuid,
        email=email,
        subscription_url=subscription_url,
        expire_at_ms=expire_at_ms,
        traffic_limit_bytes=traffic_limit_bytes,
        traffic_limit_strategy=traffic_limit_strategy,
        tag=tag,
        description=description,
    )


def delete_key_by_email(email: str) -> bool:
    return database.delete_key_by_email(email)


# Legacy database forwarders (temporary during migration to fully abstract Remnawave repository).
# These wrappers ensure the rest of the codebase interacts with the DB only through rw_repo.
_LEGACY_FORWARDERS = (
    "add_support_message",
    "add_to_balance",
    "add_to_referral_balance",
    "add_to_referral_balance_all",
    "adjust_user_balance",
    "ban_user",
    "create_gift_key",
    "create_host",
    "create_pending_transaction",
    "create_plan",
    "create_support_ticket",
    "deduct_from_balance",
    "deduct_from_referral_balance",
    "delete_host",
    "delete_key_by_id",
    "delete_plan",
    "delete_ticket",
    "delete_user_keys",
    "find_and_complete_ton_transaction",
    "get_admin_ids",
    "get_admin_stats",
    "get_all_hosts",
    "get_all_keys",
    "get_all_settings",
    "get_all_tickets_count",
    "get_all_users",
    "get_balance",
    "get_closed_tickets_count",
    "get_daily_stats_for_charts",
    "get_host",
    "get_keys_for_host",
    "get_keys_for_user",
    "get_latest_speedtest",
    "get_next_key_number",
    "get_open_tickets_count",
    "get_paginated_transactions",
    "get_plan_by_id",
    "get_plans_for_host",
    "get_recent_transactions",
    "get_referral_balance",
    "get_referral_balance_all",
    "get_referral_count",
    "get_referrals_for_user",
    "get_setting",
    "get_speedtests",
    "get_ticket",
    "get_ticket_by_thread",
    "get_ticket_messages",
    "get_tickets_paginated",
    "get_total_keys_count",
    "get_total_spent_sum",
    "get_user",
    "get_user_count",
    "get_user_keys",
    "get_user_tickets",
    "insert_host_speedtest",
    "initialize_db",
    "is_admin",
    "log_transaction",
    "register_user_if_not_exists",
    "run_migration",
    "set_referral_start_bonus_received",
    "set_terms_agreed",
    "set_ticket_status",
    "set_trial_used",
    "unban_user",
    "update_host_name",
    "update_host_ssh_settings",
    "update_host_subscription_url",
    "update_host_url",
    "update_key_comment",
    "update_key_fields",
    "update_key_host",
    "update_key_host_and_info",
    "update_key_status_from_server",
    "update_plan",
    "update_setting",
    "update_ticket_subject",
    "update_ticket_thread_info",
    "update_user_stats",
)

for _name in _LEGACY_FORWARDERS:
    if _name not in globals():
        globals()[_name] = getattr(database, _name)

__all__ = sorted(
    name for name in globals()
    if not name.startswith('_') and name not in {"logging", "sqlite3", "datetime", "Any", "database", "logger"}
)
