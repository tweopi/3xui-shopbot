import uuid
from datetime import datetime, timedelta
import logging
from urllib.parse import urlparse
from typing import List, Dict, Any

from py3xui import Api, Client, Inbound

from shop_bot.data_manager.database import get_host, get_key_by_email, get_setting

logger = logging.getLogger(__name__)


def _attribute_candidates(name: str) -> tuple[str, ...]:
    """Return snake_case, camelCase and PascalCase variants for attribute lookup."""

    parts = name.split("_")
    camel = parts[0] + "".join(word.capitalize() for word in parts[1:]) if parts else name
    pascal = camel.capitalize() if camel else camel
    candidates = [name]
    for alias in (camel, pascal):
        if alias and alias not in candidates:
            candidates.append(alias)
    return tuple(candidates)


def _get_attr_value(obj: Any, name: str, default: Any | None = None) -> Any | None:
    """Safely obtain attribute or dict value trying snake/camel variants."""

    for candidate in _attribute_candidates(name):
        try:
            if hasattr(obj, candidate):
                value = getattr(obj, candidate)
                if value is not None:
                    return value
        except Exception:
            continue
        if isinstance(obj, dict) and candidate in obj:
            value = obj[candidate]
            if value is not None:
                return value
    return default


def _set_attr_value(obj: Any, name: str, value: Any) -> bool:
    """Set attribute or dict key trying snake/camel variants."""

    assigned = False
    for candidate in _attribute_candidates(name):
        try:
            if isinstance(obj, dict):
                obj[candidate] = value
                assigned = True
                break
            if hasattr(obj, candidate):
                setattr(obj, candidate, value)
                assigned = True
                break
        except Exception:
            continue
    if not assigned:
        try:
            setattr(obj, name, value)
            assigned = True
        except Exception:
            if isinstance(obj, dict):
                try:
                    obj[name] = value
                    assigned = True
                except Exception:
                    pass
    return assigned


def _as_datetime(value: Any) -> datetime | None:
    """Convert different representations of time to datetime."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return None
        if ts > 1e12:
            ts /= 1000
        return datetime.fromtimestamp(ts)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.isdigit():
            return _as_datetime(int(raw))
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None
    return None


def _compute_target_expiry(existing_client: Any | None, days_to_add: int | None, target_expiry_ms: int | None) -> int:
    """Determine new expiry in milliseconds based on panel client data."""

    if target_expiry_ms is not None:
        return int(target_expiry_ms)
    if days_to_add is None:
        raise ValueError("Either days_to_add or target_expiry_ms must be provided")

    now = datetime.now()
    if existing_client is not None:
        expiry_candidate = _get_attr_value(existing_client, "expire_at")
        if expiry_candidate is None:
            expiry_candidate = _get_attr_value(existing_client, "expiry_time")
        current_expiry = _as_datetime(expiry_candidate)
        if current_expiry and current_expiry > now:
            return int((current_expiry + timedelta(days=days_to_add)).timestamp() * 1000)

    return int((now + timedelta(days=days_to_add)).timestamp() * 1000)


def _extract_subscription_token(client: Any) -> str | None:
    for field in ("subscription_token", "subscription_id", "subscription", "sub_token", "sub_id"):
        token = _get_attr_value(client, field)
        if token:
            return str(token)
    return None


def _set_expiry_fields(client: Any, expiry_ms: int):
    _set_attr_value(client, "expiry_time", int(expiry_ms))
    expiry_dt = datetime.fromtimestamp(int(expiry_ms) / 1000)
    if not _set_attr_value(client, "expire_at", expiry_dt):
        _set_attr_value(client, "expire_at", int(expiry_ms))


def _build_subscription_link(
    host_data: dict,
    user_uuid: str,
    *,
    sub_token: str | None = None,
    info: Any | None = None,
) -> str:
    """Build subscription link using panel info, host settings and fallback values."""

    host_url = (host_data.get("host_url") or "").strip()

    info_base = ""
    if info is not None:
        info_base = str(_get_attr_value(info, "subscription_url") or "").strip()
        sub_token = sub_token or _extract_subscription_token(info)

    host_base = str(host_data.get("subscription_url") or "").strip()
    base = info_base or host_base

    if sub_token:
        if base:
            return base.replace("{token}", sub_token) if "{token}" in base else f"{base.rstrip('/')}/{sub_token}"
        domain = (get_setting("domain") or "").strip()
        parsed = urlparse(host_url)
        hostname = domain if domain else (parsed.hostname or "")
        scheme = parsed.scheme if parsed.scheme in ("http", "https") else "https"
        return f"{scheme}://{hostname}/sub/{sub_token}"

    if base:
        return base

    domain = (get_setting("domain") or "").strip()
    parsed = urlparse(host_url)
    hostname = domain if domain else (parsed.hostname or "")
    scheme = parsed.scheme if parsed.scheme in ("http", "https") else "https"
    return f"{scheme}://{hostname}/sub/{user_uuid}?format=v2ray"

def login_to_host(host_url: str, username: str, password: str, inbound_id: int) -> tuple[Api | None, Inbound | None]:
    try:
        api = Api(host=host_url, username=username, password=password)
        api.login()
        inbounds: List[Inbound] = api.inbound.get_list()
        target_inbound = next((inbound for inbound in inbounds if inbound.id == inbound_id), None)
        
        if target_inbound is None:
            logger.error(f"Inbound с ID '{inbound_id}' не найден на хосте '{host_url}'")
            return None, None
        return api, target_inbound
    except Exception as e:
        logger.error(f"Не удалось выполнить вход или получить inbound для хоста '{host_url}': {e}", exc_info=True)
        return None, None

def get_connection_string(inbound: Inbound, user_uuid: str, host_url: str, remark: str) -> str | None:
    if not inbound: return None
    settings = inbound.stream_settings.reality_settings.get("settings")
    if not settings: return None
    
    public_key = settings.get("publicKey")
    fp = settings.get("fingerprint")
    server_names = inbound.stream_settings.reality_settings.get("serverNames")
    short_ids = inbound.stream_settings.reality_settings.get("shortIds")
    port = inbound.port
    
    if not all([public_key, server_names, short_ids]): return None
    
    parsed_url = urlparse(host_url)
    short_id = short_ids[0]
    
    connection_string = (
        f"vless://{user_uuid}@{parsed_url.hostname}:{port}"
        f"?type=tcp&security=reality&pbk={public_key}&fp={fp}&sni={server_names[0]}"
        f"&sid={short_id}&spx=%2F&flow=xtls-rprx-vision#{remark}"
    )
    return connection_string

def get_subscription_link(
    user_uuid: str,
    host_url: str,
    host_name: str | None = None,
    sub_token: str | None = None,
    *,
    info: Any | None = None,
) -> str:
    """Public helper kept for backward compatibility. Delegates to _build_subscription_link."""

    host_data: dict[str, Any] = {"host_url": host_url}
    if host_name:
        try:
            host = get_host(host_name)
            if host:
                host_data.update(host)
            else:
                host_data["host_name"] = host_name
        except Exception:
            host_data["host_name"] = host_name
    return _build_subscription_link(host_data, user_uuid, sub_token=sub_token, info=info)

def update_or_create_client_on_panel(
    api: Api,
    inbound_id: int,
    email: str,
    days_to_add: int | None = None,
    target_expiry_ms: int | None = None,
) -> tuple[Any | None, int | None, str | None]:
    try:
        inbound_to_modify = api.inbound.get_by_id(inbound_id)
        if not inbound_to_modify:
            raise ValueError(f"Could not find inbound with ID {inbound_id}")

        if inbound_to_modify.settings.clients is None:
            inbound_to_modify.settings.clients = []
            
        client_index = -1
        for i, client in enumerate(inbound_to_modify.settings.clients):
            if client.email == email:
                client_index = i
                break
        
        existing_client = None
        if client_index != -1:
            existing_client = inbound_to_modify.settings.clients[client_index]

        new_expiry_ms = _compute_target_expiry(existing_client, days_to_add, target_expiry_ms)

        client_sub_token: str | None = None

        if existing_client is not None:
            _set_attr_value(existing_client, "reset", 0)
            _set_attr_value(existing_client, "enable", True)
            _set_expiry_fields(existing_client, new_expiry_ms)

            client_sub_token = _extract_subscription_token(existing_client)
            if not client_sub_token:
                import secrets

                client_sub_token = secrets.token_hex(12)
                for attr in ("subscription_token", "subscription_id", "subscription", "sub_token", "sub_id"):
                    _set_attr_value(existing_client, attr, client_sub_token)

            client_obj: Any = existing_client
        else:
            client_uuid = str(uuid.uuid4())
            new_client = Client(
                id=client_uuid,
                email=email,
                enable=True,
                flow="xtls-rprx-vision",
                expiry_time=new_expiry_ms,
            )

            _set_attr_value(new_client, "reset", 0)
            _set_attr_value(new_client, "short_uuid", client_uuid)
            _set_expiry_fields(new_client, new_expiry_ms)

            client_sub_token = _extract_subscription_token(new_client)
            if not client_sub_token:
                import secrets

                client_sub_token = secrets.token_hex(12)
            for attr in ("subscription_token", "subscription_id", "subscription", "sub_token", "sub_id"):
                _set_attr_value(new_client, attr, client_sub_token)

            inbound_to_modify.settings.clients.append(new_client)
            client_obj = new_client

        api.inbound.update(inbound_id, inbound_to_modify)

        return client_obj, new_expiry_ms, client_sub_token

    except Exception as e:
        logger.error(f"Ошибка в update_or_create_client_on_panel: {e}", exc_info=True)
        return None, None, None

async def create_or_update_key_on_host(host_name: str, email: str, days_to_add: int | None = None, expiry_timestamp_ms: int | None = None) -> Dict | None:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Сбой рабочего процесса: Хост '{host_name}' не найден в базе данных.")
        return None

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )
    if not api or not inbound:
        logger.error(f"Сбой рабочего процесса: Не удалось войти или найти inbound на хосте '{host_name}'.")
        return None
        
    # Prefer exact expiry when provided (e.g., switching hosts), otherwise add days (purchase/extend/trial)
    client_obj, new_expiry_ms, client_sub_token = update_or_create_client_on_panel(
        api, inbound.id, email, days_to_add=days_to_add, target_expiry_ms=expiry_timestamp_ms
    )

    if not client_obj:
        logger.error(f"Сбой рабочего процесса: Не удалось создать/обновить клиента '{email}' на хосте '{host_name}'.")
        return None

    client_uuid = _get_attr_value(client_obj, "short_uuid") or _get_attr_value(client_obj, "id") or _get_attr_value(client_obj, "email")
    if client_uuid is None:
        client_uuid = str(uuid.uuid4())
    client_uuid = str(client_uuid)

    if not client_sub_token:
        client_sub_token = _extract_subscription_token(client_obj)

    connection_string = _build_subscription_link(host_data, client_uuid, sub_token=client_sub_token, info=client_obj)

    logger.info(f"Успешно обработан ключ для '{email}' на хосте '{host_name}'.")


    return {
        "client_uuid": client_uuid,
        "email": email,
        "expiry_timestamp_ms": new_expiry_ms,
        "connection_string": connection_string,
        "host_name": host_name
    }

async def get_key_details_from_host(key_data: dict) -> dict | None:
    host_name = key_data.get('host_name')
    if not host_name:
        logger.error(f"Не удалось получить данные ключа: отсутствует host_name для key_id {key_data.get('key_id')}")
        return None

    host_db_data = get_host(host_name)
    if not host_db_data:
        logger.error(f"Не удалось получить данные ключа: хост '{host_name}' не найден в базе данных.")
        return None

    api, inbound = login_to_host(
        host_url=host_db_data['host_url'],
        username=host_db_data['host_username'],
        password=host_db_data['host_pass'],
        inbound_id=host_db_data['host_inbound_id']
    )
    if not api or not inbound: return None

    client_sub_token = None
    matched_client: Any | None = None
    resolved_uuid_value = key_data.get('xui_client_uuid')
    resolved_uuid = str(resolved_uuid_value) if resolved_uuid_value else None

    try:
        if inbound.settings and inbound.settings.clients:
            for client in inbound.settings.clients:
                client_uuid = _get_attr_value(client, "short_uuid") or _get_attr_value(client, "id")
                client_email = _get_attr_value(client, "email")

                uuid_matches = resolved_uuid and client_uuid and str(client_uuid) == str(resolved_uuid)
                email_matches = client_email and client_email == key_data.get('email')

                if uuid_matches or email_matches:
                    client_sub_token = _extract_subscription_token(client)
                    matched_client = client
                    if not resolved_uuid and client_uuid:
                        resolved_uuid = str(client_uuid)
                    break
    except Exception:
        pass

    final_uuid = resolved_uuid or key_data.get('xui_client_uuid') or key_data.get('email') or ""
    final_uuid = str(final_uuid)
    connection_string = _build_subscription_link(host_db_data, final_uuid, sub_token=client_sub_token, info=matched_client)
    return {"connection_string": connection_string}

async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Не удалось удалить клиента: хост '{host_name}' не найден.")
        return False

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )

    if not api or not inbound:
        logger.error(f"Не удалось удалить клиента: ошибка входа или поиска inbound для хоста '{host_name}'.")
        return False
        
    try:
        client_to_delete = get_key_by_email(client_email)
        if client_to_delete:
            api.client.delete(inbound.id, client_to_delete['xui_client_uuid'])
            logger.info(f"Клиент '{client_email}' успешно удалён с хоста '{host_name}'.")
            return True
        else:
            logger.warning(f"Клиент с email '{client_email}' не найден на хосте '{host_name}' для удаления (возможно, уже удалён).")
            return True
            
    except Exception as e:
        logger.error(f"Не удалось удалить клиента '{client_email}' с хоста '{host_name}': {e}", exc_info=True)
        return False