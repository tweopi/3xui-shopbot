import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import quote

import httpx

from shop_bot.data_manager import remnawave_repository as rw_repo

logger = logging.getLogger(__name__)


class RemnawaveAPIError(RuntimeError):
    """Base error for Remnawave API interactions."""


def _load_config() -> dict[str, Any]:
    base_url = (rw_repo.get_setting("remnawave_base_url") or "").strip().rstrip("/")
    token = (rw_repo.get_setting("remnawave_api_token") or "").strip()
    cookies_raw = rw_repo.get_setting("remnawave_cookies") or "{}"
    try:
        cookies = json.loads(cookies_raw) if cookies_raw else {}
    except json.JSONDecodeError:
        cookies = {}
    is_local = (rw_repo.get_setting("remnawave_is_local_network") or "false").lower() == "true"

    if not base_url or not token:
        raise RemnawaveAPIError("Remnawave API settings are not configured")

    return {
        "base_url": base_url,
        "token": token,
        "cookies": cookies,
        "is_local": is_local,
    }


def _build_headers(config: dict[str, Any]) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {config['token']}",
        "Content-Type": "application/json",
    }
    if config.get("is_local"):
        headers["X-Forwarded-Proto"] = "https"
        headers["X-Forwarded-For"] = "127.0.0.1"
    return headers


async def _request(
    method: str,
    path: str,
    *,
    json_payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    expected_status: tuple[int, ...] = (200,),
) -> httpx.Response:
    config = _load_config()
    url = f"{config['base_url']}{path}"
    headers = _build_headers(config)

    async with httpx.AsyncClient(cookies=config["cookies"], timeout=30.0) as client:
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_payload,
            params=params,
        )

    if response.status_code not in expected_status:
        try:
            detail = response.json()
        except json.JSONDecodeError:
            detail = response.text
        logger.warning("Remnawave API %s %s failed: %s", method, path, detail)
        raise RemnawaveAPIError(f"Remnawave API request failed: {response.status_code} {detail}")

    return response


def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


async def get_user_by_email(email: str) -> dict[str, Any] | None:
    if not email:
        return None
    encoded_email = quote(email.strip())
    response = await _request("GET", f"/api/users/by-email/{encoded_email}", expected_status=(200, 404))
    if response.status_code == 404:
        return None
    payload = response.json()
    return payload.get("response") if isinstance(payload, dict) else None


async def get_user_by_uuid(user_uuid: str) -> dict[str, Any] | None:
    if not user_uuid:
        return None
    encoded_uuid = quote(user_uuid.strip())
    response = await _request("GET", f"/api/users/{encoded_uuid}", expected_status=(200, 404))
    if response.status_code == 404:
        return None
    payload = response.json()
    return payload.get("response") if isinstance(payload, dict) else None


async def ensure_user(
    *,
    email: str,
    squad_uuid: str,
    expire_at: datetime,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    description: str | None = None,
    tag: str | None = None,
    username: str | None = None,
) -> dict[str, Any]:
    if not email:
        raise RemnawaveAPIError("email is required for ensure_user")
    if not squad_uuid:
        raise RemnawaveAPIError("squad_uuid is required for ensure_user")

    current = await get_user_by_email(email)
    expire_iso = _to_iso(expire_at)
    traffic_limit_strategy = traffic_limit_strategy or "NO_RESET"

    payload: dict[str, Any]
    method: str
    path: str

    if current:
        current_expire = current.get("expireAt")
        if current_expire:
            try:
                current_dt = datetime.fromisoformat(current_expire.replace("Z", "+00:00"))
                if current_dt > expire_at:
                    expire_iso = _to_iso(current_dt)
            except ValueError:
                pass

        payload = {
            "uuid": current.get("uuid"),
            "status": "ACTIVE",
            "expireAt": expire_iso,
            "trafficLimitBytes": traffic_limit_bytes,
            "trafficLimitStrategy": traffic_limit_strategy,
            "activeInternalSquads": [squad_uuid],
            "description": description,
            "tag": tag,
            "email": email,
        }
        method = "PATCH"
        path = "/api/users"
    else:
        generated_username = username or email.split("@")[0] or f"user-{int(datetime.utcnow().timestamp())}"
        payload = {
            "username": generated_username,
            "status": "ACTIVE",
            "expireAt": expire_iso,
            "trafficLimitBytes": traffic_limit_bytes,
            "trafficLimitStrategy": traffic_limit_strategy,
            "activeInternalSquads": [squad_uuid],
            "description": description,
            "tag": tag,
            "email": email,
        }
        method = "POST"
        path = "/api/users"

    response = await _request(method, path, json_payload=payload, expected_status=(200, 201))
    data = response.json() or {}
    result = data.get("response") if isinstance(data, dict) else None
    if not result:
        raise RemnawaveAPIError("Remnawave API returned unexpected payload")
    return result




async def list_users(squad_uuid: str | None = None, size: int | None = 500) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if size is not None:
        params["size"] = size
    if squad_uuid:
        params["squadUuid"] = squad_uuid
    response = await _request("GET", "/api/users", params=params, expected_status=(200,))
    payload = response.json() or {}
    raw_users = []
    if isinstance(payload, dict):
        body = payload.get("response") if isinstance(payload.get("response"), dict) else payload
        raw_users = body.get("users") or body.get("data") or []
    if not isinstance(raw_users, list):
        raw_users = []
    if squad_uuid:
        filtered: list[dict[str, Any]] = []
        for user in raw_users:
            squads = user.get("activeInternalSquads") or user.get("internalSquads") or []
            if isinstance(squads, list):
                for item in squads:
                    if isinstance(item, dict):
                        if item.get("uuid") == squad_uuid:
                            filtered.append(user)
                            break
                    elif isinstance(item, str) and item == squad_uuid:
                        filtered.append(user)
                        break
            elif isinstance(squads, str) and squads == squad_uuid:
                filtered.append(user)
        return filtered
    return raw_users
async def delete_user(user_uuid: str) -> bool:
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    await _request("DELETE", f"/api/users/{encoded_uuid}", expected_status=(200, 204, 404))
    return True


async def reset_user_traffic(user_uuid: str) -> bool:
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    await _request("POST", f"/api/users/{encoded_uuid}/actions/reset-traffic", expected_status=(200, 204))
    return True


async def set_user_status(user_uuid: str, active: bool) -> bool:
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    action = "enable" if active else "disable"
    await _request("POST", f"/api/users/{encoded_uuid}/actions/{action}", expected_status=(200, 204))
    return True


def extract_subscription_url(user_payload: dict[str, Any] | None) -> str | None:
    if not user_payload:
        return None
    return user_payload.get("subscriptionUrl")




async def create_or_update_key_on_host(
    host_name: str,
    email: str,
    days_to_add: int | None = None,
    expiry_timestamp_ms: int | None = None,
    *,
    description: str | None = None,
    tag: str | None = None,
) -> dict | None:
    """Legacy совместимость: создаёт/обновляет пользователя Remnawave и возвращает данные по ключу."""
    try:
        squad = rw_repo.get_squad(host_name)
        if not squad:
            logger.error("Remnawave: не найден сквад/хост '%s'", host_name)
            return None
        squad_uuid = (squad.get('squad_uuid') or '').strip()
        if not squad_uuid:
            logger.error("Remnawave: сквад '%s' не имеет squad_uuid", host_name)
            return None

        if expiry_timestamp_ms is not None:
            target_dt = datetime.fromtimestamp(expiry_timestamp_ms / 1000, tz=timezone.utc)
        else:
            days = days_to_add if days_to_add is not None else int(rw_repo.get_setting('default_extension_days') or 30)
            if days <= 0:
                days = 1
            target_dt = datetime.now(timezone.utc) + timedelta(days=days)

        traffic_limit_bytes = squad.get('default_traffic_limit_bytes')
        traffic_limit_strategy = squad.get('default_traffic_strategy') or 'NO_RESET'

        user_payload = await ensure_user(
            email=email,
            squad_uuid=squad_uuid,
            expire_at=target_dt,
            traffic_limit_bytes=traffic_limit_bytes,
            traffic_limit_strategy=traffic_limit_strategy,
            description=description,
            tag=tag,
            username=email.split('@')[0] if email else None,
        )

        subscription_url = extract_subscription_url(user_payload) or ''
        expire_at_str = user_payload.get('expireAt')
        try:
            expire_dt = datetime.fromisoformat(expire_at_str.replace('Z', '+00:00')) if expire_at_str else target_dt
        except Exception:
            expire_dt = target_dt
        expiry_ts_ms = int(expire_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

        return {
            'client_uuid': user_payload.get('uuid'),
            'short_uuid': user_payload.get('shortUuid'),
            'email': email,
            'host_name': squad.get('host_name') or host_name,
            'squad_uuid': squad_uuid,
            'subscription_url': subscription_url,
            'traffic_limit_bytes': user_payload.get('trafficLimitBytes'),
            'traffic_limit_strategy': user_payload.get('trafficLimitStrategy'),
            'expiry_timestamp_ms': expiry_ts_ms,
            'connection_string': subscription_url,
        }
    except RemnawaveAPIError as exc:
        logger.error("Remnawave: ошибка create_or_update_key_on_host %s/%s: %s", host_name, email, exc)
    except Exception:
        logger.exception("Remnawave: непредвиденная ошибка create_or_update_key_on_host для %s/%s", host_name, email)
    return None


async def get_key_details_from_host(key_data: dict) -> dict | None:
    email = key_data.get('key_email') or key_data.get('email')
    user_uuid = key_data.get('remnawave_user_uuid') or key_data.get('xui_client_uuid')
    try:
        user_payload = None
        if email:
            user_payload = await get_user_by_email(email)
        if not user_payload and user_uuid:
            user_payload = await get_user_by_uuid(user_uuid)
        if not user_payload:
            logger.warning("Remnawave: не найден пользователь для ключа %s", key_data.get('key_id'))
            return None
        subscription_url = extract_subscription_url(user_payload)
        return {
            'connection_string': subscription_url or '',
            'subscription_url': subscription_url,
            'user': user_payload,
        }
    except RemnawaveAPIError as exc:
        logger.error("Remnawave: ошибка получения деталей ключа %s: %s", key_data.get('key_id'), exc)
    except Exception:
        logger.exception("Remnawave: непредвиденная ошибка получения деталей ключа %s", key_data.get('key_id'))
    return None


async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    try:
        user_payload = await get_user_by_email(client_email)
        if not user_payload:
            logger.info("Remnawave: пользователь %s уже отсутствует", client_email)
            return True
        user_uuid = user_payload.get('uuid')
        if not user_uuid:
            logger.warning("Remnawave: нет uuid для пользователя %s", client_email)
            return False
        await delete_user(user_uuid)
        return True
    except RemnawaveAPIError as exc:
        logger.error("Remnawave: ошибка удаления пользователя %s: %s", client_email, exc)
    except Exception:
        logger.exception("Remnawave: непредвиденная ошибка удаления пользователя %s", client_email)
    return False
