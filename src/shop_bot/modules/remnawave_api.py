import logging
from typing import Dict, Tuple
from remnawave import RemnawaveSDK

logger = logging.getLogger(__name__)

# NOTE: Это временный адаптер Remnawave с заглушками.
# Как только будут предоставлены спецификации API Remnawave (URL, аутентификация, эндпоинты),
# реализуем реальные вызовы. Интерфейс подобран под существующие места использования в проекте.


def _build_subscription_link_stub(client_uuid: str, host_url: str, host_name: str | None = None) -> str:
    """
    Заглушка генерации ссылки подписки для Remnawave.
    Когда будут правила формирования, заменим на реальную логику.
    """
    # Пока просто возвращаем плейсхолдер с UUID, чтобы ничего не падало визуально
    return f"{host_url.rstrip('/')}/sub/{client_uuid}?format=remnawave"


def _build_subscription_link(host_data: dict, client_uuid: str, email: str | None = None) -> str:
    """Сформировать ссылку подписки:
    - Если у хоста задано поле subscription_url, используем подстановки {uuid}, {email}
    - Иначе используем безопасный плейсхолдер (stub)
    """
    try:
        tpl = (host_data.get('subscription_url') or '').strip()
        if tpl:
            return tpl.replace('{uuid}', client_uuid).replace('{email}', (email or ''))
    except Exception:
        pass
    return _build_subscription_link_stub(client_uuid, host_data.get('host_url', ''), host_data.get('host_name'))


def _get_sdk(host_data: dict) -> RemnawaveSDK:
    base_url = (host_data.get('host_url') or '').strip()
    token = (host_data.get('api_key') or '').strip()
    return RemnawaveSDK(base_url=base_url, token=token)


async def login(host_data: dict) -> bool:
    """Инициализация/проверка доступа к Remnawave через простой health-check вызов."""
    try:
        sdk = _get_sdk(host_data)
        # Лёгкий вызов для проверки токена/доступности. Ограничим выборкой одного пользователя.
        # Если токен неверный — SDK/сервер вернёт ошибку, мы её залогируем и вернём False.
        await sdk.users.get_all_users_v2(limit=1)  # type: ignore
        return True
    except Exception as e:
        logger.error(f"Remnawave login failed: {e}")
        return False


async def update_or_create_client(host_data: dict, email: str, days_to_add: int | None = None,
                                  target_expiry_ms: int | None = None) -> Tuple[str | None, int | None, str | None]:
    """
    Создать или обновить клиента на Remnawave.
    Возвращает (client_uuid, expiry_timestamp_ms, client_sub_token|None)
    """
    try:
        sdk = _get_sdk(host_data)

        # 1) Поиск пользователя по email (попробуем несколько вариантов методов)
        user = None
        try:
            # Вариант 1: прямой метод
            user = await sdk.users.get_user_by_email_v2(email)  # type: ignore[attr-defined]
        except Exception as e1:
            logger.debug(f"Remnawave get_user_by_email_v2 не доступен: {e1}")
            try:
                # Вариант 2: фильтрация списка
                resp = await sdk.users.get_all_users_v2(query=email, limit=50)  # type: ignore
                for u in getattr(resp, 'users', []) or []:
                    if (getattr(u, 'email', None) or '').lower() == email.lower():
                        user = u
                        break
            except Exception as e2:
                logger.debug(f"Remnawave get_all_users_v2 с фильтром не сработал: {e2}")

        # 2) Если не найден — создаём пользователя
        if not user:
            try:
                # Попробуем создать через create_user_v2
                # Предполагаем минимальные поля: email; другие поля оставим по умолчанию.
                create_req = {"email": email}
                try:
                    user = await sdk.users.create_user_v2(create_req)  # type: ignore[attr-defined]
                except Exception as e3:
                    logger.debug(f"create_user_v2 не доступен: {e3}")
                    # Попробуем legacy-алиасы
                    user = await sdk.users.create_user(create_req)  # type: ignore[attr-defined]
            except Exception as e:
                logger.error(f"Remnawave: не удалось создать пользователя '{email}': {e}")
                user = None

        client_uuid = getattr(user, 'id', None) or getattr(user, 'uuid', None) or None

        # 3) Продление/установка срока — если SDK поддерживает соответствующий метод
        # Мы аккуратно пробуем варианты и не падаем при отсутствии метода.
        expiry_ms_final = int(target_expiry_ms) if target_expiry_ms is not None else None
        try:
            if client_uuid and (target_expiry_ms is not None or days_to_add is not None):
                if target_expiry_ms is not None:
                    # Абсолютная дата истечения (мс)
                    try:
                        await sdk.users.update_user_expiry_v2(user_id=client_uuid, expiry_ms=int(target_expiry_ms))  # type: ignore[attr-defined]
                        expiry_ms_final = int(target_expiry_ms)
                    except Exception as e4:
                        logger.debug(f"update_user_expiry_v2 не доступен: {e4}")
                        try:
                            await sdk.users.set_expiry(user_id=client_uuid, expiry_ms=int(target_expiry_ms))  # type: ignore[attr-defined]
                            expiry_ms_final = int(target_expiry_ms)
                        except Exception as e5:
                            logger.debug(f"set_expiry не доступен: {e5}")
                elif days_to_add:
                    try:
                        await sdk.users.extend_user_v2(user_id=client_uuid, days=int(days_to_add))  # type: ignore[attr-defined]
                    except Exception as e6:
                        logger.debug(f"extend_user_v2 не доступен: {e6}")
                        try:
                            await sdk.users.extend(user_id=client_uuid, days=int(days_to_add))  # type: ignore[attr-defined]
                        except Exception as e7:
                            logger.debug(f"extend не доступен: {e7}")
        except Exception as e:
            logger.warning(f"Remnawave: не удалось обновить срок пользователя '{email}': {e}")

        return client_uuid, expiry_ms_final, None
    except Exception as e:
        logger.error(f"remnawave.update_or_create_client error: {e}")
        return None, None, None


async def create_or_update_key_on_host(host_data: dict, email: str, days_to_add: int | None = None,
                                       expiry_timestamp_ms: int | None = None) -> Dict | None:
    ok = await login(host_data)
    if not ok:
        return None
    client_uuid, new_expiry_ms, client_sub_token = await update_or_create_client(
        host_data, email, days_to_add=days_to_add, target_expiry_ms=expiry_timestamp_ms
    )
    if not client_uuid:
        return None
    connection_string = _build_subscription_link(host_data, client_uuid, email)
    return {
        "client_uuid": client_uuid,
        "email": email,
        "expiry_timestamp_ms": new_expiry_ms,
        "connection_string": connection_string,
        "host_name": host_data.get('host_name')
    }


async def get_key_details_from_host(key_data: dict, host_data: dict) -> dict | None:
    try:
        if not key_data:
            return None
        client_uuid = key_data.get('xui_client_uuid') or key_data.get('client_uuid')
        if not client_uuid:
            return None
        # Если у хоста есть шаблон ссылки — применяем его.
        return {"connection_string": _build_subscription_link(host_data, client_uuid, key_data.get('key_email') or key_data.get('email'))}
    except Exception:
        return None


async def delete_client_on_host(host_data: dict, client_email: str) -> bool:
    try:
        sdk = _get_sdk(host_data)
        # Поиск пользователя по email (аналогично update_or_create_client)
        user = None
        try:
            user = await sdk.users.get_user_by_email_v2(client_email)  # type: ignore[attr-defined]
        except Exception:
            try:
                resp = await sdk.users.get_all_users_v2(query=client_email, limit=50)  # type: ignore
                for u in getattr(resp, 'users', []) or []:
                    if (getattr(u, 'email', None) or '').lower() == client_email.lower():
                        user = u
                        break
            except Exception:
                user = None

        if not user:
            logger.info(f"Remnawave: пользователь '{client_email}' не найден, считать удалённым")
            return True

        client_uuid = getattr(user, 'id', None) or getattr(user, 'uuid', None) or None
        if not client_uuid:
            logger.info(f"Remnawave: у пользователя '{client_email}' нет id/uuid, пропуск удаления")
            return True

        # Пробуем удалить
        try:
            await sdk.users.delete_user_v2(user_id=client_uuid)  # type: ignore[attr-defined]
            return True
        except Exception as e1:
            logger.debug(f"delete_user_v2 не доступен: {e1}")
            try:
                await sdk.users.delete_user(user_id=client_uuid)  # type: ignore[attr-defined]
                return True
            except Exception as e2:
                logger.error(f"Remnawave: не удалось удалить пользователя '{client_email}': {e2}")
                return False
    except Exception as e:
        logger.error(f"remnawave.delete_client_on_host error: {e}")
        return False
