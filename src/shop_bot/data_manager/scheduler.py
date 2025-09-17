import asyncio
import logging

from datetime import datetime, timedelta
from typing import Any

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import Bot

from shop_bot.bot_controller import BotController
from shop_bot.data_manager import database
from shop_bot.data_manager import speedtest_runner
from shop_bot.data_manager import backup_manager

from shop_bot.modules import xui_api
from shop_bot.bot import keyboards

CHECK_INTERVAL_SECONDS = 300
NOTIFY_BEFORE_HOURS = {72, 48, 24, 1}
notified_users = {}

logger = logging.getLogger(__name__)


def _attr_candidates(name: str) -> tuple[str, ...]:
    parts = name.split("_")
    camel = parts[0] + "".join(word.capitalize() for word in parts[1:]) if parts else name
    pascal = camel.capitalize() if camel else camel
    candidates = [name]
    for alias in (camel, pascal):
        if alias and alias not in candidates:
            candidates.append(alias)
    return tuple(candidates)


def _get_client_value(client: Any, name: str, default: Any | None = None) -> Any | None:
    for candidate in _attr_candidates(name):
        try:
            if hasattr(client, candidate):
                value = getattr(client, candidate)
                if value is not None:
                    return value
        except Exception:
            continue
        if isinstance(client, dict) and candidate in client:
            value = client[candidate]
            if value is not None:
                return value
    return default


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.isdigit():
            return int(raw)
        try:
            return int(float(raw))
        except ValueError:
            return None
    return None


def _as_datetime(value: Any) -> datetime | None:
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


def _parse_remote_expiry(client: Any) -> int:
    expiry_ms = 0
    for field in ("expire_at", "expiry_time"):
        raw = _get_client_value(client, field)
        if raw in (None, ""):
            continue
        dt = _as_datetime(raw)
        if dt:
            expiry_ms = int(dt.timestamp() * 1000)
            break
        raw_int = _coerce_int(raw)
        if raw_int is None:
            continue
        expiry_ms = raw_int if raw_int > 1e12 else (raw_int * 1000 if raw_int > 1e9 else raw_int)
        break

    reset_val = _coerce_int(_get_client_value(client, "reset")) or 0
    if reset_val:
        if reset_val > 1e12:
            expiry_ms = max(expiry_ms, reset_val)
        else:
            expiry_ms += int(reset_val) * 24 * 3600 * 1000

    return int(expiry_ms)

# Запуск обоих видов измерений 3 раза в сутки (каждые 8 часов)
SPEEDTEST_INTERVAL_SECONDS = 8 * 3600
_last_speedtests_run_at: datetime | None = None
_last_backup_run_at: datetime | None = None

def format_time_left(hours: int) -> str:
    if hours >= 24:
        days = hours // 24
        if days % 10 == 1 and days % 100 != 11:
            return f"{days} день"
        elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
            return f"{days} дня"
        else:
            return f"{days} дней"
    else:
        if hours % 10 == 1 and hours % 100 != 11:
            return f"{hours} час"
        elif 2 <= hours % 10 <= 4 and (hours % 100 < 10 or hours % 100 >= 20):
            return f"{hours} часа"
        else:
            return f"{hours} часов"

async def send_subscription_notification(bot: Bot, user_id: int, key_id: int, time_left_hours: int, expiry_date: datetime):
    try:
        time_text = format_time_left(time_left_hours)
        expiry_str = expiry_date.strftime('%d.%m.%Y в %H:%M')
        
        message = (
            f"⚠️ **Внимание!** ⚠️\n\n"
            f"Срок действия вашей подписки истекает через **{time_text}**.\n"
            f"Дата окончания: **{expiry_str}**\n\n"
            f"Продлите подписку, чтобы не остаться без доступа к VPN!"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔑 Мои ключи", callback_data="manage_keys")
        builder.button(text="➕ Продлить ключ", callback_data=f"extend_key_{key_id}")
        builder.adjust(2)
        
        await bot.send_message(chat_id=user_id, text=message, reply_markup=builder.as_markup(), parse_mode='Markdown')
        logger.debug(f"Scheduler: Отправлено уведомление пользователю {user_id} по ключу {key_id} (осталось {time_left_hours} ч).")
        
    except Exception as e:
        logger.error(f"Scheduler: Ошибка отправки уведомления пользователю {user_id}: {e}")

def _cleanup_notified_users(all_db_keys: list[dict]):
    if not notified_users:
        return

    logger.debug("Scheduler: Очищаю кэш уведомлений...")
    
    active_key_ids = {key['key_id'] for key in all_db_keys}
    
    users_to_check = list(notified_users.keys())
    
    cleaned_users = 0
    cleaned_keys = 0

    for user_id in users_to_check:
        keys_to_check = list(notified_users[user_id].keys())
        for key_id in keys_to_check:
            if key_id not in active_key_ids:
                del notified_users[user_id][key_id]
                cleaned_keys += 1
        
        if not notified_users[user_id]:
            del notified_users[user_id]
            cleaned_users += 1
    
    if cleaned_users > 0 or cleaned_keys > 0:
        logger.debug(f"Scheduler: Очистка завершена. Удалено записей пользователей: {cleaned_users}, ключей: {cleaned_keys}.")

async def check_expiring_subscriptions(bot: Bot):
    logger.debug("Scheduler: Проверяю истекающие подписки...")
    current_time = datetime.now()
    all_keys = database.get_all_keys()
    
    _cleanup_notified_users(all_keys)
    
    for key in all_keys:
        try:
            expiry_date = datetime.fromisoformat(key['expiry_date'])
            time_left = expiry_date - current_time

            if time_left.total_seconds() < 0:
                continue

            total_hours_left = int(time_left.total_seconds() / 3600)
            user_id = key['user_id']
            key_id = key['key_id']

            for hours_mark in NOTIFY_BEFORE_HOURS:
                if hours_mark - 1 < total_hours_left <= hours_mark:
                    notified_users.setdefault(user_id, {}).setdefault(key_id, set())
                    
                    if hours_mark not in notified_users[user_id][key_id]:
                        await send_subscription_notification(bot, user_id, key_id, hours_mark, expiry_date)
                        notified_users[user_id][key_id].add(hours_mark)
                    break 
                    
        except Exception as e:
            logger.error(f"Scheduler: Ошибка обработки истечения для ключа {key.get('key_id')}: {e}")

async def sync_keys_with_panels():
    logger.debug("Scheduler: Запускаю синхронизацию с XUI-панелями...")
    total_affected_records = 0
    
    all_hosts = database.get_all_hosts()
    if not all_hosts:
        logger.debug("Scheduler: Хосты в базе не настроены. Синхронизация пропущена.")
        return

    for host in all_hosts:
        host_name = host['host_name']
        logger.debug(f"Scheduler: Обрабатываю хост: '{host_name}'")
        
        try:
            api, inbound = xui_api.login_to_host(
                host_url=host['host_url'],
                username=host['host_username'],
                password=host['host_pass'],
                inbound_id=host['host_inbound_id']
            )

            if not api or not inbound:
                logger.error(f"Scheduler: Не удалось авторизоваться на хосте '{host_name}'. Пропускаю его.")
                continue
            
            full_inbound_details = api.inbound.get_by_id(inbound.id)
            clients_on_server = {client.email: client for client in (full_inbound_details.settings.clients or [])}
            logger.debug(f"Scheduler: Найдено клиентов на панели '{host_name}': {len(clients_on_server)}")

            keys_in_db = database.get_keys_for_host(host_name)
            
            for db_key in keys_in_db:
                key_email = db_key['key_email']
                expiry_date = datetime.fromisoformat(db_key['expiry_date'])
                now = datetime.now()
                if expiry_date < now - timedelta(days=5):
                    logger.debug(f"Scheduler: Ключ '{key_email}' просрочен более 5 дней. Удаляю с панели и из БД.")
                    try:
                        await xui_api.delete_client_on_host(host_name, key_email)
                    except Exception as e:
                        logger.error(f"Scheduler: Не удалось удалить клиента '{key_email}' с панели: {e}")
                    deleted = database.delete_key_by_email(key_email)
                    if deleted:
                        total_affected_records += 1
                        logger.debug(f"Scheduler: Ключ '{key_email}' удалён из локальной БД после очистки панели.")
                    else:
                        logger.warning(f"Scheduler: Попытка удалить ключ '{key_email}' из локальной БД — записей не затронуто.")
                    continue

                server_client = clients_on_server.pop(key_email, None)

                if not server_client:
                    local_uuid = str(db_key.get('xui_client_uuid') or "")
                    if local_uuid:
                        for orphan_email, candidate in list(clients_on_server.items()):
                            candidate_uuid = _get_client_value(candidate, "short_uuid") or _get_client_value(candidate, "id")
                            if candidate_uuid and str(candidate_uuid) == local_uuid:
                                server_client = candidate
                                clients_on_server.pop(orphan_email, None)
                                break

                if server_client:
                    server_uuid = _get_client_value(server_client, "short_uuid") or _get_client_value(server_client, "id")
                    local_uuid_val = db_key.get('xui_client_uuid')
                    server_expiry_ms = _parse_remote_expiry(server_client)
                    local_expiry_ms = int(expiry_date.timestamp() * 1000)

                    needs_update = False
                    if server_uuid and local_uuid_val and str(server_uuid) != str(local_uuid_val):
                        needs_update = True
                    if abs(server_expiry_ms - local_expiry_ms) > 1000:
                        needs_update = True

                    if needs_update:
                        database.update_key_status_from_server(key_email, server_client)
                        total_affected_records += 1
                        logger.debug(
                            f"Scheduler: Синхронизирован ключ '{key_email}' для хоста '{host_name}' (uuid/expiry обновлены)."
                        )
                else:
                    logger.warning(
                        f"Scheduler: Ключ '{key_email}' для хоста '{host_name}' не найден на сервере. Помечаю к удалению в локальной БД."
                    )
                    database.update_key_status_from_server(key_email, None)
                    total_affected_records += 1

            if clients_on_server:
                # Try to attach orphan clients from panel to local DB so old keys get subscriptions
                for orphan_email, orphan_client in clients_on_server.items():
                    try:
                        # Extract user_id from email like: user12345-key1-...@telegram.bot
                        import re
                        m = re.search(r"user(\d+)", orphan_email)
                        user_id = int(m.group(1)) if m else None
                        if not user_id:
                            logger.warning(
                                f"Scheduler: Найден осиротевший клиент '{orphan_email}' на '{host_name}', но не удалось определить user_id — пропускаю."
                            )
                            continue

                        # Check that user exists
                        usr = database.get_user(user_id)
                        if not usr:
                            logger.warning(
                                f"Scheduler: Осиротевший клиент '{orphan_email}' указывает на user_id={user_id}, но пользователь не найден — пропускаю."
                            )
                            continue

                        # If key already present (race/duplicate), skip insert
                        existing = database.get_key_by_email(orphan_email)
                        if existing:
                            continue

                        expiry_ms = _parse_remote_expiry(orphan_client)
                        client_uuid = (
                            _get_client_value(orphan_client, 'short_uuid')
                            or _get_client_value(orphan_client, 'id')
                            or _get_client_value(orphan_client, 'email')
                            or ''
                        )

                        if not client_uuid:
                            logger.warning(
                                f"Scheduler: У осиротевшего клиента '{orphan_email}' нет UUID/id — не могу привязать."
                            )
                            continue

                        new_id = database.add_new_key(
                            user_id=user_id,
                            host_name=host_name,
                            xui_client_uuid=str(client_uuid),
                            key_email=orphan_email,
                            expiry_timestamp_ms=expiry_ms,
                        )
                        if new_id:
                            logger.info(
                                f"Scheduler: Осиротевший клиент '{orphan_email}' на '{host_name}' привязан к пользователю {user_id} как key_id={new_id}."
                            )
                            total_affected_records += 1
                        else:
                            logger.warning(
                                f"Scheduler: Не удалось привязать осиротевшего клиента '{orphan_email}' на '{host_name}'."
                            )
                    except Exception as e:
                        logger.error(
                            f"Scheduler: Ошибка при попытке привязать осиротевшего клиента '{orphan_email}' на '{host_name}': {e}",
                            exc_info=True,
                        )

        except Exception as e:
            logger.error(f"Scheduler: Непредвиденная ошибка при обработке хоста '{host_name}': {e}", exc_info=True)
            
    logger.debug(f"Scheduler: Синхронизация с XUI-панелями завершена. Затронуто записей: {total_affected_records}.")

async def periodic_subscription_check(bot_controller: BotController):
    logger.info("Scheduler: Планировщик фоновых задач запущен.")
    await asyncio.sleep(10)

    while True:
        try:
            await sync_keys_with_panels()

            # Периодические измерения скорости по всем хостам (оба варианта: SSH и сетевой)
            await _maybe_run_periodic_speedtests()

            # Ежедневный автобэкап БД с отправкой админам
            bot = bot_controller.get_bot_instance() if bot_controller.get_status().get("is_running") else None
            if bot:
                await _maybe_run_daily_backup(bot)

            if bot_controller.get_status().get("is_running"):
                bot = bot_controller.get_bot_instance()
                if bot:
                    await check_expiring_subscriptions(bot)
                else:
                    logger.warning("Scheduler: Бот помечен как запущенный, но экземпляр недоступен.")
            else:
                logger.debug("Scheduler: Бот остановлен, уведомления пользователям пропущены.")

        except Exception as e:
            logger.error(f"Scheduler: Необработанная ошибка в основном цикле: {e}", exc_info=True)
            
        logger.info(f"Scheduler: Цикл завершён. Следующая проверка через {CHECK_INTERVAL_SECONDS} сек.")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

async def _maybe_run_periodic_speedtests():
    global _last_speedtests_run_at
    now = datetime.now()
    if _last_speedtests_run_at and (now - _last_speedtests_run_at).total_seconds() < SPEEDTEST_INTERVAL_SECONDS:
        return
    try:
        await _run_speedtests_for_all_hosts()
        _last_speedtests_run_at = now
    except Exception as e:
        logger.error(f"Scheduler: Ошибка запуска speedtests: {e}", exc_info=True)

async def _run_speedtests_for_all_hosts():
    hosts = database.get_all_hosts()
    if not hosts:
        logger.debug("Scheduler: Нет хостов для измерений скорости.")
        return
    logger.info(f"Scheduler: Запускаю speedtest для {len(hosts)} хост(ов)...")
    for h in hosts:
        host_name = h.get('host_name')
        if not host_name:
            continue
        try:
            logger.info(f"Scheduler: Speedtest для '{host_name}' запущен...")
            # Ограничим каждый хост таймаутом, чтобы не зависнуть надолго
            try:
                async with asyncio.timeout(180):
                    res = await speedtest_runner.run_both_for_host(host_name)
            except AttributeError:
                # Для Python <3.11: fallback через wait_for
                res = await asyncio.wait_for(speedtest_runner.run_both_for_host(host_name), timeout=180)
            ok = res.get('ok')
            err = res.get('error')
            if ok:
                logger.info(f"Scheduler: Speedtest для '{host_name}' завершён успешно")
            else:
                logger.warning(f"Scheduler: Speedtest для '{host_name}' завершён с ошибками: {err}")
        except asyncio.TimeoutError:
            logger.warning(f"Scheduler: Таймаут speedtest для хоста '{host_name}'")
        except Exception as e:
            logger.error(f"Scheduler: Ошибка выполнения speedtest для '{host_name}': {e}", exc_info=True)

async def _maybe_run_daily_backup(bot: Bot):
    global _last_backup_run_at
    now = datetime.now()
    # Считаем интервал из настроек (в днях). 0 или пусто — автобэкап выключен.
    try:
        s = database.get_setting("backup_interval_days") or "1"
        days = int(str(s).strip() or "1")
    except Exception:
        days = 1
    if days <= 0:
        return
    interval_seconds = max(1, days) * 24 * 3600
    if _last_backup_run_at and (now - _last_backup_run_at).total_seconds() < interval_seconds:
        return
    try:
        zip_path = backup_manager.create_backup_file()
        if zip_path and zip_path.exists():
            try:
                sent = await backup_manager.send_backup_to_admins(bot, zip_path)
                logger.info(f"Scheduler: Создан бэкап {zip_path.name}, отправлен {sent} адм.")
            except Exception as e:
                logger.error(f"Scheduler: Не удалось отправить бэкап: {e}")
            try:
                backup_manager.cleanup_old_backups(keep=7)
            except Exception:
                pass
        _last_backup_run_at = now
    except Exception as e:
        logger.error(f"Scheduler: Критическая ошибка при создании и отправке бэкапа: {e}", exc_info=True)