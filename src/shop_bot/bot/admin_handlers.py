import logging
import asyncio
import time
import uuid
import re
import html as html_escape
from datetime import datetime, timedelta
import secrets
import string

from aiogram import Bot, Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from shop_bot.bot import keyboards
from shop_bot.data_manager import speedtest_runner
from shop_bot.data_manager import resource_monitor, database
from shop_bot.data_manager.database import (
    get_all_users,
    get_setting,
    get_user,
    get_keys_for_user,
    get_key_by_id,
    update_key_email,
    update_key_host,
    create_gift_key,
    add_new_key,
    get_key_by_email,
    get_all_hosts,
    add_to_balance,
    deduct_from_balance,
    ban_user,
    unban_user,
    delete_key_by_email,
    get_admin_stats,
    get_keys_for_host,
    update_key_info,
    is_admin,
    get_referral_count,
    get_referral_balance_all,
    get_referrals_for_user,
    # Promo API
    create_promo_code,
    list_promo_codes,
    update_promo_code_status,
    get_promo_code,
)
from shop_bot.data_manager import backup_manager
from shop_bot.bot.handlers import show_main_menu
from shop_bot.modules.xui_api import create_or_update_key_on_host, delete_client_on_host

logger = logging.getLogger(__name__)

class Broadcast(StatesGroup):
    waiting_for_message = State()
    waiting_for_button_option = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    waiting_for_confirmation = State()


def get_admin_router() -> Router:
    admin_router = Router()

    # Helper: форматирование упоминания пользователя (инициатора)
    def _format_user_mention(u: types.User) -> str:
        try:
            if u.username:
                uname = u.username.lstrip('@')
                return f"@{uname}"
            # Fallback: кликабельная ссылка по ID с читаемым именем
            full_name = (u.full_name or u.first_name or "Администратор").strip()
            # html_escape — это модуль, импортированный как html; у него есть .escape
            try:
                safe_name = html_escape.escape(full_name)
            except Exception:
                safe_name = full_name
            return f"<a href='tg://user?id={u.id}'>{safe_name}</a>"
        except Exception:
            return str(getattr(u, 'id', '—'))

    async def show_admin_menu(message: types.Message, edit_message: bool = False):
        # Собираем статистику для отображения прямо в админ-меню
        stats = get_admin_stats() or {}
        today_new = stats.get('today_new_users', 0)
        today_income = float(stats.get('today_income', 0) or 0)
        today_keys = stats.get('today_issued_keys', 0)
        total_users = stats.get('total_users', 0)
        total_income = float(stats.get('total_income', 0) or 0)
        total_keys = stats.get('total_keys', 0)
        active_keys = stats.get('active_keys', 0)

        text = (
            "📊 <b>Панель Администратора</b>\n\n"
            "<b>За сегодня:</b>\n"
            f"👥 Новых пользователей: {today_new}\n"
            f"💰 Доход: {today_income:.2f} RUB\n"
            f"🔑 Выдано ключей: {today_keys}\n\n"
            "<b>За все время:</b>\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"💰 Общий доход: {total_income:.2f} RUB\n"
            f"🔑 Всего ключей: {total_keys}\n\n"
            "<b>Состояние ключей:</b>\n"
            f"✅ Активных: {active_keys}"
        )
        keyboard = keyboards.create_admin_menu_keyboard()
        if edit_message:
            try:
                await message.edit_text(text, reply_markup=keyboard)
            except Exception:
                pass
        else:
            await message.answer(text, reply_markup=keyboard)

    async def admin_keys_menu_handler(callback: types.CallbackQuery):
        """Показать меню управления ключами."""
        text = (
            "🔑 <b>Управление ключами</b>\n\n"
            "Выберите действие для управления ключами:"
        )
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🌐 Ключи на хосте", callback_data="admin_keys_host")
        keyboard.button(text="🎁 Выдать ключ", callback_data="admin_issue_key")
        keyboard.button(text="🗑️ Удалить ключ", callback_data="admin_delete_key")
        keyboard.button(text="⏰ Продлить ключ", callback_data="admin_extend_key")
        keyboard.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        keyboard.adjust(2, 2, 1)
        
        try:
            await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=keyboard.as_markup())

    def _format_monitor_metrics() -> tuple[str, dict[str, float]]:
        local = resource_monitor.get_local_metrics()
        hosts = []
        try:
            hosts = database.get_all_hosts() or []
        except Exception:
            hosts = []
        pieces = []
        worst: dict[str, float] = {
            'cpu_percent': 0.0,
            'mem_percent': 0.0,
            'disk_percent': 0.0,
        }

        def _add_line(title: str, ok: bool, cpu: float | None, mem: float | None, disk: float | None, load: dict | None, uptime: float | None, extra: str | None = None) -> str:
            cpu_txt = f"CPU {cpu:.0f}%" if cpu is not None else "CPU —"
            mem_txt = f"RAM {mem:.0f}%" if mem is not None else "RAM —"
            disk_txt = f"Disk {disk:.0f}%" if disk is not None else "Disk —"
            load_txt = ""
            if load and load.get('1m') is not None:
                load_txt = f" | load {load.get('1m'):.2f}/{load.get('5m'):.2f}/{load.get('15m'):.2f}"
            uptime_txt = ""
            if uptime is not None:
                days = int(uptime // 86400)
                hours = int((uptime % 86400) // 3600)
                uptime_txt = f" | uptime {days}д {hours}ч"
            status = "✅" if ok else "❌"
            line = f"{status} <b>{title}</b>: {cpu_txt} · {mem_txt} · {disk_txt}{load_txt}{uptime_txt}"
            if extra:
                line += f"\n    {extra}"
            return line

        cpu_local = local.get('cpu_percent') if isinstance(local, dict) else None
        mem_local = local.get('mem_percent') if isinstance(local, dict) else None
        disk_local = local.get('disk_percent') if isinstance(local, dict) else None
        pieces.append(_add_line(
            "Панель",
            bool(local.get('ok')),
            cpu_local,
            mem_local,
            disk_local,
            local.get('loadavg'),
            local.get('uptime_seconds'),
            extra=(local.get('error') if not local.get('ok') else None)
        ))
        for name in [h.get('host_name') for h in hosts if h.get('ssh_host') and h.get('ssh_user')]:
            metrics = database.get_latest_host_metrics(name) or {}
            ok = bool(metrics.get('ok'))
            cpu = metrics.get('cpu_percent')
            mem = metrics.get('mem_percent')
            disk = metrics.get('disk_percent')
            pieces.append(_add_line(
                f"Хост {name}",
                ok,
                cpu,
                mem,
                disk,
                {'1m': metrics.get('load1'), '5m': metrics.get('load5'), '15m': metrics.get('load15')},
                metrics.get('uptime_seconds'),
                extra=(metrics.get('error') if not ok else None)
            ))
            if isinstance(cpu, (int, float)) and cpu > worst['cpu_percent']:
                worst['cpu_percent'] = float(cpu)
            if isinstance(mem, (int, float)) and mem > worst['mem_percent']:
                worst['mem_percent'] = float(mem)
            if isinstance(disk, (int, float)) and disk > worst['disk_percent']:
                worst['disk_percent'] = float(disk)

        text = "📈 <b>Мониторинг системных ресурсов</b>\n" + "\n".join(pieces)
        return text, worst

    async def _send_monitor_view(message: types.Message, edit_message: bool = False):
        text, worst = _format_monitor_metrics()
        suffix = ""
        warn_parts = []
        if worst['cpu_percent'] >= 85:
            warn_parts.append(f"CPU {worst['cpu_percent']:.0f}%")
        if worst['mem_percent'] >= 85:
            warn_parts.append(f"RAM {worst['mem_percent']:.0f}%")
        if worst['disk_percent'] >= 90:
            warn_parts.append(f"Disk {worst['disk_percent']:.0f}%")
        if warn_parts:
            suffix = "\n\n⚠️ <b>Внимание:</b> " + ", ".join(warn_parts) + ""
        keyboard = keyboards.create_admin_monitor_keyboard()
        full_text = text + suffix
        if edit_message:
            try:
                await message.edit_text(full_text, reply_markup=keyboard)
            except Exception:
                pass
        else:
            await message.answer(full_text, reply_markup=keyboard)

    @admin_router.callback_query(F.data == "admin_menu")
    async def open_admin_menu_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)


    @admin_router.callback_query(F.data == "admin_speed_test")
    async def admin_speed_test_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await admin_speedtest_entry(callback)

    @admin_router.callback_query(F.data == "admin_monitoring")
    async def admin_monitoring_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await admin_monitor_open(callback)


    @admin_router.callback_query(F.data == "admin_administrators")
    async def admin_administrators_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await admin_admins_menu_entry(callback)

    @admin_router.callback_query(F.data == "admin_promo_codes")
    async def admin_promo_codes_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await admin_promo_menu(callback)

    @admin_router.callback_query(F.data == "admin_mailing")
    async def admin_mailing_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await start_broadcast_handler(callback, state)


    @admin_router.callback_query(F.data == "admin_monitor")
    async def admin_monitor_open(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await _send_monitor_view(callback.message, edit_message=True)

    @admin_router.callback_query(F.data == "admin_monitor_refresh")
    async def admin_monitor_refresh(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await _send_monitor_view(callback.message, edit_message=True)

    # --- Speedtest: кнопка в админ-меню -> выбор хоста ---
    @admin_router.callback_query(F.data == "admin_speedtest")
    async def admin_speedtest_entry(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        hosts = get_all_hosts() or []
        if not hosts:
            await callback.message.answer("⚠️ Хосты не найдены в настройках.")
            return
        await callback.message.edit_text(
            "⚡ Выберите хост для теста скорости:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="speedtest")
        )

    # --- Speedtest: запуск по выбранному хосту ---
    @admin_router.callback_query(F.data.startswith("admin_speedtest_pick_host_"))
    async def admin_speedtest_run(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.replace("admin_speedtest_pick_host_", "", 1)

        # Уведомление всем администраторам о старте
        try:
            from shop_bot.data_manager.database import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для хоста: <b>{host_name}</b>\n(инициатор: {initiator})"
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass

        # Локальный статус
        try:
            wait_msg = await callback.message.answer(f"⏳ Выполняю тест скорости для <b>{host_name}</b>…")
        except Exception:
            wait_msg = None

        # Выполнить тест (SSH + NET) и сохранить в БД
        try:
            result = await speedtest_runner.run_both_for_host(host_name)
        except Exception as e:
            result = {"ok": False, "error": str(e), "details": {}}

        # Текст результата
        def fmt_part(title: str, d: dict | None) -> str:
            if not d:
                return f"<b>{title}:</b> —"
            if not d.get("ok"):
                return f"<b>{title}:</b> ❌ {d.get('error') or 'ошибка'}"
            ping = d.get('ping_ms')
            down = d.get('download_mbps')
            up = d.get('upload_mbps')
            srv = d.get('server_name') or '—'
            return (f"<b>{title}:</b> ✅\n"
                    f"• ping: {ping if ping is not None else '—'} ms\n"
                    f"• ↓ {down if down is not None else '—'} Mbps\n"
                    f"• ↑ {up if up is not None else '—'} Mbps\n"
                    f"• сервер: {srv}")

        details = result.get('details') or {}
        text_res = (
            f"🏁 Тест скорости завершён для <b>{host_name}</b>\n\n"
            + fmt_part("SSH", details.get('ssh')) + "\n\n"
            + fmt_part("NET", details.get('net'))
        )

        # Локально обновим сообщение
        if wait_msg:
            try:
                await wait_msg.edit_text(text_res)
            except Exception:
                await callback.message.answer(text_res)
        else:
            await callback.message.answer(text_res)

        # Разослать финал всем админам
        for aid in admin_ids:
            if wait_msg and aid == callback.from_user.id:
                continue
            try:
                await callback.bot.send_message(aid, text_res)
            except Exception:
                pass

    # --- Speedtest: Назад из выбора хоста ---
    @admin_router.callback_query(F.data == "admin_speedtest_back_to_users")
    async def admin_speedtest_back(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)

    # --- Speedtest: Запуск для всех хостов ---
    @admin_router.callback_query(F.data == "admin_speedtest_run_all")
    async def admin_speedtest_run_all(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        # оповещение админам
        try:
            from shop_bot.data_manager.database import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для всех хостов\n(инициатор: {initiator})"
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass
        # пробежимся по хостам
        hosts = get_all_hosts() or []
        summary_lines = []
        for h in hosts:
            name = h.get('host_name')
            try:
                res = await speedtest_runner.run_both_for_host(name)
                ok = res.get('ok')
                det = res.get('details') or {}
                dm = det.get('ssh', {}).get('download_mbps') or det.get('net', {}).get('download_mbps')
                um = det.get('ssh', {}).get('upload_mbps') or det.get('net', {}).get('upload_mbps')
                summary_lines.append(f"• {name}: {'✅' if ok else '❌'} ↓ {dm or '—'} ↑ {um or '—'}")
            except Exception as e:
                summary_lines.append(f"• {name}: ❌ {e}")
        text = "🏁 Тест для всех завершён:\n" + "\n".join(summary_lines)
        await callback.message.answer(text)
        for aid in admin_ids:
            # Не дублируем результат инициатору/в текущий чат
            if aid == callback.from_user.id or aid == callback.message.chat.id:
                continue
            try:
                await callback.bot.send_message(aid, text)
            except Exception:
                pass

    # --- Бэкап БД: ручной запуск ---
    @admin_router.callback_query(F.data == "admin_backup_db")
    async def admin_backup_db(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            wait = await callback.message.answer("⏳ Создаю бэкап базы данных…")
        except Exception:
            wait = None
        zip_path = backup_manager.create_backup_file()
        if not zip_path:
            if wait:
                await wait.edit_text("❌ Не удалось создать бэкап БД")
            else:
                await callback.message.answer("❌ Не удалось создать бэкап БД")
            return
        # Отправим всем администраторам
        try:
            sent = await backup_manager.send_backup_to_admins(callback.bot, zip_path)
        except Exception:
            sent = 0
        txt = f"✅ Бэкап создан: <b>{zip_path.name}</b>\nОтправлено администраторам: {sent}"
        if wait:
            try:
                await wait.edit_text(txt)
            except Exception:
                await callback.message.answer(txt)
        else:
            await callback.message.answer(txt)

    # --- Восстановление БД ---
    class AdminRestoreDB(StatesGroup):
        waiting_file = State()

    @admin_router.callback_query(F.data == "admin_restore_db")
    async def admin_restore_db_prompt(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminRestoreDB.waiting_file)
        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Отмена", callback_data="admin_cancel")
        kb.adjust(1)
        text = (
            "⚠️ <b>Восстановление базы данных</b>\n\n"
            "Отправьте файл <code>.zip</code> с бэкапом или файл <code>.db</code> в ответ на это сообщение.\n"
            "Текущая БД предварительно будет сохранена."
        )
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.message(AdminRestoreDB.waiting_file)
    async def admin_restore_db_receive(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        doc = message.document
        if not doc:
            await message.answer("❌ Пришлите файл .zip или .db")
            return
        filename = (doc.file_name or "uploaded.db").lower()
        if not (filename.endswith('.zip') or filename.endswith('.db')):
            await message.answer("❌ Поддерживаются только файлы .zip или .db")
            return
        try:
            ts = datetime.now().strftime('%Y%m%d-%H%M%S')
            dest = backup_manager.BACKUPS_DIR / f"uploaded-{ts}-{filename}"
            dest.parent.mkdir(parents=True, exist_ok=True)
            await message.bot.download(doc, destination=dest)
        except Exception as e:
            await message.answer(f"❌ Не удалось скачать файл: {e}")
            return
        ok = backup_manager.restore_from_file(dest)
        await state.clear()
        if ok:
            await message.answer("✅ Восстановление выполнено успешно.\nБот и панель продолжают работу с новой БД.")
        else:
            await message.answer("❌ Восстановление не удалось. Проверьте файл и повторите.")

    # --- Speedtest: Автоустановка speedtest на выбранном хосте ---
    @admin_router.callback_query(F.data.startswith("admin_speedtest_autoinstall_"))
    async def admin_speedtest_autoinstall(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.replace("admin_speedtest_autoinstall_", "", 1)
        try:
            wait = await callback.message.answer(f"🛠 Пытаюсь установить speedtest на <b>{host_name}</b>…")
        except Exception:
            wait = None
        from shop_bot.data_manager.speedtest_runner import auto_install_speedtest_on_host
        try:
            res = await auto_install_speedtest_on_host(host_name)
        except Exception as e:
            res = {"ok": False, "log": f"Ошибка: {e}"}
        text = ("✅ Автоустановка завершена успешно" if res.get("ok") else "❌ Автоустановка завершилась с ошибкой")
        text += f"\n<pre>{(res.get('log') or '')[:3500]}</pre>"
        if wait:
            try:
                await wait.edit_text(text)
            except Exception:
                await callback.message.answer(text)
        else:
            await callback.message.answer(text)

    # --- Промокоды: меню ---
    @admin_router.callback_query(F.data == "admin_promo_menu")
    async def admin_promo_menu(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(
            "🎟 <b>Управление промокодами</b>",
            reply_markup=keyboards.create_admin_promos_menu_keyboard()
        )

    # --- Промокоды: список ---
    @admin_router.callback_query(F.data == "admin_promo_list")
    async def admin_promo_list(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        promos = list_promo_codes(include_inactive=True) or []
        if not promos:
            text = "📋 Промокоды отсутствуют."
        else:
            lines = []
            for p in promos:
                code = p.get('code')
                active = p.get('is_active') if 'is_active' in p else p.get('active', 1)
                used_total = p.get('used_total') if p.get('used_total') is not None else p.get('used_count', 0)
                limit_total = p.get('usage_limit_total')
                vf = p.get('valid_from') or '—'
                vu = p.get('valid_until') or p.get('valid_to') or '—'
                disc = None
                if p.get('discount_percent'):
                    disc = f"{float(p.get('discount_percent')):.0f}%"
                elif p.get('discount_amount'):
                    disc = f"{float(p.get('discount_amount')):.2f} RUB"
                disc = disc or '—'
                limit_str = f"{used_total}/{limit_total}" if limit_total else f"{used_total}"
                lines.append(f"• <b>{code}</b> — {'✅' if active else '❌'} | скидка: {disc} | исх./лим.: {limit_str} | {vf} → {vu}")
            text = "\n".join(lines)
        kb = InlineKeyboardBuilder()
        # Кнопки переключения активности для первых 10 кодов (чтобы не взрывать клавиатуру)
        for p in (promos[:10] if promos else []):
            code = p.get('code')
            is_act = p.get('is_active') if 'is_active' in p else p.get('active', 1)
            label = f"{'🧯 Выкл' if is_act else '✅ Вкл'} {code}"
            kb.button(text=label, callback_data=f"admin_promo_toggle_{code}")
        kb.button(text="⬅️ В меню промокодов", callback_data="admin_promo_menu")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        # 1 код на строку, затем 1 и 1
        rows = [1] * (len(promos[:10]) if promos else 0)
        rows += [1, 1]
        kb.adjust(*rows if rows else [1])
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("admin_promo_toggle_"))
    async def admin_promo_toggle(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        code = callback.data.replace("admin_promo_toggle_", "", 1)
        try:
            p = get_promo_code(code)
            if not p:
                await callback.message.answer("❌ Промокод не найден.")
                return
            current = p.get('is_active') if 'is_active' in p else p.get('active', 1)
            ok = update_promo_code_status(code, is_active=(0 if current else 1))
            if ok:
                await callback.message.answer(f"Готово: {'деактивирован' if current else 'активирован'} {code}")
            else:
                await callback.message.answer("❌ Не удалось изменить статус.")
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка: {e}")
        # Обновим список
        await admin_promo_list(callback)

    # --- Промокоды: создание (мастер) ---
    class PromoCreate(StatesGroup):
        waiting_code = State()
        waiting_discount = State()  # percent:10 или amount:100
        waiting_limits = State()    # total=100;per_user=1 (опционально)
        waiting_dates = State()     # from=YYYY-MM-DD;until=YYYY-MM-DD (опционально)
        waiting_custom_days = State()  # ручной ввод количества дней
        waiting_description = State()
        waiting_confirmation = State()

    @admin_router.callback_query(F.data == "admin_promo_create")
    async def admin_promo_create_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(PromoCreate.waiting_code)
        await callback.message.edit_text(
            "Введите код промокода (латиница/цифры) или нажмите \"Сгенерировать\":",
            reply_markup=keyboards.create_admin_promo_code_keyboard()
        )

    @admin_router.callback_query(PromoCreate.waiting_code, F.data == "admin_promo_gen_code")
    async def admin_promo_generate_code(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer("Сгенерировано")
        alphabet = string.ascii_uppercase + string.digits
        code = "".join(secrets.choice(alphabet) for _ in range(8))
        await state.update_data(code=code)
        await state.set_state(PromoCreate.waiting_discount)
        await callback.message.edit_text(
            f"Код: <b>{code}</b>\n\nУкажите скидку",
            reply_markup=keyboards.create_admin_promo_discount_keyboard()
        )

    @admin_router.message(PromoCreate.waiting_code)
    async def promo_create_code(message: types.Message, state: FSMContext):
        code = (message.text or '').strip().upper()
        if not code or len(code) < 2:
            await message.answer("❌ Код слишком короткий. Повторите ввод.")
            return
        await state.update_data(code=code)
        await state.set_state(PromoCreate.waiting_discount)
        await message.answer(
            "Укажите скидку",
            reply_markup=keyboards.create_admin_promo_discount_keyboard()
        )

    # Быстрые кнопки выбора скидки
    @admin_router.callback_query(PromoCreate.waiting_discount, F.data.startswith("admin_promo_discount_"))
    async def promo_create_discount_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        data = callback.data
        perc = None
        amt = None
        # Меню выбора типа
        if data == "admin_promo_discount_type_percent":
            await callback.message.edit_text(
                "Выберите процент скидки или введите вручную:",
                reply_markup=keyboards.create_admin_promo_discount_percent_menu_keyboard()
            )
            return
        if data == "admin_promo_discount_type_amount":
            await callback.message.edit_text(
                "Выберите фиксированную сумму скидки (RUB) или введите вручную:",
                reply_markup=keyboards.create_admin_promo_discount_amount_menu_keyboard()
            )
            return
        # Переключатели меню
        if data == "admin_promo_discount_show_amount_menu":
            await callback.message.edit_text(
                "Выберите фиксированную сумму скидки (RUB) или введите вручную:",
                reply_markup=keyboards.create_admin_promo_discount_amount_menu_keyboard()
            )
            return
        if data == "admin_promo_discount_show_percent_menu":
            await callback.message.edit_text(
                "Выберите процент скидки или введите вручную:",
                reply_markup=keyboards.create_admin_promo_discount_percent_menu_keyboard()
            )
            return
        # Ручной ввод
        if data == "admin_promo_discount_manual_percent":
            await state.update_data(manual_discount_mode="percent")
            await callback.message.edit_text(
                "Введите процент скидки (например, 10). Можно также в формате percent:10",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        if data == "admin_promo_discount_manual_amount":
            await state.update_data(manual_discount_mode="amount")
            await callback.message.edit_text(
                "Введите фиксированную сумму скидки в RUB (например, 100). Можно также amount:100",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        # Пресеты
        if data.startswith("admin_promo_discount_percent_"):
            try:
                perc = float(data.rsplit("_", 1)[-1])
            except Exception:
                perc = 10.0
        elif data.startswith("admin_promo_discount_amount_"):
            try:
                amt = float(data.rsplit("_", 1)[-1])
            except Exception:
                amt = 50.0
        # Сохраняем и идём дальше
        await state.update_data(discount_percent=perc, discount_amount=amt, manual_discount_mode=None,
                                usage_limit_total=None, usage_limit_per_user=None, limits_manual_input=None,
                                limits_both=False)
        await state.set_state(PromoCreate.waiting_limits)
        await callback.message.edit_text(
            "Лимиты (опционально)",
            reply_markup=keyboards.create_admin_promo_limits_type_keyboard()
        )

    @admin_router.message(PromoCreate.waiting_discount)
    async def promo_create_discount(message: types.Message, state: FSMContext):
        text = (message.text or '').strip().lower()
        perc = None
        amt = None
        data = await state.get_data()
        manual_mode = (data.get('manual_discount_mode') or '').strip()
        try:
            if text.startswith('percent:'):
                perc = float(text.split(':', 1)[1].strip())
            elif text.startswith('amount:'):
                amt = float(text.split(':', 1)[1].strip())
            elif manual_mode == 'percent' and re.match(r'^\d+(\.\d+)?$', text):
                perc = float(text)
            elif manual_mode == 'amount' and re.match(r'^\d+(\.\d+)?$', text):
                amt = float(text)
            else:
                await message.answer("❌ Формат не распознан. Введите число или percent:10 / amount:100")
                return
        except Exception:
            await message.answer("❌ Не удалось прочитать число. Повторите ввод.")
            return
        await state.update_data(discount_percent=perc, discount_amount=amt,
                                usage_limit_total=None, usage_limit_per_user=None, limits_manual_input=None,
                                limits_both=False)
        await state.set_state(PromoCreate.waiting_limits)
        await message.answer(
            "Лимиты (опционально)",
            reply_markup=keyboards.create_admin_promo_limits_type_keyboard()
        )

    # Кнопки для лимитов (новое меню)
    @admin_router.callback_query(PromoCreate.waiting_limits, F.data.startswith("admin_promo_limits_"))
    async def promo_create_limits_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        data = await state.get_data()
        # Тип выбора
        if callback.data == "admin_promo_limits_type_total":
            await state.update_data(limits_both=False)
            await callback.message.edit_text(
                "Общий лимит — выберите значение:",
                reply_markup=keyboards.create_admin_promo_limits_total_keyboard()
            )
            return
        if callback.data == "admin_promo_limits_type_per":
            await state.update_data(limits_both=False)
            await callback.message.edit_text(
                "Лимит на пользователя — выберите значение:",
                reply_markup=keyboards.create_admin_promo_limits_per_user_keyboard()
            )
            return
        if callback.data == "admin_promo_limits_type_both":
            await state.update_data(limits_both=True, usage_limit_total=None, usage_limit_per_user=None)
            await callback.message.edit_text(
                "Сначала укажите общий лимит:",
                reply_markup=keyboards.create_admin_promo_limits_total_keyboard()
            )
            return
        if callback.data == "admin_promo_limits_back_to_type":
            await callback.message.edit_text(
                "Лимиты (опционально)",
                reply_markup=keyboards.create_admin_promo_limits_type_keyboard()
            )
            return
        if callback.data == "admin_promo_limits_skip":
            await state.set_state(PromoCreate.waiting_dates)
            await callback.message.edit_text(
                "Даты (опционально)",
                reply_markup=keyboards.create_admin_promo_dates_keyboard()
            )
            return
        # Пресеты TOTAL
        if callback.data.startswith("admin_promo_limits_total_preset_"):
            try:
                total = int(callback.data.rsplit("_", 1)[-1])
            except Exception:
                total = None
            await state.update_data(usage_limit_total=total)
            if data.get('limits_both'):
                await callback.message.edit_text(
                    "Теперь укажите лимит на пользователя:",
                    reply_markup=keyboards.create_admin_promo_limits_per_user_keyboard()
                )
                return
            # один лимит — дальше к датам
            await state.set_state(PromoCreate.waiting_dates)
            await callback.message.edit_text(
                "Даты (опционально)",
                reply_markup=keyboards.create_admin_promo_dates_keyboard()
            )
            return
        # Пресеты PER USER
        if callback.data.startswith("admin_promo_limits_per_preset_"):
            try:
                per_user = int(callback.data.rsplit("_", 1)[-1])
            except Exception:
                per_user = None
            await state.update_data(usage_limit_per_user=per_user)
            if data.get('limits_both') and data.get('usage_limit_total') is None:
                # если вдруг пришли сюда без тотала
                await callback.message.edit_text(
                    "Сначала укажите общий лимит:",
                    reply_markup=keyboards.create_admin_promo_limits_total_keyboard()
                )
                return
            await state.set_state(PromoCreate.waiting_dates)
            await callback.message.edit_text(
                "Даты (опционально)",
                reply_markup=keyboards.create_admin_promo_dates_keyboard()
            )
            return
        # Ручной ввод: переключаемся на ввод числа
        if callback.data == "admin_promo_limits_total_manual":
            await state.update_data(limits_manual_input="total")
            await callback.message.edit_text(
                "Введите общий лимит (целое число):",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        if callback.data == "admin_promo_limits_per_manual":
            await state.update_data(limits_manual_input="per")
            await callback.message.edit_text(
                "Введите лимит на пользователя (целое число):",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return

    @admin_router.message(PromoCreate.waiting_limits)
    async def promo_create_limits(message: types.Message, state: FSMContext):
        text = (message.text or '').strip()
        data = await state.get_data()
        manual = (data.get('limits_manual_input') or '').strip()
        if not manual:
            await message.answer(
                "Пожалуйста, выберите вариант на клавиатуре.",
                reply_markup=keyboards.create_admin_promo_limits_type_keyboard()
            )
            return
        # Ручной ввод числа
        try:
            val = int(text)
            if val <= 0:
                raise ValueError()
        except Exception:
            await message.answer("❌ Введите положительное целое число.")
            return
        if manual == 'total':
            await state.update_data(usage_limit_total=val, limits_manual_input=None)
            if data.get('limits_both'):
                await message.answer(
                    "Теперь укажите лимит на пользователя:",
                    reply_markup=keyboards.create_admin_promo_limits_per_user_keyboard()
                )
                return
        elif manual == 'per':
            await state.update_data(usage_limit_per_user=val, limits_manual_input=None)
        # Переход к датам
        await state.set_state(PromoCreate.waiting_dates)
        await message.answer(
            "Даты (опционально)",
            reply_markup=keyboards.create_admin_promo_dates_keyboard()
        )

    @admin_router.message(PromoCreate.waiting_dates)
    async def promo_create_dates(message: types.Message, state: FSMContext):
        text = (message.text or '').strip()
        vf = None
        vu = None
        if text:
            parts = [p.strip() for p in text.split(';') if p.strip()]
            for p in parts:
                if p.startswith('from='):
                    vf = p.split('=', 1)[1].strip()
                elif p.startswith('until='):
                    vu = p.split('=', 1)[1].strip()
        # Попробуем привести к isoформату, если это YYYY-MM-DD
        def _to_iso(d: str | None) -> str | None:
            if not d:
                return None
            try:
                if len(d) == 10 and d.count('-') == 2:
                    return datetime.fromisoformat(d).isoformat()
                # если админ дал уже iso, просто вернём
                datetime.fromisoformat(d)
                return d
            except Exception:
                return None
        await state.update_data(valid_from=_to_iso(vf), valid_until=_to_iso(vu))
        await state.set_state(PromoCreate.waiting_description)
        await message.answer(
            "Описание (опционально). Введите текст или оставьте пустым.",
            reply_markup=keyboards.create_admin_promo_description_keyboard()
        )

    # Кнопки дат
    @admin_router.callback_query(PromoCreate.waiting_dates, F.data.startswith("admin_promo_dates_"))
    async def promo_create_dates_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        now = datetime.now()
        vf_iso = None
        vu_iso = None
        if callback.data == "admin_promo_dates_skip":
            pass
        elif callback.data == "admin_promo_dates_week":
            vf_iso = now.isoformat()
            vu_iso = (now + timedelta(days=7)).isoformat()
        elif callback.data == "admin_promo_dates_month":
            vf_iso = now.isoformat()
            vu_iso = (now + timedelta(days=30)).isoformat()
        elif callback.data.startswith("admin_promo_dates_days_"):
            try:
                days = int(callback.data.rsplit("_", 1)[-1])
                if days <= 0:
                    raise ValueError()
            except Exception:
                days = 7
            vf_iso = now.isoformat()
            vu_iso = (now + timedelta(days=days)).isoformat()
        elif callback.data == "admin_promo_dates_custom_days":
            # Переходим на ручной ввод
            await state.set_state(PromoCreate.waiting_custom_days)
            await callback.message.edit_text(
                "Введите число дней действия промокода (например, 14):",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        await state.update_data(valid_from=vf_iso, valid_until=vu_iso)
        await state.set_state(PromoCreate.waiting_description)
        await callback.message.edit_text(
            "Описание (опционально). Введите текст или оставьте пустым.",
            reply_markup=keyboards.create_admin_promo_description_keyboard()
        )

    # Ручной ввод количества дней
    @admin_router.message(PromoCreate.waiting_custom_days)
    async def promo_create_dates_custom_days(message: types.Message, state: FSMContext):
        text = (message.text or '').strip()
        try:
            days = int(text)
            if days <= 0 or days > 3650:
                raise ValueError()
        except Exception:
            await message.answer("❌ Введите целое число дней (1–3650)")
            return
        now = datetime.now()
        vf_iso = now.isoformat()
        vu_iso = (now + timedelta(days=days)).isoformat()
        await state.update_data(valid_from=vf_iso, valid_until=vu_iso)
        await state.set_state(PromoCreate.waiting_description)
        await message.answer(
            "Описание (опционально). Введите текст или оставьте пустым.",
            reply_markup=keyboards.create_admin_promo_description_keyboard()
        )

    @admin_router.message(PromoCreate.waiting_description)
    async def promo_create_finish(message: types.Message, state: FSMContext):
        desc = (message.text or '').strip() or None
        await state.update_data(description=desc)
        await state.set_state(PromoCreate.waiting_confirmation)
        await _send_promo_summary(message, state, edit=False)

    # Кнопка пропуска описания -> показать сводку
    @admin_router.callback_query(PromoCreate.waiting_description, F.data == "admin_promo_desc_skip")
    async def promo_create_finish_skip(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.update_data(description=None)
        await state.set_state(PromoCreate.waiting_confirmation)
        await _send_promo_summary(callback.message, state, edit=True)

    # Подтверждение создания
    @admin_router.callback_query(PromoCreate.waiting_confirmation, F.data == "admin_promo_confirm_create")
    async def promo_confirm_create(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer("Создаю…")
        data = await state.get_data()
        try:
            ok = create_promo_code(
                data['code'],
                discount_percent=data.get('discount_percent'),
                discount_amount=data.get('discount_amount'),
                usage_limit_total=data.get('usage_limit_total'),
                usage_limit_per_user=data.get('usage_limit_per_user'),
                valid_from=(datetime.fromisoformat(data['valid_from']) if data.get('valid_from') else None),
                valid_until=(datetime.fromisoformat(data['valid_until']) if data.get('valid_until') else None),
                description=data.get('description')
            )
        except Exception:
            ok = False
        await state.clear()
        await callback.message.edit_text(
            ("✅ Промокод создан." if ok else "❌ Не удалось создать промокод."),
            reply_markup=keyboards.create_admin_promos_menu_keyboard()
        )

    # Вспомогательное: отправка сводки
    async def _send_promo_summary(message_or_msg, state: FSMContext, edit: bool = False):
        data = await state.get_data()
        code = data.get('code') or '—'
        if data.get('discount_percent'):
            disc_txt = f"{float(data['discount_percent']):.0f}%"
        elif data.get('discount_amount'):
            disc_txt = f"{float(data['discount_amount']):.2f} RUB"
        else:
            disc_txt = '—'
        lim_total = data.get('usage_limit_total')
        lim_per = data.get('usage_limit_per_user')
        limits_txt = []
        if lim_total:
            limits_txt.append(f"total={lim_total}")
        if lim_per:
            limits_txt.append(f"per_user={lim_per}")
        limits_txt = ";".join(limits_txt) if limits_txt else '—'
        def _fmt_date(s):
            try:
                return datetime.fromisoformat(s).strftime('%Y-%m-%d')
            except Exception:
                return '—'
        dates_txt = '—'
        if data.get('valid_from') or data.get('valid_until'):
            dates_txt = f"{_fmt_date(data.get('valid_from'))} → { _fmt_date(data.get('valid_until')) }"
        desc = data.get('description') or '—'
        text = (
            "🎟 <b>Сводка промокода</b>\n\n"
            f"<b>Код:</b> {code}\n"
            f"<b>Скидка:</b> {disc_txt}\n"
            f"<b>Лимиты:</b> {limits_txt}\n"
            f"<b>Даты:</b> {dates_txt}\n"
            f"<b>Описание:</b> {html_escape.escape(desc) if desc != '—' else '—'}\n\n"
            "Подтвердите создание."
        )
        kb = keyboards.create_admin_promo_confirm_keyboard()
        if edit:
            try:
                await message_or_msg.edit_text(text, reply_markup=kb)
            except Exception:
                await message_or_msg.answer(text, reply_markup=kb)
        else:
            await message_or_msg.answer(text, reply_markup=kb)

    # --- Пользователи: список, пагинация, просмотр ---
    @admin_router.callback_query(F.data.startswith("admin_users"))
    async def admin_users_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        page = 0
        if callback.data.startswith("admin_users_page_"):
            try:
                page = int(callback.data.split("_")[-1])
            except Exception:
                page = 0
        await callback.message.edit_text(
            "👥 <b>Пользователи</b>",
            reply_markup=keyboards.create_admin_users_keyboard(users, page=page)
        )

    @admin_router.callback_query(F.data.startswith("admin_view_user_"))
    async def admin_view_user_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        user = get_user(user_id)
        if not user:
            await callback.message.answer("❌ Пользователь не найден")
            return
        # Собираем краткую информацию
        username = user.get('username') or '—'
        # Формируем кликабельный тег пользователя
        if user.get('username'):
            uname = user.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        is_banned = user.get('is_banned', False)
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: {'да' if is_banned else 'нет'}\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        await callback.message.edit_text(
            text,
            reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=is_banned)
        )

    # --- Бан/разбан пользователя ---
    @admin_router.callback_query(F.data.startswith("admin_ban_user_"))
    async def admin_ban_user(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        try:
            ban_user(user_id)
            await callback.message.answer(f"🚫 Пользователь {user_id} забанен")
            try:
                # Уведомление пользователю: только кнопка поддержки, без "Назад в меню"
                from shop_bot.data_manager.database import get_setting as _get_setting
                support = (_get_setting("support_bot_username") or _get_setting("support_user") or "").strip()
                kb = InlineKeyboardBuilder()
                url = None
                if support:
                    if support.startswith("@"):  # @username
                        url = f"tg://resolve?domain={support[1:]}"
                    elif support.startswith("tg://"):
                        url = support
                    elif support.startswith("http://") or support.startswith("https://"):
                        try:
                            part = support.split("/")[-1].split("?")[0]
                            if part:
                                url = f"tg://resolve?domain={part}"
                        except Exception:
                            url = support
                    else:
                        url = f"tg://resolve?domain={support}"
                if url:
                    kb.button(text="🆘 Написать в поддержку", url=url)
                else:
                    kb.button(text="🆘 Поддержка", callback_data="show_help")
                await callback.bot.send_message(
                    user_id,
                    "🚫 Ваш аккаунт заблокирован администратором. Если это ошибка — напишите в поддержку.",
                    reply_markup=kb.as_markup()
                )
            except Exception:
                pass
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось забанить пользователя: {e}")
            return
        # Обновить карточку пользователя
        user = get_user(user_id) or {}
        username = user.get('username') or '—'
        if user.get('username'):
            uname = user.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: да\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=True)
            )
        except Exception:
            pass

    # --- Подменю администраторов ---
    @admin_router.callback_query(F.data == "admin_admins_menu")
    async def admin_admins_menu_entry(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(
            "👮 <b>Управление администраторами</b>",
            reply_markup=keyboards.create_admins_menu_keyboard()
        )

    @admin_router.callback_query(F.data == "admin_view_admins")
    async def admin_view_admins(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            from shop_bot.data_manager.database import get_admin_ids
            ids = list(get_admin_ids() or [])
        except Exception:
            ids = []
        if not ids:
            text = "📋 Список администраторов пуст."
        else:
            lines = []
            for aid in ids:
                try:
                    u = get_user(int(aid)) or {}
                except Exception:
                    u = {}
                uname = (u.get('username') or '').strip()
                if uname:
                    uname_clean = uname.lstrip('@')
                    tag = f"<a href='https://t.me/{uname_clean}'>@{uname_clean}</a>"
                else:
                    tag = f"<a href='tg://user?id={aid}'>Профиль</a>"
                lines.append(f"• ID: {aid} — {tag}")
            text = "📋 <b>Администраторы</b>:\n" + "\n".join(lines)
        # Кнопки назад
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Назад", callback_data="admin_admins_menu")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        kb.adjust(1, 1)
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("admin_unban_user_"))
    async def admin_unban_user(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        try:
            unban_user(user_id)
            await callback.message.answer(f"✅ Пользователь {user_id} разбанен")
            try:
                # Отправляем пользователю уведомление о разбане с кнопкой в главное меню
                kb = InlineKeyboardBuilder()
                kb.row(keyboards.get_main_menu_button())
                await callback.bot.send_message(
                    user_id,
                    "✅ Доступ к аккаунту восстановлен администратором.",
                    reply_markup=kb.as_markup()
                )
            except Exception:
                pass
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось разбанить пользователя: {e}")
            return
        # Обновить карточку пользователя
        user = get_user(user_id) or {}
        username = user.get('username') or '—'
        # Формируем кликабельный тег пользователя
        if user.get('username'):
            uname = user.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: нет\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=False)
            )
        except Exception:
            pass

    # --- Ключи пользователя: список и карточка ключа ---
    @admin_router.callback_query(F.data.startswith("admin_user_keys_"))
    async def admin_user_keys(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        keys = get_keys_for_user(user_id)
        await callback.message.edit_text(
            f"🔑 Ключи пользователя {user_id}:",
            reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
        )

    @admin_router.callback_query(F.data.startswith("admin_user_referrals_"))
    async def admin_user_referrals(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        inviter = get_user(user_id)
        if not inviter:
            await callback.message.answer("❌ Пользователь не найден")
            return
        refs = get_referrals_for_user(user_id) or []
        ref_count = len(refs)
        try:
            total_ref_earned = float(get_referral_balance_all(user_id) or 0)
        except Exception:
            total_ref_earned = 0.0
        # Сформируем список с ограничением по длине
        max_items = 30
        lines = []
        for r in refs[:max_items]:
            rid = r.get('telegram_id')
            uname = r.get('username') or '—'
            rdate = r.get('registration_date') or '—'
            spent = float(r.get('total_spent') or 0)
            lines.append(f"• @{uname} (ID: {rid}) — рег: {rdate}, потратил: {spent:.2f} RUB")
        more_suffix = "\n… и ещё {}".format(ref_count - max_items) if ref_count > max_items else ""
        text = (
            f"🤝 <b>Рефералы пользователя {user_id}</b>\n\n"
            f"Всего приглашено: {ref_count}\n"
            f"Заработано по рефералке (всего): {total_ref_earned:.2f} RUB\n\n"
            + ("\n".join(lines) if lines else "Пока нет рефералов")
            + more_suffix
        )
        # Кнопки: назад к карточке пользователя и в админ-меню
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ К пользователю", callback_data=f"admin_view_user_{user_id}")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        kb.adjust(1, 1)
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("admin_edit_key_"))
    async def admin_edit_key(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        key = get_key_by_id(key_id)
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        text = (
            f"🔑 <b>Ключ #{key_id}</b>\n"
            f"Хост: {key.get('host_name') or '—'}\n"
            f"Email: {key.get('key_email') or '—'}\n"
            f"Истекает: {key.get('expiry_date') or '—'}\n"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )
        except Exception as e:
            logger.debug(f"edit_text failed in delete cancel for key #{key_id}: {e}")
            await callback.message.answer(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )

    # --- Удаление ключа: подтверждение (prompt) ---
    # Матчим только вариант admin_key_delete_{id}, без confirm/cancel
    @admin_router.callback_query(F.data.regexp(r"^admin_key_delete_\d+$"))
    async def admin_key_delete_prompt(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        logger.info(f"admin_key_delete_prompt received: data='{callback.data}' from {callback.from_user.id}")
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        key = get_key_by_id(key_id)
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        email = key.get('key_email') or '—'
        host = key.get('host_name') or '—'
        try:
            await callback.message.edit_text(
                f"Вы уверены, что хотите удалить ключ #{key_id}?\nEmail: {email}\nСервер: {host}",
                reply_markup=keyboards.create_admin_delete_key_confirm_keyboard(key_id)
            )
        except Exception as e:
            logger.debug(f"edit_text failed in delete prompt for key #{key_id}: {e}")
            await callback.message.answer(
                f"Вы уверены, что хотите удалить ключ #{key_id}?\nEmail: {email}\nСервер: {host}",
                reply_markup=keyboards.create_admin_delete_key_confirm_keyboard(key_id)
            )

    # --- Продление конкретного ключа из карточки ---
    class AdminExtendSingleKey(StatesGroup):
        waiting_days = State()

    @admin_router.callback_query(F.data.startswith("admin_key_extend_"))
    async def admin_key_extend_prompt(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        await state.update_data(extend_key_id=key_id)
        await state.set_state(AdminExtendSingleKey.waiting_days)
        await callback.message.edit_text(
            f"Укажите, на сколько дней продлить ключ #{key_id} (число):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminExtendSingleKey.waiting_days)
    async def admin_key_extend_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        key_id = int(data.get("extend_key_id", 0))
        if not key_id:
            await state.clear()
            await message.answer("❌ Не удалось определить ключ.")
            return
        try:
            days = int((message.text or '').strip())
        except Exception:
            await message.answer("❌ Введите число дней")
            return
        if days <= 0:
            await message.answer("❌ Дней должно быть положительное число")
            return
        key = get_key_by_id(key_id)
        if not key:
            await message.answer("❌ Ключ не найден")
            await state.clear()
            return
        host = key.get('host_name')
        email = key.get('key_email')
        if not host or not email:
            await message.answer("❌ У ключа отсутствует сервер или email")
            await state.clear()
            return
        # Продление на хосте
        try:
            resp = await create_or_update_key_on_host(host, email, days_to_add=days)
        except Exception as e:
            logger.error(f"Admin key extend: host update failed for key #{key_id}: {e}")
            resp = None
        if not resp or not resp.get('client_uuid') or not resp.get('expiry_timestamp_ms'):
            await message.answer("❌ Не удалось продлить ключ на сервере")
            return
        # Обновление в БД
        try:
            update_key_info(key_id, resp['client_uuid'], int(resp['expiry_timestamp_ms']))
        except Exception as e:
            logger.error(f"Admin key extend: DB update failed for key #{key_id}: {e}")
        await state.clear()
        # Повторный показ карточки ключа
        new_key = get_key_by_id(key_id)
        text = (
            f"🔑 <b>Ключ #{key_id}</b>\n"
            f"Хост: {new_key.get('host_name') or '—'}\n"
            f"Email: {new_key.get('key_email') or '—'}\n"
            f"Истекает: {new_key.get('expiry_date') or '—'}\n"
        )
        await message.answer(f"✅ Ключ продлён на {days} дн.")
        await message.answer(text, reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(new_key.get('user_id')) if new_key and new_key.get('user_id') else None))

    # --- Управление администраторами: добавить админа ---
    class AdminAddAdmin(StatesGroup):
        waiting_for_input = State()

    @admin_router.callback_query(F.data == "admin_add_admin")
    async def admin_add_admin_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminAddAdmin.waiting_for_input)
        await callback.message.edit_text(
            "Введите ID пользователя или его @username, которого нужно сделать администратором:\n\n"
            "Примеры: 123456789 или @username",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminAddAdmin.waiting_for_input)
    async def admin_add_admin_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        target_id: int | None = None
        # Попытка распарсить как число
        if raw.isdigit():
            try:
                target_id = int(raw)
            except Exception:
                target_id = None
        # Если @username
        if target_id is None and raw.startswith('@'):
            uname = raw.lstrip('@')
            # 1) Пробуем как передано (@username)
            try:
                chat = await message.bot.get_chat(raw)
                target_id = int(chat.id)
            except Exception:
                target_id = None
            # 2) Пробуем без @ (username)
            if target_id is None:
                try:
                    chat = await message.bot.get_chat(uname)
                    target_id = int(chat.id)
                except Exception:
                    target_id = None
            # 3) Фолбэк: ищем пользователя в локальной БД по username
            if target_id is None:
                try:
                    users = get_all_users() or []
                    uname_low = uname.lower()
                    for u in users:
                        u_un = (u.get('username') or '').lstrip('@').lower()
                        if u_un and u_un == uname_low:
                            target_id = int(u.get('telegram_id') or u.get('user_id') or u.get('id'))
                            break
                except Exception:
                    target_id = None
        if target_id is None:
            await message.answer("❌ Не удалось распознать ID/username. Отправьте корректное значение или нажмите Отмена.")
            return
        # Обновляем настройки админов
        try:
            from shop_bot.data_manager.database import get_admin_ids, update_setting
            ids = set(get_admin_ids())
            ids.add(int(target_id))
            # Сохраняем в admin_telegram_ids строкой CSV
            ids_str = ",".join(str(i) for i in sorted(ids))
            update_setting("admin_telegram_ids", ids_str)
            await message.answer(f"✅ Пользователь {target_id} добавлен в администраторы.")
        except Exception as e:
            await message.answer(f"❌ Ошибка при сохранении: {e}")
        await state.clear()
        # Показать админ-меню снова
        try:
            await show_admin_menu(message)
        except Exception:
            pass

    # --- Снятие прав администратора ---
    class AdminRemoveAdmin(StatesGroup):
        waiting_for_input = State()

    @admin_router.callback_query(F.data == "admin_remove_admin")
    async def admin_remove_admin_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminRemoveAdmin.waiting_for_input)
        await callback.message.edit_text(
            "Введите ID пользователя или его @username, которого нужно снять из админов:\n\n"
            "Примеры: 123456789 или @username",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminRemoveAdmin.waiting_for_input)
    async def admin_remove_admin_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        target_id: int | None = None
        # Попытка распарсить как число
        if raw.isdigit():
            try:
                target_id = int(raw)
            except Exception:
                target_id = None
        # Резолвим username (@username или username)
        if target_id is None:
            uname = raw.lstrip('@')
            # 1) Пробуем как введено
            try:
                chat = await message.bot.get_chat(raw)
                target_id = int(chat.id)
            except Exception:
                target_id = None
            # 2) Пробуем без @
            if target_id is None and uname:
                try:
                    chat = await message.bot.get_chat(uname)
                    target_id = int(chat.id)
                except Exception:
                    target_id = None
            # 3) Фолбэк: поиск в БД
            if target_id is None and uname:
                try:
                    users = get_all_users() or []
                    uname_low = uname.lower()
                    for u in users:
                        u_un = (u.get('username') or '').lstrip('@').lower()
                        if u_un and u_un == uname_low:
                            target_id = int(u.get('telegram_id') or u.get('user_id') or u.get('id'))
                            break
                except Exception:
                    target_id = None
        if target_id is None:
            await message.answer("❌ Не удалось распознать ID/username. Отправьте корректное значение или нажмите Отмена.")
            return
        # Обновляем настройки админов
        try:
            from shop_bot.data_manager.database import get_admin_ids, update_setting
            ids = set(get_admin_ids())
            if target_id not in ids:
                await message.answer(f"ℹ️ Пользователь {target_id} не является администратором.")
                await state.clear()
                try:
                    await show_admin_menu(message)
                except Exception:
                    pass
                return
            if len(ids) <= 1:
                await message.answer("❌ Нельзя снять последнего администратора.")
                return
            ids.discard(int(target_id))
            ids_str = ",".join(str(i) for i in sorted(ids))
            update_setting("admin_telegram_ids", ids_str)
            await message.answer(f"✅ Пользователь {target_id} снят с администраторов.")
        except Exception as e:
            await message.answer(f"❌ Ошибка при сохранении: {e}")
        await state.clear()
        # Показать админ-меню снова
        try:
            await show_admin_menu(message)
        except Exception:
            pass

    # --- Удаление ключа: отмена ---
    @admin_router.callback_query(F.data.startswith("admin_key_delete_cancel_"))
    async def admin_key_delete_cancel(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        try:
            await callback.answer("Отменено")
        except Exception:
            pass
        logger.info(f"admin_key_delete_cancel received: data='{callback.data}' from {callback.from_user.id}")
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        key = get_key_by_id(key_id)
        if not key:
            return
        text = (
            f"🔑 <b>Ключ #{key_id}</b>\n"
            f"Хост: {key.get('host_name') or '—'}\n"
            f"Email: {key.get('key_email') or '—'}\n"
            f"Истекает: {key.get('expiry_date') or '—'}\n"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )
        except Exception as e:
            logger.debug(f"edit_text failed in delete cancel for key #{key_id}: {e}")
            await callback.message.answer(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )

    # --- Удаление ключа: подтверждение и выполнение ---
    @admin_router.callback_query(F.data.startswith("admin_key_delete_confirm_"))
    async def admin_key_delete_confirm(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        try:
            await callback.answer("Удаляю…")
        except Exception:
            pass
        logger.info(f"admin_key_delete_confirm received: data='{callback.data}' from {callback.from_user.id}")
        try:
            key_id = int(callback.data.split('_')[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        try:
            key = get_key_by_id(key_id)
        except Exception as e:
            logger.error(f"DB get_key_by_id failed for #{key_id}: {e}")
            key = None
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        try:
            user_id = int(key.get('user_id'))
        except Exception as e:
            logger.error(f"Invalid user_id for key #{key_id}: {key.get('user_id')}, err={e}")
            await callback.message.answer("❌ Ошибка данных ключа: некорректный пользователь")
            return
        host = key.get('host_name')
        email = key.get('key_email')
        ok_host = True
        if host and email:
            try:
                ok_host = await delete_client_on_host(host, email)
            except Exception as e:
                ok_host = False
                logger.error(f"Failed to delete client on host '{host}' for key #{key_id}: {e}")
        ok_db = False
        try:
            ok_db = delete_key_by_email(email)
        except Exception as e:
            logger.error(f"Failed to delete key in DB for email '{email}': {e}")
        if ok_db:
            await callback.message.answer("✅ Ключ удалён" + (" (с хоста тоже)" if ok_host else " (но удалить на хосте не удалось)"))
            # Обновить список ключей пользователя
            keys = get_keys_for_user(user_id)
            try:
                await callback.message.edit_text(
                    f"🔑 Ключи пользователя {user_id}:",
                    reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
                )
            except Exception as e:
                logger.debug(f"edit_text failed in delete confirm list refresh for user {user_id}: {e}")
                await callback.message.answer(
                    f"🔑 Ключи пользователя {user_id}:",
                    reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
                )
            # Уведомление пользователю (если получится)
            try:
                await callback.bot.send_message(
                    user_id,
                    "ℹ️ Администратор удалил один из ваших ключей. Если это ошибка — напишите в поддержку.",
                    reply_markup=keyboards.create_support_keyboard()
                )
            except Exception:
                pass
        else:
            await callback.message.answer("❌ Не удалось удалить ключ из базы данных")

    class AdminEditKeyEmail(StatesGroup):
        waiting_for_email = State()

    @admin_router.callback_query(F.data.startswith("admin_key_edit_email_"))
    async def admin_key_edit_email_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        await state.update_data(edit_key_id=key_id)
        await state.set_state(AdminEditKeyEmail.waiting_for_email)
        await callback.message.edit_text(
            f"Введите новый email для ключа #{key_id}",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminEditKeyEmail.waiting_for_email)
    async def admin_key_edit_email_commit(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        key_id = int(data.get('edit_key_id'))
        new_email = (message.text or '').strip()
        if not new_email:
            await message.answer("❌ Введите корректный email")
            return
        ok = update_key_email(key_id, new_email)
        if ok:
            await message.answer("✅ Email обновлён")
        else:
            await message.answer("❌ Не удалось обновить email (возможно, уже занят)")
        await state.clear()

    class AdminEditKeyHost(StatesGroup):
        waiting_for_host = State()

    @admin_router.callback_query(F.data.startswith("admin_key_edit_host_"))
    async def admin_key_edit_host_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        await state.update_data(edit_key_id=key_id)
        await state.set_state(AdminEditKeyHost.waiting_for_host)
        await callback.message.edit_text(
            f"Введите новое имя сервера (host) для ключа #{key_id}",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminEditKeyHost.waiting_for_host)
    async def admin_key_edit_host_commit(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        key_id = int(data.get('edit_key_id'))
        new_host = (message.text or '').strip()
        if not new_host:
            await message.answer("❌ Введите корректное имя сервера")
            return
        ok = update_key_host(key_id, new_host)
        if ok:
            await message.answer("✅ Сервер обновлён")
        else:
            await message.answer("❌ Не удалось обновить сервер")
        await state.clear()

    # --- Начисление реф. баланса: удалено ---

    # --- Выдача подарочного ключа ---
    class AdminGiftKey(StatesGroup):
        picking_user = State()
        picking_host = State()
        picking_days = State()

    @admin_router.callback_query(F.data == "admin_gift_key")
    async def admin_gift_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await state.clear()
        await state.set_state(AdminGiftKey.picking_user)
        await callback.message.edit_text(
            "🎁 Выдача подарочного ключа\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="gift")
        )

    # Запуск выдачи подарка сразу для выбранного пользователя из карточки пользователя
    @admin_router.callback_query(F.data.startswith("admin_gift_key_"))
    async def admin_gift_key_for_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.clear()
        await state.update_data(target_user_id=user_id)
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(
            f"👤 Пользователь {user_id}. Выберите сервер:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_user, F.data.startswith("admin_gift_pick_user_page_"))
    async def admin_gift_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        users = get_all_users()
        await callback.message.edit_text(
            "🎁 Выдача подарочного ключа\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=page, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_user, F.data.startswith("admin_gift_pick_user_"))
    async def admin_gift_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(
            f"👤 Пользователь {user_id}. Выберите сервер:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_host, F.data == "admin_gift_back_to_users")
    async def admin_gift_back_to_users(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await state.set_state(AdminGiftKey.picking_user)
        await callback.message.edit_text(
            "🎁 Выдача подарочного ключа\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_host, F.data.startswith("admin_gift_pick_host_"))
    async def admin_gift_pick_host(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.split("admin_gift_pick_host_")[-1]
        await state.update_data(host_name=host_name)
        await state.set_state(AdminGiftKey.picking_days)
        await callback.message.edit_text(
            f"🌍 Сервер: {host_name}. Введите срок действия ключа в днях (целое число):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.callback_query(AdminGiftKey.picking_days, F.data == "admin_gift_back_to_hosts")
    async def admin_gift_back_to_hosts(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(
            f"👤 Пользователь {user_id}. Выберите сервер:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="gift")
        )
    @admin_router.message(AdminGiftKey.picking_days)
    async def admin_gift_pick_days(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        host_name = data.get('host_name')
        try:
            days = int(message.text.strip())
        except Exception:
            await message.answer("❌ Введите целое число дней")
            return
        if days <= 0:
            await message.answer("❌ Срок должен быть положительным")
            return
        # Сгенерируем уникальный техн. email
        user = get_user(user_id) or {}
        username = (user.get('username') or f'user{user_id}').lower()
        username_slug = re.sub(r"[^a-z0-9._-]", "_", username).strip("_")[:16] or f"user{user_id}"
        base_local = f"gift_{username_slug}"
        candidate_local = base_local
        attempt = 1
        while True:
            candidate_email = f"{candidate_local}@bot.local"
            existing = get_key_by_email(candidate_email)
            if not existing:
                break
            attempt += 1
            candidate_local = f"{base_local}-{attempt}"
            if attempt > 100:
                candidate_local = f"{base_local}-{int(time.time())}"
                candidate_email = f"{candidate_local}@bot.local"
                break
        generated_email = candidate_email

        # Создаём/обновляем клиента на хосте с days_to_add
        try:
            host_resp = await create_or_update_key_on_host(host_name, generated_email, days_to_add=days)
        except Exception as e:
            host_resp = None
            logging.error(f"Подарочный поток: не удалось создать клиента на хосте '{host_name}' для пользователя {user_id}: {e}")

        if not host_resp or not host_resp.get("client_uuid") or not host_resp.get("expiry_timestamp_ms"):
            await message.answer("❌ Не удалось выдать ключ на сервере. Проверьте настройки хоста и доступность панели XUI.")
            await state.clear()
            await show_admin_menu(message)
            return

        client_uuid = host_resp["client_uuid"]
        expiry_ms = int(host_resp["expiry_timestamp_ms"])  # в мс
        connection_link = host_resp.get("connection_string")

        key_id = add_new_key(user_id, host_name, client_uuid, generated_email, expiry_ms)
        if key_id:
            username_readable = (user.get('username') or '').strip()
            user_part = f"{user_id} (@{username_readable})" if username_readable else f"{user_id}"
            text_admin = (
                f"✅ 🎁 Подарочный ключ #{key_id} выдан пользователю {user_part} (сервер: {host_name}, {days} дн.)\n"
                f"Email: {generated_email}"
            )
            await message.answer(text_admin)
            try:
                notify_text = (
                    f"🎁 Администратор выдал вам подарочный ключ #{key_id}\n"
                    f"Сервер: {host_name}\n"
                    f"Срок: {days} дн.\n"
                )
                if connection_link:
                    cs = html_escape.escape(connection_link)
                    notify_text += f"\n🔗 Подписка:\n<pre><code>{cs}</code></pre>"
                await message.bot.send_message(user_id, notify_text, parse_mode='HTML', disable_web_page_preview=True)
            except Exception:
                pass
        else:
            await message.answer("❌ Не удалось сохранить ключ в базе данных.")
        await state.clear()
        await show_admin_menu(message)

    # Текстовые обработчики больше не используются в новом потоке выдачи ключа

    # --- Начисление основного баланса ---
    class AdminMainRefill(StatesGroup):
        waiting_for_pair = State()
        waiting_for_amount = State()

    @admin_router.callback_query(F.data == "admin_add_balance")
    async def admin_add_balance_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await callback.message.edit_text(
            "➕ Начисление баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="add_balance")
        )

    @admin_router.callback_query(F.data.startswith("admin_add_balance_"))
    async def admin_add_balance_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainRefill.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму начисления (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    # Пагинация списка пользователей для начисления баланса
    @admin_router.callback_query(F.data.startswith("admin_add_balance_pick_user_page_"))
    async def admin_add_balance_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        users = get_all_users()
        await callback.message.edit_text(
            "➕ Начисление баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=page, action="add_balance")
        )

    # Выбор пользователя для начисления: дальше админ вводит только сумму
    @admin_router.callback_query(F.data.startswith("admin_add_balance_pick_user_"))
    async def admin_add_balance_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainRefill.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму начисления (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminMainRefill.waiting_for_amount)
    async def handle_main_amount(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        try:
            amount = float(message.text.strip().replace(',', '.'))
        except Exception:
            await message.answer("❌ Введите число — сумму в рублях")
            return
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной")
            return
        try:
            ok = add_to_balance(user_id, amount)
            if ok:
                await message.answer(f"✅ Начислено {amount:.2f} RUB на баланс пользователю {user_id}")
                try:
                    await message.bot.send_message(user_id, f"💰 Вам начислено {amount:.2f} RUB на баланс администратором.")
                except Exception:
                    pass
            else:
                await message.answer("❌ Пользователь не найден или ошибка БД")
        except Exception as e:
            await message.answer(f"❌ Ошибка начисления: {e}")
        await state.clear()
        await show_admin_menu(message)

    # Back from key actions to keys list
    @admin_router.callback_query(F.data.startswith("admin_key_back_"))
    async def admin_key_back(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        key = get_key_by_id(key_id)
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        # Если мы находимся в контексте просмотра ключей хоста — вернёмся к списку ключей этого хоста
        host_from_state = None
        try:
            data = await state.get_data()
            host_from_state = (data or {}).get('hostkeys_host')
        except Exception:
            host_from_state = None

        if host_from_state:
            host_name = host_from_state
            keys = get_keys_for_host(host_name)
            await callback.message.edit_text(
                f"🔑 Ключи на хосте {host_name}:",
                reply_markup=keyboards.create_admin_keys_for_host_keyboard(host_name, keys)
            )
        else:
            user_id = int(key.get('user_id'))
            keys = get_keys_for_user(user_id)
            await callback.message.edit_text(
                f"🔑 Ключи пользователя {user_id}:",
                reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
            )

    # noop callback to safely ignore placeholder buttons
    @admin_router.callback_query(F.data == "noop")
    async def admin_noop(callback: types.CallbackQuery):
        await callback.answer()

    @admin_router.callback_query(F.data == "admin_cancel")
    async def admin_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Отменено")
        await state.clear()
        await show_admin_menu(callback.message, edit_message=True)

    # --- Списание средств администратором (UI) ---
    class AdminMainDeduct(StatesGroup):
        waiting_for_amount = State()

    # Вход из админ-меню: показать список пользователей
    @admin_router.callback_query(F.data == "admin_deduct_balance")
    async def admin_deduct_balance_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await callback.message.edit_text(
            "➖ Списание баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="deduct_balance")
        )

    # Быстрый путь из карточки пользователя
    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_"))
    async def admin_deduct_balance_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainDeduct.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму списания (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    # Пагинация списка пользователей
    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_pick_user_page_"))
    async def admin_deduct_balance_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        users = get_all_users()
        await callback.message.edit_text(
            "➖ Списание баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=page, action="deduct_balance")
        )

    # Выбор пользователя -> ввод суммы
    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_pick_user_"))
    async def admin_deduct_balance_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainDeduct.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму списания (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminMainDeduct.waiting_for_amount)
    async def handle_deduct_amount(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        try:
            amount = float(message.text.strip().replace(',', '.'))
        except Exception:
            await message.answer("❌ Введите число — сумму в рублях")
            return
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной")
            return
        try:
            ok = deduct_from_balance(user_id, amount)
            if ok:
                await message.answer(f"✅ Списано {amount:.2f} RUB с баланса пользователя {user_id}")
                try:
                    await message.bot.send_message(
                        user_id,
                        f"➖ С вашего баланса списано {amount:.2f} RUB администратором.\nЕсли это ошибка — напишите в поддержку.",
                        reply_markup=keyboards.create_support_keyboard()
                    )
                except Exception:
                    pass
            else:
                await message.answer("❌ Пользователь не найден или недостаточно средств")
        except Exception as e:
            await message.answer(f"❌ Ошибка списания: {e}")
        await state.clear()
        await show_admin_menu(message)

    # --- Просмотр ключей на хосте ---
    class AdminHostKeys(StatesGroup):
        picking_host = State()

    @admin_router.callback_query(F.data == "admin_host_keys")
    async def admin_host_keys_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.clear()
        await state.set_state(AdminHostKeys.picking_host)
        hosts = get_all_hosts()
        await callback.message.edit_text(
            "🌍 Выберите хост для просмотра ключей:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="hostkeys")
        )

    @admin_router.callback_query(AdminHostKeys.picking_host, F.data.startswith("admin_hostkeys_pick_host_"))
    async def admin_host_keys_pick_host(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.split("admin_hostkeys_pick_host_")[-1]
        # Сохраняем контекст текущего хоста, чтобы корректно работать с кнопкой "Назад"
        try:
            await state.update_data(hostkeys_host=host_name)
        except Exception:
            pass
        keys = get_keys_for_host(host_name)
        await callback.message.edit_text(
            f"🔑 Ключи на хосте {host_name}:",
            reply_markup=keyboards.create_admin_keys_for_host_keyboard(host_name, keys, page=0)
        )

    @admin_router.callback_query(AdminHostKeys.picking_host, F.data.startswith("admin_hostkeys_page_"))
    async def admin_host_keys_page_nav(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        # Определяем номер страницы и текущий хост
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        data = await state.get_data()
        host_name = (data or {}).get("hostkeys_host")
        if not host_name:
            # Если по какой-то причине контекст потерялся — возвращаемся к выбору хоста
            hosts = get_all_hosts()
            await callback.message.edit_text(
                "🌍 Выберите хост для просмотра ключей:",
                reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="hostkeys")
            )
            return
        keys = get_keys_for_host(host_name)
        await callback.message.edit_text(
            f"🔑 Ключи на хосте {host_name}:",
            reply_markup=keyboards.create_admin_keys_for_host_keyboard(host_name, keys, page=page)
        )

    @admin_router.callback_query(AdminHostKeys.picking_host, F.data == "admin_hostkeys_back_to_hosts")
    async def admin_hostkeys_back_to_hosts(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        # Сбрасываем контекст выбранного хоста
        try:
            await state.update_data(hostkeys_host=None)
        except Exception:
            pass
        hosts = get_all_hosts()
        await callback.message.edit_text(
            "🌍 Выберите хост для просмотра ключей:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="hostkeys")
        )

    @admin_router.callback_query(F.data == "admin_hostkeys_back_to_users")
    async def admin_hostkeys_back_to_users(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)

    # --- Быстрое удаление ключа по ID/Email ---
    class AdminQuickDeleteKey(StatesGroup):
        waiting_for_identifier = State()

    @admin_router.callback_query(F.data == "admin_delete_key")
    async def admin_delete_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminQuickDeleteKey.waiting_for_identifier)
        await callback.message.edit_text(
            "🗑 Введите <code>key_id</code> или <code>email</code> ключа для удаления:",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminQuickDeleteKey.waiting_for_identifier)
    async def admin_delete_key_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        text = (message.text or '').strip()
        key = None
        # сначала попробуем как ID
        try:
            key_id = int(text)
            key = get_key_by_id(key_id)
        except Exception:
            # затем как email
            key = get_key_by_email(text)
        if not key:
            await message.answer("❌ Ключ не найден. Пришлите корректный key_id или email.")
            return
        key_id = int(key.get('key_id'))
        email = key.get('key_email') or '—'
        host = key.get('host_name') or '—'
        await state.clear()
        await message.answer(
            f"Подтвердите удаление ключа #{key_id}\nEmail: {email}\nСервер: {host}",
            reply_markup=keyboards.create_admin_delete_key_confirm_keyboard(key_id)
        )

    # --- Продление ключа на N дней ---
    class AdminExtendKey(StatesGroup):
        waiting_for_pair = State()

    @admin_router.callback_query(F.data == "admin_extend_key")
    async def admin_extend_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminExtendKey.waiting_for_pair)
        await callback.message.edit_text(
            "➕ Введите: <code>key_id дни</code> (сколько дней добавить к ключу)",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminExtendKey.waiting_for_pair)
    async def admin_extend_key_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        parts = (message.text or '').strip().split()
        if len(parts) != 2:
            await message.answer("❌ Формат: <code>key_id дни</code>")
            return
        try:
            key_id = int(parts[0])
            days = int(parts[1])
        except Exception:
            await message.answer("❌ Оба значения должны быть числами")
            return
        if days <= 0:
            await message.answer("❌ Количество дней должно быть положительным")
            return
        key = get_key_by_id(key_id)
        if not key:
            await message.answer("❌ Ключ не найден")
            return
        host = key.get('host_name')
        email = key.get('key_email')
        if not host or not email:
            await message.answer("❌ У ключа отсутствуют данные о хосте или email")
            return
        # Обновим на хосте
        resp = None
        try:
            resp = await create_or_update_key_on_host(host, email, days_to_add=days)
        except Exception as e:
            logger.error(f"Extend flow: failed to update client on host '{host}' for key #{key_id}: {e}")
        if not resp or not resp.get('client_uuid') or not resp.get('expiry_timestamp_ms'):
            await message.answer("❌ Не удалось продлить ключ на сервере")
            return
        # Обновим в БД
        try:
            update_key_info(key_id, resp['client_uuid'], int(resp['expiry_timestamp_ms']))
        except Exception as e:
            logger.error(f"Extend flow: failed update DB for key #{key_id}: {e}")
        await state.clear()
        await message.answer(f"✅ Ключ #{key_id} продлён на {days} дн.")
        # Попробуем уведомить пользователя
        try:
            await message.bot.send_message(int(key.get('user_id')), f"ℹ️ Администратор продлил ваш ключ #{key_id} на {days} дн.")
        except Exception:
            pass

    @admin_router.callback_query(F.data == "start_broadcast")
    async def start_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(
            "Пришлите сообщение, которое вы хотите разослать всем пользователям.\n"
            "Вы можете использовать форматирование (<b>жирный</b>, <i>курсив</i>).\n"
            "Также поддерживаются фото, видео и документы.\n",
            reply_markup=keyboards.create_broadcast_cancel_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_message)

    @admin_router.message(Broadcast.waiting_for_message)
    async def broadcast_message_received_handler(message: types.Message, state: FSMContext):
        # сохраняем оригинальное сообщение целиком, чтобы потом скопировать
        await state.update_data(message_to_send=message.model_dump_json())
        await message.answer(
            "Сообщение получено. Хотите добавить к нему кнопку со ссылкой?",
            reply_markup=keyboards.create_broadcast_options_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_button_option)

    @admin_router.callback_query(Broadcast.waiting_for_button_option, F.data == "broadcast_add_button")
    async def add_button_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.edit_text(
            "Хорошо. Теперь отправьте мне текст для кнопки.",
            reply_markup=keyboards.create_broadcast_cancel_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_button_text)

    @admin_router.message(Broadcast.waiting_for_button_text)
    async def button_text_received_handler(message: types.Message, state: FSMContext):
        await state.update_data(button_text=message.text)
        await message.answer(
            "Текст кнопки получен. Теперь отправьте ссылку (URL), куда она будет вести.",
            reply_markup=keyboards.create_broadcast_cancel_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_button_url)

    @admin_router.message(Broadcast.waiting_for_button_url)
    async def button_url_received_handler(message: types.Message, state: FSMContext, bot: Bot):
        url_to_check = message.text
        # Простая проверка схемы. Дальнейшую валидацию можно расширить при необходимости.
        if not (url_to_check.startswith("http://") or url_to_check.startswith("https://")):
            await message.answer(
                "❌ Ссылка должна начинаться с http:// или https://. Попробуйте еще раз.")
            return
        await state.update_data(button_url=url_to_check)
        await show_broadcast_preview(message, state, bot)

    @admin_router.callback_query(Broadcast.waiting_for_button_option, F.data == "broadcast_skip_button")
    async def skip_button_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        await state.update_data(button_text=None, button_url=None)
        await show_broadcast_preview(callback.message, state, bot)

    async def show_broadcast_preview(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        message_json = data.get('message_to_send')
        original_message = types.Message.model_validate_json(message_json)

        button_text = data.get('button_text')
        button_url = data.get('button_url')

        preview_keyboard = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.button(text=button_text, url=button_url)
            preview_keyboard = builder.as_markup()

        await message.answer(
            "Вот так будет выглядеть ваше сообщение. Отправляем?",
            reply_markup=keyboards.create_broadcast_confirmation_keyboard()
        )

        await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=original_message.chat.id,
            message_id=original_message.message_id,
            reply_markup=preview_keyboard
        )

        await state.set_state(Broadcast.waiting_for_confirmation)

    @admin_router.callback_query(Broadcast.waiting_for_confirmation, F.data == "confirm_broadcast")
    async def confirm_broadcast_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.message.edit_text("⏳ Начинаю рассылку... Это может занять некоторое время.")

        data = await state.get_data()
        message_json = data.get('message_to_send')
        original_message = types.Message.model_validate_json(message_json)

        button_text = data.get('button_text')
        button_url = data.get('button_url')

        final_keyboard = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.button(text=button_text, url=button_url)
            final_keyboard = builder.as_markup()

        await state.clear()

        users = get_all_users()
        logger.info(f"Broadcast: Starting to iterate over {len(users)} users.")

        sent_count = 0
        failed_count = 0
        banned_count = 0

        for user in users:
            user_id = user['telegram_id']
            if user.get('is_banned'):
                banned_count += 1
                continue
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=original_message.chat.id,
                    message_id=original_message.message_id,
                    reply_markup=final_keyboard
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                failed_count += 1
                logger.warning(f"Failed to send broadcast message to user {user_id}: {e}")

        await callback.message.answer(
            f"✅ Рассылка завершена!\n\n"
            f"👍 Отправлено: {sent_count}\n"
            f"👎 Не удалось отправить: {failed_count}\n"
            f"🚫 Пропущено (забанены): {banned_count}"
        )
        await show_admin_menu(callback.message)

    @admin_router.callback_query(StateFilter(Broadcast), F.data == "cancel_broadcast")
    async def cancel_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Рассылка отменена.")
        await state.clear()
        await show_admin_menu(callback.message, edit_message=True)

    # --- Админ-команды для управления заявками на вывод ---
    @admin_router.message(Command(commands=["approve_withdraw"]))
    async def approve_withdraw_handler(message: types.Message):
        if not is_admin(message.from_user.id):
            return
        try:
            user_id = int(message.text.split("_")[-1])
            user = get_user(user_id)
            balance = user.get('referral_balance', 0)
            if balance < 100:
                await message.answer("Баланс пользователя менее 100 руб.")
                return
            set_referral_balance(user_id, 0)
            set_referral_balance_all(user_id, 0)
            await message.answer(f"✅ Выплата {balance:.2f} RUB пользователю {user_id} подтверждена.")
            await message.bot.send_message(
                user_id,
                f"✅ Ваша заявка на вывод {balance:.2f} RUB одобрена. Деньги будут переведены в ближайшее время."
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")

    @admin_router.message(Command(commands=["decline_withdraw"]))
    async def decline_withdraw_handler(message: types.Message):
        if not is_admin(message.from_user.id):
            return
        try:
            user_id = int(message.text.split("_")[-1])
            await message.answer(f"❌ Заявка пользователя {user_id} отклонена.")
            await message.bot.send_message(
                user_id,
                "❌ Ваша заявка на вывод отклонена. Проверьте корректность реквизитов и попробуйте снова."
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")

    return admin_router
