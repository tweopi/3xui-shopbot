import logging

from datetime import datetime

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from shop_bot.data_manager.database import get_setting

logger = logging.getLogger(__name__)

main_reply_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🏠 Главное меню")]],
    resize_keyboard=True
)

def create_main_menu_keyboard(user_keys: list, trial_available: bool, is_admin: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    if trial_available and get_setting("trial_enabled") == "true":
        builder.button(text=(get_setting("btn_try") or "🎁 Попробовать бесплатно"), callback_data="get_trial")

    builder.button(text=(get_setting("btn_profile") or "👤 Мой профиль"), callback_data="show_profile")
    keys_label_tpl = (get_setting("btn_my_keys") or "🔑 Мои ключи ({count})")
    builder.button(text=keys_label_tpl.replace("{count}", str(len(user_keys))), callback_data="manage_keys")
    builder.button(text=(get_setting("btn_buy_key") or "💳 Купить ключ"), callback_data="buy_new_key")
    builder.button(text=(get_setting("btn_top_up") or "➕ Пополнить баланс"), callback_data="top_up_start")
    builder.button(text=(get_setting("btn_referral") or "🤝 Реферальная программа"), callback_data="show_referral_program")
    builder.button(text=(get_setting("btn_support") or "🆘 Поддержка"), callback_data="show_help")
    builder.button(text=(get_setting("btn_about") or "ℹ️ О проекте"), callback_data="show_about")
    builder.button(text=(get_setting("btn_howto") or "❓ Как использовать"), callback_data="howto_vless")
    if is_admin:
        builder.button(text=(get_setting("btn_admin") or "⚙️ Админка"), callback_data="admin_menu")

    layout = [
        1 if trial_available and get_setting("trial_enabled") == "true" else 0,  # триал
        2,  # профиль + мои ключи
        2,  # купить ключ + пополнить баланс
        1,  # рефералка
        2,  # поддержка + о проекте
        1,  # как использовать
        1 if is_admin else 0,  # админка
    ]
    actual_layout = [size for size in layout if size > 0]
    builder.adjust(*actual_layout)
    
    return builder.as_markup()

def create_admin_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="🌍 Ключи на хосте", callback_data="admin_host_keys")
    builder.button(text="🎁 Выдать ключ", callback_data="admin_gift_key")
    builder.button(text="⚡ Тест скорости", callback_data="admin_speedtest")
    builder.button(text="🗄 Бэкап БД", callback_data="admin_backup_db")
    builder.button(text="♻️ Восстановить БД", callback_data="admin_restore_db")
    builder.button(text="👮 Администраторы", callback_data="admin_admins_menu")
    builder.button(text="📢 Рассылка", callback_data="start_broadcast")
    builder.button(text="⬅️ Назад в меню", callback_data="back_to_main_menu")
    # 4 ряда по 2 кнопки (включая бэкап/восстановление), затем "Назад"
    builder.adjust(2, 2, 2, 2, 1)
    return builder.as_markup()

def create_admins_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить админа", callback_data="admin_add_admin")
    builder.button(text="➖ Снять админа", callback_data="admin_remove_admin")
    builder.button(text="📋 Список админов", callback_data="admin_view_admins")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(2, 2)
    return builder.as_markup()

def create_admin_users_keyboard(users: list[dict], page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * page_size
    end = start + page_size
    for u in users[start:end]:
        user_id = u.get('telegram_id') or u.get('user_id') or u.get('id')
        username = u.get('username') or '—'
        title = f"{user_id} • @{username}" if username != '—' else f"{user_id}"
        builder.button(text=title, callback_data=f"admin_view_user_{user_id}")
    # pagination
    total = len(users)
    have_prev = page > 0
    have_next = end < total
    if have_prev:
        builder.button(text="⬅️ Назад", callback_data=f"admin_users_page_{page-1}")
    if have_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_users_page_{page+1}")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    # layout: list (1 per row), then pagination/buttons (2), then back (1)
    rows = [1] * len(users[start:end])
    tail = []
    if have_prev or have_next:
        tail.append(2 if (have_prev and have_next) else 1)
    tail.append(1)
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1]))
    return builder.as_markup()

def create_admin_user_actions_keyboard(user_id: int, is_banned: bool | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Начислить баланс", callback_data=f"admin_add_balance_{user_id}")
    builder.button(text="➖ Списать баланс", callback_data=f"admin_deduct_balance_{user_id}")
    builder.button(text="🎁 Выдать ключ", callback_data=f"admin_gift_key_{user_id}")
    builder.button(text="🤝 Рефералы пользователя", callback_data=f"admin_user_referrals_{user_id}")
    if is_banned is True:
        builder.button(text="✅ Разбанить", callback_data=f"admin_unban_user_{user_id}")
    else:
        builder.button(text="🚫 Забанить", callback_data=f"admin_ban_user_{user_id}")
    builder.button(text="✏️ Ключи пользователя", callback_data=f"admin_user_keys_{user_id}")
    builder.button(text="⬅️ К списку", callback_data="admin_users")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    # Сделаем шире: 2 колонки, затем назад и в админ-меню
    builder.adjust(2, 2, 2, 2, 1)
    return builder.as_markup()

def create_admin_user_keys_keyboard(user_id: int, keys: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if keys:
        for k in keys:
            kid = k.get('key_id')
            host = k.get('host_name') or '—'
            email = k.get('key_email') or '—'
            title = f"#{kid} • {host} • {email[:20]}"
            builder.button(text=title, callback_data=f"admin_edit_key_{kid}")
    else:
        builder.button(text="Ключей нет", callback_data="noop")
    builder.button(text="⬅️ Назад", callback_data=f"admin_view_user_{user_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_key_actions_keyboard(key_id: int, user_id: int | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🌍 Изменить сервер", callback_data=f"admin_key_edit_host_{key_id}")
    builder.button(text="➕ Добавить дни", callback_data=f"admin_key_extend_{key_id}")
    builder.button(text="🗑 Удалить ключ", callback_data=f"admin_key_delete_{key_id}")
    builder.button(text="⬅️ Назад к ключам", callback_data=f"admin_key_back_{key_id}")
    if user_id is not None:
        builder.button(text="👤 Перейти к пользователю", callback_data=f"admin_view_user_{user_id}")
        builder.adjust(2, 2, 1)
    else:
        builder.adjust(2, 2)
    return builder.as_markup()

def create_admin_delete_key_confirm_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить удаление", callback_data=f"admin_key_delete_confirm_{key_id}")
    builder.button(text="❌ Отмена", callback_data=f"admin_key_delete_cancel_{key_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    return builder.as_markup()

def create_broadcast_options_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить кнопку", callback_data="broadcast_add_button")
    builder.button(text="➡️ Пропустить", callback_data="broadcast_skip_button")
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    builder.adjust(2, 1)
    return builder.as_markup()

def create_broadcast_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Отправить всем", callback_data="confirm_broadcast")
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    builder.adjust(2)
    return builder.as_markup()

def create_broadcast_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    return builder.as_markup()

def create_about_keyboard(channel_url: str | None, terms_url: str | None, privacy_url: str | None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if channel_url:
        builder.button(text=(get_setting("btn_channel") or "📰 Наш канал"), url=channel_url)
    if terms_url:
        builder.button(text=(get_setting("btn_terms") or "📄 Условия использования"), url=terms_url)
    if privacy_url:
        builder.button(text=(get_setting("btn_privacy") or "🔒 Политика конфиденциальности"), url=privacy_url)
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()
    
def create_support_keyboard(support_user: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # Определяем username для поддержки
    username = (support_user or "").strip()
    if not username:
        username = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
    # Преобразуем в tg:// ссылку, если есть username/ссылка
    url: str | None = None
    if username:
        if username.startswith("@"):  # @username
            url = f"tg://resolve?domain={username[1:]}"
        elif username.startswith("tg://"):  # уже tg-схема
            url = username
        elif username.startswith("http://") or username.startswith("https://"):
            # http(s) ссылки на t.me/telegram.me -> в tg://
            # Попробуем извлечь domain
            try:
                # простое извлечение последнего сегмента
                part = username.split("/")[-1].split("?")[0]
                if part:
                    url = f"tg://resolve?domain={part}"
            except Exception:
                url = username
        else:
            # просто username без @
            url = f"tg://resolve?domain={username}"

    if url:
        builder.button(text=(get_setting("btn_support") or "🆘 Поддержка"), url=url)
        builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    else:
        # Фолбэк: встроенное меню поддержки
        builder.button(text=(get_setting("btn_support") or "🆘 Поддержка"), callback_data="show_help")
        builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_support_bot_link_keyboard(support_bot_username: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    username = support_bot_username.lstrip("@")
    deep_link = f"tg://resolve?domain={username}&start=new"
    builder.button(text=(get_setting("btn_support_open") or "🆘 Открыть поддержку"), url=deep_link)
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_support_menu_keyboard(has_external: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_support_new_ticket") or "✍️ Новое обращение"), callback_data="support_new_ticket")
    builder.button(text=(get_setting("btn_support_my_tickets") or "📨 Мои обращения"), callback_data="support_my_tickets")
    if has_external:
        builder.button(text=(get_setting("btn_support_external") or "🆘 Внешняя поддержка"), callback_data="support_external")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_tickets_list_keyboard(tickets: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if tickets:
        for t in tickets:
            title = f"#{t['ticket_id']} • {t.get('status','open')}"
            if t.get('subject'):
                title += f" • {t['subject'][:20]}"
            builder.button(text=title, callback_data=f"support_view_{t['ticket_id']}")
    builder.button(text="⬅️ Назад", callback_data="support_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_ticket_actions_keyboard(ticket_id: int, is_open: bool = True) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if is_open:
        builder.button(text="💬 Ответить", callback_data=f"support_reply_{ticket_id}")
        builder.button(text="✅ Закрыть", callback_data=f"support_close_{ticket_id}")
    builder.button(text="⬅️ К списку", callback_data="support_my_tickets")
    builder.adjust(1)
    return builder.as_markup()

def create_host_selection_keyboard(hosts: list, action: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for host in hosts:
        callback_data = f"select_host_{action}_{host['host_name']}"
        builder.button(text=host['host_name'], callback_data=callback_data)
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="manage_keys" if action == 'new' else "back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_plans_keyboard(plans: list[dict], action: str, host_name: str, key_id: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for plan in plans:
        callback_data = f"buy_{host_name}_{plan['plan_id']}_{action}_{key_id}"
        builder.button(text=f"{plan['plan_name']} - {plan['price']:.0f} RUB", callback_data=callback_data)
    back_callback = "manage_keys" if action == "extend" else "buy_new_key"
    builder.button(text=(get_setting("btn_back") or "⬅️ Назад"), callback_data=back_callback)
    builder.adjust(1) 
    return builder.as_markup()

def create_skip_email_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_skip_email") or "➡️ Продолжить без почты"), callback_data="skip_email")
    builder.button(text=(get_setting("btn_back_to_plans") or "⬅️ Назад к тарифам"), callback_data="back_to_plans")
    builder.adjust(1)
    return builder.as_markup()

def create_payment_method_keyboard(
    payment_methods: dict,
    action: str,
    key_id: int,
    show_balance: bool | None = None,
    main_balance: float | None = None,
    price: float | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    # Кнопки оплаты с балансов (если разрешено/достаточно средств)
    if show_balance:
        label = get_setting("btn_pay_with_balance") or "💼 Оплатить с баланса"
        if main_balance is not None:
            try:
                label += f" ({main_balance:.0f} RUB)"
            except Exception:
                pass
        builder.button(text=label, callback_data="pay_balance")

    # Внешние способы оплаты
    if payment_methods and payment_methods.get("yookassa"):
        if get_setting("sbp_enabled"):
            builder.button(text="🏦 СБП / Банковская карта", callback_data="pay_yookassa")
        else:
            builder.button(text="🏦 Банковская карта", callback_data="pay_yookassa")
    if payment_methods and payment_methods.get("heleket"):
        builder.button(text="💎 Криптовалюта", callback_data="pay_heleket")
    if payment_methods and payment_methods.get("cryptobot"):
        builder.button(text="🤖 CryptoBot", callback_data="pay_cryptobot")
    if payment_methods and payment_methods.get("yoomoney"):
        builder.button(text="💜 ЮMoney (кошелёк)", callback_data="pay_yoomoney")
    if payment_methods and payment_methods.get("stars"):
        builder.button(text="⭐ Telegram Stars", callback_data="pay_stars")
    if payment_methods and payment_methods.get("tonconnect"):
        callback_data_ton = "pay_tonconnect"
        logger.info(f"Creating TON button with callback_data: '{callback_data_ton}'")
        builder.button(text="🪙 TON Connect", callback_data=callback_data_ton)

    builder.button(text=(get_setting("btn_back") or "⬅️ Назад"), callback_data="back_to_email_prompt")
    builder.adjust(1)
    return builder.as_markup()

def create_ton_connect_keyboard(connect_url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🚀 Открыть кошелек", url=connect_url)
    return builder.as_markup()

def create_payment_keyboard(payment_url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_go_to_payment") or "Перейти к оплате"), url=payment_url)
    return builder.as_markup()

def create_payment_with_check_keyboard(payment_url: str, check_callback: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_go_to_payment") or "Перейти к оплате"), url=payment_url)
    builder.button(text=(get_setting("btn_check_payment") or "✅ Проверить оплату"), callback_data=check_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_topup_payment_method_keyboard(payment_methods: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # Только внешние способы оплаты, без оплаты с баланса
    if payment_methods and payment_methods.get("yookassa"):
        if get_setting("sbp_enabled"):
            builder.button(text="🏦 СБП / Банковская карта", callback_data="topup_pay_yookassa")
        else:
            builder.button(text="🏦 Банковская карта", callback_data="topup_pay_yookassa")
    if payment_methods and payment_methods.get("heleket"):
        builder.button(text="💎 Криптовалюта", callback_data="topup_pay_heleket")
    if payment_methods and payment_methods.get("cryptobot"):
        builder.button(text="🤖 CryptoBot", callback_data="topup_pay_cryptobot")
    if payment_methods and payment_methods.get("yoomoney"):
        builder.button(text="💜 ЮMoney (кошелёк)", callback_data="topup_pay_yoomoney")
    if payment_methods and payment_methods.get("stars"):
        builder.button(text="⭐ Telegram Stars", callback_data="topup_pay_stars")
    if payment_methods and payment_methods.get("tonconnect"):
        builder.button(text="🪙 TON Connect", callback_data="topup_pay_tonconnect")

    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="show_profile")
    builder.adjust(1)
    return builder.as_markup()

def create_keys_management_keyboard(keys: list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if keys:
        for i, key in enumerate(keys):
            expiry_date = datetime.fromisoformat(key['expiry_date'])
            status_icon = "✅" if expiry_date > datetime.now() else "❌"
            host_name = key.get('host_name', 'Неизвестный хост')
            button_text = f"{status_icon} Ключ #{i+1} ({host_name}) (до {expiry_date.strftime('%d.%m.%Y')})"
            builder.button(text=button_text, callback_data=f"show_key_{key['key_id']}")
    builder.button(text=(get_setting("btn_buy_key") or "➕ Купить новый ключ"), callback_data="buy_new_key")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_key_info_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_extend_key") or "➕ Продлить этот ключ"), callback_data=f"extend_key_{key_id}")
    builder.button(text=(get_setting("btn_show_qr") or "📱 Показать QR-код"), callback_data=f"show_qr_{key_id}")
    builder.button(text=(get_setting("btn_instruction") or "📖 Инструкция"), callback_data=f"howto_vless_{key_id}")
    builder.button(text=(get_setting("btn_switch_server") or "🌍 Сменить сервер"), callback_data=f"switch_server_{key_id}")
    builder.button(text=(get_setting("btn_back_to_keys") or "⬅️ Назад к списку ключей"), callback_data="manage_keys")
    builder.adjust(1)
    return builder.as_markup()

def create_howto_vless_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_howto_android") or "📱 Android"), callback_data="howto_android")
    builder.button(text=(get_setting("btn_howto_ios") or "📱 iOS"), callback_data="howto_ios")
    builder.button(text=(get_setting("btn_howto_windows") or "💻 Windows"), callback_data="howto_windows")
    builder.button(text=(get_setting("btn_howto_linux") or "🐧 Linux"), callback_data="howto_linux")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def create_howto_vless_keyboard_key(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_howto_android") or "📱 Android"), callback_data="howto_android")
    builder.button(text=(get_setting("btn_howto_ios") or "📱 iOS"), callback_data="howto_ios")
    builder.button(text=(get_setting("btn_howto_windows") or "💻 Windows"), callback_data="howto_windows")
    builder.button(text=(get_setting("btn_howto_linux") or "🐧 Linux"), callback_data="howto_linux")
    builder.button(text=(get_setting("btn_back_to_key") or "⬅️ Назад к ключу"), callback_data=f"show_key_{key_id}")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def create_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    return builder.as_markup()

def create_profile_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_top_up") or "➕ Пополнить баланс"), callback_data="top_up_start")
    builder.button(text=(get_setting("btn_referral") or "🤝 Реферальная программа"), callback_data="show_referral_program")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_welcome_keyboard(channel_url: str | None, is_subscription_forced: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    if channel_url and is_subscription_forced:
        builder.button(text="📢 Перейти в канал", url=channel_url)
        builder.button(text="✅ Я подписался", callback_data="check_subscription_and_agree")
    elif channel_url:
        builder.button(text="📢 Наш канал (не обязательно)", url=channel_url)
        builder.button(text="✅ Принимаю условия", callback_data="check_subscription_and_agree")
    else:
        builder.button(text="✅ Принимаю условия", callback_data="check_subscription_and_agree")
        
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="🏠 В главное меню", callback_data="show_main_menu")

def get_buy_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="💳 Купить подписку", callback_data="buy_vpn")


def create_admin_users_pick_keyboard(users: list[dict], page: int = 0, page_size: int = 10, action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * page_size
    end = start + page_size
    for u in users[start:end]:
        user_id = u.get('telegram_id') or u.get('user_id') or u.get('id')
        username = u.get('username') or '—'
        title = f"{user_id} • @{username}" if username != '—' else f"{user_id}"
        builder.button(text=title, callback_data=f"admin_{action}_pick_user_{user_id}")
    total = len(users)
    have_prev = page > 0
    have_next = end < total
    if have_prev:
        builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_pick_user_page_{page-1}")
    if have_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_{action}_pick_user_page_{page+1}")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    rows = [1] * len(users[start:end])
    tail = []
    if have_prev or have_next:
        tail.append(2 if (have_prev and have_next) else 1)
    tail.append(1)
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1]))
    return builder.as_markup()

def create_admin_hosts_pick_keyboard(hosts: list[dict], action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if hosts:
        for h in hosts:
            name = h.get('host_name')
            if action == "speedtest":
                # Две кнопки в строке: запуск теста и автоустановка
                builder.button(text=name, callback_data=f"admin_{action}_pick_host_{name}")
                builder.button(text="🛠 Автоустановка", callback_data=f"admin_speedtest_autoinstall_{name}")
            else:
                builder.button(text=name, callback_data=f"admin_{action}_pick_host_{name}")
    else:
        builder.button(text="Хостов нет", callback_data="noop")
    # Дополнительные опции для speedtest
    if action == "speedtest":
        builder.button(text="🚀 Запустить для всех", callback_data="admin_speedtest_run_all")
    builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_back_to_users")
    # Сетка: по 2 в ряд для speedtest (хост + автоустановка), иначе по 1
    if action == "speedtest":
        rows = [2] * (len(hosts) if hosts else 1)
        tail = [1, 1]
    else:
        rows = [1] * (len(hosts) if hosts else 1)
        tail = [1]
    builder.adjust(*(rows + tail))
    return builder.as_markup()

def create_admin_keys_for_host_keyboard(
    host_name: str,
    keys: list[dict],
    page: int = 0,
    page_size: int = 20,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # Если ключей нет — показываем заглушку и кнопки назад
    if not keys:
        builder.button(text="Ключей на хосте нет", callback_data="noop")
        builder.button(text="⬅️ К выбору хоста", callback_data="admin_hostkeys_back_to_hosts")
        builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        builder.adjust(1)
        return builder.as_markup()

    # Пагинация
    start = page * page_size
    end = start + page_size
    for k in keys[start:end]:
        kid = k.get('key_id')
        email = k.get('key_email') or '—'
        expiry = k.get('expiry_date') or '—'
        title = f"#{kid} • {email[:24]} • до {expiry}"
        builder.button(text=title, callback_data=f"admin_edit_key_{kid}")

    total = len(keys)
    have_prev = page > 0
    have_next = end < total
    if have_prev:
        builder.button(text="⬅️ Назад", callback_data=f"admin_hostkeys_page_{page-1}")
    if have_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_hostkeys_page_{page+1}")

    # Кнопки навигации
    builder.button(text="⬅️ К выбору хоста", callback_data="admin_hostkeys_back_to_hosts")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")

    # Сетка: список (по 1 в ряд) + пагинация (1 или 2 в ряд) + две кнопки назад
    rows = [1] * len(keys[start:end])
    tail = []
    if have_prev or have_next:
        tail.append(2 if (have_prev and have_next) else 1)
    tail.extend([1, 1])
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1, 1]))
    return builder.as_markup()

def create_admin_months_pick_keyboard(action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for m in (1, 3, 6, 12):
        builder.button(text=f"{m} мес.", callback_data=f"admin_{action}_pick_months_{m}")
    builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_back_to_hosts")
    builder.adjust(2, 2, 1)
    return builder.as_markup()
