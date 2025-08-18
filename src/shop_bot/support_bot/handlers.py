import logging
from aiogram import Bot, Router, F, types, html
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ChatMemberStatus

from shop_bot.data_manager.database import (
    get_setting,
    create_support_ticket,
    add_support_message,
    get_user_tickets,
    get_ticket,
    get_ticket_messages,
    set_ticket_status,
    update_ticket_thread_info,
    get_ticket_by_thread,
)

logger = logging.getLogger(__name__)

class SupportDialog(StatesGroup):
    waiting_for_subject = State()
    waiting_for_message = State()
    waiting_for_reply = State()


def get_support_router() -> Router:
    router = Router()

    def _get_latest_open_ticket(user_id: int) -> dict | None:
        try:
            tickets = get_user_tickets(user_id) or []
            open_tickets = [t for t in tickets if t.get('status') == 'open']
            if not open_tickets:
                return None
            return max(open_tickets, key=lambda t: int(t['ticket_id']))
        except Exception:
            return None

    @router.message(CommandStart())
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot):
        # If started with /start new, immediately prompt to create a ticket
        args = (message.text or "").split(maxsplit=1)
        arg = None
        if len(args) > 1:
            # Telegram uses /start <payload>, aiogram packs it in text for deep-link
            arg = args[1].strip()
        if arg == "new":
            existing = _get_latest_open_ticket(message.from_user.id)
            if existing:
                await message.answer(
                    f"У вас уже есть открытый тикет #{existing['ticket_id']}. Пожалуйста, продолжайте переписку в этом тикете. Новый тикет можно создать после его закрытия."
                )
            else:
                await message.answer("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')")
                await state.set_state(SupportDialog.waiting_for_subject)
            return
        support_text = get_setting("support_text") or "Раздел поддержки. Вы можете создать обращение или открыть существующее."
        # Show a ReplyKeyboard (buttons under input field)
        await message.answer(
            support_text,
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="✍️ Новое обращение")],
                    [types.KeyboardButton(text="📨 Мои обращения")],
                ],
                resize_keyboard=True
            ),
        )

    @router.callback_query(F.data == "support_new_ticket")
    async def support_new_ticket_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        existing = _get_latest_open_ticket(callback.from_user.id)
        if existing:
            await callback.message.edit_text(
                f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём. Новый тикет можно создать после закрытия текущего."
            )
        else:
            await callback.message.edit_text("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')")
            await state.set_state(SupportDialog.waiting_for_subject)

    @router.message(SupportDialog.waiting_for_subject)
    async def support_subject_received(message: types.Message, state: FSMContext):
        subject = (message.text or "").strip()
        await state.update_data(subject=subject)
        await message.answer("✉️ Опишите проблему максимально подробно одним сообщением.")
        await state.set_state(SupportDialog.waiting_for_message)

    @router.message(SupportDialog.waiting_for_message)
    async def support_message_received(message: types.Message, state: FSMContext, bot: Bot):
        user_id = message.from_user.id
        data = await state.get_data()
        subject = data.get("subject")
        # Reuse existing open ticket if present
        existing = _get_latest_open_ticket(user_id)
        created_new = False
        if existing:
            ticket_id = int(existing['ticket_id'])
            add_support_message(ticket_id, sender="user", content=message.text or "")
            ticket = get_ticket(ticket_id)
        else:
            ticket_id = create_support_ticket(user_id, subject)
            if not ticket_id:
                await message.answer("❌ Не удалось создать обращение. Попробуйте позже.")
                await state.clear()
                return
            add_support_message(ticket_id, sender="user", content=message.text or "")
            ticket = get_ticket(ticket_id)
            created_new = True
        # Create forum topic in support group if configured
        support_forum_chat_id = get_setting("support_forum_chat_id")
        thread_id = None
        if support_forum_chat_id and not (ticket and ticket.get('message_thread_id')):
            try:
                chat_id = int(support_forum_chat_id)
                topic_name = f"#{ticket_id} {subject[:40] if subject else 'Обращение'}"
                forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                thread_id = forum_topic.message_thread_id
                update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                header = (
                    "🆘 Новое обращение\n"
                    f"Тикет: #{ticket_id}\n"
                    f"Пользователь: @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\n"
                    f"Тема: {subject or '—'}\n\n"
                    f"Сообщение:\n{message.text or ''}"
                )
                await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id)
            except Exception as e:
                logger.warning(f"Failed to create forum topic or send message for ticket {ticket_id}: {e}")
        await state.clear()
        if created_new:
            await message.answer(
                f"✅ Обращение создано: #{ticket_id}. Мы ответим вам как можно скорее.",
            )
        else:
            await message.answer(
                f"✉️ Сообщение добавлено в ваш открытый тикет #{ticket_id}.",
            )
        # Notify admin
        admin_id = get_setting("admin_telegram_id")
        if admin_id:
            try:
                await bot.send_message(
                    int(admin_id),
                    (
                        "🆘 Новое обращение в поддержку\n"
                        f"ID тикета: #{ticket_id}\n"
                        f"От пользователя: @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\n"
                        f"Тема: {subject or '—'}\n\n"
                        f"Сообщение:\n{message.text or ''}"
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to notify admin about ticket {ticket_id}: {e}")

    @router.callback_query(F.data == "support_my_tickets")
    async def support_my_tickets_handler(callback: types.CallbackQuery):
        await callback.answer()
        tickets = get_user_tickets(callback.from_user.id)
        text = "Ваши обращения:" if tickets else "У вас пока нет обращений."
        rows = []
        if tickets:
            for t in tickets:
                status_text = "🟢 Открыт" if t.get('status') == 'open' else "Закрыт"
                title = f"#{t['ticket_id']} • {status_text}"
                if t.get('subject'):
                    title += f" • {t['subject'][:20]}"
                rows.append([types.InlineKeyboardButton(text=title, callback_data=f"support_view_{t['ticket_id']}")])
        # add back button
        rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="start_over")])
        await callback.message.edit_text(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows))

    @router.callback_query(F.data.startswith("support_view_"))
    async def support_view_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id:
            await callback.message.edit_text("Тикет не найден или доступ запрещён.")
            return
        messages = get_ticket_messages(ticket_id)
        human_status = "🟢 Открыт" if ticket.get('status') == 'open' else "Закрыт"
        parts = [f"🧾 Тикет #{ticket_id} — статус: {human_status}\nТема: {ticket.get('subject') or '—'}\n"]
        for m in messages:
            who = "Вы" if m.get('sender') == 'user' else 'Поддержка'
            created = m.get('created_at')
            parts.append(f"{who} ({created}):\n{m.get('content','')}\n")
        final_text = "\n".join(parts)
        is_open = (ticket.get('status') == 'open')
        buttons = []
        if is_open:
            buttons.append([types.InlineKeyboardButton(text="💬 Ответить", callback_data=f"support_reply_{ticket_id}")])
            buttons.append([types.InlineKeyboardButton(text="✅ Закрыть", callback_data=f"support_close_{ticket_id}")])
        buttons.append([types.InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")])
        await callback.message.edit_text(final_text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=buttons))

    @router.callback_query(F.data.startswith("support_reply_"))
    async def support_reply_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id or ticket.get('status') != 'open':
            await callback.message.edit_text("Нельзя ответить на этот тикет.")
            return
        await state.update_data(reply_ticket_id=ticket_id)
        await callback.message.edit_text("Напишите ваш ответ одним сообщением.")
        await state.set_state(SupportDialog.waiting_for_reply)

    @router.message(SupportDialog.waiting_for_reply)
    async def support_reply_received(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        ticket_id = data.get('reply_ticket_id')
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != message.from_user.id or ticket.get('status') != 'open':
            await message.answer("Нельзя ответить на этот тикет.")
            await state.clear()
            return
        add_support_message(ticket_id, sender='user', content=message.text or '')
        await state.clear()
        await message.answer("Сообщение отправлено.")
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            # Auto-create thread if missing
            if not (forum_chat_id and thread_id):
                support_forum_chat_id = get_setting("support_forum_chat_id")
                if support_forum_chat_id:
                    try:
                        chat_id = int(support_forum_chat_id)
                        topic_name = f"#{ticket_id} {ticket.get('subject')[:40] if ticket.get('subject') else 'Обращение'}"
                        forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                        thread_id = forum_topic.message_thread_id
                        forum_chat_id = chat_id
                        update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                        header = (
                            "📌 Тред создан автоматически\n"
                            f"Тикет: #{ticket_id}\n"
                            f"Пользователь: ID {ticket.get('user_id')}\n"
                            f"Тема: {ticket.get('subject') or '—'}"
                        )
                        await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id)
                    except Exception as e:
                        logger.warning(f"Failed to auto-create forum topic for ticket {ticket_id}: {e}")
            if forum_chat_id and thread_id:
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):",
                    message_thread_id=int(thread_id)
                )
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e:
            logger.warning(f"Failed to mirror user reply to forum: {e}")
        admin_id = get_setting("admin_telegram_id")
        if admin_id:
            try:
                await bot.send_message(
                    int(admin_id),
                    (
                        "📩 Новое сообщение в тикете\n"
                        f"ID тикета: #{ticket_id}\n"
                        f"От пользователя: @{message.from_user.username or message.from_user.full_name} (ID: {message.from_user.id})\n\n"
                        f"Сообщение:\n{message.text or ''}"
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to notify admin about ticket message #{ticket_id}: {e}")

    # Relay messages from forum thread to the ticket owner (admin -> user)
    @router.message(F.is_topic_message == True)
    async def forum_thread_message_handler(message: types.Message, bot: Bot):
        try:
            if not message.message_thread_id:
                return
            forum_chat_id = message.chat.id
            thread_id = message.message_thread_id
            ticket = get_ticket_by_thread(str(forum_chat_id), int(thread_id))
            if not ticket:
                return
            user_id = int(ticket.get('user_id'))
            # Ignore messages from the bot itself
            me = await bot.get_me()
            if message.from_user and message.from_user.id == me.id:
                return
            # Allow only admins to relay messages
            try:
                admin_setting = get_setting("admin_telegram_id")
                is_admin_by_setting = admin_setting and int(admin_setting) == message.from_user.id
            except Exception:
                is_admin_by_setting = False
            is_admin_in_chat = False
            try:
                member = await bot.get_chat_member(chat_id=forum_chat_id, user_id=message.from_user.id)
                is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
            except Exception:
                pass
            if not (is_admin_by_setting or is_admin_in_chat):
                return
            # Log as admin message and relay any content to user
            content = (message.text or message.caption or "").strip()
            if content:
                add_support_message(ticket_id=int(ticket['ticket_id']), sender='admin', content=content)
            admin_name = (message.from_user and (message.from_user.username and f"@{message.from_user.username}")) or (message.from_user.full_name if message.from_user else "Админ")
            header = await bot.send_message(
                chat_id=user_id,
                text=f"💬 Ответ поддержки по тикету #{ticket['ticket_id']} от {admin_name}"
            )
            # Copy original message to preserve media/formatting
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    reply_to_message_id=header.message_id
                )
            except Exception:
                # Fallback: send text if copy fails
                if content:
                    await bot.send_message(chat_id=user_id, text=content)
        except Exception as e:
            logger.warning(f"Failed to relay forum thread message: {e}")

    @router.callback_query(F.data.startswith("support_close_"))
    async def support_close_ticket_handler(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id:
            await callback.message.edit_text("Тикет не найден или доступ запрещён.")
            return
        if ticket.get('status') == 'closed':
            await callback.message.edit_text("Тикет уже закрыт.")
            return
        ok = set_ticket_status(ticket_id, 'closed')
        if ok:
            # Close related forum topic if exists (do not delete)
            try:
                forum_chat_id = ticket.get('forum_chat_id')
                thread_id = ticket.get('message_thread_id')
                if forum_chat_id and thread_id:
                    await bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id))
            except Exception as e:
                logger.warning(f"Failed to close forum topic for ticket {ticket_id} from bot: {e}")
            await callback.message.edit_text("✅ Тикет закрыт.", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")]]))
        else:
            await callback.message.edit_text("❌ Не удалось закрыть тикет. Попробуйте позже.")

    # Message handlers for ReplyKeyboard buttons
    @router.message(F.text == "▶️ Начать")
    async def start_text_button(message: types.Message, state: FSMContext):
        existing = _get_latest_open_ticket(message.from_user.id)
        if existing:
            await message.answer(
                f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём."
            )
        else:
            await message.answer("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')")
            await state.set_state(SupportDialog.waiting_for_subject)

    @router.message(F.text == "✍️ Новое обращение")
    async def new_ticket_text_button(message: types.Message, state: FSMContext):
        existing = _get_latest_open_ticket(message.from_user.id)
        if existing:
            await message.answer(
                f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём."
            )
        else:
            await message.answer("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')")
            await state.set_state(SupportDialog.waiting_for_subject)

    @router.message(F.text == "📨 Мои обращения")
    async def my_tickets_text_button(message: types.Message):
        tickets = get_user_tickets(message.from_user.id)
        text = "Ваши обращения:" if tickets else "У вас пока нет обращений."
        rows = []
        if tickets:
            for t in tickets:
                title = f"#{t['ticket_id']} • {t.get('status','open')}"
                if t.get('subject'):
                    title += f" • {t['subject'][:20]}"
                rows.append([types.InlineKeyboardButton(text=title, callback_data=f"support_view_{t['ticket_id']}")])
        rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="start_over")])
        await message.answer(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows))

    # Catch-all: relay any message from user to the open ticket's forum thread
    @router.message()
    async def relay_user_message_to_forum(message: types.Message, bot: Bot, state: FSMContext):
        # Ignore if we are in the middle of FSM flow
        current_state = await state.get_state()
        if current_state is not None:
            return

        user_id = message.from_user.id if message.from_user else None
        if not user_id:
            return

        tickets = get_user_tickets(user_id)
        content = (message.text or message.caption or '')
        # If no tickets or no open ones — create a new ticket automatically
        ticket = None
        if not tickets:
            ticket_id = create_support_ticket(user_id, None)
            add_support_message(ticket_id, sender='user', content=content)
            ticket = get_ticket(ticket_id)
            created_new = True
        else:
            open_tickets = [t for t in tickets if t.get('status') == 'open']
            if not open_tickets:
                ticket_id = create_support_ticket(user_id, None)
                add_support_message(ticket_id, sender='user', content=content)
                ticket = get_ticket(ticket_id)
                created_new = True
            else:
                # Use latest open ticket
                ticket = max(open_tickets, key=lambda t: int(t['ticket_id']))
                ticket_id = int(ticket['ticket_id'])
                add_support_message(ticket_id, sender='user', content=content)
                created_new = False

        # Mirror to forum thread, auto-create topic if missing
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if not (forum_chat_id and thread_id):
                support_forum_chat_id = get_setting("support_forum_chat_id")
                if support_forum_chat_id:
                    try:
                        chat_id = int(support_forum_chat_id)
                        topic_name = f"#{ticket_id} {ticket.get('subject')[:40] if ticket.get('subject') else 'Обращение'}"
                        forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                        thread_id = forum_topic.message_thread_id
                        forum_chat_id = chat_id
                        update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                        header = (
                            ("🆘 Новое обращение\n" if created_new else "📌 Тред создан автоматически\n") +
                            f"Тикет: #{ticket_id}\n" \
                            f"Пользователь: ID {ticket.get('user_id')}\n" \
                            f"Тема: {ticket.get('subject') or '—'}"
                        )
                        await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id)
                    except Exception as e:
                        logger.warning(f"Failed to auto-create forum topic for ticket {ticket_id}: {e}")
            if forum_chat_id and thread_id:
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=(
                        f"🆘 Новое обращение от {username} (ID: {message.from_user.id}) по тикету #{ticket_id}:" if created_new
                        else f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):"
                    ),
                    message_thread_id=int(thread_id)
                )
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e:
            logger.warning(f"Failed to mirror user free-form message to forum for ticket {ticket_id}: {e}")

        # Acknowledge to user
        try:
            if created_new:
                await message.answer(f"✅ Обращение создано: #{ticket_id}. Мы ответим вам как можно скорее.")
            else:
                await message.answer("Сообщение принято. Поддержка скоро ответит.")
        except Exception:
            pass

    return router
