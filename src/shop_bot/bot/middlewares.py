from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery, Chat
from shop_bot.data_manager.database import get_user

class BanMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user = data.get('event_from_user')
        if not user:
            return await handler(event, data)

        user_data = get_user(user.id)
        if user_data and user_data.get('is_banned'):
            ban_message_text = "Вы заблокированы и не можете использовать этого бота."
            if isinstance(event, CallbackQuery):
                await event.answer(ban_message_text, show_alert=True)
            elif isinstance(event, Message):
                await event.answer(ban_message_text)
            return
        
        return await handler(event, data)
