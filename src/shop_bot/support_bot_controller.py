import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from shop_bot.data_manager import database
from shop_bot.support_bot.handlers import get_support_router

logger = logging.getLogger(__name__)

class SupportBotController:
    def __init__(self):
        self._dp: Dispatcher | None = None
        self._bot: Bot | None = None
        self._task = None
        self._is_running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        logger.info("SupportBotController: Event loop has been set.")

    def get_bot_instance(self) -> Bot | None:
        return self._bot

    async def _start_polling(self):
        self._is_running = True
        logger.info("SupportBotController: Polling task has been started.")
        try:
            await self._dp.start_polling(self._bot)
        except asyncio.CancelledError:
            logger.info("SupportBotController: Polling task was cancelled.")
        except Exception as e:
            logger.error(f"SupportBotController: An error occurred during polling: {e}", exc_info=True)
        finally:
            logger.info("SupportBotController: Polling has gracefully stopped.")
            self._is_running = False
            self._task = None
            if self._bot:
                await self._bot.close()
            self._bot = None
            self._dp = None

    def start(self):
        if self._is_running:
            return {"status": "error", "message": "Support-бот уже запущен."}

        if not self._loop or not self._loop.is_running():
            return {"status": "error", "message": "Критическая ошибка: цикл событий не установлен."}

        token = database.get_setting("support_bot_token")
        bot_username = database.get_setting("support_bot_username")
        admin_id = database.get_setting("admin_telegram_id")

        if not all([token, bot_username, admin_id]):
            return {
                "status": "error",
                "message": "Невозможно запустить support-бот: заполните support_bot_token, support_bot_username и admin_telegram_id."
            }

        try:
            self._bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self._dp = Dispatcher()

            # Only support module router
            router = get_support_router()
            self._dp.include_router(router)

            # Ensure no Telegram webhook is set before starting polling
            try:
                asyncio.run_coroutine_threadsafe(self._bot.delete_webhook(drop_pending_updates=True), self._loop)
            except Exception as e:
                logger.warning(f"SupportBotController: Failed to delete webhook before polling: {e}")

            self._task = asyncio.run_coroutine_threadsafe(self._start_polling(), self._loop)
            logger.info("SupportBotController: Start command sent to event loop.")
            return {"status": "success", "message": "Команда на запуск support-бота отправлена."}
        except Exception as e:
            logger.error(f"Failed to start support-bot: {e}", exc_info=True)
            self._bot = None
            self._dp = None
            return {"status": "error", "message": f"Ошибка при запуске support-бота: {e}"}

    def stop(self):
        if not self._is_running:
            return {"status": "error", "message": "Support-бот не запущен."}

        if not self._loop or not self._dp:
            return {"status": "error", "message": "Критическая ошибка: компоненты бота недоступны."}

        logger.info("SupportBotController: Sending graceful stop signal...")
        asyncio.run_coroutine_threadsafe(self._dp.stop_polling(), self._loop)
        return {"status": "success", "message": "Команда на остановку support-бота отправлена."}

    def get_status(self):
        return {"is_running": self._is_running}
