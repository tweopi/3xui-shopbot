import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from shop_bot.data_manager import database
from shop_bot.data_manager.database import get_admin_ids
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
        logger.info("SupportBotController: цикл событий установлен.")

    def get_bot_instance(self) -> Bot | None:
        return self._bot

    async def _start_polling(self):
        self._is_running = True
        logger.info("SupportBotController: задача polling запущена.")
        try:
            await self._dp.start_polling(self._bot)
        except asyncio.CancelledError:
            logger.info("SupportBotController: задача polling отменена.")
        except Exception as e:
            logger.error(f"SupportBotController: ошибка во время polling: {e}", exc_info=True)
        finally:
            logger.info("SupportBotController: polling корректно остановлен.")
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
        # допускаем отсутствие одиночного admin_telegram_id, если настроены admin_telegram_ids
        admin_id = database.get_setting("admin_telegram_id")
        admin_ids = get_admin_ids()

        if not all([token, bot_username]) or (not admin_id and not admin_ids):
            return {
                "status": "error",
                "message": "Невозможно запустить support-бот: заполните support_bot_token, support_bot_username и хотя бы одного администратора (admin_telegram_id или admin_telegram_ids)."
            }

        try:
            self._bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self._dp = Dispatcher()
            
            router = get_support_router()
            self._dp.include_router(router)
            
            try:
                asyncio.run_coroutine_threadsafe(self._bot.delete_webhook(drop_pending_updates=True), self._loop)
            except Exception as e:
                logger.warning(f"SupportBotController: не удалось удалить вебхук перед polling: {e}")

            self._task = asyncio.run_coroutine_threadsafe(self._start_polling(), self._loop)
            logger.info("SupportBotController: команда запуска отправлена в цикл событий.")
            return {"status": "success", "message": "Команда на запуск support-бота отправлена."}
        except Exception as e:
            logger.error(f"Не удалось запустить support-бот: {e}", exc_info=True)
            self._bot = None
            self._dp = None
            return {"status": "error", "message": f"Ошибка при запуске support-бота: {e}"}

    def stop(self):
        if not self._is_running:
            return {"status": "error", "message": "Support-бот не запущен."}

        if not self._loop or not self._dp:
            return {"status": "error", "message": "Критическая ошибка: компоненты бота недоступны."}

        logger.info("SupportBotController: отправка сигнала на корректную остановку...")
        asyncio.run_coroutine_threadsafe(self._dp.stop_polling(), self._loop)
        return {"status": "success", "message": "Команда на остановку support-бота отправлена."}

    def get_status(self):
        return {"is_running": self._is_running}
