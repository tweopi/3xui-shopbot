import asyncio
import logging

from yookassa import Configuration
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode 

from shop_bot.data_manager import database
from shop_bot.bot.handlers import get_user_router
from shop_bot.bot.middlewares import BanMiddleware
from shop_bot.bot import handlers

logger = logging.getLogger(__name__)

class BotController:
    def __init__(self):
        self._dp = None
        self._bot = None
        self._task = None
        self._is_running = False
        self._loop = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        logger.info("BotController: Event loop has been set.")

    def get_bot_instance(self) -> Bot | None:
        return self._bot

    async def _start_polling(self):
        self._is_running = True
        logger.info("BotController: Polling task has been started.")
        try:
            await self._dp.start_polling(self._bot)
        except asyncio.CancelledError:
            logger.info("BotController: Polling task was cancelled.")
        except Exception as e:
            logger.error(f"BotController: An error occurred during polling: {e}", exc_info=True)
        finally:
            logger.info("BotController: Polling has gracefully stopped.")
            self._is_running = False
            self._task = None
            if self._bot:
                await self._bot.close()
            self._bot = None
            self._dp = None

    def start(self):
        if self._is_running:
            return {"status": "error", "message": "Бот уже запущен."}
        
        if not self._loop or not self._loop.is_running():
            return {"status": "error", "message": "Критическая ошибка: цикл событий не установлен."}

        token = database.get_setting("telegram_bot_token")
        bot_username = database.get_setting("telegram_bot_username")
        admin_id = database.get_setting("admin_telegram_id")

        if not all([token, bot_username, admin_id]):
            return {
                "status": "error",
                "message": "Невозможно запустить: не все обязательные настройки Telegram заполнены (токен, username, ID админа)."
            }

        try:
            self._bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self._dp = Dispatcher()
            
            self._dp.update.middleware(BanMiddleware())
            
            user_router = get_user_router()
            
            self._dp.include_router(user_router)
            
            try:
                asyncio.run_coroutine_threadsafe(self._bot.delete_webhook(drop_pending_updates=True), self._loop)
            except Exception as e:
                logger.warning(f"BotController: Failed to delete webhook before polling: {e}")

            yookassa_shop_id = database.get_setting("yookassa_shop_id")
            yookassa_secret_key = database.get_setting("yookassa_secret_key")
            yookassa_enabled = bool(yookassa_shop_id and yookassa_secret_key)

            cryptobot_token = database.get_setting("cryptobot_token")
            cryptobot_enabled = bool(cryptobot_token)

            heleket_shop_id = database.get_setting("heleket_merchant_id")
            heleket_api_key = database.get_setting("heleket_api_key")
            heleket_enabled = bool(heleket_api_key and heleket_shop_id)
            
            ton_wallet_address = database.get_setting("ton_wallet_address")
            tonapi_key = database.get_setting("tonapi_key")
            tonconnect_enabled = bool(ton_wallet_address and tonapi_key)

            if yookassa_enabled:
                Configuration.account_id = yookassa_shop_id
                Configuration.secret_key = yookassa_secret_key
            
            handlers.PAYMENT_METHODS = {
                "yookassa": yookassa_enabled,
                "heleket": heleket_enabled,
                "cryptobot": cryptobot_enabled,
                "tonconnect": tonconnect_enabled
            }
            handlers.TELEGRAM_BOT_USERNAME = bot_username
            handlers.ADMIN_ID = admin_id

            self._task = asyncio.run_coroutine_threadsafe(self._start_polling(), self._loop)
            logger.info("BotController: Start command sent to event loop.")
            return {"status": "success", "message": "Команда на запуск бота отправлена."}
            
        except Exception as e:
            logger.error(f"Failed to start bot: {e}", exc_info=True)
            self._bot = None
            self._dp = None
            return {"status": "error", "message": f"Ошибка при запуске: {e}"}

    def stop(self):
        if not self._is_running:
            return {"status": "error", "message": "Бот не запущен."}

        if not self._loop or not self._dp:
            return {"status": "error", "message": "Критическая ошибка: компоненты бота недоступны."}

        logger.info("BotController: Sending graceful stop signal...")
        asyncio.run_coroutine_threadsafe(self._dp.stop_polling(), self._loop)
        
        return {"status": "success", "message": "Команда на остановку бота отправлена."}

    def get_status(self):
        return {"is_running": self._is_running}