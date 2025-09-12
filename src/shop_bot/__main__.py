import logging
import threading
import asyncio
import signal
import sys
import os
import time

from shop_bot.webhook_server.app import create_webhook_app
from shop_bot.data_manager.scheduler import periodic_subscription_check
from shop_bot.data_manager import database
from shop_bot.bot_controller import BotController

def _is_anim_enabled() -> bool:
    try:
        if os.getenv("SHOPBOT_ANIM", "").strip() == "1":
            return True
        return sys.stdout.isatty()
    except Exception:
        return False

def _print_banner():
    if not _is_anim_enabled():
        return
    banner = (
        "\n"
        "  ______   __  __  _    _  ___       ____  _           _           _   \n"
        " |___  /  |  \\\u200b/  || |  | |/ _ \\     / ___|| |__   ___ | |__   ___ | |_ \n"
        "    / /   | |\\/| || |  | | | | |____\\\\___ \\| '_ \\ / _ \\| '_ \\ / _ \\| __|\n"
        "   / /__  | |  | || |__| | |_| |_____|__) | | | | (_) | |_) | (_) | |_ \n"
        "  /_____\\ |_|  |_| \\____/ \\___/     |____/|_| |_|\\___/|_.__/ \\___/ \\__|\n"
        "\n            3xui-ShopBot — запуск\n"
    )
    try:
        sys.stdout.write(banner)
        sys.stdout.flush()
    except Exception:
        pass

class Spinner:
    def __init__(self, message: str = "Загрузка..."):
        self.message = message
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._frames = ['|', '/', '-', '\\']

    def _run(self):
        if not _is_anim_enabled():
            return
        i = 0
        try:
            while not self._stop.is_set():
                frame = self._frames[i % len(self._frames)]
                sys.stdout.write(f"\r{self.message} {frame}")
                sys.stdout.flush()
                i += 1
                time.sleep(0.1)
        except Exception:
            pass
        finally:
            try:
                sys.stdout.write("\r" + " " * (len(self.message) + 4) + "\r")
                sys.stdout.flush()
            except Exception:
                pass

    def start(self):
        if _is_anim_enabled():
            self._thread.start()

    def stop(self):
        if _is_anim_enabled():
            self._stop.set()
            self._thread.join(timeout=1)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
    )
    # Replace default formatter with compact, colored formatter
    class ColorFormatter(logging.Formatter):
        COLORS = {
            'DEBUG': '\x1b[36m',    # cyan
            'INFO': '\x1b[32m',     # green
            'WARNING': '\x1b[33m',  # yellow
            'ERROR': '\x1b[31m',    # red
            'CRITICAL': '\x1b[41m', # red bg
            'RESET': '\x1b[0m',
            'DIM': '\x1b[2m'
        }

        def __init__(self, use_color: bool = True):
            super().__init__(datefmt='%H:%M:%S')
            self.use_color = use_color

        def format(self, record: logging.LogRecord) -> str:
            # short logger name (last two parts)
            name_parts = (record.name or '').split('.')
            short_name = '.'.join(name_parts[-2:]) if len(name_parts) > 1 else (name_parts[0] or '')
            level = record.levelname
            msg = record.getMessage()
            if self.use_color and _is_anim_enabled() or os.getenv('SHOPBOT_COLOR', '') == '1':
                color = self.COLORS.get(level, '')
                reset = self.COLORS['RESET']
                dim = self.COLORS['DIM']
                # 23:24:55 [INFO] short.name: message
                base = f"%(asctime)s {dim}[%(levelname)s]{reset} {short_name}: {color}%(message)s{reset}"
            else:
                base = "%(asctime)s [%(levelname)s] %s: %%(message)s" % short_name
            fmt = logging.Formatter(base, datefmt='%H:%M:%S')
            return fmt.format(record)

    class MigrationNoiseFilter(logging.Filter):
        """Фильтрует повторяющиеся/многословные сообщения миграций, чтобы логи были чище."""
        DROP_SUBSTRINGS = (
            " -> Колонка '",
            "уже существует",
            "Миграция таблицы '",
            "Создаю новую таблицу",
            "Таблица 'transactions' не найдена",
        )
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
                # Keep errors and warnings always
                if record.levelno >= logging.WARNING:
                    return True
                # Allow start/finish summary lines
                if (
                    "Запуск миграции базы данных" in msg
                    or "успешно завершена" in msg
                    or "успешно инициализирована" in msg
                    or "Таблица 'host_speedtests' готова" in msg
                ):
                    return True
                # Drop noisy per-column notices
                return not any(s in msg for s in self.DROP_SUBSTRINGS)
            except Exception:
                return True

    root = logging.getLogger()
    use_color = True
    cf = ColorFormatter(use_color=use_color)
    for h in root.handlers:
        h.setFormatter(cf)
    # Apply migration noise filter only to database logger
    logging.getLogger("shop_bot.data_manager.database").addFilter(MigrationNoiseFilter())
    # Reduce verbosity of third-party libraries while keeping our important logs
    logging.getLogger("werkzeug").setLevel(logging.WARNING)      # suppress HTTP access logs
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    # Keep only aiogram.event (handled updates) at INFO, reduce others
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("aiogram.event").setLevel(logging.INFO)
    logging.getLogger("aiogram.dispatcher").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)

    _print_banner()

    sp = Spinner("Инициализация базы данных")
    sp.start()
    database.initialize_db()
    sp.stop()
    logger.info("Проверка инициализации базы данных завершена.")

    bot_controller = BotController()
    flask_app = create_webhook_app(bot_controller)
    
    async def shutdown(sig: signal.Signals, loop: asyncio.AbstractEventLoop):
        logger.info(f"Получен сигнал: {sig.name}. Завершение работы...")
        if bot_controller.get_status()["is_running"]:
            bot_controller.stop()
            await asyncio.sleep(2)
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    async def start_services():
        loop = asyncio.get_running_loop()
        bot_controller.set_loop(loop)
        flask_app.config['EVENT_LOOP'] = loop
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda sig=sig: asyncio.create_task(shutdown(sig, loop)))
        
        sp2 = Spinner("Запуск веб-панели")
        sp2.start()
        flask_thread = threading.Thread(
            target=lambda: flask_app.run(host='0.0.0.0', port=1488, use_reloader=False, debug=False),
            daemon=True
        )
        flask_thread.start()
        # Дадим Flask время стартовать, чтобы анимация выглядела приятно
        await asyncio.sleep(1)
        sp2.stop()
        
        logger.info("Flask-сервер запущен в фоновом потоке на http://0.0.0.0:1488")
            
        logger.info("Приложение запущено. Бота можно стартовать из веб‑панели.")
        
        # Короткая анимация при запуске планировщика
        sp3 = Spinner("Запуск планировщика")
        sp3.start()
        asyncio.create_task(periodic_subscription_check(bot_controller))
        await asyncio.sleep(0.5)
        sp3.stop()

        await asyncio.Future()

    try:
        asyncio.run(start_services())
    finally:
        logger.info("Приложение завершает работу.")

if __name__ == "__main__":
    main()
