import subprocess
import json
import logging
import asyncio
import time
from typing import Dict, Optional
from datetime import datetime, timedelta
import os
import shutil

logger = logging.getLogger(__name__)

class SpeedTestManager:
    """Менеджер для измерения скорости интернета с помощью speedtest-cli"""
    
    def __init__(self):
        self.last_test_time = None
        self.last_test_results = None
        self.test_in_progress = False
        self.min_test_interval = 300  # Минимальный интервал между тестами (5 минут)
    
    def _resolve_speedtest_path(self) -> Optional[str]:
        """Ищет путь к бинарнику speedtest с приоритетами:
        1) ENV SPEEDTEST_BIN
        2) shutil.which('speedtest')
        3) стандартные пути: /usr/local/bin/speedtest, /usr/bin/speedtest
        """
        env_path = (os.getenv('SPEEDTEST_BIN') or '').strip()
        if env_path and os.path.isfile(env_path) and os.access(env_path, os.X_OK):
            return env_path
        which_path = shutil.which('speedtest')
        if which_path:
            return which_path
        for p in ('/usr/local/bin/speedtest', '/usr/bin/speedtest'):
            if os.path.isfile(p) and os.access(p, os.X_OK):
                return p
        return None

    def _check_speedtest_installed(self) -> bool:
        """Проверяет, установлен ли speedtest (Ookla CLI)"""
        path = self._resolve_speedtest_path()
        if not path:
            return False
        try:
            result = subprocess.run([path, '-V'], capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
    
    def _run_speedtest(self) -> Optional[Dict]:
        """Запускает тест скорости и возвращает результаты"""
        try:
            # Запускаем официальный speedtest-cli (Ookla) с JSON выводом
            # Требует акцепт лицензии и GDPR
            bin_path = self._resolve_speedtest_path()
            if not bin_path:
                logger.error("Speedtest (Ookla CLI) не найден в PATH")
                return None
            result = subprocess.run([
                bin_path,
                '--accept-license',
                '--accept-gdpr',
                '-f', 'json'
            ], capture_output=True, text=True, timeout=180)
            
            if result.returncode != 0:
                logger.error(f"Speedtest завершился с ошибкой: {result.stderr}")
                return None
            
            # Парсим JSON результат
            data = json.loads(result.stdout)
            
            # Извлекаем нужные данные из формата Ookla
            # bandwidth в байтах/секунду; конвертируем в Мбит/с: bandwidth * 8 / 1_000_000
            dl_bw = 0
            ul_bw = 0
            ping_ms = None
            isp = None
            external_ip = None
            server_name = None
            server_country = None
            timestamp = data.get('timestamp') or datetime.now().isoformat()

            try:
                dl_bw = int((data.get('download') or {}).get('bandwidth') or 0)
            except Exception:
                dl_bw = 0
            try:
                ul_bw = int((data.get('upload') or {}).get('bandwidth') or 0)
            except Exception:
                ul_bw = 0
            try:
                ping_ms = float((data.get('ping') or {}).get('latency') or 0)
            except Exception:
                ping_ms = 0.0

            try:
                isp = data.get('isp') or (data.get('ispName'))
            except Exception:
                isp = None
            try:
                external_ip = (data.get('interface') or {}).get('externalIp')
            except Exception:
                external_ip = None
            try:
                server = data.get('server') or {}
                server_name = server.get('name') or server.get('host')
                server_country = server.get('country') or server.get('location')
            except Exception:
                server_name = None
                server_country = None

            return {
                'timestamp': timestamp,
                'download_speed': round(dl_bw * 8 / 1_000_000, 2),  # Мбит/с
                'upload_speed': round(ul_bw * 8 / 1_000_000, 2),    # Мбит/с
                'ping': round(ping_ms or 0, 2),                     # мс
                'server_name': server_name or 'Unknown',
                'server_country': server_country or 'Unknown',
                'isp': isp or 'Unknown',
                'external_ip': external_ip or 'Unknown'
            }
            
        except subprocess.TimeoutExpired:
            logger.error("Таймаут при выполнении speedtest")
            return None
    
    def can_run_test(self) -> bool:
        """Проверяет, можно ли запустить новый тест"""
        if self.test_in_progress:
            return False
        
        if self.last_test_time is None:
            return True
        
        time_since_last = datetime.now() - self.last_test_time
        return time_since_last.total_seconds() >= self.min_test_interval
    
    def get_cached_results(self) -> Optional[Dict]:
        """Возвращает кэшированные результаты последнего теста"""
        return self.last_test_results
    
    def run_speed_test(self, force: bool = False) -> Optional[Dict]:
        """
        Запускает тест скорости
        
        Args:
            force: Принудительно запустить тест, игнорируя интервал
            
        Returns:
            Результаты теста или None в случае ошибки
        """
        if not force and not self.can_run_test():
            logger.info("Тест скорости пропущен: слишком рано или уже выполняется")
            return self.last_test_results
        
        if self.test_in_progress:
            logger.info("Тест скорости уже выполняется")
            return self.last_test_results
        
        self.test_in_progress = True
        
        try:
            # Проверяем, установлен ли speedtest
            if not self._check_speedtest_installed():
                logger.error("Speedtest (Ookla CLI) не найден в PATH")
                return None
            
            logger.info("Запуск теста скорости...")
            results = self._run_speedtest()
            
            if results:
                self.last_test_results = results
                self.last_test_time = datetime.now()
                logger.info(f"Тест скорости завершен: {results['download_speed']} Мбит/с ↓, "
                           f"{results['upload_speed']} Мбит/с ↑, {results['ping']} мс")
            
            return results
            
        finally:
            self.test_in_progress = False
    
    async def run_speed_test_async(self, force: bool = False) -> Optional[Dict]:
        """Асинхронная версия теста скорости"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run_speed_test, force)
    
    def get_test_status(self) -> Dict:
        """Возвращает статус тестирования"""
        return {
            'test_in_progress': self.test_in_progress,
            'last_test_time': self.last_test_time.isoformat() if self.last_test_time else None,
            'can_run_test': self.can_run_test(),
            'min_interval_seconds': self.min_test_interval
        }

# Глобальный экземпляр менеджера
speedtest_manager = SpeedTestManager()
