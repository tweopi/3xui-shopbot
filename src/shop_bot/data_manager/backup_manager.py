import logging
import shutil
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path

from aiogram import Bot
from aiogram.types import FSInputFile

from . import database

logger = logging.getLogger(__name__)

# Папка для хранения локальных архивов бэкапов
BACKUPS_DIR = Path("/app/project/backups")
BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

# Имя файла БД см. в database.DB_FILE
DB_FILE: Path = database.DB_FILE


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def create_backup_file() -> Path | None:
    """
    Создаёт zip-архив с консистентной копией SQLite-БД.
    Возвращает путь к архиву или None при ошибке.
    """
    try:
        if not DB_FILE.exists():
            logger.error(f"Backup: DB file not found: {DB_FILE}")
            return None
        ts = _timestamp()
        tmp_db_copy = BACKUPS_DIR / f"users-{ts}.db"
        zip_path = BACKUPS_DIR / f"db-backup-{ts}.zip"

        # Безопасное резервное копирование через SQLite backup API
        with sqlite3.connect(DB_FILE) as src:
            with sqlite3.connect(tmp_db_copy) as dst:
                src.backup(dst)

        # Упакуем в zip
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(tmp_db_copy, arcname=tmp_db_copy.name)

        # Удалим временную копию .db
        try:
            tmp_db_copy.unlink(missing_ok=True)
        except Exception:
            pass

        logger.info(f"Backup: created {zip_path}")
        return zip_path
    except Exception as e:
        logger.error(f"Backup: failed to create: {e}", exc_info=True)
        return None


def cleanup_old_backups(keep: int = 7) -> None:
    """Хранить только N последних архивов, остальные удалять."""
    try:
        files = sorted(BACKUPS_DIR.glob("db-backup-*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        for f in files[keep:]:
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Backup: cleanup failed: {e}")


async def send_backup_to_admins(bot: Bot, zip_path: Path) -> int:
    """
    Отправляет архив всем администраторам. Возвращает число успешных отправок.
    """
    cnt = 0
    try:
        try:
            from .database import get_admin_ids
            admin_ids = list(get_admin_ids() or [])
        except Exception:
            admin_ids = []
        if not admin_ids:
            logger.warning("Backup: no admin ids to send backup to")
            return 0
        caption = f"🗄 Бэкап БД: {zip_path.name}"
        file = FSInputFile(str(zip_path))
        for uid in admin_ids:
            try:
                await bot.send_document(chat_id=int(uid), document=file, caption=caption)
                cnt += 1
            except Exception as e:
                logger.error(f"Backup: failed to send to {uid}: {e}")
        return cnt
    except Exception as e:
        logger.error(f"Backup: send failed: {e}", exc_info=True)
        return cnt


def validate_db_file(db_path: Path) -> bool:
    """
    Простая валидация файла БД: доступность основных таблиц.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            # Проверим наличие таблиц, которые есть у нас всегда
            required_tables = {
                'users', 'vpn_keys', 'transactions', 'bot_settings', 'xui_hosts'
            }
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            present = {row[0] for row in cur.fetchall()}
            missing = required_tables - present
            if missing:
                logger.warning(f"Restore: missing tables in uploaded DB: {missing}")
            # Минимальная проверка: users и bot_settings должны быть
            return 'users' in present and 'bot_settings' in present
    except Exception as e:
        logger.error(f"Restore: validation error: {e}")
        return False


def restore_from_file(uploaded_path: Path) -> bool:
    """
    Восстанавливает основную БД из переданного файла .db или .zip (внутри .db).
    Делает резервную копию текущей БД на случай отката.
    """
    try:
        if not uploaded_path.exists():
            logger.error(f"Restore: file not found: {uploaded_path}")
            return False

        # Распакуем, если архив
        tmp_dir = BACKUPS_DIR / f"restore-{_timestamp()}"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        candidate_db: Path | None = None

        if uploaded_path.suffix.lower() == '.zip':
            try:
                with zipfile.ZipFile(uploaded_path, 'r') as zf:
                    for n in zf.namelist():
                        if n.lower().endswith('.db'):
                            zf.extract(n, path=tmp_dir)
                            candidate_db = tmp_dir / n
                            break
            except Exception as e:
                logger.error(f"Restore: zip extract failed: {e}")
                return False
        else:
            # Ожидаем, что это .db
            candidate_db = uploaded_path

        if not candidate_db or not candidate_db.exists():
            logger.error("Restore: no .db file found in upload")
            return False

        # Валидация
        if not validate_db_file(candidate_db):
            logger.error("Restore: validation failed")
            return False

        # Бэкап текущей БД
        backup_before = BACKUPS_DIR / f"before-restore-{_timestamp()}.zip"
        cur_backup = create_backup_file()
        if cur_backup and cur_backup.exists():
            try:
                shutil.copy(cur_backup, backup_before)
            except Exception:
                pass

        # Атомарная замена: используем SQLite backup API в обратную сторону
        with sqlite3.connect(candidate_db) as src:
            with sqlite3.connect(DB_FILE) as dst:
                src.backup(dst)
        
        # Миграции на всякий случай
        try:
            database.run_migration()
        except Exception:
            pass

        logger.info("Restore: database replaced successfully")
        return True
    except Exception as e:
        logger.error(f"Restore: failed: {e}", exc_info=True)
        return False
