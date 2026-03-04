from logging import exception
import re
import asyncio
import sqlite3
import os
from aiogram import Bot, types, Dispatcher, Router
from aiogram import F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from keyboards import hll, buy_final, sublim, help, prem, buyprem, buy_with_promo, buy_prem_with_promo
import requests
import hmac
import hashlib
import json
import aiohttp
from aiohttp import web
import random
import string
from aiogram.types import InputFile
from aiogram.types import InputMediaPhoto
from aiogram import filters
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter
from aiogram.filters import BaseFilter
from aiogram.filters import Command
import logging
import pytz
import datetime
import base64
from urllib.parse import parse_qsl
from html import escape
import shutil


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                # .env must be source of truth for this project deployment.
                os.environ[key] = value


def bootstrap_env() -> None:
    # 1) CWD .env (текущее поведение)
    load_env_file(".env")
    # 2) .env рядом с main.py (если сервис стартует из другого cwd)
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_env_path = os.path.join(script_dir, ".env")
        if os.path.exists(script_env_path):
            load_env_file(script_env_path)
    except Exception:
        pass


bootstrap_env()


def env_flag(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return bool(default)
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on", "y"}


API_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ADMIN_ID = 382257126
ADMIN_IDS = [OWNER_ADMIN_ID, 7545158182, 6342564240] # owner, support, nikita
GROUP_CHAT_ID = -1002812420141
GROUP_CHAT_ID2 = -1003001456496
REVIEWS_GROUP_CHAT_ID = -1005117261485
LEGACY_REVIEWS_GROUP_CHAT_IDS = {-1002506761866}
ALLOWED_REVIEWS_GROUP_CHAT_IDS = {int(REVIEWS_GROUP_CHAT_ID), *LEGACY_REVIEWS_GROUP_CHAT_IDS}
CHANNEL_ID = "@starslixx"

if not API_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()


PALLY_API_TOKEN = os.getenv("PALLY_API_TOKEN")      # legacy provider
PALLY_SHOP_ID = os.getenv("PALLY_SHOP_ID")          # legacy provider
PALLY_API_BASE = "https://pal24.pro/api/v1"         # legacy provider
PALLY_REDIRECT_SUCCESS = "https://t.me/RuTelegramStars_bot?start=payment_success"
PALLY_REDIRECT_FAIL = "https://t.me/RuTelegramStars_bot?start=payment_fail"

FK_API_BASE_URL = str(
    os.getenv("FK_API_BASE_URL")
    or os.getenv("FREEKASSA_API_BASE_URL")
    or "https://api.fk.life/v1"
).strip().rstrip("/")
FK_MERCHANT_ID = str(
    os.getenv("FK_MERCHANT_ID")
    or os.getenv("FREEKASSA_MERCHANT_ID")
    or ""
).strip()
FK_API_KEY = str(
    os.getenv("FK_API_KEY")
    or os.getenv("FREEKASSA_API_KEY")
    or ""
).strip()
FK_SECRET_WORD_2 = str(
    os.getenv("FK_SECRET_WORD_2")
    or os.getenv("FREEKASSA_SECRET_WORD_2")
    or ""
).strip()
FK_DEFAULT_PAYMENT_SYSTEM = str(
    os.getenv("FK_DEFAULT_PAYMENT_SYSTEM")
    or os.getenv("FREEKASSA_DEFAULT_PAYMENT_SYSTEM")
    or "44"
).strip()
FK_ORDER_IP_FALLBACK = str(
    os.getenv("FK_ORDER_IP_FALLBACK")
    or os.getenv("FREEKASSA_ORDER_IP_FALLBACK")
    or ""
).strip()
FK_SUCCESS_URL = str(
    os.getenv("FK_SUCCESS_URL")
    or os.getenv("FREEKASSA_SUCCESS_URL")
    or ""
).strip()
FK_FAIL_URL = str(
    os.getenv("FK_FAIL_URL")
    or os.getenv("FREEKASSA_FAIL_URL")
    or ""
).strip()
FK_WEBHOOK_STRICT_MERCHANT = env_flag("FK_WEBHOOK_STRICT_MERCHANT", default=True)
APP_TIMEZONE_NAME = str(os.getenv("APP_TIMEZONE") or "Europe/Simferopol").strip() or "Europe/Simferopol"


def _resolve_app_timezone(timezone_name: str):
    safe_name = str(timezone_name or "").strip() or "Europe/Simferopol"
    try:
        return pytz.timezone(safe_name)
    except Exception:
        try:
            return pytz.timezone("Europe/Simferopol")
        except Exception:
            return pytz.UTC


APP_TIMEZONE = _resolve_app_timezone(APP_TIMEZONE_NAME)


def app_now() -> datetime.datetime:
    return datetime.datetime.now(APP_TIMEZONE)


def app_now_str() -> str:
    return app_now().strftime("%Y-%m-%d %H:%M:%S")


def app_now_iso() -> str:
    return app_now().replace(microsecond=0).isoformat()


def app_today() -> datetime.date:
    return app_now().date()


def app_today_str() -> str:
    return app_today().strftime("%Y-%m-%d")


def app_datetime_threshold_str(*, days: int = 0, hours: int = 0) -> str:
    threshold = app_now() - datetime.timedelta(days=int(days or 0), hours=int(hours or 0))
    return threshold.strftime("%Y-%m-%d %H:%M:%S")


def to_app_timezone(dt_value: datetime.datetime) -> datetime.datetime:
    if not isinstance(dt_value, datetime.datetime):
        raise ValueError("Invalid datetime value")
    if dt_value.tzinfo is None:
        dt_value = pytz.UTC.localize(dt_value)
    return dt_value.astimezone(APP_TIMEZONE)

MINIAPP_API_HOST = os.getenv("MINIAPP_API_HOST", "0.0.0.0")
MINIAPP_API_PORT = int(os.getenv("MINIAPP_API_PORT", "8080"))
MINIAPP_USD_RUB_RATE = float(os.getenv("MINIAPP_USD_RUB_RATE", "76.5"))
SPEND_SERVICE_BASE_URL = str(
    os.getenv("SPEND_SERVICE_BASE_URL")
    or os.getenv("SPEND_BASE_URL")
    or ""
).strip().rstrip("/")
SPEND_SERVICE_TOKEN = str(
    os.getenv("SPEND_SERVICE_TOKEN")
    or os.getenv("SPEND_TOKEN")
    or ""
).strip()
try:
    SPEND_SERVICE_FEE_PERCENT = float(os.getenv("SPEND_SERVICE_FEE_PERCENT", "2"))
except (TypeError, ValueError):
    SPEND_SERVICE_FEE_PERCENT = 2.0
try:
    SPEND_SERVICE_STARS_SAMPLE_AMOUNT = int(os.getenv("SPEND_SERVICE_STARS_SAMPLE_AMOUNT", "100"))
except (TypeError, ValueError):
    SPEND_SERVICE_STARS_SAMPLE_AMOUNT = 100
try:
    SPEND_SERVICE_TIMEOUT_SECONDS = float(os.getenv("SPEND_SERVICE_TIMEOUT_SECONDS", "8"))
except (TypeError, ValueError):
    SPEND_SERVICE_TIMEOUT_SECONDS = 8.0
FRAGMENT_API_BASE_URL = str(
    os.getenv("FRAGMENT_API_BASE_URL")
    or os.getenv("FRAGMENT_BASE_URL")
    or "https://api.fragment-api.net"
).strip().rstrip("/")
FRAGMENT_API_KEY = str(
    os.getenv("FRAGMENT_API_KEY")
    or os.getenv("FRAGMENT_KEY")
    or ""
).strip()
FRAGMENT_API_SEED = str(
    os.getenv("FRAGMENT_API_SEED")
    or os.getenv("FRAGMENT_SEED")
    or ""
).strip()
FRAGMENT_API_COOKIES = str(
    os.getenv("FRAGMENT_API_COOKIES")
    or os.getenv("FRAGMENT_COOKIES")
    or ""
).strip()
FRAGMENT_API_USE_KYC = env_flag("FRAGMENT_API_USE_KYC", default=False)
FRAGMENT_API_SHOW_SENDER = env_flag("FRAGMENT_API_SHOW_SENDER", default=False)
FRAGMENT_API_AUTO_FULFILL = env_flag("FRAGMENT_API_AUTO_FULFILL", default=True)
try:
    FRAGMENT_API_TIMEOUT_SECONDS = float(os.getenv("FRAGMENT_API_TIMEOUT_SECONDS", "25"))
except (TypeError, ValueError):
    FRAGMENT_API_TIMEOUT_SECONDS = 25.0
PREMIUM_PRICES_RUB = {
    3: 1190,
    6: 1490,
    12: 2690,
}

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "database.db")
MINIAPP_EVENT_SUBSCRIBERS = {}
MINIAPP_EVENT_SUBSCRIBER_SEQ = 0
MINIAPP_EVENT_LOCK = asyncio.Lock()
FK_LAST_NONCE = 0


def _safe_table_count(db_path: str, table_name: str) -> int:
    if not db_path or not os.path.exists(db_path):
        return 0
    conn_local = None
    try:
        conn_local = sqlite3.connect(db_path)
        cur_local = conn_local.cursor()
        cur_local.execute(f"SELECT COUNT(*) FROM {table_name}")
        row = cur_local.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0
    finally:
        if conn_local is not None:
            conn_local.close()


def _score_database(db_path: str) -> int:
    if not db_path or not os.path.exists(db_path):
        return 0
    score = 0
    score += _safe_table_count(db_path, "users")
    score += _safe_table_count(db_path, "purchases")
    score += _safe_table_count(db_path, "miniapp_purchase_history")
    score += _safe_table_count(db_path, "payments")
    if os.path.getsize(db_path) > 0:
        score += 1
    return score


def _ensure_parent_directory(path: str) -> None:
    parent_path = os.path.dirname(os.path.abspath(path))
    if parent_path:
        os.makedirs(parent_path, exist_ok=True)


def _copy_database_if_richer(source_path: str, target_path: str) -> bool:
    source_abs = os.path.abspath(source_path)
    target_abs = os.path.abspath(target_path)
    if source_abs == target_abs:
        return False
    if not os.path.exists(source_abs):
        return False

    source_score = _score_database(source_abs)
    target_score = _score_database(target_abs)
    if source_score <= target_score:
        return False

    try:
        _ensure_parent_directory(target_abs)
        shutil.copy2(source_abs, target_abs)
        return True
    except Exception as error:
        logging.warning("Failed to migrate database from %s to %s: %s", source_abs, target_abs, error)
        return False


def resolve_database_path() -> str:
    env_db_path = str(os.getenv("DATABASE_PATH") or os.getenv("DB_PATH") or "").strip()
    default_path = os.path.abspath(DEFAULT_DB_PATH)
    cwd_path = os.path.abspath("database.db")
    local_candidates = [default_path, cwd_path]

    if env_db_path:
        configured_path = os.path.abspath(env_db_path)
        for source_path in local_candidates:
            if _copy_database_if_richer(source_path, configured_path):
                break
        return configured_path

    # Production-safe default: keep DB outside repo, so `git stash -u`/`git pull`
    # never hide or replace runtime data.
    if os.name != "nt" and os.path.abspath(BASE_DIR).startswith("/opt/"):
        persistent_path = os.path.abspath("/opt/starslix-data/database.db")
        for source_path in local_candidates:
            if _copy_database_if_richer(source_path, persistent_path):
                break
        if os.path.exists(persistent_path):
            return persistent_path

    if cwd_path == default_path:
        return default_path

    default_score = _score_database(default_path)
    cwd_score = _score_database(cwd_path)

    # Миграция: если в CWD-версии данных больше, копируем её в проектный путь.
    if cwd_score > default_score and cwd_score > 0 and os.path.exists(cwd_path):
        try:
            shutil.copy2(cwd_path, default_path)
            return default_path
        except Exception as error:
            logging.warning("Failed to migrate legacy database from cwd path: %s", error)
            return cwd_path

    if os.path.exists(default_path):
        return default_path
    if os.path.exists(cwd_path):
        return cwd_path
    return default_path


DATABASE_PATH = resolve_database_path()


def _get_table_columns(table_name: str, db_cursor=None) -> set:
    active_cursor = db_cursor or cursor
    try:
        active_cursor.execute(f"PRAGMA table_info({table_name})")
        return {str(row[1]).strip() for row in (active_cursor.fetchall() or []) if len(row) > 1}
    except Exception:
        return set()


def _ensure_miniapp_purchase_history_schema(db_cursor=None) -> tuple:
    active_cursor = db_cursor or cursor
    status_column_added = False
    counters_column_added = False
    existing_columns = _get_table_columns("miniapp_purchase_history", active_cursor)
    history_column_migrations = [
        ("user_id", "ALTER TABLE miniapp_purchase_history ADD COLUMN user_id INTEGER DEFAULT 0"),
        ("buyer_username", "ALTER TABLE miniapp_purchase_history ADD COLUMN buyer_username TEXT DEFAULT ''"),
        ("buyer_first_name", "ALTER TABLE miniapp_purchase_history ADD COLUMN buyer_first_name TEXT DEFAULT ''"),
        ("buyer_last_name", "ALTER TABLE miniapp_purchase_history ADD COLUMN buyer_last_name TEXT DEFAULT ''"),
        ("target_username", "ALTER TABLE miniapp_purchase_history ADD COLUMN target_username TEXT DEFAULT ''"),
        ("item_type", "ALTER TABLE miniapp_purchase_history ADD COLUMN item_type TEXT DEFAULT ''"),
        ("amount", "ALTER TABLE miniapp_purchase_history ADD COLUMN amount TEXT DEFAULT ''"),
        ("price_rub", "ALTER TABLE miniapp_purchase_history ADD COLUMN price_rub REAL DEFAULT 0"),
        ("price_usd", "ALTER TABLE miniapp_purchase_history ADD COLUMN price_usd REAL DEFAULT 0"),
        ("promo_code", "ALTER TABLE miniapp_purchase_history ADD COLUMN promo_code TEXT DEFAULT ''"),
        ("promo_discount", "ALTER TABLE miniapp_purchase_history ADD COLUMN promo_discount INTEGER DEFAULT 0"),
        ("promo_error", "ALTER TABLE miniapp_purchase_history ADD COLUMN promo_error TEXT DEFAULT ''"),
        ("source", "ALTER TABLE miniapp_purchase_history ADD COLUMN source TEXT DEFAULT 'api'"),
        ("status", "ALTER TABLE miniapp_purchase_history ADD COLUMN status TEXT DEFAULT 'pending'"),
        ("counters_applied", "ALTER TABLE miniapp_purchase_history ADD COLUMN counters_applied INTEGER DEFAULT 0"),
        ("created_at", "ALTER TABLE miniapp_purchase_history ADD COLUMN created_at TEXT DEFAULT ''"),
    ]

    for column_name, statement in history_column_migrations:
        if column_name in existing_columns:
            continue
        try:
            active_cursor.execute(statement)
            existing_columns.add(column_name)
            if column_name == "status":
                status_column_added = True
            elif column_name == "counters_applied":
                counters_column_added = True
        except sqlite3.OperationalError:
            pass

    return status_column_added, counters_column_added


def init_db():
    global conn, cursor
    _ensure_parent_directory(DATABASE_PATH)
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS total_stars (
        id INTEGER PRIMARY KEY,
        total INTEGER DEFAULT 21670
    )
    """)

    cursor.execute("""
        INSERT OR IGNORE INTO total_stars (id, total)
        VALUES (1, 71670);
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS total_premium_months (
        id INTEGER PRIMARY KEY,
        total INTEGER DEFAULT 0
    )
    """)

    # Создание новой таблицы с нужной структурой
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            user_id INTEGER,
            bill_id TEXT PRIMARY KEY,
            amount REAL,
            status TEXT,
            description TEXT,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS users (
               user_id INTEGER PRIMARY KEY,
               username TEXT,
               first_name TEXT,
               last_name TEXT,
               inviter_id INTEGER,
               ref_code TEXT UNIQUE,
               referred_by INTEGER,
               referrals_count INTEGER DEFAULT 0,
               referrals_with_purchase INTEGER DEFAULT 0,
               reward_claimed INTEGER DEFAULT 0
           )
       ''')

    # Таблица реферальных кодов
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS ref_codes (
               code TEXT PRIMARY KEY,
               user_id INTEGER,
               ref_count INTEGER DEFAULT 0
           )
       ''')

    # Таблица продуктов
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS products (
               product_id INTEGER PRIMARY KEY AUTOINCREMENT,
               name TEXT,
               description TEXT,
               price INTEGER
           )
       ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            discount_percent INTEGER,
            min_stars INTEGER,
            expires_at TEXT,
            max_uses INTEGER,
            uses_count INTEGER DEFAULT 0,
            condition TEXT DEFAULT 'all',
            max_free_stars INTEGER DEFAULT 0,
            target TEXT DEFAULT 'stars'
        )
    ''')

    # Добавляем новый столбец, если его нет
    try:
        cursor.execute("ALTER TABLE promo_codes ADD COLUMN max_uses_per_user INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass  # уже добавлен
    try:
        cursor.execute("ALTER TABLE promo_codes ADD COLUMN effect_type TEXT DEFAULT 'discount_percent'")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE promo_codes ADD COLUMN effect_value INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    # Таблица использованных промокодов
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS used_promo (
               user_id INTEGER,
               code TEXT,
               PRIMARY KEY (user_id, code)
           )
       ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS promo_usages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_promo_usages_user_code
        ON promo_usages (user_id, code)
    ''')

    # Таблица покупок
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS purchases (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               user_id INTEGER,
               username TEXT,
               item_type TEXT, -- "stars" или "premium"
               amount INTEGER, -- число звёзд или месяцев премиума
               cost REAL,      -- сколько заплатил пользователь
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )
       ''')
    cursor.execute(
        '''
        INSERT OR IGNORE INTO total_premium_months (id, total)
        SELECT 1, COALESCE(SUM(CASE WHEN item_type='premium' THEN amount ELSE 0 END), 0)
        FROM purchases
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS miniapp_purchase_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            buyer_username TEXT,
            buyer_first_name TEXT,
            buyer_last_name TEXT,
            target_username TEXT NOT NULL,
            item_type TEXT NOT NULL,
            amount TEXT NOT NULL,
            price_rub REAL NOT NULL,
            price_usd REAL NOT NULL,
            promo_code TEXT,
            promo_discount INTEGER DEFAULT 0,
            promo_error TEXT,
            status TEXT DEFAULT 'pending',
            counters_applied INTEGER DEFAULT 0,
            source TEXT DEFAULT 'api',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_miniapp_purchase_history_user_created
        ON miniapp_purchase_history (user_id, created_at DESC)
        '''
    )
    history_status_column_added, history_counters_column_added = (
        _ensure_miniapp_purchase_history_schema()
    )
    if history_status_column_added:
        cursor.execute(
            """
            UPDATE miniapp_purchase_history
            SET status='success'
            WHERE status IS NULL OR TRIM(status)='' OR LOWER(TRIM(status))='pending'
            """
        )
    if history_counters_column_added:
        # Исторически счётчики обновлялись на этапе создания заявки,
        # поэтому старые записи считаем уже учтёнными.
        cursor.execute("UPDATE miniapp_purchase_history SET counters_applied = 1")
    cursor.execute("INSERT OR IGNORE INTO total_premium_months (id, total) VALUES (1, 0)")
    cursor.execute(
        """
        UPDATE total_premium_months
        SET total = (
            COALESCE((SELECT SUM(CASE WHEN item_type='premium' THEN amount ELSE 0 END) FROM purchases), 0) +
            COALESCE((SELECT SUM(CASE WHEN LOWER(item_type)='premium' THEN CAST(amount AS INTEGER) ELSE 0 END) FROM miniapp_purchase_history), 0)
        )
        WHERE id = 1 AND total <= 0
        """
    )
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS admin_action_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER NOT NULL,
            admin_username TEXT,
            action TEXT NOT NULL,
            payload TEXT,
            status TEXT NOT NULL DEFAULT 'success',
            error_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_admin_action_logs_created
        ON admin_action_logs (created_at DESC, id DESC)
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS miniapp_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            reviewer_user_id INTEGER NOT NULL,
            reviewer_username TEXT,
            reviewer_first_name TEXT,
            reviewer_last_name TEXT,
            review_text TEXT NOT NULL,
            avatar_file_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chat_id, message_id)
        )
        '''
    )
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_miniapp_reviews_created
        ON miniapp_reviews (created_at DESC, id DESC)
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS miniapp_support_chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            title TEXT DEFAULT '',
            user_unread_count INTEGER DEFAULT 0,
            admins_unread_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_miniapp_support_chats_user
        ON miniapp_support_chats (user_id, updated_at DESC, id DESC)
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS miniapp_support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            sender_role TEXT NOT NULL,
            sender_username TEXT,
            sender_full_name TEXT,
            text TEXT DEFAULT '',
            photo_blob BLOB,
            photo_mime TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_miniapp_support_messages_chat
        ON miniapp_support_messages (chat_id, id DESC)
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS miniapp_support_admin_notices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            admin_user_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chat_id, admin_user_id)
        )
        '''
    )
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_miniapp_support_admin_notices_admin
        ON miniapp_support_admin_notices (admin_user_id, chat_id DESC)
        '''
    )
    conn.commit()

    cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('star_cost', '1.33')")
    conn.commit()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS star_rates (
            range_name TEXT PRIMARY KEY,
            rate REAL
        )
    ''')

    cursor.executemany('''
        INSERT OR IGNORE INTO star_rates (range_name, rate) VALUES (?, ?)
    ''', [
        ('50_75', 1.7),
        ('76_100', 1.6),
        ('101_250', 1.55),
        ('251_plus', 1.5)
    ])
    conn.commit()
    _load_admin_ids_from_settings()


def _normalize_admin_ids(raw_values) -> list:
    normalized = []
    seen = set()
    for raw_value in raw_values or []:
        try:
            admin_id = int(raw_value)
        except (TypeError, ValueError):
            continue
        if admin_id <= 0 or admin_id in seen:
            continue
        seen.add(admin_id)
        normalized.append(admin_id)
    return normalized


def _save_admin_ids_to_settings() -> None:
    global ADMIN_IDS
    normalized = _normalize_admin_ids(ADMIN_IDS)
    if OWNER_ADMIN_ID not in normalized:
        normalized.insert(0, OWNER_ADMIN_ID)
    ADMIN_IDS = normalized
    cursor.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        ("admin_ids", json.dumps(ADMIN_IDS)),
    )
    conn.commit()


def _load_admin_ids_from_settings() -> None:
    global ADMIN_IDS
    cursor.execute("SELECT value FROM settings WHERE key='admin_ids'")
    row = cursor.fetchone()
    if row and row[0]:
        try:
            ADMIN_IDS = _normalize_admin_ids(json.loads(row[0]))
        except Exception:
            ADMIN_IDS = _normalize_admin_ids(ADMIN_IDS)
    else:
        ADMIN_IDS = _normalize_admin_ids(ADMIN_IDS)

    if OWNER_ADMIN_ID not in ADMIN_IDS:
        ADMIN_IDS.insert(0, OWNER_ADMIN_ID)
    _save_admin_ids_to_settings()


def get_star_keyboard():
    rates = get_all_star_rates()

    stars = [50, 75, 100, 150, 250, 350, 500, 750, 1000, 1500, 2500, 5000, 10000]

    def calc_price(stars_count):
        multiplier = get_star_rate_for_range(stars_count)
        return round(stars_count * multiplier, 2)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести количество звёзд", callback_data="buy_custom")]
    ])

    for i in range(0, len(stars), 2):
        row = []
        for s in stars[i:i + 2]:
            row.append(InlineKeyboardButton(text=f"⭐️{s} | {calc_price(s)}₽", callback_data=f"{s}stars"))
        keyboard.inline_keyboard.append(row)

    keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️Назад", callback_data="back_first")])
    return keyboard
#

#conn = sqlite3.connect('database.db')
#cursor = conn.cursor()

# Выполнение SQL-запроса для получения всех данных из таблицы users
#cursor.execute('SELECT * FROM users')
#rows = cursor.fetchall()

# Вывод результата в консоль
#for row in rows:
#    print(row)

# Закрытие соединения
#conn.close()

# Глобльные словари для хранения состояния и платежей
user_payments = {}  # user_id: amount
user_states = {}  # user_id: {'awaiting_stars': bool}
# В начале файла или перед функцией, где используется
user_purchase_data = {}
user_premium_data = {}

async def check_subscription(user_id):
    # Проверка подписки через get_chat_member
    chat_member = await bot.get_chat_member(CHANNEL_ID, user_id)
    return chat_member.status in ['member', 'administrator', 'creator']

def generate_ref_code(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


@dp.message(CommandStart())
async def handle_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Пользователь"
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    photo_url = "https://ibb.co/XrPBvfbS"           #https://ibb.co/sdvyxnmq

    # Получаем текст и реферальный код, если есть
    text = message.text or ""
    parts = text.split(maxsplit=1)
    referral_code = parts[1].strip() if len(parts) > 1 else None

    # Проверка существования пользователя
    cursor.execute('SELECT user_id, ref_code FROM users WHERE user_id=?', (user_id,))
    user_record = cursor.fetchone()
    if user_record:
        ref_code = user_record[1] or generate_ref_code()
        if not user_record[1]:
            cursor.execute('UPDATE users SET ref_code=? WHERE user_id=?', (ref_code, user_id))
            conn.commit()
    else:
        ref_code = generate_ref_code()
        cursor.execute('''
            INSERT INTO users (user_id, username, first_name, last_name, ref_code)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, last_name, ref_code))
        conn.commit()

    # Обработка реферала
    if referral_code:
        cursor.execute('SELECT user_id FROM users WHERE ref_code=?', (referral_code,))
        inviter = cursor.fetchone()
        if inviter:
            inviter_id = inviter[0]
            cursor.execute('SELECT referred_by FROM users WHERE user_id=?', (user_id,))
            invited_by_record = cursor.fetchone()
            if not invited_by_record or not invited_by_record[0]:
                cursor.execute('''
                    UPDATE users SET referred_by=?, invited_by=?
                    WHERE user_id=?
                ''', (inviter_id, inviter_id, user_id))
                cursor.execute('UPDATE users SET referrals_count=referrals_count+1 WHERE user_id=?', (inviter_id,))
                conn.commit()

    # Читаем общее количество проданных звёзд из таблицы total_stars (чтобы совпадало с админкой)
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    total_stars = row[0] if (row and row[0] is not None) else 0
    approx_usd = total_stars * 0.013  # примерный курс доллара
    stars_info = f"<b>У нас уже купили:</b> {total_stars:,}⭐️ (~${approx_usd:.2f})".replace(",", " ")

    # --- Текст 1: новый пользователь с рефералом ---
    if referral_code and inviter:
        text1 = (
            f"<b>Добро пожаловать в STARSLIX!</b>\n\n"
            f"<b>Привет, {username}!</b>\n"
            f"<b>{stars_info}</b>\n"
            "<b>Покупай звёзды и Premium, дари подарки, сияй ярче всех!</b>\n\n"
            "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>Соглашение</a></b> | "
            "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>Политика</a></b>\n"
            "<b>Время работы: 06:00-00:00 (МСК)</b>"
        )
        await message.answer_photo(photo=photo_url, caption=text1, reply_markup=hll, parse_mode='HTML')
        return

    # --- Текст 2: уже был зарегистрирован ---
    if user_record:
        text2 = (
            f"<b>Добро пожаловать в STARSLIX!</b>\n\n"
            f"<b>Привет, {username}!</b>\n"
            f"<b>{stars_info}</b>\n"
            "<b>Покупай звёзды и Premium, дари подарки, сияй ярче всех!</b>\n\n"
            "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>Соглашение</a></b> | "
            "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>Политика</a></b>\n"
            "<b>Время работы: 06:00-00:00 (МСК)</b>"
        )
        await message.answer_photo(photo=photo_url, caption=text2, reply_markup=hll, parse_mode='HTML')
        return

    # --- Текст 3: обычный старт без реферала ---
    text3 = (
        f"<b>Добро пожаловать в STARSLIX!</b>\n\n"
        f"<b>Привет, {username}!</b>\n"
        f"<b>{stars_info}</b>\n"
        "<b>Покупай звёзды и Premium, дари подарки, сияй ярче всех!</b>\n\n"
        "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>Соглашение</a></b> | "
        "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>Политика</a></b>\n"
        "<b>Время работы: 06:00-00:00 (МСК)</b>"
    )
    await message.answer_photo(photo=photo_url, caption=text3, reply_markup=hll, parse_mode='HTML')




@dp.callback_query(lambda c: c.data == "check_subscription")
async def handle_check_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("Ошибка при проверке подписки. Попробуйте позже.", show_alert=True)
        return
    if is_subscribed:
        await callback.answer("Спасибо! Вы успешно подписались.\U00002764", show_alert=True)
        await ref_system(callback)
    else:
        await callback.answer("Вы еще не подписались на канал.\U0001F61E", show_alert=True)

@dp.callback_query(lambda c: c.data == "ref_system")
async def ref_system(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    # Проверка подписки
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # Обработка ошибок (например, если бот не в состоянии проверить)
        await callback.message.answer("Ошибка проверки подписки. Попробуйте позже.")
        return
    if not is_subscribed:
        # Пользователь не подписан — удаляем сообщение и просим подписаться
        await callback.message.delete()

        # Отправляем сообщение с кнопкой "Подписаться" и "Проверить"
        subscribe_button = InlineKeyboardButton(text="Подписаться", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="✅ Готово", callback_data="check_subscription")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "Пожалуйста, подпишитесь на наш канал, чтобы продолжить.",
            reply_markup=keyboard
        )
        return


    # Получаем user's ref_code
    cursor.execute(
        'SELECT ref_code FROM users WHERE user_id=?',
        (user_id,)
    )
    row = cursor.fetchone()

    # Initialize placeholders for message rendering
    message_text = ""
    photo_url = ""
    keyboard = None

    if row and row[0]:
        ref_code = row[0]

        # Подсчитываем общее количество рефералов
        cursor.execute(
            'SELECT COUNT(*) FROM users WHERE referred_by=?',
            (user_id,)
        )
        total_referrals = cursor.fetchone()[0]

        # Подсчитываем активных рефералов
        cursor.execute(
            'SELECT COUNT(*) FROM users WHERE referred_by=? AND referrals_with_purchase=1',
            (user_id,)
        )
        active_referrals_count = cursor.fetchone()[0]

        ref_link = f"https://t.me/starslixbot?start={ref_code}"

        message_text = (
            f"👥 <b>Зарабатывай звёзды с друзьями!</b>\n\n"
            f"Приглашай друзей в наш бот и получай щедрые бонусы! Это просто!\n\n"
            f"🎁<b> Условия реферальной системы:</b>\n"
            f"За каждые <b>10</b> активных рефералов (тех, кто купил звёзды через твою ссылку), <b>ты получаешь награду в 100 звёзд на свой счёт!</b>\n\n"
            f"<b>Как это работает:</b>\n"
            f"1. Делишься своей уникальной реферальной ссылкой с друзьями.\n"
            f"2. Твои друзья переходят по ссылке и покупают звёзды.\n"
            f"3. Как только у тебя набирается 10 активных рефералов, нажми кнопку 'За подарком'.\n"
            f"4. Мы проверяем активность и мгновенно начисляем тебе 100 звёзд!\n\n"
            f"<b>Твоя персональная ссылка:</b>\n<b><code>{ref_link}</code></b>\n\n"
            f"<b>Статистика:</b>\n📊 <b>Твоих рефералов:</b> {total_referrals}\n🎁 <b>Готово к награде:</b> {active_referrals_count}/10"
        )

        photo_url = "https://ibb.co/XrPBvfbS"

        keyboard_buttons = [
            [InlineKeyboardButton(text="Мои активные рефералы", callback_data="show_referrals")]
        ]

        if active_referrals_count >= 10:
            keyboard_buttons.append([InlineKeyboardButton(text="За подарком", callback_data="apply")])

        keyboard_buttons.append([InlineKeyboardButton(text="⬅️Назад", callback_data="back_first")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

    else:
        message_text = "Профиль не найден или у вас нет реферального кода."
        photo_url = "https://ibb.co/XrPBvfbS"

        # Создаем клавиатуру с кнопками
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Мои рефералы", callback_data="show_referrals")],
            [InlineKeyboardButton(text="⬅️Назад", callback_data="back_start")]
        ])

    # Создаем InputMediaPhoto с URL
    media = InputMediaPhoto(
        media=photo_url,
        caption=message_text,
        parse_mode='HTML'
    )

    # Обновляем сообщение с фото и клавиатурой
    await callback.message.edit_media(media)
    await callback.message.edit_reply_markup(reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "show_referrals")
async def show_referrals(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    # Query only active referrals (referrals_with_purchase=1)
    cursor.execute(
        'SELECT username FROM users WHERE referred_by=? AND user_id != ? AND referrals_with_purchase=1',
        (user_id, user_id)
    )
    rows = cursor.fetchall()

    if rows:
        usernames_list = [row[0] for row in rows if row[0]]
        if usernames_list:
            usernames = "\n".join(f"@{username}" for username in usernames_list)
            message_text = f"Ваши активные рефералы:\n{usernames}"
        else:
            message_text = "У вас пока нет активных рефералов."
    else:
        message_text = "У вас пока нет активных рефералов."

    await callback.message.answer(message_text)

@dp.callback_query(lambda c: c.data == "check_subscriptionstar")
async def handle_check_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("Ошибка при проверке подписки. Попробуйте позже.", show_alert=True)
        return

    if is_subscribed:
        await callback.answer("Спасибо! Вы успешно подписались.\U00002764", show_alert=True)
        # Продолжайте выполнение логики
        await buy_stars_callback(callback)
    else:
        await callback.answer("Вы еще не подписались на канал.\U0001F61E", show_alert=True)
# Обработка нажатия "купить звезды"

@dp.callback_query(lambda c: c.data == "apply")
async def handle_apply(callback: types.CallbackQuery):
    # Получаем username пользователя, который нажал кнопку
    username = callback.from_user.username
    user_id = callback.from_user.id
    # Обновляем состояние пользователя
    user_states[user_id] = {'waiting_username_apply': True}
    # Удаляем сообщение с кнопкой
    await callback.message.delete()
    # Отправляем сообщение пользователю
    await callback.message.answer("Куда отправить звезды? Введите username получателя (например, @username).")




@dp.message(Command("stats"))
async def admin_commands(message: types.Message):
    user_id = message.from_user.id

    if user_id not in ADMIN_IDS:
        return

    # Считаем количество пользователей в БД
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    await message.answer(f"👥 Всего пользователей: <b>{total_users}</b>", parse_mode="HTML")



@dp.message(lambda message: message.text and (message.text.startswith("ref_") or message.text.startswith("delref_")))
async def admin_commands(message: types.Message):
    user_id = message.from_user.id


    if user_id not in ADMIN_IDS:
        await message.answer("У вас нет доступа к этой команде.")

        return

    command = message.text.strip()


    if command.startswith("ref_"):
        target_id_str = command[4:]
        action = "ref"

    elif command.startswith("delref_"):
        target_id_str = command[7:]
        action = "delref"

    else:
        await message.answer("Некорректная команда.")

        return

    if not target_id_str.isdigit():
        await message.answer("Некорректный ID пользователя.")

        return

    target_id = int(target_id_str)


    if action == "ref":

        cursor.execute(
            'SELECT username FROM users WHERE referred_by=? AND user_id != ? AND referrals_with_purchase=1',
            (target_id, target_id)
        )
        rows = cursor.fetchall()


        if rows:
            usernames_list = [row[0] for row in rows if row[0]]

            if usernames_list:
                usernames = "\n".join(f"@{username}" for username in usernames_list)
                await message.answer(f"Активные рефералы пользователя {target_id}:\n{usernames}")
            else:
                await message.answer(f"У пользователя {target_id} пока нет активных рефералов.")
        else:
            await message.answer(f"У пользователя {target_id} пока нет активных рефералов.")
    elif action == "delref":

        # Обновляем статус активных рефералов на неактивных
        cursor.execute(
                'UPDATE users SET referrals_with_purchase=0 WHERE referred_by=? AND referrals_with_purchase=1',
                (target_id,)
        )
        conn.commit()

        await message.answer(f"Все активные рефералы пользователя {target_id} успешно сброшены.")


@dp.callback_query(lambda c: c.data == "check_subscriptionprem")
async def handle_check_subscriptionprem(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("Ошибка при проверке подписки. Попробуйте позже.", show_alert=True)
        return

    if is_subscribed:
        await callback.answer("Спасибо! Вы успешно подписались.\U00002764", show_alert=True)
        # Продолжайте выполнение логики
        await premium_handler(callback)
    else:
        await callback.answer("Вы еще не подписались на канал.\U0001F61E", show_alert=True)


@dp.callback_query(lambda c: c.data == "premium")
async def premium_handler(callback: types.CallbackQuery):
    # Проверка подписки
    user_id = callback.from_user.id
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # Обработка ошибок (например, если бот не в состоянии проверить)
        await callback.message.answer("Ошибка проверки подписки. Попробуйте позже.")
        return

    if not is_subscribed:
        # Пользователь не подписан — удаляем сообщение и просим подписаться
        await callback.message.delete()

        # Отправляем сообщение с кнопкой "Подписаться" и "Проверить"
        subscribe_button = InlineKeyboardButton(text="Подписаться", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="✅ Готово", callback_data="check_subscriptionprem")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "Пожалуйста, подпишитесь на наш канал, чтобы продолжить.",
            reply_markup=keyboard
        )
        return



    photo_url = "https://ibb.co/MyFDq6zx"  # замените на нужную ссылку
    caption = (
        f"<b>💙Telegram Premium💙</b>\n\n"
        f"<b>Выберите срок подписки:</b>"
    )
    await callback.message.edit_media(
        media=InputMediaPhoto(media=photo_url, caption=caption, parse_mode="HTML"),
        reply_markup=prem
    )

@dp.callback_query(lambda c: c.data in ['1190', '1490', '2690'])
async def premium_months_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    price = int(callback.data)
    months_map = {1190: 3, 1490: 6, 2690: 12}
    months = months_map.get(price, 0)

    if months == 0:
        await callback.answer("Ошибка выбора срока подписки.", show_alert=True)
        return

    # Сохраняем данные о покупке Premium
    user_premium_data[user_id] = {
        'months': months,
        'price': price,
        'username': None
    }

    # Просим пользователя ввести username
    photo_url = "https://ibb.co/MyFDq6zx"
    caption = (
        "📛 <b>Теперь укажи username Telegram-аккаунта, куда нужно приобрести Premium.</b>\n\n"
        "Важно:\n"
        "• <b>Убедись</b>, что твой username корректно указан (например, `@example`).\n"
        "• <b>Если у тебя нет username</b>, его нужно создать в настройках Telegram.\n"
        "• Premium будет зачислен <b>в течение 5-ти минут</b> после оплаты"
    )

    await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=caption, parse_mode='HTML')
    )

    user_states[user_id] = {'awaiting_premium_username': True}
    await callback.answer()


# Генерация уникального номера платежа
def generate_unique_label_prem(user_id):
    now = datetime.datetime.now()
    return f"{now.year:02d}{now.month:02d}{now.day:02d}{now.hour:02d}{now.minute:02d}{now.second:02d}{user_id}"


@dp.callback_query(lambda c: c.data == "pay_prem")
async def pay_pally_prem_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in user_premium_data:
        await callback.message.answer(
            "⚠️ Ошибка: данные о покупке не найдены. Попробуйте заново выбрать тариф и ввести username."
        )
        return

    data = user_premium_data[user_id]
    months = data.get("months")
    price = data.get("price")
    target_username = data.get("username")

    if not months or not price:
        await callback.message.answer("Ошибка: данные о подписке не найдены. Попробуйте выбрать тариф заново.")
        return

    if not target_username:
        user_states[user_id] = {'awaiting_premium_username': True}
        await callback.message.answer(
            "❗ Пожалуйста, сначала укажи username для активации Premium."
        )
        return

    # Применяем промо, если есть
    amount_value = data.get("promo_pending", {}).get("new_price", price)

    label_str = generate_unique_label_prem(user_id)
    target_username = data.get("username")
    if not target_username:
        user_states[user_id] = {'awaiting_premium_username': True}
        await callback.message.answer(
            "❗ Пожалуйста, сначала укажи username для активации Premium."
        )
        return

    try:
        payment_id, payment_url = await create_pally_payment(
            amount_value,
            f"Покупка Premium {months} мес. для {target_username}",
            label_str
        )
    except Exception as e:
        print(f"Ошибка создания платежа: {e}")
        await callback.message.answer("Ошибка при создании платежа. Попробуйте позже.")
        return

    # Сохраняем ID платежа и сумму
    user_premium_data[user_id]["pally_payment_id"] = payment_id
    user_premium_data[user_id]["label"] = label_str
    user_premium_data[user_id]["amount"] = amount_value

    # Отправляем кнопку оплаты
    pay_button = InlineKeyboardButton(text=f"💳 Оплатить {amount_value}₽", url=payment_url)
    markup = InlineKeyboardMarkup(inline_keyboard=[[pay_button]])

    await callback.message.edit_text(
        "Нажмите кнопку ниже для перехода к оплате. Оплата будет проверяться автоматически.",
        reply_markup=markup
    )

    # Проверяем оплату 10 минут
    start_time = asyncio.get_event_loop().time()
    payment_confirmed = False
    while True:
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > 600:
            break

        try:
            if await check_pally_payment_status(payment_id):
                payment_confirmed = True
                break
        except Exception as e:
            print(f"Ошибка проверки статуса платежа Premium: {e}")

        await asyncio.sleep(3)

    if payment_confirmed:
        await process_payment_confirmationprem(user_id, target_username, months, callback)
    else:
        await callback.message.answer("Оплата не была подтверждена за 10 минут. Попробуйте снова.")


# Обработка успешной оплаты Premium
async def process_payment_confirmationprem(user_id, target_username, months, callback):
    """
    Полная обработка подтверждения Premium:
    - Обновляет статус пользователя
    - Сохраняет покупку
    - Уведомляет пользователя и группу
    """
    payment_id = str((user_premium_data.get(user_id) or {}).get("pally_payment_id") or "").strip()
    premium_operation_id = f"pally:{payment_id}" if payment_id else f"premium:{user_id}:{int(months or 0)}"
    fulfill_result = await fragment_fulfill_order(
        item_type="premium",
        target_username=str(target_username or ""),
        amount_value=int(months or 0),
        operation_id=premium_operation_id,
    )
    if not fulfill_result.get("ok"):
        fail_stage = str(fulfill_result.get("stage") or "unknown")
        fail_status = int(fulfill_result.get("statusCode") or 0)
        error_text = str(fulfill_result.get("error") or "unknown error")
        debug_error_text = f"[stage={fail_stage} status={fail_status}] {error_text}"
        logging.error(
            "Premium auto-fulfillment failed user=%s target=%s months=%s op=%s error=%s",
            user_id,
            target_username,
            months,
            premium_operation_id,
            debug_error_text,
        )
        if payment_id:
            try:
                cursor.execute(
                    "UPDATE payments SET status = ?, description = ? WHERE bill_id = ?",
                    ("warning", f"Premium auto-fulfillment failed: {debug_error_text[:190]}", payment_id),
                )
                conn.commit()
            except Exception:
                pass
        await bot.send_message(
            user_id,
            "⚠️ Оплата подтверждена, но автовыдача Premium не выполнена. Поддержка уже уведомлена.",
        )
        try:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠️ Ошибка автовыдачи Premium\nUser: <code>{int(user_id)}</code>\n"
                f"Target: @{escape(str(target_username or ''))}\n"
                f"Months: <b>{int(months or 0)}</b>\nError: {escape(debug_error_text)}",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    cursor.execute('SELECT referrals_with_purchase, referred_by FROM users WHERE user_id=?', (user_id,))
    row = cursor.fetchone()

    inviter_id = None
    if row is None:
        cursor.execute('INSERT INTO users (user_id, referrals_with_purchase) VALUES (?, ?)', (user_id, 1))
        conn.commit()
    else:
        current_status = row[0]
        inviter_id = row[1]
        if current_status == 0:
            cursor.execute('UPDATE users SET referrals_with_purchase=1 WHERE user_id=?', (user_id,))
            conn.commit()
            if inviter_id:
                await send_new_active_ref_message(inviter_id)

    # Промо-код
    final_price = user_premium_data[user_id]["price"]
    if "promo_pending" in user_premium_data[user_id]:
        promo_data = user_premium_data[user_id].pop("promo_pending")
        final_price = promo_data["new_price"]
        promo_code_used = promo_data["code"]
        confirm_promo_usage(promo_code_used, user_id)

    # Сохраняем покупку
    buyer_username = callback.from_user.username or callback.from_user.first_name
    cursor.execute(
        "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
        (user_id, target_username, "premium", months, final_price)
    )
    cursor.execute(
        "UPDATE total_premium_months SET total = total + ? WHERE id = 1",
        (int(months),),
    )
    conn.commit()

    # Уведомляем группу
    message_text_group = (
        f"@{buyer_username} — оплата подтверждена для аккаунта {target_username} PREMIUM на {months} мес."
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅ Готово", callback_data="delete_msg")]]
    )
    await bot.send_message(GROUP_CHAT_ID, message_text_group, reply_markup=keyboard)

    # Уведомляем пользователя
    await bot.send_message(
        user_id,
        "✅ Оплата подтверждена! Спасибо за покупку!\n"
        f"Premium скоро поступит на ваш аккаунт {target_username}.\n"
        f'💬 <a href="https://t.me/+Qkb-Q43fRf40NGFk">Оценить наш сервис</a>',
        parse_mode="HTML",
        disable_web_page_preview=True
    )

    # Очищаем данные
    user_premium_data.pop(user_id, None)


@dp.callback_query(lambda c: c.data == "buy_stars")
async def buy_stars_callback(callback: types.CallbackQuery):
    # Проверка подписки
    user_id = callback.from_user.id
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # Обработка ошибок (например, если бот не в состоянии проверить)
        await callback.message.answer("Ошибка проверки подписки. Попробуйте позже.")
        return

    if not is_subscribed:
        # Пользователь не подписан — удаляем сообщение и просим подписаться
        await callback.message.delete()

        # Отправляем сообщение с кнопкой "Подписаться" и "Проверить"
        subscribe_button = InlineKeyboardButton(text="Подписаться", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="✅ Готово", callback_data="check_subscriptionstar")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "Пожалуйста, подпишитесь на наш канал, чтобы продолжить.",
            reply_markup=keyboard
        )
        return

    photo_url = "https://ibb.co/MyFDq6zx"
    caption = 'Выбери один из популярных пакетов или укажи своё количество:'
    await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=caption),
        reply_markup=get_star_keyboard()
    )


@dp.callback_query(lambda c: c.data.endswith("stars"))
async def select_stars(callback: types.CallbackQuery):
    await callback.answer()  # обязательно отвечаем на callback, чтобы убрать "часики"
    photo_url = "https://ibb.co/MyFDq6zx"
    caption = (
        "📛 <b>Теперь укажи username Telegram-аккаунта, куда нужно зачислить звёзды.</b>\n\n"
        "Важно:\n"
        "• <b>Убедись</b>, что твой username корректно указан (например, @example).\n"
        "• <b>Если у тебя нет username</b>, его нужно создать в настройках Telegram.\n"
        "• Звёзды будут зачислены <b>в течение 5-ти минут</b> после оплаты"
    )
    data_str = callback.data[:-5]  # убираем "stars"

    try:
        stars_amount = int(data_str)  # Сколько звезд выбрал пользователь
        multiplier = get_star_rate_for_range(stars_amount)  # <-- динамический курс из базы
        cost = round(stars_amount * multiplier, 2)

        user_id = callback.from_user.id
        user_purchase_data[user_id] = {
            'stars': stars_amount,
            'cost': cost,
            'username': None
        }
        user_states[user_id] = {'awaiting_username': True}

        await callback.message.edit_media(
            InputMediaPhoto(media=photo_url, caption=caption, parse_mode='HTML')
        )
    except ValueError:
        await callback.message.answer("Произошла ошибка при обработке выбора. Попробуйте ещё раз.")



def build_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats_menu"),
         InlineKeyboardButton(text="🎟 Промо-коды", callback_data="admin_promo_menu")],

        [InlineKeyboardButton(text="👥 Рефералы", callback_data="admin_ref_menu"),
         InlineKeyboardButton(text="⭐ Проданные звёзды", callback_data="admin_stars_total")],

        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast_start"),
         InlineKeyboardButton(text="💳 Платёжка", callback_data="payment_menu")],
        [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_find_user")],
        [InlineKeyboardButton(text="➕ Начислить звёзды", callback_data="cad")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_first")]
    ])


def build_admin_panel_text() -> str:
    return (
        "⚙️ <b>Админ-панель</b>\n\n"
        "Выберите нужный раздел:"
    )


@dp.message(Command("ad"))
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(
        build_admin_panel_text(),
        parse_mode="HTML",
        reply_markup=build_admin_panel_keyboard(),
    )

class AddStarsForm(StatesGroup):
    waiting_user_id = State()
    waiting_amount = State()

@dp.callback_query(F.data == "cad")
async def admin_add_stars_start(callback: types.CallbackQuery, state: FSMContext):

    await callback.answer()
    await state.set_state(AddStarsForm.waiting_user_id)
    await callback.message.answer(
        "Введите <b>ID пользователя</b>, которому нужно начислить звёзды:", parse_mode="HTML"
    )

@dp.message(AddStarsForm.waiting_user_id)
async def admin_add_stars_get_user(message: types.Message, state: FSMContext):

    if not message.text.isdigit():

        await message.answer("⚠️ Введите корректный числовой ID пользователя.")
        return

    await state.update_data(user_id=int(message.text))
    await state.set_state(AddStarsForm.waiting_amount)

    await message.answer("Теперь введите <b>количество звёзд</b> для начисления:", parse_mode="HTML")


@dp.message(AddStarsForm.waiting_amount)
async def admin_add_stars_confirm(message: types.Message, state: FSMContext):

    data = await state.get_data()
    user_id = data.get("user_id")


    if not message.text.isdigit():

        await message.answer("⚠️ Введите число (количество звёзд).")
        return

    stars_to_add = int(message.text)
    await state.clear()


    try:
        cursor.execute("""
            INSERT INTO purchases (user_id, username, item_type, amount, cost)
            VALUES (?, ?, 'stars', ?, 0)
        """, (user_id, None, stars_to_add))
        conn.commit()

    except Exception as e:

        await message.answer("❌ Произошла ошибка при добавлении в базу данных.")
        return

    await message.answer(f"✅ Пользователю <b>{user_id}</b> начислено <b>{stars_to_add}⭐️</b>.", parse_mode="HTML")

    # Пытаемся уведомить пользователя
    try:
        await bot.send_message(user_id, f"🎁 Вам начислено {stars_to_add}⭐️ от администрации!")

    except Exception as e:

        await message.answer("⚠️ Не удалось отправить уведомление пользователю (возможно, бот заблокирован).")





# === 1. Админ нажимает кнопку "Найти пользователя" ===
@dp.callback_query(lambda c: c.data == "admin_find_user")
async def ask_username_or_id(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("🚫 Нет доступа", show_alert=True)
        return

    user_states[callback.from_user.id] = {"awaiting_user_lookup": True}
    await callback.message.answer("🧾 Введите username (например: @example) или ID пользователя:")


# === 2. Поиск по username или ID ===
@dp.message(lambda m: user_states.get(m.from_user.id, {}).get("awaiting_user_lookup"))
async def lookup_user_data(message: types.Message):
    query = message.text.strip().lstrip("@")
    user_states.pop(message.from_user.id, None)

    # Определяем — это ID или username
    if query.isdigit():
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (int(query),))
    else:
        cursor.execute("SELECT * FROM users WHERE username = ?", (query,))
    user_row = cursor.fetchone()

    if not user_row:
        await message.answer("❌ Пользователь не найден в базе.")
        return

    user_id = user_row[0]

    # --- Покупки ---
    cursor.execute("SELECT item_type, amount, cost, created_at FROM purchases WHERE user_id = ?", (user_id,))
    purchases = cursor.fetchall()

    # --- Промокоды ---
    cursor.execute("SELECT code FROM used_promo WHERE user_id = ?", (user_id,))
    used_promos = [row[0] for row in cursor.fetchall()]

    # --- Платежи ---
    cursor.execute("SELECT amount, status, date FROM payments WHERE user_id = ?", (user_id,))
    payments = cursor.fetchall()

    # --- Формируем ответ ---
    text = f"<b>🔎 Информация о пользователе:</b>\n\n"
    text += f"👤 <b>ID:</b> <code>{user_row[0]}</code>\n"
    text += f"🪪 <b>Username:</b> @{user_row[1] or '—'}\n"
    text += f"🧷 <b>Имя:</b> {user_row[2] or '-'} {user_row[3] or ''}\n"
    text += f"👥 <b>Пригласил:</b> {user_row[4] or '—'}\n"
    text += f"🔗 <b>Реф. код:</b> {user_row[5] or '—'}\n"
    text += f"👤 <b>Рефералов:</b> {user_row[7]} (с покупкой: {user_row[8]})\n\n"

    # Покупки
    if purchases:
        text += "<b>🛒 Покупки:</b>\n"
        for p in purchases:
            text += f"• {p[0]} — {p[1]} шт. за {p[2]}₽ ({p[3]})\n"
    else:
        text += "🛒 Нет покупок.\n"

    # Промокоды
    if used_promos:
        text += "\n<b>🎟 Использованные промокоды:</b>\n" + ", ".join(used_promos)
    else:
        text += "\n🎟 Промокоды не использовал.\n"

    # Платежи
    if payments:
        text += "\n<b>💳 Платежи:</b>\n"
        for pay in payments:
            text += f"• {pay[0]}₽ — {pay[1]} ({pay[2]})\n"
    else:
        text += "\n💳 Платежей нет.\n"

    await message.answer(text, parse_mode="HTML")







@dp.callback_query(lambda c: c.data == "payment_menu")
async def payment_menu(callback: types.CallbackQuery):
    await callback.answer()  # Подтверждаем callback

    # Здесь добавим красивое сообщение с кнопками для действий.
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Сделать выплату", callback_data="payment_withdraw")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="payment_refresh")],
        [InlineKeyboardButton(text="📜 История пополнений", callback_data="payment_history")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
    ])

    # Покажем баланс сразу при открытии меню
    balance = await get_pally_balance()
    await callback.message.edit_text(
        f"💳 <b>Платёжка</b>\n\n"
        f"Ваш текущий баланс: <b>{balance:.2f} ₽</b>\n\n"
        f"Что бы вы хотели сделать дальше?",
        parse_mode="HTML",
        reply_markup=keyboard
    )


async def initiate_payment_withdraw(amount: float, user_id: str):
    """
    Запрос на выполнение выплаты через Pally API.
    """
    url = f"{PALLY_API_BASE}/merchant/withdraw"  # Примерный endpoint для запроса
    headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}

    data = {
        "user_id": user_id,
        "amount": amount
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            response_data = await resp.json()
            if response_data.get("success"):
                return response_data  # Возвращаем успешный ответ
            else:
                return None  # Возвращаем None, если неудачно

async def get_pally_balance():
    url = f"{PALLY_API_BASE}/merchant/balance"
    headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()


            if str(data.get("success")).lower() == "true":
                # ищем баланс в RUB
                for b in data.get("balances", []):
                    if b.get("currency") == "RUB":
                        return float(b.get("balance_available", 0))
                return 0.0
            else:
                return 0.0



@dp.callback_query(lambda c: c.data == "payment_balance")
async def payment_balance(callback: types.CallbackQuery):
    balance = await get_pally_balance()
    await callback.message.edit_text(
        f"💰 Баланс аккаунта: <b>{balance:.2f} ₽</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="payment_menu")]
        ])
    )


@dp.callback_query(lambda c: c.data == "payment_refresh")
async def payment_refresh(callback: types.CallbackQuery):
    balance = await get_pally_balance()  # Получаем обновленный баланс
    await callback.message.edit_text(
        f"🔄 Обновлено!\n\n"
        f"Ваш текущий баланс: <b>{balance:.2f} ₽</b>\n\n"
        f"Что бы вы хотели сделать дальше?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Сделать выплату", callback_data="payment_withdraw")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="payment_refresh")],
            [InlineKeyboardButton(text="📜 История пополнений", callback_data="payment_history")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
        ])
    )


def get_payment_history():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(
            'SELECT bill_id, amount, status, description, date, user_id FROM payments WHERE status="paid" ORDER BY date DESC LIMIT 10',
        )
        rows = cursor.fetchall()

        if rows:
            history_message = "<b>📜 История пополнений:</b>\n\n"
            for idx, row in enumerate(rows, start=1):
                bill_id, amount, status, description, date, user_id = row
                status_emoji = "✅ Success"  # Статус "paid" всегда будет Success
                formatted_date = format_date(date)  # Преобразуем дату с учётом часового пояса

                # Получаем имя пользователя, если необходимо (можно извлечь его из базы данных)
                history_message += f"<b>🔹 №{idx} 🧾 Bill ID:</b> `{bill_id}`\n"
                history_message += f"<b>💰 Сумма:</b> {amount}₽\n"
                history_message += f"<b>🔄 Статус:</b> {status_emoji}\n"
                history_message += f"<b>📄 Описание:</b> {description}\n"
                history_message += f"<b>📅 Дата:</b> {formatted_date}\n"
                history_message += f"<b>👤 Пользователь:</b> {user_id}\n"  # Если необходимо, показываем user_id
                history_message += "<b>---------------------------------</b>\n"

            history_message += "<b>📝 Показаны последние 10 пополнений.</b>"
        else:
            history_message = "<b>⚠️ Нет истории пополнений.</b>"

        return history_message

    except sqlite3.Error as e:
        logging.error(f"Ошибка при получении истории платежей: {e}")
        return "<b>⚠️ Ошибка при получении истории платежей.</b>"

    finally:
        conn.close()




def debug_check_paid_payments(user_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Запрос для получения только оплаченных платежей
    cursor.execute(
        'SELECT bill_id, amount, status, description, date FROM payments WHERE user_id=? AND status="paid" ORDER BY date DESC',
        (user_id,)
    )
    rows = cursor.fetchall()

    conn.close()





async def get_pally_bill_info(bill_id: str):
        url = f"{PALLY_API_BASE}/bill/payments"
        headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}
        params = {"id": bill_id, "per_page": 10}  # Ограничиваем до 10 записей для теста

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("success") and "data" in data:
                            return data["data"]
                        else:
                            logging.error(f"Ошибка в ответе от Pally: {data.get('message')}")
                            return []
                    else:
                        logging.error(f"Ошибка запроса: {resp.status}")
                        return []
            except Exception as e:
                logging.error(f"Ошибка при запросе истории платежей: {e}")
                return []


from dateutil import parser

def format_date(date_str):
    try:
        original_date = parser.parse(str(date_str or ""))
        local_dt = to_app_timezone(original_date)
        return local_dt.strftime("%Y-%m-%d  %H:%M:%S")
    except Exception as error:
        logging.warning("Ошибка при форматировании даты: %s", error)
        return str(date_str or "")



@dp.callback_query(lambda c: c.data == "payment_history")
async def history_payments_callback(callback: types.CallbackQuery):
    # Убираем передачу user_id, так как запрос теперь не зависит от него
    history_message = get_payment_history()

    # Создаем кнопки "Назад" и "Обновить"
    back_button = InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    refresh_button = InlineKeyboardButton(text="🔄 Обновить", callback_data="payment_history")
    markup = InlineKeyboardMarkup(inline_keyboard=[[back_button, refresh_button]])

    try:
        await callback.message.edit_text(history_message, parse_mode="HTML", reply_markup=markup)

    except sqlite3.Error as e:
        logging.error(f"Ошибка при получении истории платежей: {e}")
        await callback.message.answer(f"<b>⚠️ Ошибка при получении истории платежей: {e}</b>", parse_mode="HTML")


@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await callback.answer()  # Подтверждаем callback

    # Здесь добавим красивое сообщение с кнопками для действий.
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Сделать выплату", callback_data="payment_withdraw")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="payment_refresh")],
        [InlineKeyboardButton(text="📜 История пополнений", callback_data="payment_history")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
    ])

    # Покажем баланс сразу при открытии меню
    balance = await get_pally_balance()
    await callback.message.edit_text(
        f"💳 <b>Платёжка</b>\n\n"
        f"Ваш текущий баланс: <b>{balance:.2f} ₽</b>\n\n"
        f"Что бы вы хотели сделать дальше?",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "payment_history")
async def update_payment_history(callback: types.CallbackQuery):
    # Перезапускаем функцию для получения истории
    user_id = callback.from_user.id
    history_message = get_payment_history(user_id)

    # Создаем кнопки "Назад" и "Обновить"
    back_button = InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    refresh_button = InlineKeyboardButton(text="🔄 Обновить", callback_data="payment_history")
    markup = InlineKeyboardMarkup(inline_keyboard=[[back_button, refresh_button]])

    await callback.message.edit_text(history_message, parse_mode="HTML", reply_markup=markup)



@dp.callback_query(lambda c: c.data == "admin_stats_menu")
async def admin_stats_menu(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="stats_users")],
        [InlineKeyboardButton(text="💸 Продажи", callback_data="stats_sales")],
        [InlineKeyboardButton(text="🔄 Очистить статистику продаж", callback_data="stats_clear")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
    ])

    await callback.message.edit_text(
        "📊 <b>Статистика</b>\n\nВыберите нужный пункт:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# 👤 Кол-во пользователей
@dp.callback_query(lambda c: c.data == "stats_users")
async def stats_users(callback: types.CallbackQuery):
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    await callback.message.answer(f"👥 Всего пользователей: <b>{total_users}</b>", parse_mode="HTML")


def get_star_cost():
    """Получить текущий курс звезды"""
    cursor.execute("SELECT value FROM settings WHERE key='star_cost'")
    row = cursor.fetchone()
    return float(row[0]) if row else 1.33


def set_star_cost(new_cost: float):
    """Обновить курс звезды"""
    cursor.execute("UPDATE settings SET value=? WHERE key='star_cost'", (new_cost,))
    conn.commit()



@dp.callback_query(lambda c: c.data == "stats_sales")
async def stats_sales(callback: types.CallbackQuery):
    today_date = app_today_str()
    cursor.execute("""
        SELECT item_type, SUM(amount), SUM(cost)
        FROM purchases
        WHERE date(created_at) = ?
        GROUP BY item_type
    """, (today_date,))
    rows = cursor.fetchall()

    total_revenue = 0
    total_cost_price = 0
    total_items_text = []

    star_cost = get_star_cost()

    for row in rows:
        item_type, total_amount, total_cost = row
        total_revenue += total_cost

        if item_type == "stars":
            cost_price = total_amount * star_cost
            total_cost_price += cost_price
            total_items_text.append(
                f"⭐ Звёзды: {total_amount}шт — {total_cost:.2f}₽ (себестоимость {cost_price:.2f}₽, курс {star_cost}₽)"
            )

        elif item_type == "premium":
            cost_price = 0
            cursor.execute("""
                SELECT amount, cost FROM purchases
                WHERE item_type='premium' AND date(created_at) = ?
            """, (today_date,))
            premiums = cursor.fetchall()
            months_map = {3: 1000, 6: 1325, 12: 2400}
            for months, cost in premiums:
                cost_price += months_map.get(months, 0)
            total_cost_price += cost_price
            total_items_text.append(
                f"💙 Premium: {total_amount} мес — {total_cost:.2f}₽ (себестоимость {cost_price:.2f}₽)"
            )

    total_revenue *= 0.97
    profit = total_revenue - total_cost_price

    # Топ 5 покупателей
    cursor.execute("""
        SELECT username, SUM(cost) as total_spent
        FROM purchases
        WHERE date(created_at) = ?
        GROUP BY user_id
        ORDER BY total_spent DESC
        LIMIT 5
    """, (today_date,))
    top_buyers = cursor.fetchall()
    top_text = "\n".join([f"{i+1}. {row[0]} — {row[1]:.2f}₽" for i, row in enumerate(top_buyers)]) or "Нет покупок"

    text = (
        f"📊 <b>Статистика за сегодня:</b>\n\n"
        + "\n".join(total_items_text) + "\n\n"
        f"💰 Общая выручка: {total_revenue:.2f}₽\n"
        f"📉 Себестоимость: {total_cost_price:.2f}₽\n"
        f"✅ Чистая прибыль: {profit:.2f}₽\n\n"
        f"🏆 Топ покупателей:\n{top_text}"
    )

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Изменить курс звезды", callback_data="change_star_cost")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_stats_menu")]
    ])

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)


class AdminState(StatesGroup):
    awaiting_new_star_cost = State()


@dp.callback_query(lambda c: c.data == "change_star_cost")
async def change_star_cost(callback: types.CallbackQuery, state: FSMContext):
    current_cost = get_star_cost()
    await callback.message.edit_text(
        f"💱 Текущий курс за 1⭐: <b>{current_cost}₽</b>\n\n"
        f"Введите новый курс (например: <code>1.5</code>):",
        parse_mode="HTML"
    )
    await state.set_state(AdminState.awaiting_new_star_cost)


@dp.message(AdminState.awaiting_new_star_cost)
async def set_new_star_cost(message: types.Message, state: FSMContext):
    try:
        new_cost = float(message.text.replace(",", "."))
        if new_cost <= 0:
            raise ValueError

        set_star_cost(new_cost)
        await message.answer(f"✅ Курс успешно обновлён: 1⭐ = {new_cost}₽")

    except ValueError:
        await message.answer("⚠️ Введите корректное число (например: 1.5)")

    await state.clear()

@dp.callback_query(lambda c: c.data == "stats_clear")
async def stats_clear(callback: types.CallbackQuery):
    threshold_24h = app_datetime_threshold_str(days=1)
    # Проверяем, есть ли продажи за последние 24 часа
    cursor.execute("""
        SELECT COUNT(*) FROM purchases
        WHERE datetime(created_at) >= datetime(?)
    """, (threshold_24h,))
    count = cursor.fetchone()[0]

    if count == 0:
        await callback.message.edit_text(
            "❌ За последние 24 часа продаж не было.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_stats_menu")]
            ])
        )
        return

    # Удаляем продажи за последние 24 часа
    cursor.execute("""
        DELETE FROM purchases
        WHERE datetime(created_at) >= datetime(?)
    """, (threshold_24h,))
    conn.commit()

    await callback.message.edit_text(
        "🗑 Статистика продаж за последние 24 часа успешно очищена.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_stats_menu")]
        ])
    )


@dp.callback_query(lambda c: c.data == "leaders")
async def show_leaders(callback: types.CallbackQuery):
    viewer_id = callback.from_user.id

    # Вычисляем начало недели (понедельник)
    today = app_today()
    start_of_week = today - datetime.timedelta(days=today.weekday())  # понедельник
    start_of_week_str = start_of_week.strftime("%Y-%m-%d")

    # Начало месяца
    start_of_month = today.replace(day=1)
    start_of_month_str = start_of_month.strftime("%Y-%m-%d")

    # Топ за неделю
    cursor.execute("""
        SELECT user_id, SUM(amount) as total_stars
        FROM purchases
        WHERE item_type = 'stars' AND created_at >= ?
        GROUP BY user_id
        ORDER BY total_stars DESC
        LIMIT 5
    """, (start_of_week_str,))
    weekly = cursor.fetchall()

    # Топ за месяц
    cursor.execute("""
        SELECT user_id, SUM(amount) as total_stars
        FROM purchases
        WHERE item_type = 'stars' AND created_at >= ?
        GROUP BY user_id
        ORDER BY total_stars DESC
        LIMIT 5
    """, (start_of_month_str,))
    monthly = cursor.fetchall()

    # Функция отображения пользователя
    def get_user_display(user_id, viewer_id):
        cursor.execute("SELECT username, first_name FROM users WHERE user_id=?", (user_id,))
        user = cursor.fetchone()
        short_hash = hashlib.sha1(str(user_id).encode()).hexdigest()[:3]

        if user:
            username, first_name = user
            if user_id == viewer_id:
                return f"@{username}" if username else first_name
            else:
                if username:
                    return f"@{username[:3]}…{short_hash}"
                elif first_name:
                    return f"{first_name[:2]}…{short_hash}"
        return f"ID{str(user_id)[-3:]}…{short_hash}"

    def format_list(rows):
        if not rows:
            return "— пока нет данных"
        return "\n".join([f"{i+1}. {get_user_display(r[0], viewer_id)} — {int(r[1])}⭐️"
                          for i, r in enumerate(rows)])

    text = (
        "🏆 <b>Лидеры по звёздам</b>\n"
        "<i>Почему видно только мой юз?\n</i>"
        "<b>Каждый пользователь видит полноценно только свой юз❗️</b>\n\n"
        f"📅 <b>За неделю:</b>\n{format_list(weekly)}\n\n"
        f"🗓 <b>За месяц:</b>\n{format_list(monthly)}"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_first")]
    ])

    photo_url = "https://ibb.co/MyFDq6zx"
    await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=text, parse_mode="HTML"),
        reply_markup=keyboard
    )

@dp.message(Command("my_rank"))
async def myrank(message: types.Message):
    user_id = message.from_user.id
    today = app_today()
    start_of_week = today - datetime.timedelta(days=today.weekday())
    start_of_month = today.replace(day=1)

    def get_position(start_date):
        cursor.execute("""
            SELECT user_id, SUM(amount) AS total_stars
            FROM purchases
            WHERE item_type = 'stars' AND created_at >= ?
            GROUP BY user_id
            ORDER BY total_stars DESC
        """, (start_date.strftime("%Y-%m-%d"),))
        rows = cursor.fetchall()
        for pos, row in enumerate(rows, start=1):
            if row[0] == user_id:
                return pos, row[1]
        return None, 0

    week_pos, week_stars = get_position(start_of_week)
    month_pos, month_stars = get_position(start_of_month)

    text = "<b>📊 Ваше место в рейтинге</b>\n\n"

    if week_pos:
        text += f"📅 <b>За неделю:</b>\n🏅 Место: <b>{week_pos}</b>\n⭐️ Звёзды: <b>{week_stars}</b>\n\n"
    else:
        text += "📅 <b>За неделю:</b>\nНет звёзд за этот период.\n\n"

    if month_pos:
        text += f"🗓 <b>За месяц:</b>\n🏅 Место: <b>{month_pos}</b>\n⭐️ Звёзды: <b>{month_stars}</b>\n"
    else:
        text += "🗓 <b>За месяц:</b>\nНет звёзд за этот период.\n"

    await message.answer(text, parse_mode="HTML")


# === FSM для пошагового создания промо ===
class PromoForm(StatesGroup):
    code = State()
    discount = State()
    min_stars = State()
    expires_at = State()
    max_uses = State()
    max_uses_per_user = State()
    condition = State()
    max_free_stars = State()
    target = State()
    confirm = State()

# --- Главное меню промокодов ---
@dp.callback_query(lambda c: c.data == "admin_promo_menu")
async def admin_promo_menu(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промо", callback_data="admin_promo_create")],
        [InlineKeyboardButton(text="📋 Активные промо", callback_data="admin_promo_list")],
        [InlineKeyboardButton(text="🗑 Удалить промо", callback_data="admin_promo_delete")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
    ])
    await callback.message.edit_text(
        "🎟 <b>Промо-коды</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# --- 1. Начало создания промокода ---
@dp.callback_query(lambda c: c.data == "admin_promo_create")
async def start_promo_form(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите название промо-кода (например: NEWYEAR2026):")
    await state.set_state(PromoForm.code)

# --- 2. Скидка ---
@dp.message(PromoForm.code)
async def promo_code_step(message: types.Message, state: FSMContext):
    await state.update_data(code=message.text.upper())
    await message.answer("Введите размер скидки в % (например: 10):")
    await state.set_state(PromoForm.discount)

# --- 3. Минимум звёзд ---
@dp.message(PromoForm.discount)
async def promo_discount_step(message: types.Message, state: FSMContext):
    try:
        discount = int(message.text)
    except:
        return await message.answer("⚠️ Введите число для скидки.")
    await state.update_data(discount=discount)
    await message.answer("Введите минимальное количество звёзд для активации промо:")
    await state.set_state(PromoForm.min_stars)

# --- 4. Срок действия ---
@dp.message(PromoForm.min_stars)
async def promo_min_stars_step(message: types.Message, state: FSMContext):
    try:
        min_stars = int(message.text)
    except:
        return await message.answer("⚠️ Введите число.")
    await state.update_data(min_stars=min_stars)
    await message.answer("Введите дату окончания промо (в формате YYYY-MM-DD):")
    await state.set_state(PromoForm.expires_at)

# --- 5. Максимальное количество активаций ---
@dp.message(PromoForm.expires_at)
async def promo_expires_step(message: types.Message, state: FSMContext):
    await state.update_data(expires_at=message.text)
    await message.answer("Введите общее количество активаций промо (например: 100):")
    await state.set_state(PromoForm.max_uses)

# --- 6. Максимум активаций на пользователя ---
@dp.message(PromoForm.max_uses)
async def promo_max_uses_step(message: types.Message, state: FSMContext):
    try:
        max_uses = int(message.text)
    except:
        return await message.answer("⚠️ Введите число.")
    await state.update_data(max_uses=max_uses)
    await message.answer("Введите, сколько раз один человек может активировать этот промо (например: 1 или 3):")
    await state.set_state(PromoForm.max_uses_per_user)

# --- 7. Условие (кнопками) ---
@dp.message(PromoForm.max_uses_per_user)
async def promo_condition_step(message: types.Message, state: FSMContext):
    try:
        per_user = int(message.text)
    except:
        return await message.answer("⚠️ Введите число.")
    await state.update_data(max_uses_per_user=per_user)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎟 Всем", callback_data="cond_all"),
         InlineKeyboardButton(text="💰 Только покупателям", callback_data="cond_buyers")]
    ])
    await message.answer("Выберите условие использования:", reply_markup=kb)
    await state.set_state(PromoForm.condition)

@dp.callback_query(lambda c: c.data.startswith("cond_"))
async def promo_condition_choice(callback: types.CallbackQuery, state: FSMContext):
    condition = "all" if callback.data == "cond_all" else "buyers"
    await state.update_data(condition=condition)
    await callback.message.answer("Введите макс. бесплатных звёзд (или 0):")
    await state.set_state(PromoForm.max_free_stars)

# --- 8. Макс. бесплатные звёзды ---
@dp.message(PromoForm.max_free_stars)
async def promo_max_free_step(message: types.Message, state: FSMContext):
    try:
        max_free = int(message.text)
    except:
        return await message.answer("⚠️ Введите число.")
    await state.update_data(max_free_stars=max_free)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Звёзды", callback_data="target_stars"),
         InlineKeyboardButton(text="💎 Premium", callback_data="target_premium"),
         InlineKeyboardButton(text="🎁 Всё", callback_data="target_all")]
    ])
    await message.answer("Выберите, к чему применим промокод:", reply_markup=kb)
    await state.set_state(PromoForm.target)

# --- 9. Тип промо (звёзды/премиум/всё) ---
@dp.callback_query(lambda c: c.data.startswith("target_"))
async def promo_target_choice(callback: types.CallbackQuery, state: FSMContext):
    target = callback.data.replace("target_", "")
    await state.update_data(target=target)
    data = await state.get_data()

    text = (
        f"🎟 <b>Проверь данные перед сохранением:</b>\n\n"
        f"🔸 Код: <code>{data['code']}</code>\n"
        f"💰 Скидка: {data['discount']}%\n"
        f"⭐ Мин. звёзд: {data['min_stars']}\n"
        f"📅 Истекает: {data['expires_at']}\n"
        f"🔢 Всего активаций: {data['max_uses']}\n"
        f"👤 На пользователя: {data['max_uses_per_user']}\n"
        f"🎯 Условие: {data['condition']}\n"
        f"🌟 Макс. бесплатных звёзд: {data['max_free_stars']}\n"
        f"🎯 Применяется к: {data['target']}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="promo_save")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="promo_cancel")]
    ])
    await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(PromoForm.confirm)

# --- 10. Сохранение в БД ---
@dp.callback_query(lambda c: c.data in ["promo_save", "promo_cancel"])
async def promo_finish(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "promo_cancel":
        await state.clear()
        return await callback.message.answer("❌ Создание промокода отменено.")

    data = await state.get_data()
    cursor.execute("""
        INSERT OR REPLACE INTO promo_codes
        (code, discount_percent, min_stars, expires_at, max_uses, uses_count, condition,
         max_free_stars, target, max_uses_per_user)
        VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
    """, (
        data['code'], data['discount'], data['min_stars'], data['expires_at'],
        data['max_uses'], data['condition'], data['max_free_stars'], data['target'],
        data['max_uses_per_user']
    ))
    conn.commit()

    await state.clear()
    await callback.message.answer(f"✅ Промо <b>{data['code']}</b> успешно создан!", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "admin_promo_list")
async def promo_list(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return

    now = datetime.datetime.now().strftime("%Y-%m-%d")
    cursor.execute(
        "SELECT code, discount_percent, min_stars, expires_at, max_uses, uses_count, condition "
        "FROM promo_codes "
        "WHERE expires_at >= ? AND uses_count < max_uses",
        (now,)
    )
    rows = cursor.fetchall()

    if not rows:
        return await callback.message.answer("❌ Активных промо-кодов нет.")

    text = "📊 <b>Активные промо-коды:</b>\n\n"
    for row in rows:
        code, discount, min_stars, expires_at, max_uses, uses_count, condition = row
        text += (
            f"<b>Код:</b> <code>{code}</code>\n"
            f"<b>Скидка:</b> {discount}%\n"
            f"<b>Мин. звёзд:</b> {min_stars}\n"
            f"<b>Действует до:</b> {expires_at}\n"
            f"<b>Макс. активаций:</b> {max_uses}\n"
            f"<b>Использовано:</b> {uses_count}\n"
            f"<b>Условие:</b> {condition}\n\n"
        )

    # Показываем в том же сообщении, чтобы не плодить новые
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_promo_menu")]
                                     ]))


class DeletePromoForm(StatesGroup):
    code = State()

@dp.callback_query(lambda c: c.data == "admin_promo_delete")
async def start_delete_promo(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return

    await callback.message.answer("Введите код промо-кода, который нужно удалить:")
    await state.set_state(DeletePromoForm.code)

@dp.message(DeletePromoForm.code)
async def delete_promo_step(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    code = message.text.upper()
    cursor.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
    if cursor.fetchone() is None:
        await message.answer(f"❌ Промо-код <b>{code}</b> не найден.", parse_mode="HTML")
        await state.clear()
        return

    cursor.execute("DELETE FROM promo_codes WHERE code=?", (code,))
    conn.commit()
    await message.answer(f"✅ Промо-код <b>{code}</b> успешно удалён.", parse_mode="HTML")
    await state.clear()


class ReferralsForm(StatesGroup):
    show = State()
    reset = State()

@dp.callback_query(lambda c: c.data == "admin_ref_menu")
async def admin_ref_menu(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Показать рефералов", callback_data="ref_show")],
        [InlineKeyboardButton(text="♻️ Сбросить рефералов", callback_data="ref_reset")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
    ])

    await callback.message.edit_text(
        "👥 <b>Реферальная система</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "ref_show")
async def admin_ref_show(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID пользователя, чтобы показать его активных рефералов:")
    await state.set_state(ReferralsForm.show)

@dp.message(ReferralsForm.show)
async def process_show_referrals(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Некорректный ID. Введите только число.")
        return

    user_id = int(message.text)
    cursor.execute(
        'SELECT username FROM users WHERE referred_by=? AND referrals_with_purchase=1',
        (user_id,)
    )
    rows = cursor.fetchall()
    await state.clear()

    if not rows:
        await message.answer(f"❌ У пользователя {user_id} нет активных рефералов.")
        return

    usernames = "\n".join(f"@{row[0]}" for row in rows if row[0])
    await message.answer(f"📋 Активные рефералы пользователя {user_id}:\n{usernames}")


@dp.callback_query(lambda c: c.data == "ref_reset")
async def admin_ref_reset(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID пользователя, чтобы сбросить его активных рефералов:")
    await state.set_state(ReferralsForm.reset)

@dp.message(ReferralsForm.reset)
async def process_reset_referrals(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Некорректный ID. Введите только число.")
        return

    user_id = int(message.text)
    cursor.execute(
        'UPDATE users SET referrals_with_purchase=0 WHERE referred_by=? AND referrals_with_purchase=1',
        (user_id,)
    )
    conn.commit()
    await state.clear()
    await message.answer(f"♻️ Все активные рефералы пользователя {user_id} успешно сброшены.")


@dp.callback_query(lambda c: c.data == "admin_back_main")
async def admin_back_main(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats_menu"),
         InlineKeyboardButton(text="🎟 Промо-коды", callback_data="admin_promo_menu")],

        [InlineKeyboardButton(text="👥 Рефералы", callback_data="admin_ref_menu"),
         InlineKeyboardButton(text="⭐ Проданные звёзды", callback_data="admin_stars_total")],

        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast_start"),
         InlineKeyboardButton(text="💳 Платёжка", callback_data="payment_menu")],
        [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_find_user")],
        [InlineKeyboardButton(text="➕ Начислить звёзды", callback_data="cad")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_first")]
    ])

    text = (
        "⚙️ <b>Админ-панель</b>\n\n"
        "Выберите нужный раздел:"
    )

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

# --- Словарь для отслеживания действий ---
pending_action = {}  # {user_id: "add" | "remove"}

# --- Главное меню звёзд ---
@dp.callback_query(lambda c: c.data == "admin_stars_total")
async def admin_stars_total(callback: types.CallbackQuery):
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    total_stars = cursor.fetchone()[0] or 0

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить", callback_data="admin_stars_add"),
            InlineKeyboardButton(text="➖ Убрать", callback_data="admin_stars_remove")
        ],
        [
            InlineKeyboardButton(text='ЦЕНА НА ЗВЕЗДЫ', callback_data='open_change_rate_menu')
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_main")]
    ])

    await callback.message.edit_text(
        f"⭐ <b>Всего продано звёзд:</b> {total_stars:,}".replace(",", " "),
        parse_mode="HTML",
        reply_markup=keyboard
    )



# --- Обработка нажатий ---
@dp.callback_query(lambda c: c.data == "admin_stars_add")
async def admin_stars_add(callback: types.CallbackQuery):
    pending_action[callback.from_user.id] = "add"
    await callback.message.answer("Введите, сколько звёзд добавить:")

@dp.callback_query(lambda c: c.data == "admin_stars_remove")
async def admin_stars_remove(callback: types.CallbackQuery):
    pending_action[callback.from_user.id] = "remove"
    await callback.message.answer("Введите, сколько звёзд убрать:")

# --- Обработка чисел ---
@dp.message(lambda m: bool(m.text) and m.text.isdigit() and m.from_user.id in pending_action)
async def handle_stars_edit(message: types.Message):
    action = pending_action.get(message.from_user.id)
    stars_value = int(message.text)

    # Проверяем текущее значение
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    total_stars = cursor.fetchone()[0] or 0

    # Обновляем в зависимости от действия
    if action == "add":
        new_total = total_stars + stars_value
        msg_action = f"✅ Добавлено {stars_value}⭐"
    elif action == "remove":
        new_total = max(total_stars - stars_value, 0)
        msg_action = f"✅ Убрано {stars_value}⭐"
    else:
        return

    cursor.execute("UPDATE total_stars SET total = ? WHERE id = 1", (new_total,))
    conn.commit()

    # Отправляем результат
    await message.answer(
        f"{msg_action}\n"
        f"Теперь всего продано: <b>{new_total:,}</b>⭐".replace(",", " "),
        parse_mode="HTML"
    )

    # Убираем из словаря
    pending_action.pop(message.from_user.id, None)




# --- Кнопка "Рассылка" ---
@dp.callback_query(lambda c: c.data == "broadcast_start")
async def broadcast_start(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return

    user_id = callback.from_user.id
    user_states[user_id] = {"awaiting_broadcast": True}

    await callback.message.answer(
        "📢 Отправь сообщение для рассылки.\n\n"
        "Можно отправить:\n"
        "• просто текст 💬\n"
        "• фото 📸\n"
        "• фото с подписью 🖼\n\n"
        "После этого я спрошу — добавить ли кнопки."
    )



# --- Приём текста или фото (с подписью или без) ---
@dp.message(lambda m: m.from_user.id in ADMIN_IDS and user_states.get(m.from_user.id, {}).get("awaiting_broadcast"))
async def handle_broadcast_content(message: types.Message):
    user_id = message.from_user.id

    content = {"type": None, "text": None, "photo": None}

    if message.photo:
        content["type"] = "photo"
        content["photo"] = message.photo[-1].file_id
        content["text"] = message.caption or ""
    elif message.text:
        content["type"] = "text"
        content["text"] = message.text
    else:
        return await message.answer("⚠️ Поддерживаются только текст и фото.")

    user_states[user_id]["broadcast_content"] = content
    user_states[user_id]["awaiting_broadcast"] = False
    user_states[user_id]["awaiting_buttons"] = True

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить кнопки", callback_data="broadcast_add_buttons")],
        [InlineKeyboardButton(text="🚀 Отправить без кнопок", callback_data="broadcast_send_now")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back_main")]
    ])

    await message.answer("Хочешь добавить кнопки к сообщению?", reply_markup=kb)



@dp.callback_query(lambda c: c.data == "broadcast_add_buttons")
async def broadcast_add_buttons(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    state = user_states.get(user_id)
    if not state:
        return await callback.answer("Сессия устарела. Начни заново.")

    state["awaiting_buttons_text"] = True
    state["awaiting_buttons"] = False

    await callback.message.answer(
        "📲 Отправь кнопки в формате:\n\n"
        "Текст кнопки - https://ссылка\n"
        "Можно несколько, по одной на строку.\n"
        "Пример:\n"
        "Наш сайт - https://example.com\n"
        "Канал - https://t.me/example\n\n"
        "После этого я отправлю рассылку."
    )

@dp.message(lambda m: m.from_user.id in ADMIN_IDS and user_states.get(m.from_user.id, {}).get("awaiting_buttons_text"))
async def broadcast_buttons_step(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state:
        return await message.answer("Сессия устарела.")

    # Список рядов кнопок
    inline_keyboard = []

    # парсим кнопки
    for line in message.text.splitlines():
        if " - " in line:
            text, url = line.split(" - ", 1)
            inline_keyboard.append([InlineKeyboardButton(text=text.strip(), url=url.strip())])

    # создаём клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    state["broadcast_buttons"] = keyboard
    state.pop("awaiting_buttons_text", None)

    await send_broadcast(user_id)



async def send_broadcast(user_id):
    state = user_states.get(user_id)
    if not state:
        return

    content = state.get("broadcast_content")
    keyboard = state.get("broadcast_buttons", None)

    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()

    success = 0
    fail = 0

    for user in users:
        try:
            if content["type"] == "photo":
                await bot.send_photo(
                    chat_id=user[0],
                    photo=content["photo"],
                    caption=content["text"],
                    parse_mode="HTML",
                    reply_markup=keyboard,
                    disable_web_page_preview=True  # ← добавлено
                )
            else:
                await bot.send_message(
                    chat_id=user[0],
                    text=content["text"],
                    parse_mode="HTML",
                    reply_markup=keyboard,
                    disable_web_page_preview=True  # ← добавлено
                )
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            fail += 1

    await bot.send_message(
        user_id,
        f"📢 Рассылка завершена!\n✅ Успешно: {success}\n❌ Ошибка: {fail}"
    )
    user_states.pop(user_id, None)



import datetime  # если у тебя так

@dp.callback_query(lambda c: c.data == "enter_promo")
async def ask_promo(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_states[user_id] = {"awaiting_promo": True}
    await callback.message.answer("Введите ваш промо-код:")

@dp.callback_query(lambda c: c.data == "enter_promo_prem")
async def ask_promo_prem(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_states[user_id] = {"awaiting_promo_prem": True}
    await callback.message.answer("Введите ваш промо-код для Premium:")


def _normalize_promo_effect(raw_effect_type, raw_effect_value, discount_percent, max_free_stars):
    effect_type = str(raw_effect_type or "").strip().lower()
    if effect_type not in {"discount_percent", "free_stars"}:
        if int(max_free_stars or 0) > 0 and int(discount_percent or 0) == 100:
            effect_type = "free_stars"
            effect_value = int(max_free_stars or 0)
        else:
            effect_type = "discount_percent"
            effect_value = int(discount_percent or 0)
    else:
        try:
            effect_value = int(raw_effect_value)
        except (TypeError, ValueError):
            effect_value = 0

    return effect_type, int(effect_value or 0)


def _get_user_promo_uses_count(user_id: int, promo_code: str) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM promo_usages WHERE user_id = ? AND code = ?",
        (int(user_id), str(promo_code).upper()),
    )
    row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def _user_has_any_purchases(user_id: int) -> bool:
    cursor.execute("SELECT 1 FROM purchases WHERE user_id = ? LIMIT 1", (int(user_id),))
    if cursor.fetchone():
        return True
    cursor.execute(
        "SELECT 1 FROM miniapp_purchase_history WHERE user_id = ? LIMIT 1",
        (int(user_id),),
    )
    return bool(cursor.fetchone())


async def apply_promo_code(user_id: int, promo_code: str, target_type: str, stars_amount: int = 0) -> dict:
    promo_code = str(promo_code or "").strip().upper()
    target_type = str(target_type or "").strip().lower()
    if not promo_code:
        return {"ok": False, "discount": 0, "error": "❌ Укажите промо-код."}

    cursor.execute(
        """
        SELECT discount_percent, min_stars, expires_at, max_uses, uses_count,
               condition, max_free_stars, target, max_uses_per_user, effect_type, effect_value
        FROM promo_codes WHERE code=?
        """,
        (promo_code,)
    )
    row = cursor.fetchone()

    if not row:
        return {"ok": False, "discount": 0, "error": "❌ Неверный промо-код."}

    (
        discount_percent,
        min_stars,
        expires_at,
        max_uses,
        uses_count,
        condition,
        max_free_stars,
        target,
        max_uses_per_user,
        raw_effect_type,
        raw_effect_value,
    ) = row

    target = str(target or "stars").strip().lower()
    if target not in {"stars", "premium", "ton", "all"}:
        target = "stars"
    if target_type not in {"stars", "premium", "ton"}:
        return {"ok": False, "discount": 0, "error": "❌ Некорректный тип заказа для промокода."}
    if target not in (target_type, "all"):
        return {"ok": False, "discount": 0, "error": f"❌ Этот промо-код нельзя применить для {target_type}."}

    # Проверка даты истечения
    if expires_at:
        try:
            exp_date = datetime.datetime.strptime(str(expires_at), "%Y-%m-%d").date()
            if app_today() > exp_date:
                return {"ok": False, "discount": 0, "error": "❌ Срок действия промо-кода истёк."}
        except Exception:
            return {"ok": False, "discount": 0, "error": "❌ Ошибка формата даты в промо-коде."}

    max_uses = int(max_uses or 0)
    uses_count = int(uses_count or 0)
    if max_uses > 0 and uses_count >= max_uses:
        return {"ok": False, "discount": 0, "error": "❌ Промо-код больше недоступен."}

    max_uses_per_user = int(max_uses_per_user or 1)
    used_by_user = _get_user_promo_uses_count(user_id, promo_code)
    if max_uses_per_user > 0 and used_by_user >= max_uses_per_user:
        return {
            "ok": False,
            "discount": 0,
            "error": f"❌ Этот промо-код можно активировать не более {max_uses_per_user} раз(а) на пользователя.",
        }

    condition = str(condition or "all").strip().lower()
    if condition == "buyers" and not _user_has_any_purchases(user_id):
        return {"ok": False, "discount": 0, "error": "❌ Этот промо-код доступен только покупателям."}

    min_stars = int(min_stars or 0)
    if target_type == "stars" and min_stars > 0 and int(stars_amount or 0) < min_stars:
        return {
            "ok": False,
            "discount": 0,
            "error": f"❌ Для этого промокода нужно минимум {min_stars}⭐️.",
        }

    effect_type, effect_value = _normalize_promo_effect(
        raw_effect_type, raw_effect_value, discount_percent, max_free_stars
    )

    if effect_type == "free_stars":
        if target_type != "stars":
            return {"ok": False, "discount": 0, "error": "❌ Бесплатные звёзды работают только для покупки звёзд."}
        if effect_value <= 0:
            return {"ok": False, "discount": 0, "error": "❌ Некорректное значение бесплатных звёзд в промокоде."}
        if int(stars_amount or 0) != effect_value:
            return {
                "ok": False,
                "discount": 0,
                "error": f"❌ Этот промокод выдаёт бесплатно только {effect_value}⭐️.",
            }
        return {
            "ok": True,
            "discount": 100,
            "effectType": "free_stars",
            "effectValue": effect_value,
            "error": "",
        }

    discount_value = int(effect_value or 0)
    if discount_value <= 0 or discount_value > 100:
        return {"ok": False, "discount": 0, "error": "❌ Некорректная скидка в промокоде."}

    return {
        "ok": True,
        "discount": discount_value,
        "effectType": "discount_percent",
        "effectValue": discount_value,
        "error": "",
    }




change_star_rate_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="50–75 ⭐", callback_data="change_rate_50_75")],
    [InlineKeyboardButton(text="76–100 ⭐", callback_data="change_rate_76_100")],
    [InlineKeyboardButton(text="101–250 ⭐", callback_data="change_rate_101_250")],
    [InlineKeyboardButton(text="251+ ⭐", callback_data="change_rate_251_plus")],
    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_stars_total")]
])

def get_star_rate_for_range(stars_count: int) -> float:
    cursor.execute("SELECT range_name, rate FROM star_rates")
    rates = dict(cursor.fetchall())

    if 50 <= stars_count <= 75:
        return rates.get('50_75', 1.7)
    elif 76 <= stars_count <= 100:
        return rates.get('76_100', 1.6)
    elif 101 <= stars_count <= 250:
        return rates.get('101_250', 1.55)
    else:
        return rates.get('251_plus', 1.5)


def _spend_service_is_configured() -> bool:
    return bool(SPEND_SERVICE_BASE_URL and SPEND_SERVICE_TOKEN)


def _normalize_spend_service_base_url(raw_base: str) -> str:
    base = str(raw_base or "").strip().rstrip("/")
    if not base:
        return ""
    # Частая ошибка: вставляют swagger URL вместо API base
    lower = base.lower()
    if lower.endswith("/swagger/index.html"):
        base = base[: -len("/swagger/index.html")]
    elif lower.endswith("/swagger"):
        base = base[: -len("/swagger")]
    return base.rstrip("/")


def _spend_service_estimate_path_candidates() -> tuple:
    base = _normalize_spend_service_base_url(SPEND_SERVICE_BASE_URL).lower()
    if base.endswith("/api/service"):
        return ("/estimate",)
    if base.endswith("/service"):
        return ("/estimate", "/service/estimate")
    if base.endswith("/api"):
        return ("/service/estimate",)
    return ("/service/estimate", "/api/service/estimate")


def _spend_service_extract_error(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    result = payload.get("result")
    if isinstance(result, dict):
        message = str(result.get("message") or "").strip()
        if message:
            return message
    message = str(payload.get("message") or "").strip()
    if message:
        return message
    return ""


async def _spend_service_estimate_ton_price(item_type: int, amount: int, fee_percent: float) -> float:
    if not _spend_service_is_configured():
        raise RuntimeError("SPEND API не настроен")

    payload = {
        "type": int(item_type),
        "amount": int(amount),
        "fee": round(float(fee_percent), 2),
    }
    headers = {
        "Authorization": f"Bearer {SPEND_SERVICE_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=max(3.0, float(SPEND_SERVICE_TIMEOUT_SECONDS or 8.0)))
    last_error = ""

    normalized_base = _normalize_spend_service_base_url(SPEND_SERVICE_BASE_URL)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        path_candidates = _spend_service_estimate_path_candidates()
        for path_index, path in enumerate(path_candidates):
            url = f"{normalized_base}{path}"
            is_last_path = path_index == len(path_candidates) - 1
            try:
                async with session.post(url, json=payload, headers=headers) as response:
                    raw_text = await response.text()
                    try:
                        response_payload = json.loads(raw_text) if raw_text else {}
                    except Exception:
                        response_payload = {}

                    if response.status == 404 and not is_last_path:
                        last_error = "endpoint не найден"
                        continue

                    if response.status >= 400:
                        error_text = _spend_service_extract_error(response_payload) or f"HTTP {response.status}"
                        raise RuntimeError(f"SPEND API: {error_text}")

                    result = response_payload.get("result") if isinstance(response_payload, dict) else {}
                    code = response_payload.get("code") if isinstance(response_payload, dict) else None
                    success = bool(isinstance(result, dict) and result.get("success") is True and int(code or 0) == 0)
                    if not success:
                        error_text = _spend_service_extract_error(response_payload) or "ошибка расчета"
                        raise RuntimeError(f"SPEND API: {error_text}")

                    data = result.get("data") if isinstance(result, dict) else {}
                    ton_price_raw = data.get("ton_price") if isinstance(data, dict) else None
                    try:
                        ton_price = float(ton_price_raw)
                    except (TypeError, ValueError):
                        ton_price = 0.0
                    if ton_price <= 0:
                        raise RuntimeError("SPEND API вернул некорректный ton_price")
                    return ton_price
            except Exception as error:
                last_error = str(error)
                if not is_last_path:
                    continue
                raise RuntimeError(last_error or "не удалось получить расчет SPEND API")

    raise RuntimeError(last_error or "не удалось получить расчет SPEND API")


async def _admin_get_spend_market_rates_payload() -> dict:
    sample_amount = int(SPEND_SERVICE_STARS_SAMPLE_AMOUNT or 100)
    sample_amount = max(50, min(sample_amount, 1_000_000))
    fee_percent = float(SPEND_SERVICE_FEE_PERCENT or 2.0)
    fee_percent = max(2.0, min(fee_percent, 50.0))
    rub_per_star = round(float(get_star_rate_for_range(sample_amount)), 4)
    base_payload = {
        "ok": False,
        "source": "spend_estimate",
        "rubPerTon": 0.0,
        "rubPerStar": rub_per_star,
        "starsPerRub": round(1.0 / rub_per_star, 6) if rub_per_star > 0 else 0.0,
        "estimate": {
            "type": 0,
            "amount": sample_amount,
            "fee": round(fee_percent, 2),
            "tonPrice": 0.0,
        },
        "error": "",
    }

    if not _spend_service_is_configured():
        missing = []
        if not _normalize_spend_service_base_url(SPEND_SERVICE_BASE_URL):
            missing.append("SPEND_SERVICE_BASE_URL")
        if not SPEND_SERVICE_TOKEN:
            missing.append("SPEND_SERVICE_TOKEN")
        missing_text = ", ".join(missing) if missing else "SPEND_SERVICE_BASE_URL/SPEND_SERVICE_TOKEN"
        base_payload["error"] = f"SPEND API не настроен: {missing_text}"
        return base_payload

    try:
        ton_price = await _spend_service_estimate_ton_price(0, sample_amount, fee_percent)
        total_rub = float(sample_amount) * rub_per_star
        if total_rub <= 0:
            raise RuntimeError("некорректный рублевый тариф для stars")
        rub_per_ton = round(total_rub / ton_price, 4)
        base_payload["ok"] = True
        base_payload["rubPerTon"] = rub_per_ton
        base_payload["estimate"]["tonPrice"] = round(ton_price, 6)
        return base_payload
    except Exception as error:
        base_payload["error"] = str(error)
        return base_payload


async def _admin_get_spend_procurement_payload() -> dict:
    fee_percent = float(SPEND_SERVICE_FEE_PERCENT or 2.0)
    fee_percent = max(2.0, min(fee_percent, 50.0))
    sample_stars_amount = int(SPEND_SERVICE_STARS_SAMPLE_AMOUNT or 100)
    sample_stars_amount = max(50, min(sample_stars_amount, 1_000_000))

    items = {
        "star": {
            "label": "Stars",
            "type": 0,
            "amount": sample_stars_amount,
            "tonPrice": 0.0,
            "unitTonPrice": 0.0,
            "error": "",
        },
        "premium3": {
            "label": "Premium 3 мес.",
            "type": 1,
            "amount": 3,
            "tonPrice": 0.0,
            "unitTonPrice": 0.0,
            "error": "",
        },
        "premium6": {
            "label": "Premium 6 мес.",
            "type": 1,
            "amount": 6,
            "tonPrice": 0.0,
            "unitTonPrice": 0.0,
            "error": "",
        },
        "premium12": {
            "label": "Premium 12 мес.",
            "type": 1,
            "amount": 12,
            "tonPrice": 0.0,
            "unitTonPrice": 0.0,
            "error": "",
        },
        "ton": {
            "label": "TON",
            "type": 3,
            "amount": 1,
            "tonPrice": 0.0,
            "unitTonPrice": 0.0,
            "error": "",
        },
    }
    base_payload = {
        "ok": False,
        "feePercent": round(fee_percent, 2),
        "items": items,
        "error": "",
    }

    if not _spend_service_is_configured():
        missing = []
        if not _normalize_spend_service_base_url(SPEND_SERVICE_BASE_URL):
            missing.append("SPEND_SERVICE_BASE_URL")
        if not SPEND_SERVICE_TOKEN:
            missing.append("SPEND_SERVICE_TOKEN")
        missing_text = ", ".join(missing) if missing else "SPEND_SERVICE_BASE_URL/SPEND_SERVICE_TOKEN"
        base_payload["error"] = f"SPEND API не настроен: {missing_text}"
        return base_payload

    item_keys = tuple(items.keys())
    estimate_tasks = [
        _spend_service_estimate_ton_price(items[item_key]["type"], items[item_key]["amount"], fee_percent)
        for item_key in item_keys
    ]
    estimates = await asyncio.gather(*estimate_tasks, return_exceptions=True)

    has_errors = False
    for index, estimate_result in enumerate(estimates):
        item_key = item_keys[index]
        item_payload = items[item_key]
        if isinstance(estimate_result, Exception):
            has_errors = True
            item_payload["error"] = str(estimate_result)
            continue

        ton_price = round(float(estimate_result), 6)
        amount_value = int(item_payload["amount"])
        item_payload["tonPrice"] = ton_price
        item_payload["unitTonPrice"] = round(ton_price / float(amount_value), 6) if amount_value > 0 else 0.0

    base_payload["ok"] = not has_errors
    if has_errors:
        base_payload["error"] = "Часть закупочных цен недоступна"
    return base_payload


def _normalize_fragment_username(raw_username: str) -> str:
    username = str(raw_username or "").strip().lstrip("@")
    username = re.sub(r"[^A-Za-z0-9_]", "", username)
    return username[:32]


def _fragment_api_base_headers() -> dict:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if FRAGMENT_API_KEY:
        headers["api-key"] = FRAGMENT_API_KEY
    return headers


def _fragment_is_success_payload(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("success") is True:
        return True
    if payload.get("ok") is True:
        return True
    result = payload.get("result")
    if isinstance(result, dict) and result.get("success") is True:
        return True
    status_value = str(payload.get("status") or "").strip().lower()
    if status_value in {"ok", "success", "completed"}:
        return True
    message_value = str(payload.get("message") or "").strip().lower()
    if "success" in message_value and "not success" not in message_value:
        return True
    return False


def _fragment_extract_error(payload: dict, fallback: str = "") -> str:
    if isinstance(payload, dict):
        for key in ("message", "error", "detail"):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        result = payload.get("result")
        if isinstance(result, dict):
            value = str(result.get("message") or result.get("error") or "").strip()
            if value:
                return value
    return str(fallback or "").strip() or "Fragment API error"


def extract_operation_error_code(error_text: str) -> str:
    safe_text = str(error_text or "").strip()
    if not safe_text:
        return ""
    match = re.search(r"\[code=([A-Za-z0-9\-]+)\]", safe_text)
    if not match:
        return ""
    return str(match.group(1) or "").strip().upper()


def build_miniapp_fulfill_error_code(
    stage: str = "",
    status_code: int = 0,
    error_text: str = "",
    skipped: bool = False,
) -> str:
    safe_stage = str(stage or "").strip().lower() or "unknown"
    safe_error = str(error_text or "").strip().lower()
    safe_status = int(status_code or 0)

    if skipped:
        if "disabled" in safe_error:
            return "MAF-CFG-000"
        return "MAF-SKIP-000"

    if safe_stage == "config":
        if "base_url" in safe_error:
            return "MAF-CFG-001"
        if "seed" in safe_error:
            return "MAF-CFG-002"
        if "disabled" in safe_error:
            return "MAF-CFG-000"
        return "MAF-CFG-999"

    if safe_stage == "validate":
        if "username" in safe_error:
            return "MAF-VAL-001"
        if "premium duration" in safe_error:
            return "MAF-VAL-002"
        return "MAF-VAL-999"

    if safe_stage == "provider":
        if safe_status in {401, 403}:
            return "MAF-PRV-401"
        if safe_status == 404:
            return "MAF-PRV-404"
        if safe_status == 0:
            return "MAF-PRV-000"
        if 400 <= safe_status < 500:
            return "MAF-PRV-4XX"
        if safe_status >= 500:
            return "MAF-PRV-5XX"
        return "MAF-PRV-999"

    return "MAF-UNK-000"


def format_miniapp_fulfill_error_text(error_code: str, stage: str, status_code: int, error_text: str) -> str:
    safe_code = str(error_code or "").strip().upper() or "MAF-UNK-000"
    safe_stage = str(stage or "").strip().lower() or "unknown"
    safe_status = int(status_code or 0)
    safe_error = str(error_text or "").strip() or "unknown error"
    return f"[code={safe_code}] [stage={safe_stage} status={safe_status}] {safe_error}"


def _fragment_is_configured() -> tuple:
    if not FRAGMENT_API_AUTO_FULFILL:
        return False, "disabled"
    if not FRAGMENT_API_BASE_URL:
        return False, "FRAGMENT_API_BASE_URL is missing"
    if not FRAGMENT_API_SEED:
        return False, "FRAGMENT_API_SEED is missing"
    return True, ""


async def _fragment_api_post(path: str, payload: dict) -> tuple:
    url = f"{FRAGMENT_API_BASE_URL}{path}"
    timeout = aiohttp.ClientTimeout(total=max(5.0, float(FRAGMENT_API_TIMEOUT_SECONDS or 25.0)))
    headers = _fragment_api_base_headers()
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.post(url, json=payload) as response:
                raw_text = await response.text()
                response_payload = {}
                if raw_text:
                    try:
                        response_payload = json.loads(raw_text)
                    except Exception:
                        response_payload = {"raw": raw_text}
                if response.status >= 400:
                    error_text = _fragment_extract_error(
                        response_payload,
                        fallback=f"HTTP {response.status}",
                    )
                    return False, response.status, response_payload, error_text
                if _fragment_is_success_payload(response_payload):
                    return True, response.status, response_payload, ""
                error_text = _fragment_extract_error(response_payload, fallback="Unexpected response")
                return False, response.status, response_payload, error_text
    except Exception as error:
        return False, 0, {}, str(error)


def _fragment_response_reference(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    data_obj = payload.get("data")
    if isinstance(data_obj, dict):
        for key in ("hash", "tx_hash", "transaction_hash", "id", "order_id"):
            value = str(data_obj.get(key) or "").strip()
            if value:
                return value
    for key in ("hash", "tx_hash", "transaction_hash", "id", "order_id"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


async def fragment_fulfill_order(item_type: str, target_username: str, amount_value: int, operation_id: str = "") -> dict:
    configured, config_error = _fragment_is_configured()
    if not configured:
        if config_error == "disabled":
            return {
                "ok": True,
                "skipped": True,
                "error": config_error,
                "provider": "fragment-api",
                "operationId": operation_id,
                "stage": "config",
            }
        return {
            "ok": False,
            "skipped": False,
            "error": config_error,
            "provider": "fragment-api",
            "operationId": operation_id,
            "stage": "config",
        }

    normalized_type = str(item_type or "").strip().lower()
    username_value = _normalize_fragment_username(target_username)
    if normalized_type not in {"stars", "premium"}:
        return {
            "ok": True,
            "skipped": True,
            "error": "unsupported item type",
            "provider": "fragment-api",
            "operationId": operation_id,
        }
    if not username_value:
        return {
            "ok": False,
            "skipped": False,
            "error": "target username is required",
            "provider": "fragment-api",
            "operationId": operation_id,
            "stage": "validate",
        }

    safe_amount = int(amount_value or 0)
    if normalized_type == "stars":
        safe_amount = max(50, min(safe_amount, 1_000_000))
    else:
        if safe_amount not in {3, 6, 12}:
            return {
                "ok": False,
                "skipped": False,
                "error": "premium duration must be 3, 6 or 12",
                "provider": "fragment-api",
                "operationId": operation_id,
                "stage": "validate",
            }

    base_payload = {
        "username": username_value,
        "seed": FRAGMENT_API_SEED,
    }
    if FRAGMENT_API_COOKIES:
        base_payload["fragment_cookies"] = FRAGMENT_API_COOKIES

    requests_to_try = []
    if normalized_type == "stars":
        no_kyc_payload = {
            **base_payload,
            "amount": safe_amount,
        }
        kyc_payload = {
            **base_payload,
            "amount": safe_amount,
            "show_sender": bool(FRAGMENT_API_SHOW_SENDER),
        }
        if FRAGMENT_API_USE_KYC:
            requests_to_try.append(("/buyStars", kyc_payload))
            requests_to_try.append(("/buyStarsNoKyc", no_kyc_payload))
        else:
            requests_to_try.append(("/buyStarsNoKyc", no_kyc_payload))
            requests_to_try.append(("/buyStars", kyc_payload))
    else:
        no_kyc_payload = {
            **base_payload,
            "duration": safe_amount,
        }
        kyc_payload = {
            **base_payload,
            "duration": safe_amount,
            "show_sender": bool(FRAGMENT_API_SHOW_SENDER),
        }
        if FRAGMENT_API_USE_KYC:
            requests_to_try.append(("/buyPremium", kyc_payload))
            requests_to_try.append(("/buyPremiumNoKyc", no_kyc_payload))
        else:
            requests_to_try.append(("/buyPremiumNoKyc", no_kyc_payload))
            requests_to_try.append(("/buyPremium", kyc_payload))

    last_error = "Fragment API request failed"
    last_status = 0
    attempts = []
    for path, payload in requests_to_try:
        ok, status_code, response_payload, error_text = await _fragment_api_post(path, payload)
        attempt_payload = {
            "endpoint": path,
            "statusCode": int(status_code or 0),
            "error": str(error_text or ""),
        }
        attempts.append(attempt_payload)
        if ok:
            response_ref = _fragment_response_reference(response_payload)
            logging.info(
                "Fragment fulfill success op=%s endpoint=%s ref=%s",
                operation_id,
                path,
                response_ref or "-",
            )
            return {
                "ok": True,
                "skipped": False,
                "provider": "fragment-api",
                "endpoint": path,
                "reference": response_ref,
                "response": response_payload,
                "operationId": operation_id,
                "stage": "provider",
                "attempts": attempts,
            }
        last_status = int(status_code or 0)
        last_error = str(error_text or last_error)
        logging.warning(
            "Fragment fulfill attempt failed op=%s endpoint=%s status=%s error=%s",
            operation_id,
            path,
            last_status,
            last_error,
        )
        if last_status == 404:
            continue
        if last_status in {401, 403}:
            break

    return {
        "ok": False,
        "skipped": False,
        "provider": "fragment-api",
        "error": last_error,
        "statusCode": last_status,
        "operationId": operation_id,
        "stage": "provider",
        "attempts": attempts,
    }


def get_profile_level_info(total_spent_rub: float) -> dict:
    spent = max(0.0, float(total_spent_rub or 0.0))

    if spent > 50000:
        return {
            "level": 4,
            "minSpentRub": 50000.0,
            "nextLevelTargetRub": None,
            "cashbackPercent": 7,
            "personalOffers": True,
            "fixedStarRateRub": 1.5,
        }
    if spent >= 20000:
        return {
            "level": 3,
            "minSpentRub": 20000.0,
            "nextLevelTargetRub": 50000.0,
            "cashbackPercent": 4,
            "personalOffers": True,
            "fixedStarRateRub": None,
        }
    if spent >= 5000:
        return {
            "level": 2,
            "minSpentRub": 5000.0,
            "nextLevelTargetRub": 20000.0,
            "cashbackPercent": 2,
            "personalOffers": False,
            "fixedStarRateRub": None,
        }
    return {
        "level": 1,
        "minSpentRub": 0.0,
        "nextLevelTargetRub": 5000.0,
        "cashbackPercent": 0,
        "personalOffers": False,
        "fixedStarRateRub": None,
    }


def get_all_star_rates() -> dict:
    cursor.execute("SELECT range_name, rate FROM star_rates")
    return dict(cursor.fetchall())


def set_star_rate(range_name: str, new_rate: float):
    cursor.execute("UPDATE star_rates SET rate = ? WHERE range_name = ?", (new_rate, range_name))
    conn.commit()


def miniapp_json_response(payload: dict, status: int = 200) -> web.Response:
    return web.json_response(payload, status=status, headers=CORS_HEADERS)


def normalize_target_username(raw_value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "", (raw_value or "").replace("@", ""))
    return cleaned[:32]


def parse_months_value(raw_value: str) -> int:
    digits = re.sub(r"[^0-9]", "", raw_value or "")
    if not digits:
        return 0
    try:
        return int(digits)
    except ValueError:
        return 0


def is_freekassa_api_configured() -> bool:
    return bool(FK_API_BASE_URL and FK_MERCHANT_ID and FK_API_KEY)


def is_freekassa_webhook_configured() -> bool:
    return bool(FK_MERCHANT_ID and FK_SECRET_WORD_2)


def _is_local_ip(ip_value: str) -> bool:
    safe_ip = str(ip_value or "").strip().lower()
    if not safe_ip:
        return True
    return (
        safe_ip == "localhost"
        or safe_ip == "::1"
        or safe_ip.startswith("127.")
    )


def extract_request_ip(request: web.Request) -> str:
    header_candidates = [
        "CF-Connecting-IP",
        "X-Real-IP",
        "X-Forwarded-For",
    ]
    for header_name in header_candidates:
        raw_value = str(request.headers.get(header_name) or "").strip()
        if not raw_value:
            continue
        if header_name == "X-Forwarded-For":
            raw_value = raw_value.split(",")[0].strip()
        if raw_value:
            return raw_value
    return str(request.remote or "").strip()


def resolve_freekassa_order_ip(raw_ip: str) -> str:
    candidate = str(raw_ip or "").strip()
    if _is_local_ip(candidate):
        fallback = str(FK_ORDER_IP_FALLBACK or "").strip()
        if fallback and not _is_local_ip(fallback):
            candidate = fallback
    if _is_local_ip(candidate):
        return ""
    return candidate


def _normalize_freekassa_signature_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        if isinstance(value, float):
            return f"{value:.2f}".rstrip("0").rstrip(".")
        return str(value)
    return str(value).strip()


def build_freekassa_api_signature(payload: dict) -> str:
    if not FK_API_KEY:
        return ""
    if not isinstance(payload, dict):
        return ""
    sign_values = []
    for key in sorted(payload.keys()):
        sign_values.append(_normalize_freekassa_signature_value(payload.get(key)))
    sign_base = "|".join(sign_values)
    return hmac.new(
        FK_API_KEY.encode("utf-8"),
        sign_base.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_freekassa_webhook_signature(merchant_id: str, amount: str, merchant_order_id: str) -> str:
    if not FK_SECRET_WORD_2:
        return ""
    sign_base = f"{merchant_id}:{amount}:{FK_SECRET_WORD_2}:{merchant_order_id}"
    return hashlib.md5(sign_base.encode("utf-8")).hexdigest()


def next_freekassa_nonce() -> int:
    global FK_LAST_NONCE
    now_value = int(datetime.datetime.utcnow().timestamp() * 1000)
    if now_value <= FK_LAST_NONCE:
        now_value = FK_LAST_NONCE + 1
    FK_LAST_NONCE = now_value
    return now_value


def extract_history_id_from_payment_id(payment_id: str) -> int:
    safe_payment_id = str(payment_id or "").strip().lower()
    if not safe_payment_id:
        return 0
    direct_match = re.search(r"^miniapp[-_:]?(\d+)$", safe_payment_id)
    if direct_match:
        try:
            return int(direct_match.group(1))
        except (TypeError, ValueError):
            return 0
    generic_match = re.search(r"miniapp[-_:]?(\d+)", safe_payment_id)
    if generic_match:
        try:
            return int(generic_match.group(1))
        except (TypeError, ValueError):
            return 0
    return 0


async def create_freekassa_order(
    *,
    payment_id: str,
    amount_rub: float,
    email: str,
    client_ip: str,
    payment_system: str = "",
) -> dict:
    if not is_freekassa_api_configured():
        return {
            "ok": False,
            "errorCode": "FK-CFG-001",
            "error": "FreeKassa API is not configured",
            "stage": "config",
            "statusCode": 0,
        }

    try:
        amount_value = round(float(amount_rub), 2)
    except (TypeError, ValueError):
        amount_value = 0.0
    if amount_value <= 0:
        return {
            "ok": False,
            "errorCode": "FK-ORD-AMT-001",
            "error": "Invalid amount",
            "stage": "validation",
            "statusCode": 0,
        }

    safe_payment_id = re.sub(r"[^A-Za-z0-9:_-]", "", str(payment_id or "").strip())
    if not safe_payment_id:
        return {
            "ok": False,
            "errorCode": "FK-ORD-ID-001",
            "error": "Invalid payment id",
            "stage": "validation",
            "statusCode": 0,
        }

    safe_ip = resolve_freekassa_order_ip(client_ip)
    if not safe_ip:
        return {
            "ok": False,
            "errorCode": "FK-ORD-IP-001",
            "error": "Не удалось определить валидный IP для FreeKassa (127.0.0.1 запрещён)",
            "stage": "validation",
            "statusCode": 0,
        }

    safe_email = str(email or "").strip().lower()
    if not safe_email:
        safe_email = f"{safe_payment_id}@telegram.org"

    safe_method = str(payment_system or FK_DEFAULT_PAYMENT_SYSTEM or "44").strip()
    if safe_method not in {"36", "43", "44"}:
        safe_method = str(FK_DEFAULT_PAYMENT_SYSTEM or "44").strip() or "44"

    try:
        shop_id_value = int(str(FK_MERCHANT_ID).strip())
    except (TypeError, ValueError):
        shop_id_value = str(FK_MERCHANT_ID).strip()

    unsigned_payload = {
        "shopId": shop_id_value,
        "nonce": int(next_freekassa_nonce()),
        "paymentId": safe_payment_id,
        "i": int(safe_method),
        "email": safe_email,
        "ip": safe_ip,
        "amount": f"{amount_value:.2f}",
        "currency": "RUB",
    }
    if FK_SUCCESS_URL:
        unsigned_payload["success_url"] = FK_SUCCESS_URL
    if FK_FAIL_URL:
        unsigned_payload["failure_url"] = FK_FAIL_URL

    request_signature = build_freekassa_api_signature(unsigned_payload)
    request_payload = dict(unsigned_payload)
    request_payload["signature"] = request_signature

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    endpoint_url = f"{FK_API_BASE_URL}/orders/create"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                endpoint_url,
                json=request_payload,
                headers=headers,
                timeout=20,
            ) as response:
                status_code = int(response.status or 0)
                response_location_header = str(response.headers.get("Location") or "").strip()
                raw_text = await response.text()
                try:
                    response_payload = json.loads(raw_text or "{}")
                except Exception:
                    response_payload = {}
    except Exception as error:
        return {
            "ok": False,
            "errorCode": "FK-API-NET-001",
            "error": f"FreeKassa create order request failed: {error}",
            "stage": "network",
            "statusCode": 0,
        }

    if status_code < 200 or status_code >= 300:
        provider_message = (
            str(response_payload.get("msg") or "")
            if isinstance(response_payload, dict)
            else ""
        ).strip()
        error_text = provider_message or raw_text or f"HTTP {status_code}"
        return {
            "ok": False,
            "errorCode": f"FK-API-HTTP-{status_code}",
            "error": error_text[:240],
            "stage": "provider",
            "statusCode": status_code,
        }

    if not isinstance(response_payload, dict):
        return {
            "ok": False,
            "errorCode": "FK-API-PARSE-001",
            "error": "Invalid FreeKassa response payload",
            "stage": "provider",
            "statusCode": status_code,
        }

    if str(response_payload.get("type") or "").strip().lower() != "success":
        provider_message = str(response_payload.get("msg") or "FreeKassa order create failed").strip()
        return {
            "ok": False,
            "errorCode": "FK-API-ORD-001",
            "error": provider_message[:240],
            "stage": "provider",
            "statusCode": status_code,
        }

    payment_url = ""
    if isinstance(response_payload, dict):
        payment_url = str(
            response_payload.get("location")
            or response_payload.get("Location")
            or response_payload.get("url")
            or response_payload.get("paymentUrl")
            or response_payload.get("payment_url")
            or ""
        ).strip()
    if not payment_url and response_location_header:
        payment_url = response_location_header
    if not payment_url:
        return {
            "ok": False,
            "errorCode": "FK-API-URL-001",
            "error": "FreeKassa response has no payment URL",
            "stage": "provider",
            "statusCode": status_code,
        }

    return {
        "ok": True,
        "paymentId": safe_payment_id,
        "paymentUrl": payment_url,
        "orderId": str(response_payload.get("orderId") or "").strip(),
        "stage": "provider",
        "statusCode": status_code,
        "payload": response_payload,
    }


def parse_telegram_init_data(init_data: str) -> dict:
    if not init_data:
        raise ValueError("Missing Telegram init data")

    parsed_items = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed_items.pop("hash", "")
    if not received_hash:
        raise ValueError("Missing Telegram hash")

    data_check_string = "\n".join(
        f"{key}={value}" for key, value in sorted(parsed_items.items(), key=lambda item: item[0])
    )
    secret_key = hmac.new(b"WebAppData", API_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    computed_hash = hmac.new(
        secret_key, data_check_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        raise ValueError("Invalid Telegram init data signature")

    user_payload = parsed_items.get("user")
    if not user_payload:
        raise ValueError("Telegram user data is missing")

    try:
        user_data = json.loads(user_payload)
    except json.JSONDecodeError as error:
        raise ValueError("Invalid Telegram user payload") from error

    user_id = user_data.get("id")
    if not user_id:
        raise ValueError("Telegram user id is missing")

    return {
        "id": int(user_id),
        "username": (user_data.get("username") or "").strip(),
        "first_name": (user_data.get("first_name") or "").strip(),
        "last_name": (user_data.get("last_name") or "").strip(),
    }


async def miniapp_register_event_subscriber(user_id: int):
    global MINIAPP_EVENT_SUBSCRIBER_SEQ
    queue: asyncio.Queue = asyncio.Queue(maxsize=20)
    async with MINIAPP_EVENT_LOCK:
        MINIAPP_EVENT_SUBSCRIBER_SEQ += 1
        subscriber_id = MINIAPP_EVENT_SUBSCRIBER_SEQ
        MINIAPP_EVENT_SUBSCRIBERS[subscriber_id] = {
            "user_id": int(user_id),
            "queue": queue,
        }
    return subscriber_id, queue


async def miniapp_unregister_event_subscriber(subscriber_id: int) -> None:
    async with MINIAPP_EVENT_LOCK:
        MINIAPP_EVENT_SUBSCRIBERS.pop(int(subscriber_id), None)


async def miniapp_broadcast_event(event_type: str, payload: dict) -> None:
    safe_payload = payload if isinstance(payload, dict) else {}
    safe_event_type = str(event_type or "update").strip() or "update"

    async with MINIAPP_EVENT_LOCK:
        subscribers_snapshot = list(MINIAPP_EVENT_SUBSCRIBERS.values())

    for subscriber in subscribers_snapshot:
        queue = subscriber.get("queue")
        if not isinstance(queue, asyncio.Queue):
            continue
        event = {
            "type": safe_event_type,
            "payload": safe_payload,
        }
        if queue.full():
            try:
                queue.get_nowait()
            except Exception:
                pass
        try:
            queue.put_nowait(event)
        except Exception:
            pass


async def miniapp_events_handler(request: web.Request) -> web.StreamResponse:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    init_data = str(request.query.get("initData") or "").strip()
    if not init_data:
        return miniapp_json_response({"ok": False, "error": "Missing Telegram init data"}, status=400)

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = int(telegram_user.get("id") or 0)
    if user_id <= 0:
        return miniapp_json_response({"ok": False, "error": "Invalid Telegram user id"}, status=401)

    subscriber_id, queue = await miniapp_register_event_subscriber(user_id)

    stream_headers = {
        **CORS_HEADERS,
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    response = web.StreamResponse(status=200, reason="OK", headers=stream_headers)
    await response.prepare(request)

    async def write_event(event_type: str, payload: dict) -> None:
        event_name = str(event_type or "update").strip() or "update"
        event_payload = json.dumps(payload or {}, ensure_ascii=False)
        chunk = f"event: {event_name}\ndata: {event_payload}\n\n"
        await response.write(chunk.encode("utf-8"))

    try:
        await write_event(
            "ready",
            {
                "ok": True,
                "userId": user_id,
                "totals": {
                    "totalStars": int(_admin_get_stars_total()),
                    "totalPremiumMonths": int(_admin_get_premium_months_total()),
                },
            },
        )
        while True:
            try:
                queued_event = await asyncio.wait_for(queue.get(), timeout=25)
                await write_event(
                    str(queued_event.get("type") or "update"),
                    queued_event.get("payload") or {},
                )
            except asyncio.TimeoutError:
                try:
                    await response.write(b": ping\n\n")
                except (ConnectionResetError, aiohttp.ClientConnectionError, RuntimeError):
                    break
            except ConnectionResetError:
                break
            except Exception:
                break
    finally:
        await miniapp_unregister_event_subscriber(subscriber_id)
        try:
            await response.write_eof()
        except Exception:
            pass

    return response


def upsert_user_from_telegram(telegram_user: dict) -> None:
    user_id = int(telegram_user.get("id") or 0)
    if not user_id:
        return

    username = (telegram_user.get("username") or "").strip()
    first_name = (telegram_user.get("first_name") or "").strip()
    last_name = (telegram_user.get("last_name") or "").strip()

    cursor.execute("SELECT ref_code FROM users WHERE user_id=?", (user_id,))
    row = cursor.fetchone()

    if row:
        cursor.execute(
            '''
            UPDATE users
            SET username=?, first_name=?, last_name=?
            WHERE user_id=?
            ''',
            (username, first_name, last_name, user_id),
        )
    else:
        cursor.execute(
            '''
            INSERT INTO users (user_id, username, first_name, last_name, ref_code)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (user_id, username, first_name, last_name, generate_ref_code()),
        )
    conn.commit()


def normalize_review_text(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    normalized_lines = [line.strip() for line in text.splitlines()]
    normalized = "\n".join(line for line in normalized_lines if line)
    return normalized[:1200]


def upsert_miniapp_review(
    *,
    chat_id: int,
    message_id: int,
    reviewer_user_id: int,
    reviewer_username: str,
    reviewer_first_name: str,
    reviewer_last_name: str,
    review_text: str,
    avatar_file_id: str = "",
    created_at: str = "",
) -> None:
    safe_text = normalize_review_text(review_text)
    if not safe_text:
        return

    created_at_value = str(created_at or "").strip() or app_now_str()
    cursor.execute(
        """
        INSERT INTO miniapp_reviews (
            chat_id,
            message_id,
            reviewer_user_id,
            reviewer_username,
            reviewer_first_name,
            reviewer_last_name,
            review_text,
            avatar_file_id,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, message_id) DO UPDATE SET
            reviewer_user_id = excluded.reviewer_user_id,
            reviewer_username = excluded.reviewer_username,
            reviewer_first_name = excluded.reviewer_first_name,
            reviewer_last_name = excluded.reviewer_last_name,
            review_text = excluded.review_text,
            avatar_file_id = CASE
                WHEN TRIM(COALESCE(excluded.avatar_file_id, '')) <> '' THEN excluded.avatar_file_id
                ELSE miniapp_reviews.avatar_file_id
            END,
            created_at = excluded.created_at
        """,
        (
            int(chat_id),
            int(message_id),
            int(reviewer_user_id),
            str(reviewer_username or "").strip().lstrip("@"),
            str(reviewer_first_name or "").strip(),
            str(reviewer_last_name or "").strip(),
            safe_text,
            str(avatar_file_id or "").strip(),
            created_at_value,
        ),
    )
    conn.commit()


def get_miniapp_reviews_payload(limit: int = 10) -> list:
    safe_limit = max(1, min(10, int(limit or 10)))
    select_sql = """
        SELECT
            id,
            reviewer_user_id,
            reviewer_username,
            reviewer_first_name,
            reviewer_last_name,
            review_text,
            created_at,
            avatar_file_id
        FROM miniapp_reviews
        {where_clause}
        ORDER BY id DESC
        LIMIT ?
    """

    rows = []
    try:
        cursor.execute(
            select_sql.format(where_clause="WHERE chat_id = ?"),
            (int(REVIEWS_GROUP_CHAT_ID), safe_limit),
        )
        rows = cursor.fetchall()

        if not rows and ALLOWED_REVIEWS_GROUP_CHAT_IDS:
            allowed_ids = tuple(int(chat_id) for chat_id in sorted(ALLOWED_REVIEWS_GROUP_CHAT_IDS))
            placeholders = ",".join("?" for _ in allowed_ids)
            cursor.execute(
                select_sql.format(where_clause=f"WHERE chat_id IN ({placeholders})"),
                (*allowed_ids, safe_limit),
            )
            rows = cursor.fetchall()

        if not rows:
            cursor.execute(
                select_sql.format(where_clause=""),
                (safe_limit,),
            )
            rows = cursor.fetchall()
    except Exception as error:
        logging.warning("Failed to load miniapp reviews payload: %s", error)
        return []
    payload = []
    for (
        review_id,
        reviewer_user_id,
        reviewer_username,
        reviewer_first_name,
        reviewer_last_name,
        review_text,
        created_at,
        avatar_file_id,
    ) in rows:
        avatar_url = ""
        if str(avatar_file_id or "").strip():
            avatar_url = f"/api/miniapp/reviews/avatar/{int(review_id)}"
        payload.append(
            {
                "id": int(review_id or 0),
                "userId": int(reviewer_user_id or 0),
                "username": str(reviewer_username or "").strip().lstrip("@"),
                "firstName": str(reviewer_first_name or "").strip(),
                "lastName": str(reviewer_last_name or "").strip(),
                "text": str(review_text or "").strip(),
                "createdAt": created_at,
                "avatarUrl": avatar_url,
            }
        )
    return payload


async def get_user_avatar_file_id(user_id: int) -> str:
    safe_user_id = int(user_id or 0)
    if safe_user_id <= 0:
        return ""
    try:
        photos = await bot.get_user_profile_photos(user_id=safe_user_id, limit=1)
    except Exception:
        return ""
    if not photos or not photos.photos:
        return ""
    first_photo_group = photos.photos[0]
    if not first_photo_group:
        return ""
    largest_photo = first_photo_group[-1]
    return str(getattr(largest_photo, "file_id", "") or "").strip()


def _support_now_iso() -> str:
    return app_now_str()


def _support_normalize_text(raw_value: str, max_len: int = 1800) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:max_len]


def _support_normalize_title(raw_value: str, max_len: int = 72) -> str:
    title = str(raw_value or "").strip()
    title = re.sub(r"\s{2,}", " ", title)
    return title[:max_len]


def _support_decode_photo_payload(image_base64: str, image_mime: str) -> tuple:
    raw_value = str(image_base64 or "").strip()
    if not raw_value:
        return b"", ""

    if "," in raw_value:
        raw_value = raw_value.split(",", 1)[1]
    compact = re.sub(r"\s+", "", raw_value)
    if not compact:
        raise ValueError("Пустое изображение")

    try:
        image_bytes = base64.b64decode(compact, validate=True)
    except Exception:
        raise ValueError("Некорректное изображение")

    if not image_bytes:
        raise ValueError("Пустое изображение")
    if len(image_bytes) > 4 * 1024 * 1024:
        raise ValueError("Изображение слишком большое (до 4 МБ)")

    mime_value = str(image_mime or "").strip().lower()
    if mime_value == "image/jpg":
        mime_value = "image/jpeg"
    allowed_mimes = {"image/jpeg", "image/png", "image/webp", "image/gif"}
    if mime_value not in allowed_mimes:
        mime_value = "image/jpeg"

    return image_bytes, mime_value


def _support_build_chat_title(
    *,
    title: str,
    username: str,
    first_name: str,
    last_name: str,
    user_id: int,
) -> str:
    explicit_title = _support_normalize_title(title)
    if explicit_title:
        return explicit_title

    username_value = str(username or "").strip().lstrip("@")
    if username_value:
        return f"@{username_value}"

    full_name = " ".join(
        part for part in (str(first_name or "").strip(), str(last_name or "").strip()) if part
    ).strip()
    if full_name:
        return full_name

    return f"ID {int(user_id or 0)}"


def _support_get_latest_chat_for_user(user_id: int):
    cursor.execute(
        """
        SELECT
            id,
            user_id,
            username,
            first_name,
            last_name,
            title,
            user_unread_count,
            admins_unread_count,
            created_at,
            updated_at
        FROM miniapp_support_chats
        WHERE user_id = ?
        ORDER BY datetime(updated_at) DESC, id DESC
        LIMIT 1
        """,
        (int(user_id),),
    )
    return cursor.fetchone()


def _support_ensure_chat_for_user(telegram_user: dict, preferred_username: str = "") -> int:
    user_id = int(telegram_user.get("id") or 0)
    if user_id <= 0:
        raise ValueError("Некорректный пользователь")

    existing_row = _support_get_latest_chat_for_user(user_id)
    preferred_username_value = str(preferred_username or "").strip().lstrip("@")
    if existing_row:
        chat_id = int(existing_row[0] or 0)
        username_value = preferred_username_value or str(telegram_user.get("username") or "").strip().lstrip("@")
        first_name = str(telegram_user.get("first_name") or "").strip()
        last_name = str(telegram_user.get("last_name") or "").strip()
        cursor.execute(
            """
            UPDATE miniapp_support_chats
            SET username = ?, first_name = ?, last_name = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                username_value,
                first_name,
                last_name,
                _support_now_iso(),
                chat_id,
            ),
        )
        conn.commit()
        return chat_id

    username_value = preferred_username_value or str(telegram_user.get("username") or "").strip().lstrip("@")
    first_name = str(telegram_user.get("first_name") or "").strip()
    last_name = str(telegram_user.get("last_name") or "").strip()
    now_value = _support_now_iso()
    cursor.execute(
        """
        INSERT INTO miniapp_support_chats
            (user_id, username, first_name, last_name, title, user_unread_count, admins_unread_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, '', 0, 0, ?, ?)
        """,
        (
            user_id,
            username_value,
            first_name,
            last_name,
            now_value,
            now_value,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid or 0)


def _support_insert_message(
    *,
    chat_id: int,
    sender_user_id: int,
    sender_role: str,
    sender_username: str,
    sender_full_name: str,
    text: str,
    photo_blob: bytes = b"",
    photo_mime: str = "",
) -> int:
    safe_chat_id = int(chat_id or 0)
    if safe_chat_id <= 0:
        raise ValueError("Некорректный чат")

    normalized_role = str(sender_role or "").strip().lower()
    if normalized_role not in {"user", "admin"}:
        normalized_role = "user"

    safe_text = _support_normalize_text(text)
    safe_blob = bytes(photo_blob or b"")
    safe_mime = str(photo_mime or "").strip().lower()
    if not safe_text and not safe_blob:
        raise ValueError("Введите сообщение или добавьте фото")

    now_value = _support_now_iso()
    cursor.execute(
        """
        INSERT INTO miniapp_support_messages
            (chat_id, sender_user_id, sender_role, sender_username, sender_full_name, text, photo_blob, photo_mime, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            safe_chat_id,
            int(sender_user_id or 0),
            normalized_role,
            str(sender_username or "").strip().lstrip("@"),
            str(sender_full_name or "").strip(),
            safe_text,
            sqlite3.Binary(safe_blob) if safe_blob else None,
            safe_mime if safe_blob else "",
            now_value,
        ),
    )
    message_id = int(cursor.lastrowid or 0)
    if normalized_role == "user":
        cursor.execute(
            """
            UPDATE miniapp_support_chats
            SET
                admins_unread_count = COALESCE(admins_unread_count, 0) + 1,
                updated_at = ?
            WHERE id = ?
            """,
            (now_value, safe_chat_id),
        )
    else:
        cursor.execute(
            """
            UPDATE miniapp_support_chats
            SET
                user_unread_count = COALESCE(user_unread_count, 0) + 1,
                admins_unread_count = 0,
                updated_at = ?
            WHERE id = ?
            """,
            (now_value, safe_chat_id),
        )
    conn.commit()
    return message_id


def _support_build_message_payload(row) -> dict:
    (
        message_id,
        chat_id,
        sender_user_id,
        sender_role,
        sender_username,
        sender_full_name,
        text_value,
        photo_mime,
        has_photo,
        created_at,
    ) = row
    message_photo_url = ""
    if int(has_photo or 0) == 1:
        message_photo_url = f"/api/miniapp/support/photo/{int(message_id or 0)}"

    username_value = str(sender_username or "").strip().lstrip("@")
    sender_label = str(sender_full_name or "").strip()
    if not sender_label and username_value:
        sender_label = f"@{username_value}"
    if not sender_label:
        sender_label = "Пользователь" if str(sender_role or "") == "user" else "Админ"

    return {
        "id": int(message_id or 0),
        "chatId": int(chat_id or 0),
        "senderUserId": int(sender_user_id or 0),
        "senderRole": str(sender_role or "").strip().lower() or "user",
        "senderUsername": username_value,
        "senderName": sender_label,
        "text": str(text_value or "").strip(),
        "photoUrl": message_photo_url,
        "photoMime": str(photo_mime or "").strip(),
        "createdAt": str(created_at or ""),
    }


def _support_get_chat_messages(chat_id: int, limit: int = 120) -> list:
    safe_chat_id = int(chat_id or 0)
    safe_limit = max(1, min(int(limit or 120), 300))
    cursor.execute(
        """
        SELECT
            id,
            chat_id,
            sender_user_id,
            sender_role,
            sender_username,
            sender_full_name,
            text,
            photo_mime,
            CASE WHEN photo_blob IS NULL OR length(photo_blob) = 0 THEN 0 ELSE 1 END AS has_photo,
            created_at
        FROM miniapp_support_messages
        WHERE chat_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (safe_chat_id, safe_limit),
    )
    rows = cursor.fetchall()
    rows.reverse()
    return [_support_build_message_payload(row) for row in rows]


def _support_build_chat_payload(row) -> dict:
    (
        chat_id,
        user_id,
        username,
        first_name,
        last_name,
        title,
        user_unread_count,
        admins_unread_count,
        created_at,
        updated_at,
    ) = row
    return {
        "id": int(chat_id or 0),
        "userId": int(user_id or 0),
        "username": str(username or "").strip().lstrip("@"),
        "firstName": str(first_name or "").strip(),
        "lastName": str(last_name or "").strip(),
        "title": _support_build_chat_title(
            title=str(title or ""),
            username=str(username or ""),
            first_name=str(first_name or ""),
            last_name=str(last_name or ""),
            user_id=int(user_id or 0),
        ),
        "customTitle": str(title or "").strip(),
        "userUnreadCount": int(user_unread_count or 0),
        "adminsUnreadCount": int(admins_unread_count or 0),
        "createdAt": str(created_at or ""),
        "updatedAt": str(updated_at or ""),
    }


def _support_get_user_state_payload(user_id: int, messages_limit: int = 120) -> dict:
    row = _support_get_latest_chat_for_user(int(user_id))
    if not row:
        return {
            "hasChat": False,
            "chat": None,
            "messages": [],
            "userUnreadCount": 0,
        }

    chat_payload = _support_build_chat_payload(row)
    chat_id = int(chat_payload.get("id") or 0)
    messages = _support_get_chat_messages(chat_id, limit=messages_limit)
    has_admin_messages = any(
        str(item.get("senderRole") or "").strip().lower() == "admin"
        for item in messages
    )
    user_unread_count = int(chat_payload.get("userUnreadCount") or 0)
    if user_unread_count > 0 and not has_admin_messages and chat_id > 0:
        # Защита от "залипшего" счетчика: если сообщений от админа нет,
        # пользовательский unread должен быть нулем.
        cursor.execute(
            "UPDATE miniapp_support_chats SET user_unread_count = 0 WHERE id = ?",
            (chat_id,),
        )
        conn.commit()
        user_unread_count = 0
        chat_payload["userUnreadCount"] = 0

    return {
        "hasChat": True,
        "chat": chat_payload,
        "messages": messages,
        "userUnreadCount": user_unread_count,
    }


def _support_mark_user_chat_read(user_id: int, chat_id: int = 0) -> None:
    safe_user_id = int(user_id or 0)
    if safe_user_id <= 0:
        return
    safe_chat_id = int(chat_id or 0)
    if safe_chat_id > 0:
        cursor.execute(
            "UPDATE miniapp_support_chats SET user_unread_count = 0 WHERE id = ? AND user_id = ?",
            (safe_chat_id, safe_user_id),
        )
    else:
        cursor.execute(
            """
            UPDATE miniapp_support_chats
            SET user_unread_count = 0
            WHERE id = (
                SELECT id
                FROM miniapp_support_chats
                WHERE user_id = ?
                ORDER BY datetime(updated_at) DESC, id DESC
                LIMIT 1
            )
            """,
            (safe_user_id,),
        )
    conn.commit()


def _support_get_admin_chats_payload(page: int = 1, limit: int = 12, query: str = "") -> dict:
    safe_page = max(1, int(page or 1))
    safe_limit = max(1, min(int(limit or 12), 30))
    query_value = str(query or "").strip()
    query_normalized = query_value.lstrip("@").lower()

    where_clause = ""
    where_params = []
    if query_normalized:
        like_value = f"%{query_normalized}%"
        where_clause = (
            "WHERE lower(COALESCE(c.username, '')) LIKE ? "
            "OR lower(COALESCE(c.first_name, '')) LIKE ? "
            "OR lower(COALESCE(c.last_name, '')) LIKE ? "
            "OR lower(COALESCE(c.title, '')) LIKE ?"
        )
        where_params = [like_value, like_value, like_value, like_value]
        if query_normalized.isdigit():
            where_clause = f"WHERE c.user_id = ? OR ({where_clause[6:]})"
            where_params = [int(query_normalized), *where_params]

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM miniapp_support_chats c
        {where_clause}
        """,
        tuple(where_params),
    )
    total_items = int((cursor.fetchone() or [0])[0] or 0)
    total_pages = max(1, (total_items + safe_limit - 1) // safe_limit)
    safe_page = min(safe_page, total_pages)
    offset = (safe_page - 1) * safe_limit

    cursor.execute(
        f"""
        SELECT
            c.id,
            c.user_id,
            c.username,
            c.first_name,
            c.last_name,
            c.title,
            c.user_unread_count,
            c.admins_unread_count,
            c.created_at,
            c.updated_at,
            m.text,
            CASE WHEN m.photo_blob IS NULL OR length(m.photo_blob) = 0 THEN 0 ELSE 1 END AS last_has_photo,
            m.created_at,
            m.sender_role
        FROM miniapp_support_chats c
        LEFT JOIN miniapp_support_messages m
            ON m.id = (
                SELECT id
                FROM miniapp_support_messages
                WHERE chat_id = c.id
                ORDER BY id DESC
                LIMIT 1
            )
        {where_clause}
        ORDER BY datetime(COALESCE(m.created_at, c.updated_at)) DESC, c.id DESC
        LIMIT ? OFFSET ?
        """,
        tuple([*where_params, safe_limit, offset]),
    )
    rows = cursor.fetchall()

    items = []
    for row in rows:
        chat_row = row[:10]
        last_text = str(row[10] or "").strip()
        has_photo = int(row[11] or 0) == 1
        last_message_at = str(row[12] or row[9] or "")
        last_sender_role = str(row[13] or "").strip().lower()
        preview_text = last_text if last_text else ("[Фото]" if has_photo else "")
        if len(preview_text) > 140:
            preview_text = preview_text[:140] + "..."
        chat_payload = _support_build_chat_payload(chat_row)
        chat_payload.update(
            {
                "lastMessageText": preview_text,
                "lastMessageAt": last_message_at,
                "lastMessageBy": last_sender_role or "user",
            }
        )
        items.append(chat_payload)

    return {
        "items": items,
        "page": safe_page,
        "pageSize": safe_limit,
        "total": total_items,
        "totalPages": total_pages,
        "query": query_value,
    }


def _support_get_chat_row(chat_id: int):
    cursor.execute(
        """
        SELECT
            id,
            user_id,
            username,
            first_name,
            last_name,
            title,
            user_unread_count,
            admins_unread_count,
            created_at,
            updated_at
        FROM miniapp_support_chats
        WHERE id = ?
        LIMIT 1
        """,
        (int(chat_id),),
    )
    return cursor.fetchone()


def _support_get_admin_chat_payload(chat_id: int, messages_limit: int = 160, mark_admin_read: bool = True) -> dict:
    row = _support_get_chat_row(int(chat_id))
    if not row:
        raise ValueError("Чат не найден")

    safe_chat_id = int(row[0] or 0)
    if mark_admin_read:
        cursor.execute(
            "UPDATE miniapp_support_chats SET admins_unread_count = 0 WHERE id = ?",
            (safe_chat_id,),
        )
        conn.commit()
        row = _support_get_chat_row(safe_chat_id)
        if not row:
            raise ValueError("Чат не найден")

    chat_payload = _support_build_chat_payload(row)
    messages = _support_get_chat_messages(safe_chat_id, limit=messages_limit)
    return {
        "chat": chat_payload,
        "messages": messages,
    }


def _support_rename_chat(chat_id: int, title: str) -> dict:
    safe_chat_id = int(chat_id or 0)
    if safe_chat_id <= 0:
        raise ValueError("Некорректный чат")
    normalized_title = _support_normalize_title(title)
    if not normalized_title:
        raise ValueError("Введите название чата")

    cursor.execute(
        "UPDATE miniapp_support_chats SET title = ?, updated_at = ? WHERE id = ?",
        (normalized_title, _support_now_iso(), safe_chat_id),
    )
    if int(cursor.rowcount or 0) <= 0:
        conn.commit()
        raise ValueError("Чат не найден")
    conn.commit()

    row = _support_get_chat_row(safe_chat_id)
    if not row:
        raise ValueError("Чат не найден")
    return _support_build_chat_payload(row)


def _support_delete_chat(chat_id: int) -> dict:
    safe_chat_id = int(chat_id or 0)
    row = _support_get_chat_row(safe_chat_id)
    if not row:
        raise ValueError("Чат не найден")
    user_id = int(row[1] or 0)

    cursor.execute("DELETE FROM miniapp_support_messages WHERE chat_id = ?", (safe_chat_id,))
    cursor.execute("DELETE FROM miniapp_support_admin_notices WHERE chat_id = ?", (safe_chat_id,))
    cursor.execute("DELETE FROM miniapp_support_chats WHERE id = ?", (safe_chat_id,))
    conn.commit()

    return {"chatId": safe_chat_id, "userId": user_id}


def _support_get_admin_notice_message_id(chat_id: int, admin_user_id: int) -> int:
    safe_chat_id = int(chat_id or 0)
    safe_admin_id = int(admin_user_id or 0)
    if safe_chat_id <= 0 or safe_admin_id <= 0:
        return 0
    cursor.execute(
        """
        SELECT message_id
        FROM miniapp_support_admin_notices
        WHERE chat_id = ? AND admin_user_id = ?
        LIMIT 1
        """,
        (safe_chat_id, safe_admin_id),
    )
    row = cursor.fetchone()
    return int((row or [0])[0] or 0)


def _support_upsert_admin_notice(chat_id: int, admin_user_id: int, message_id: int) -> None:
    safe_chat_id = int(chat_id or 0)
    safe_admin_id = int(admin_user_id or 0)
    safe_message_id = int(message_id or 0)
    if safe_chat_id <= 0 or safe_admin_id <= 0 or safe_message_id <= 0:
        return
    now_value = _support_now_iso()
    cursor.execute(
        """
        INSERT INTO miniapp_support_admin_notices (chat_id, admin_user_id, message_id, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chat_id, admin_user_id)
        DO UPDATE SET message_id = excluded.message_id, created_at = excluded.created_at
        """,
        (safe_chat_id, safe_admin_id, safe_message_id, now_value),
    )
    conn.commit()


def _support_delete_admin_notice(chat_id: int, admin_user_id: int) -> None:
    safe_chat_id = int(chat_id or 0)
    safe_admin_id = int(admin_user_id or 0)
    if safe_chat_id <= 0 or safe_admin_id <= 0:
        return
    cursor.execute(
        "DELETE FROM miniapp_support_admin_notices WHERE chat_id = ? AND admin_user_id = ?",
        (safe_chat_id, safe_admin_id),
    )
    conn.commit()


async def _support_clear_admin_notice_for_admin(chat_id: int, admin_user_id: int) -> None:
    safe_chat_id = int(chat_id or 0)
    safe_admin_id = int(admin_user_id or 0)
    if safe_chat_id <= 0 or safe_admin_id <= 0:
        return
    message_id = _support_get_admin_notice_message_id(safe_chat_id, safe_admin_id)
    if message_id <= 0:
        return
    try:
        await bot.delete_message(chat_id=safe_admin_id, message_id=message_id)
    except Exception:
        pass
    _support_delete_admin_notice(safe_chat_id, safe_admin_id)


async def _support_notify_admins_new_message(chat_payload: dict, message_payload: dict) -> None:
    safe_chat_id = int(chat_payload.get("id") or 0)
    user_id = int(chat_payload.get("userId") or 0)
    username_value = str(chat_payload.get("username") or "").strip().lstrip("@")
    title_value = str(chat_payload.get("title") or "").strip()
    message_text = str(message_payload.get("text") or "").strip()
    has_photo = bool(str(message_payload.get("photoUrl") or "").strip())
    preview = message_text if message_text else ("[Фото]" if has_photo else "Новое сообщение")
    if len(preview) > 240:
        preview = preview[:240] + "..."
    user_label = f"@{escape(username_value)}" if username_value else "—"

    lines = [
        "🛟 <b>Новое обращение в поддержку</b>",
        f"Чат: <code>{safe_chat_id}</code>",
        f"Пользователь: {user_label} (ID: <code>{user_id}</code>)",
        f"Название: {escape(title_value or 'Без названия')}",
        f"Сообщение: {escape(preview)}",
    ]
    text = "\n".join(lines)

    for admin_id in ADMIN_IDS:
        try:
            safe_admin_id = int(admin_id)
            previous_message_id = _support_get_admin_notice_message_id(safe_chat_id, safe_admin_id)
            if previous_message_id > 0:
                try:
                    await bot.delete_message(chat_id=safe_admin_id, message_id=previous_message_id)
                except Exception:
                    pass
                _support_delete_admin_notice(safe_chat_id, safe_admin_id)

            sent_message = await bot.send_message(safe_admin_id, text, parse_mode="HTML")
            _support_upsert_admin_notice(
                safe_chat_id,
                safe_admin_id,
                int(getattr(sent_message, "message_id", 0) or 0),
            )
        except Exception:
            continue


def add_miniapp_purchase_history(
    *,
    user_id: int,
    buyer_username: str,
    buyer_first_name: str,
    buyer_last_name: str,
    target_username: str,
    item_type: str,
    amount,
    price_rub: float,
    price_usd: float,
    promo_code: str = "",
    promo_discount: int = 0,
    promo_error: str = "",
    status: str = "pending",
    counters_applied: int = 0,
    source: str = "api",
) -> int:
    local_cursor = conn.cursor()
    _ensure_miniapp_purchase_history_schema(local_cursor)

    row_payload = {
        "user_id": int(user_id),
        "buyer_username": (buyer_username or "").strip(),
        "buyer_first_name": (buyer_first_name or "").strip(),
        "buyer_last_name": (buyer_last_name or "").strip(),
        "target_username": (target_username or "").strip(),
        "item_type": (item_type or "").strip(),
        "amount": str(amount),
        "price_rub": float(price_rub),
        "price_usd": float(price_usd),
        "promo_code": (promo_code or "").strip().upper(),
        "promo_discount": int(promo_discount or 0),
        "promo_error": (promo_error or "").strip(),
        "status": str(status or "pending").strip().lower() or "pending",
        "counters_applied": int(counters_applied or 0),
        "source": (source or "api").strip(),
        "created_at": app_now_iso(),
    }

    table_columns = _get_table_columns("miniapp_purchase_history", local_cursor)
    if "username" in table_columns and "buyer_username" not in table_columns:
        row_payload["username"] = row_payload.get("target_username") or row_payload.get("buyer_username") or ""
    if "cost" in table_columns and "price_rub" not in table_columns:
        row_payload["cost"] = row_payload["price_rub"]

    preferred_columns = [
        "user_id",
        "buyer_username",
        "buyer_first_name",
        "buyer_last_name",
        "target_username",
        "item_type",
        "amount",
        "price_rub",
        "price_usd",
        "promo_code",
        "promo_discount",
        "promo_error",
        "status",
        "counters_applied",
        "source",
        "created_at",
        "username",
        "cost",
    ]

    insert_columns = [column for column in preferred_columns if column in table_columns and column in row_payload]
    if not insert_columns:
        raise RuntimeError("miniapp_purchase_history has no compatible columns")

    insert_sql = (
        f"INSERT INTO miniapp_purchase_history ({', '.join(insert_columns)}) "
        f"VALUES ({', '.join(['?'] * len(insert_columns))})"
    )
    insert_values = tuple(row_payload[column] for column in insert_columns)
    local_cursor.execute(insert_sql, insert_values)

    history_id = int(local_cursor.lastrowid or 0)
    if history_id <= 0:
        try:
            local_cursor.execute("SELECT last_insert_rowid()")
            history_id = int((local_cursor.fetchone() or [0])[0] or 0)
        except Exception:
            history_id = 0
    if history_id <= 0:
        raise RuntimeError("Failed to determine purchase history id")
    conn.commit()
    return history_id


def delete_miniapp_purchase_history(history_id: int) -> None:
    cursor.execute("DELETE FROM miniapp_purchase_history WHERE id = ?", (int(history_id),))
    conn.commit()


def set_miniapp_purchase_history_status(history_id: int, status: str) -> bool:
    safe_status = str(status or "").strip().lower()
    if safe_status not in {"pending", "success", "warning", "error"}:
        safe_status = "pending"
    cursor.execute(
        "UPDATE miniapp_purchase_history SET status = ? WHERE id = ?",
        (safe_status, int(history_id)),
    )
    conn.commit()
    return cursor.rowcount > 0


def set_miniapp_purchase_history_error(history_id: int, error_text: str, status: str = "error") -> bool:
    safe_status = str(status or "").strip().lower()
    if safe_status not in {"pending", "success", "warning", "error"}:
        safe_status = "error"
    safe_error = str(error_text or "").strip()
    if len(safe_error) > 900:
        safe_error = safe_error[:900]
    cursor.execute(
        "UPDATE miniapp_purchase_history SET status = ?, promo_error = ? WHERE id = ?",
        (safe_status, safe_error, int(history_id)),
    )
    conn.commit()
    return cursor.rowcount > 0


def get_miniapp_purchase_history_row(history_id: int) -> dict:
    safe_history_id = int(history_id or 0)
    if safe_history_id <= 0:
        return {}
    cursor.execute(
        """
        SELECT id, user_id, target_username, item_type, amount, status, created_at
        FROM miniapp_purchase_history
        WHERE id = ?
        LIMIT 1
        """,
        (safe_history_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {}
    return {
        "id": int(row[0] or 0),
        "userId": int(row[1] or 0),
        "targetUsername": str(row[2] or ""),
        "itemType": str(row[3] or ""),
        "amountRaw": str(row[4] or ""),
        "status": str(row[5] or ""),
        "createdAt": str(row[6] or ""),
    }


def get_miniapp_purchase_history_details(history_id: int) -> dict:
    safe_history_id = int(history_id or 0)
    if safe_history_id <= 0:
        return {}
    cursor.execute(
        """
        SELECT
            id,
            user_id,
            buyer_username,
            buyer_first_name,
            buyer_last_name,
            target_username,
            item_type,
            amount,
            price_rub,
            price_usd,
            promo_code,
            promo_discount,
            promo_error,
            status,
            created_at,
            COALESCE(counters_applied, 0)
        FROM miniapp_purchase_history
        WHERE id = ?
        LIMIT 1
        """,
        (safe_history_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {}
    return {
        "id": int(row[0] or 0),
        "userId": int(row[1] or 0),
        "buyerUsername": str(row[2] or "").strip(),
        "buyerFirstName": str(row[3] or "").strip(),
        "buyerLastName": str(row[4] or "").strip(),
        "targetUsername": str(row[5] or "").strip(),
        "itemType": str(row[6] or "").strip().lower(),
        "amountRaw": str(row[7] or "").strip(),
        "priceRub": float(row[8] or 0.0),
        "priceUsd": float(row[9] or 0.0),
        "promoCode": str(row[10] or "").strip().upper(),
        "promoDiscount": int(row[11] or 0),
        "promoError": str(row[12] or "").strip(),
        "status": str(row[13] or "").strip().lower(),
        "createdAt": str(row[14] or "").strip(),
        "countersApplied": int(row[15] or 0),
    }


def find_history_id_for_payment(payment_id: str) -> int:
    direct_history_id = extract_history_id_from_payment_id(payment_id)
    if direct_history_id > 0:
        return direct_history_id

    safe_payment_id = str(payment_id or "").strip()
    if not safe_payment_id:
        return 0

    try:
        cursor.execute("SELECT description FROM payments WHERE bill_id = ? LIMIT 1", (safe_payment_id,))
        row = cursor.fetchone()
    except Exception:
        row = None
    raw_description = str((row or [""])[0] or "")
    match = re.search(r"(?:history_id|historyId)\s*[:=]\s*(\d+)", raw_description, re.IGNORECASE)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return 0


def try_confirm_history_promo_after_payment(history_id: int) -> dict:
    details = get_miniapp_purchase_history_details(history_id)
    if not details:
        return {"ok": False, "error": "history_not_found"}

    current_status = str(details.get("status") or "").strip().lower()
    promo_code = str(details.get("promoCode") or "").strip().upper()
    if not promo_code:
        return {"ok": True, "skipped": True, "reason": "no_promo"}
    if current_status != "pending":
        return {"ok": True, "skipped": True, "reason": f"status_{current_status or 'unknown'}"}

    user_id = int(details.get("userId") or 0)
    if user_id <= 0:
        return {"ok": False, "error": "invalid_user_id"}

    if confirm_promo_usage(promo_code, user_id):
        return {"ok": True, "skipped": False}

    logging.warning(
        "Promo confirmation after payment failed. history_id=%s user_id=%s promo=%s",
        int(history_id or 0),
        user_id,
        promo_code,
    )
    return {"ok": False, "error": "promo_confirm_failed"}


async def try_fragment_fulfill_miniapp_history(history_id: int) -> dict:
    purchase_row = get_miniapp_purchase_history_row(history_id)
    if not purchase_row:
        error_code = "MAF-HST-404"
        return {
            "ok": False,
            "error": "Purchase history not found",
            "errorCode": error_code,
            "historyId": int(history_id or 0),
            "operationId": "",
            "userId": 0,
            "stage": "history",
            "statusCode": 0,
        }

    current_status = str(purchase_row.get("status") or "").strip().lower()
    if current_status == "success":
        return {
            "ok": True,
            "skipped": True,
            "error": "already finalized",
            "errorCode": "MAF-SKIP-ALREADY",
            "historyId": int(purchase_row.get("id") or 0),
            "operationId": "",
            "userId": int(purchase_row.get("userId") or 0),
            "stage": "history",
            "statusCode": 0,
        }

    item_type = str(purchase_row.get("itemType") or "").strip().lower()
    amount_raw = str(purchase_row.get("amountRaw") or "")
    amount_value = parse_months_value(amount_raw)
    operation_id = build_operation_id(
        "miniapp",
        int(purchase_row.get("id") or 0),
        str(purchase_row.get("createdAt") or ""),
    )
    fulfill_result = await fragment_fulfill_order(
        item_type=item_type,
        target_username=str(purchase_row.get("targetUsername") or ""),
        amount_value=amount_value,
        operation_id=operation_id,
    )
    result_payload = dict(fulfill_result or {})
    result_payload["historyId"] = int(purchase_row.get("id") or 0)
    result_payload["operationId"] = str(result_payload.get("operationId") or operation_id)
    result_payload["userId"] = int(purchase_row.get("userId") or 0)

    fail_stage = str(result_payload.get("stage") or "unknown")
    fail_status = int(result_payload.get("statusCode") or 0)
    fail_error = str(result_payload.get("error") or "fragment auto-fulfillment failed").strip()
    skipped = bool(result_payload.get("skipped"))
    error_code = build_miniapp_fulfill_error_code(
        stage=fail_stage,
        status_code=fail_status,
        error_text=fail_error,
        skipped=skipped,
    )
    result_payload["errorCode"] = error_code

    logging.warning(
        "Miniapp fulfill result history_id=%s operation_id=%s ok=%s skipped=%s stage=%s status=%s code=%s error=%s",
        int(purchase_row.get("id") or 0),
        str(result_payload.get("operationId") or operation_id),
        bool(result_payload.get("ok")),
        skipped,
        fail_stage,
        fail_status,
        error_code,
        fail_error,
    )

    if result_payload.get("ok"):
        if skipped:
            skip_debug = format_miniapp_fulfill_error_text(
                error_code,
                fail_stage,
                fail_status,
                fail_error or "fulfillment skipped",
            )
            set_miniapp_purchase_history_error(int(purchase_row.get("id") or 0), skip_debug, status="warning")
            return result_payload
        set_miniapp_purchase_history_error(int(purchase_row.get("id") or 0), "", status="pending")
        return result_payload

    fail_debug = format_miniapp_fulfill_error_text(error_code, fail_stage, fail_status, fail_error)
    set_miniapp_purchase_history_error(int(purchase_row.get("id") or 0), fail_debug, status="error")
    return result_payload


def finalize_miniapp_purchase_history(history_id: int) -> dict:
    history_id = int(history_id or 0)
    if history_id <= 0:
        return {"ok": False, "historyId": history_id}

    try:
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            """
            SELECT
                user_id,
                item_type,
                amount,
                LOWER(TRIM(COALESCE(status, ''))),
                COALESCE(counters_applied, 0),
                created_at
            FROM miniapp_purchase_history
            WHERE id = ?
            """,
            (history_id,),
        )
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return {"ok": False, "historyId": history_id}

        user_id, item_type, raw_amount, current_status, counters_applied, created_at = row
        normalized_type = str(item_type or "").strip().lower()
        amount_value = parse_months_value(str(raw_amount or ""))
        counters_already_applied = int(counters_applied or 0) > 0

        cursor.execute(
            "UPDATE miniapp_purchase_history SET status = 'success' WHERE id = ?",
            (history_id,),
        )

        if not counters_already_applied and amount_value > 0:
            if normalized_type == "stars":
                cursor.execute("UPDATE total_stars SET total = total + ? WHERE id = 1", (amount_value,))
            elif normalized_type == "premium":
                cursor.execute("INSERT OR IGNORE INTO total_premium_months (id, total) VALUES (1, 0)")
                cursor.execute(
                    "UPDATE total_premium_months SET total = total + ? WHERE id = 1",
                    (amount_value,),
                )
            cursor.execute(
                "UPDATE miniapp_purchase_history SET counters_applied = 1 WHERE id = ?",
                (history_id,),
            )
        elif not counters_already_applied:
            cursor.execute(
                "UPDATE miniapp_purchase_history SET counters_applied = 1 WHERE id = ?",
                (history_id,),
            )

        conn.commit()
        return {
            "ok": True,
            "changed": current_status != "success" or not counters_already_applied,
            "historyId": history_id,
            "operationId": build_operation_id("miniapp", history_id, str(created_at or "")),
            "userId": int(user_id or 0),
            "itemType": normalized_type,
            "amount": int(amount_value or 0),
            "totals": {
                "totalStars": int(_admin_get_stars_total()),
                "totalPremiumMonths": int(_admin_get_premium_months_total()),
            },
        }
    except Exception:
        conn.rollback()
        return {"ok": False, "historyId": history_id}


def build_history_amount_label(item_type: str, amount_value: str) -> str:
    clean_amount = (amount_value or "").strip()
    if item_type == "premium":
        digits = re.sub(r"[^0-9]", "", clean_amount)
        return f"{digits} мес." if digits else "—"
    return clean_amount or "—"


def build_history_price_label(price_usd: float, price_rub: float) -> str:
    rub_value = float(price_rub)
    usd_value = float(price_usd)
    rub_symbol = "\u20BD"
    rub_text = f"{int(rub_value)}{rub_symbol}" if rub_value.is_integer() else f"{rub_value:.2f}{rub_symbol}"
    return f"{usd_value:.2f}$/{rub_text}"


def _extract_operation_day(created_at: str) -> tuple:
    raw_value = str(created_at or "").strip()
    matched = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw_value)
    if not matched:
        return "", ""
    year, month, day = matched.groups()
    return f"{year}-{month}-{day}", f"{day}{month}{year[2:]}"


def _resolve_operation_created_at(source_key: str, record_id: int, created_at: str = "") -> str:
    created_value = str(created_at or "").strip()
    if created_value:
        return created_value

    try:
        safe_id = int(record_id or 0)
    except (TypeError, ValueError):
        safe_id = 0
    if safe_id <= 0:
        return ""

    try:
        if source_key == "miniapp":
            cursor.execute(
                "SELECT created_at FROM miniapp_purchase_history WHERE id = ? LIMIT 1",
                (safe_id,),
            )
        elif source_key == "legacy":
            cursor.execute(
                "SELECT created_at FROM purchases WHERE id = ? LIMIT 1",
                (safe_id,),
            )
        else:
            return ""
        row = cursor.fetchone()
        return str((row or [""])[0] or "").strip()
    except Exception:
        return ""


def _get_operation_daily_index(source_key: str, record_id: int, created_at: str) -> int:
    created_value = str(created_at or "").strip()
    day_iso, _ = _extract_operation_day(created_value)
    try:
        safe_id = int(record_id or 0)
    except (TypeError, ValueError):
        safe_id = 0
    fallback_index = max(1, safe_id)
    if not day_iso or not created_value:
        return fallback_index

    try:
        if source_key == "miniapp":
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM miniapp_purchase_history
                WHERE date(created_at) = ?
                  AND (
                    datetime(created_at) < datetime(?)
                    OR (datetime(created_at) = datetime(?) AND id <= ?)
                  )
                """,
                (day_iso, created_value, created_value, safe_id),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM miniapp_purchase_history
                WHERE date(created_at) = ?
                  AND datetime(created_at) <= datetime(?)
                """,
                (day_iso, created_value),
            )
        miniapp_count = int((cursor.fetchone() or [0])[0] or 0)

        if source_key == "legacy":
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM purchases
                WHERE date(created_at) = ?
                  AND (
                    datetime(created_at) < datetime(?)
                    OR (datetime(created_at) = datetime(?) AND id <= ?)
                  )
                """,
                (day_iso, created_value, created_value, safe_id),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM purchases
                WHERE date(created_at) = ?
                  AND datetime(created_at) <= datetime(?)
                """,
                (day_iso, created_value),
            )
        legacy_count = int((cursor.fetchone() or [0])[0] or 0)

        return max(1, miniapp_count + legacy_count)
    except Exception:
        return fallback_index


def build_operation_id(source: str, record_id: int, created_at: str = "") -> str:
    source_key = str(source or "").strip().lower()
    if source_key not in {"miniapp", "legacy"}:
        source_key = "miniapp"

    try:
        safe_id = int(record_id or 0)
    except (TypeError, ValueError):
        safe_id = 0

    created_value = _resolve_operation_created_at(source_key, safe_id, created_at)
    _, day_label = _extract_operation_day(created_value)
    if not day_label:
        day_label = app_now().strftime("%d%m%y")
        return f"{day_label}{max(1, safe_id)}"

    daily_index = _get_operation_daily_index(source_key, safe_id, created_value)
    return f"{day_label}{daily_index}"


def normalize_operation_status(raw_status: str, promo_error: str = "") -> str:
    status_value = str(raw_status or "").strip().lower()
    if status_value not in {"pending", "success", "warning", "error"}:
        status_value = "warning" if str(promo_error or "").strip() else "success"
    return status_value


def build_operation_payload(
    *,
    source: str,
    record_id: int,
    user_id: int,
    buyer_username: str,
    buyer_first_name: str,
    buyer_last_name: str,
    target_username: str,
    item_type: str,
    amount,
    price_usd: float,
    price_rub: float,
    promo_code: str = "",
    promo_discount: int = 0,
    promo_error: str = "",
    status: str = "success",
    created_at: str = "",
) -> dict:
    source_key = str(source or "").strip().lower() or "legacy"
    normalized_type = str(item_type or "").strip().lower()
    if normalized_type not in {"stars", "premium", "ton"}:
        normalized_type = "stars"

    promo_code_value = str(promo_code or "").strip().upper()
    promo_error_value = str(promo_error or "").strip()
    error_code_value = extract_operation_error_code(promo_error_value)
    status_value = normalize_operation_status(status, promo_error_value)
    buyer_username_value = str(buyer_username or "").strip().lstrip("@")
    target_username_value = str(target_username or "").strip().lstrip("@")
    buyer_full_name = " ".join(
        part for part in (str(buyer_first_name or "").strip(), str(buyer_last_name or "").strip()) if part
    ).strip()
    operation_id = build_operation_id(source_key, record_id, str(created_at or ""))
    safe_price_rub = round(float(price_rub or 0.0), 2)
    safe_price_usd = round(float(price_usd or 0.0), 2)

    try:
        promo_discount_value = max(0, int(promo_discount or 0))
    except (TypeError, ValueError):
        promo_discount_value = 0

    return {
        "type": normalized_type,
        "itemType": normalized_type,
        "main": build_history_amount_label(normalized_type, str(amount)),
        "amountRaw": str(amount or ""),
        "price": build_history_price_label(safe_price_usd, safe_price_rub),
        "priceRub": safe_price_rub,
        "priceUsd": safe_price_usd,
        "status": status_value,
        "promoCode": promo_code_value,
        "promoDiscount": promo_discount_value,
        "promoError": promo_error_value,
        "errorCode": error_code_value,
        "createdAt": str(created_at or ""),
        "operationId": operation_id,
        "operationSource": source_key,
        "operationNumericId": int(record_id or 0),
        "buyerUserId": int(user_id or 0),
        "buyerUsername": buyer_username_value,
        "buyerFullName": buyer_full_name,
        "targetUsername": target_username_value,
        "channel": "miniapp_test" if source_key == "miniapp" else "legacy",
    }


def get_miniapp_purchase_history_payload(user_id: int, limit: int = 20) -> list:
    user_id = int(user_id)
    limit = max(1, int(limit))
    per_source_limit = max(50, limit * 5)
    combined_rows = []

    cursor.execute(
        '''
        SELECT
            id, user_id, buyer_username, buyer_first_name, buyer_last_name, target_username,
            item_type, amount, price_usd, price_rub, promo_code, promo_discount, promo_error, status, created_at
        FROM miniapp_purchase_history
        WHERE user_id=?
        ORDER BY id DESC
        LIMIT ?
        ''',
        (user_id, per_source_limit),
    )
    miniapp_rows = cursor.fetchall()
    for (
        row_id,
        buyer_user_id,
        buyer_username,
        buyer_first_name,
        buyer_last_name,
        target_username,
        item_type,
        amount,
        price_usd,
        price_rub,
        promo_code,
        promo_discount,
        promo_error,
        raw_status,
        created_at,
    ) in miniapp_rows:
        combined_rows.append(
            (
                "miniapp",
                int(row_id or 0),
                int(buyer_user_id or user_id),
                str(buyer_username or ""),
                str(buyer_first_name or ""),
                str(buyer_last_name or ""),
                str(target_username or ""),
                item_type,
                amount,
                float(price_usd or 0.0),
                float(price_rub or 0.0),
                promo_code,
                int(promo_discount or 0),
                promo_error,
                raw_status,
                created_at,
            )
        )

    cursor.execute(
        '''
        SELECT id, user_id, username, item_type, amount, cost, created_at
        FROM purchases
        WHERE user_id=?
        ORDER BY id DESC
        LIMIT ?
        ''',
        (user_id, per_source_limit),
    )
    legacy_rows = cursor.fetchall()
    for row_id, buyer_user_id, username_value, item_type, amount, cost, created_at in legacy_rows:
        price_rub = float(cost or 0.0)
        price_usd = round(price_rub / MINIAPP_USD_RUB_RATE, 2) if MINIAPP_USD_RUB_RATE > 0 else 0.0
        combined_rows.append(
            (
                "legacy",
                int(row_id or 0),
                int(buyer_user_id or user_id),
                "",
                "",
                "",
                str(username_value or ""),
                item_type,
                amount,
                price_usd,
                price_rub,
                "",
                0,
                "",
                "success",
                created_at,
            )
        )

    combined_rows.sort(key=lambda row: (str(row[15] or ""), int(row[1] or 0)), reverse=True)

    payload = []
    for row in combined_rows[:limit]:
        (
            source,
            row_id,
            buyer_user_id,
            buyer_username,
            buyer_first_name,
            buyer_last_name,
            target_username,
            item_type,
            amount,
            price_usd,
            price_rub,
            promo_code,
            promo_discount,
            promo_error,
            raw_status,
            created_at,
        ) = row
        payload.append(
            build_operation_payload(
                source=source,
                record_id=row_id,
                user_id=buyer_user_id,
                buyer_username=buyer_username,
                buyer_first_name=buyer_first_name,
                buyer_last_name=buyer_last_name,
                target_username=target_username,
                item_type=item_type,
                amount=amount,
                price_usd=price_usd,
                price_rub=price_rub,
                promo_code=promo_code,
                promo_discount=promo_discount,
                promo_error=promo_error,
                status=raw_status,
                created_at=created_at,
            )
        )
    return payload


def get_miniapp_profile_stats(user_id: int) -> dict:
    user_id = int(user_id)
    cursor.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(status, 'success')))='success' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(status, 'success')))='success' THEN price_rub ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(status, 'success')))='success' AND TRIM(COALESCE(promo_code, '')) <> '' THEN 1 ELSE 0 END), 0),
            COALESCE(
                SUM(
                    CASE
                        WHEN LOWER(TRIM(COALESCE(status, 'success'))) <> 'success' THEN 0
                        WHEN COALESCE(promo_discount, 0) <= 0 THEN 0
                        WHEN COALESCE(promo_discount, 0) >= 100 THEN price_rub
                        ELSE (price_rub * promo_discount) / (100 - promo_discount)
                    END
                ),
                0
            )
        FROM miniapp_purchase_history
        WHERE user_id = ?
        """,
        (user_id,),
    )
    history_row = cursor.fetchone() or (0, 0, 0, 0)
    history_total_purchases = int(history_row[0] or 0)
    history_total_spent = float(history_row[1] or 0.0)
    promo_uses = int(history_row[2] or 0)
    promo_savings_rub = float(history_row[3] or 0.0)

    cursor.execute(
        "SELECT COUNT(*), COALESCE(SUM(cost), 0) FROM purchases WHERE user_id = ?",
        (user_id,),
    )
    legacy_row = cursor.fetchone() or (0, 0)
    legacy_total_purchases = int(legacy_row[0] or 0)
    legacy_total_spent = float(legacy_row[1] or 0.0)

    total_purchases = history_total_purchases + legacy_total_purchases
    total_spent_rub = history_total_spent + legacy_total_spent
    level_info = get_profile_level_info(total_spent_rub)
    promo_savings_usd = (
        round(promo_savings_rub / MINIAPP_USD_RUB_RATE, 2)
        if MINIAPP_USD_RUB_RATE > 0
        else 0.0
    )

    cursor.execute("SELECT COUNT(*) FROM used_promo WHERE user_id = ?", (user_id,))
    used_promos_count = int((cursor.fetchone() or [0])[0] or 0)
    promo_uses = max(promo_uses, used_promos_count)

    return {
        "totalPurchases": total_purchases,
        "totalSpentRub": round(total_spent_rub, 2),
        "usedPromos": promo_uses,
        "promoSavingsRub": round(promo_savings_rub, 2),
        "promoSavingsUsd": promo_savings_usd,
        "levelInfo": level_info,
    }


def get_admin_operations_payload(page: int = 1, limit: int = 8, query: str = "") -> dict:
    page = max(1, int(page or 1))
    limit = max(1, min(int(limit or 8), 20))
    query_value = str(query or "").strip()
    query_normalized = query_value.lower().lstrip("@")
    query_compact = re.sub(r"[^a-z0-9]+", "", query_normalized)
    per_source_limit = max(200, page * limit * 14)
    operations = []

    cursor.execute(
        """
        SELECT
            id, user_id, buyer_username, buyer_first_name, buyer_last_name, target_username,
            item_type, amount, price_usd, price_rub, promo_code, promo_discount, promo_error, status, created_at
        FROM miniapp_purchase_history
        ORDER BY id DESC
        LIMIT ?
        """,
        (per_source_limit,),
    )
    miniapp_rows = cursor.fetchall()
    for (
        row_id,
        user_id,
        buyer_username,
        buyer_first_name,
        buyer_last_name,
        target_username,
        item_type,
        amount,
        price_usd,
        price_rub,
        promo_code,
        promo_discount,
        promo_error,
        raw_status,
        created_at,
    ) in miniapp_rows:
        operations.append(
            build_operation_payload(
                source="miniapp",
                record_id=int(row_id or 0),
                user_id=int(user_id or 0),
                buyer_username=str(buyer_username or ""),
                buyer_first_name=str(buyer_first_name or ""),
                buyer_last_name=str(buyer_last_name or ""),
                target_username=str(target_username or ""),
                item_type=item_type,
                amount=amount,
                price_usd=float(price_usd or 0.0),
                price_rub=float(price_rub or 0.0),
                promo_code=str(promo_code or ""),
                promo_discount=int(promo_discount or 0),
                promo_error=str(promo_error or ""),
                status=str(raw_status or ""),
                created_at=str(created_at or ""),
            )
        )

    cursor.execute(
        """
        SELECT id, user_id, username, item_type, amount, cost, created_at
        FROM purchases
        ORDER BY id DESC
        LIMIT ?
        """,
        (per_source_limit,),
    )
    legacy_rows = cursor.fetchall()
    for row_id, user_id, username, item_type, amount, cost, created_at in legacy_rows:
        safe_price_rub = float(cost or 0.0)
        safe_price_usd = round(safe_price_rub / MINIAPP_USD_RUB_RATE, 2) if MINIAPP_USD_RUB_RATE > 0 else 0.0
        operations.append(
            build_operation_payload(
                source="legacy",
                record_id=int(row_id or 0),
                user_id=int(user_id or 0),
                buyer_username="",
                buyer_first_name="",
                buyer_last_name="",
                target_username=str(username or ""),
                item_type=item_type,
                amount=amount,
                price_usd=safe_price_usd,
                price_rub=safe_price_rub,
                promo_code="",
                promo_discount=0,
                promo_error="",
                status="success",
                created_at=str(created_at or ""),
            )
        )

    operations.sort(
        key=lambda item: (
            str(item.get("createdAt") or ""),
            int(item.get("operationNumericId") or 0),
        ),
        reverse=True,
    )

    if query_normalized:
        filtered_operations = []
        for item in operations:
            searchable = " ".join(
                [
                    str(item.get("operationId") or ""),
                    str(item.get("buyerUserId") or ""),
                    str(item.get("buyerUsername") or ""),
                    str(item.get("buyerFullName") or ""),
                    str(item.get("targetUsername") or ""),
                    str(item.get("itemType") or ""),
                    str(item.get("main") or ""),
                    str(item.get("status") or ""),
                    str(item.get("createdAt") or ""),
                    str(item.get("priceRub") or ""),
                    str(item.get("promoCode") or ""),
                ]
            ).lower()
            searchable_compact = re.sub(r"[^a-z0-9]+", "", searchable)
            if query_normalized in searchable or (query_compact and query_compact in searchable_compact):
                filtered_operations.append(item)
    else:
        filtered_operations = operations

    total_items = len(filtered_operations)
    total_pages = max(1, (total_items + limit - 1) // limit)
    page = min(page, total_pages)
    start_index = (page - 1) * limit
    end_index = start_index + limit
    page_items = filtered_operations[start_index:end_index]

    return {
        "items": page_items,
        "page": page,
        "pageSize": limit,
        "total": total_items,
        "totalPages": total_pages,
        "query": query_value,
    }


def build_miniapp_config_payload() -> dict:
    star_amounts = [100, 500, 1000]
    star_rates = get_all_star_rates()
    total_stars = _admin_get_stars_total()
    total_premium_months = _admin_get_premium_months_total()
    stars = []
    for amount in star_amounts:
        rub = round(amount * get_star_rate_for_range(amount), 2)
        usd = round(rub / MINIAPP_USD_RUB_RATE, 2)
        stars.append(
            {
                "amount": amount,
                "rub": rub,
                "usd": usd,
            }
        )

    premium = []
    for months, rub in PREMIUM_PRICES_RUB.items():
        premium.append(
            {
                "months": months,
                "rub": float(rub),
                "usd": round(float(rub) / MINIAPP_USD_RUB_RATE, 2),
            }
        )

    return {
        "ok": True,
        "usdRubRate": MINIAPP_USD_RUB_RATE,
        "totalStars": int(total_stars),
        "totalPremiumMonths": int(total_premium_months),
        "totals": {
            "totalStars": int(total_stars),
            "totalPremiumMonths": int(total_premium_months),
        },
        "starRates": {
            "50_75": star_rates.get("50_75", 1.7),
            "76_100": star_rates.get("76_100", 1.6),
            "101_250": star_rates.get("101_250", 1.55),
            "251_plus": star_rates.get("251_plus", 1.5),
        },
        "stars": stars,
        "premium": premium,
    }


async def miniapp_config_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)
    return miniapp_json_response(build_miniapp_config_payload())


async def miniapp_reviews_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    limit_raw = str(request.query.get("limit") or "").strip()
    try:
        limit = int(limit_raw) if limit_raw else 10
    except ValueError:
        limit = 10

    return miniapp_json_response(
        {
            "ok": True,
            "data": {
                "items": get_miniapp_reviews_payload(limit=limit),
            },
        }
    )


async def miniapp_review_avatar_handler(request: web.Request) -> web.Response:
    review_id_raw = str(request.match_info.get("review_id") or "").strip()
    if not review_id_raw.isdigit():
        return web.Response(status=404, headers=CORS_HEADERS)
    review_id = int(review_id_raw)

    cursor.execute(
        "SELECT avatar_file_id FROM miniapp_reviews WHERE id = ? LIMIT 1",
        (review_id,),
    )
    row = cursor.fetchone()
    avatar_file_id = str((row or [""])[0] or "").strip()
    if not avatar_file_id:
        return web.Response(status=404, headers=CORS_HEADERS)

    try:
        file_meta = await bot.get_file(avatar_file_id)
    except Exception:
        return web.Response(status=404, headers=CORS_HEADERS)

    file_path = str(getattr(file_meta, "file_path", "") or "").strip()
    if not file_path:
        return web.Response(status=404, headers=CORS_HEADERS)

    file_url = f"https://api.telegram.org/file/bot{API_TOKEN}/{file_path}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(file_url) as response:
                if response.status != 200:
                    return web.Response(status=404, headers=CORS_HEADERS)
                image_bytes = await response.read()
                content_type = str(response.headers.get("Content-Type") or "image/jpeg")
    except Exception:
        return web.Response(status=502, headers=CORS_HEADERS)

    return web.Response(
        body=image_bytes,
        status=200,
        headers={
            **CORS_HEADERS,
            "Cache-Control": "public, max-age=300",
            "Content-Type": content_type,
        },
    )


async def miniapp_support_photo_handler(request: web.Request) -> web.Response:
    message_id_raw = str(request.match_info.get("message_id") or "").strip()
    if not message_id_raw.isdigit():
        return web.Response(status=404, headers=CORS_HEADERS)
    message_id = int(message_id_raw)

    cursor.execute(
        """
        SELECT photo_blob, photo_mime
        FROM miniapp_support_messages
        WHERE id = ?
        LIMIT 1
        """,
        (message_id,),
    )
    row = cursor.fetchone()
    if not row:
        return web.Response(status=404, headers=CORS_HEADERS)

    image_blob = row[0]
    mime_type = str(row[1] or "").strip().lower() or "image/jpeg"
    if not image_blob:
        return web.Response(status=404, headers=CORS_HEADERS)

    try:
        body = bytes(image_blob)
    except Exception:
        return web.Response(status=404, headers=CORS_HEADERS)

    return web.Response(
        body=body,
        status=200,
        headers={
            **CORS_HEADERS,
            "Content-Type": mime_type,
            "Cache-Control": "private, max-age=300",
        },
    )


async def miniapp_support_action_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    init_data = str(payload.get("initData") or "").strip()
    if not init_data:
        init_data = str(request.headers.get("X-Telegram-Init-Data") or "").strip()
    if not init_data:
        return miniapp_json_response({"ok": False, "error": "Missing Telegram init data"}, status=400)

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = int(telegram_user.get("id") or 0)
    if user_id <= 0:
        return miniapp_json_response({"ok": False, "error": "Invalid Telegram user id"}, status=401)

    upsert_user_from_telegram(telegram_user)

    action = str(payload.get("action") or "").strip()
    action_payload = payload.get("payload")
    if not isinstance(action_payload, dict):
        action_payload = {}

    try:
        if action == "support_state":
            data = _support_get_user_state_payload(user_id, messages_limit=120)
            return miniapp_json_response({"ok": True, "data": data})

        if action == "support_mark_read":
            chat_id = int(action_payload.get("chatId") or 0)
            _support_mark_user_chat_read(user_id, chat_id=chat_id)
            data = _support_get_user_state_payload(user_id, messages_limit=120)
            return miniapp_json_response({"ok": True, "data": data})

        if action == "support_send":
            text = _support_normalize_text(action_payload.get("text") or "", max_len=1800)
            username_from_form = normalize_target_username(str(action_payload.get("username") or ""))
            image_base64 = str(action_payload.get("imageBase64") or "").strip()
            image_mime = str(action_payload.get("imageMime") or "").strip()
            image_bytes, safe_mime = _support_decode_photo_payload(image_base64, image_mime)

            if not text and not image_bytes:
                raise ValueError("Введите вопрос или добавьте фото")

            chat_id = _support_ensure_chat_for_user(
                telegram_user,
                preferred_username=username_from_form,
            )
            sender_full_name = " ".join(
                part
                for part in (
                    str(telegram_user.get("first_name") or "").strip(),
                    str(telegram_user.get("last_name") or "").strip(),
                )
                if part
            ).strip()
            message_id = _support_insert_message(
                chat_id=int(chat_id),
                sender_user_id=int(user_id),
                sender_role="user",
                sender_username=str(telegram_user.get("username") or ""),
                sender_full_name=sender_full_name,
                text=text,
                photo_blob=image_bytes,
                photo_mime=safe_mime,
            )
            chat_payload = _support_get_admin_chat_payload(int(chat_id), messages_limit=1, mark_admin_read=False).get("chat", {})
            latest_messages = _support_get_chat_messages(int(chat_id), limit=1) if message_id > 0 else []
            message_payload = latest_messages[-1] if latest_messages else {}

            await _support_notify_admins_new_message(chat_payload, message_payload)
            await miniapp_broadcast_event(
                "support_update",
                {
                    "kind": "message",
                    "chatId": int(chat_id),
                    "userId": int(user_id),
                    "messageId": int(message_id),
                    "actorRole": "user",
                },
            )

            data = _support_get_user_state_payload(user_id, messages_limit=120)
            return miniapp_json_response({"ok": True, "data": data})

        return miniapp_json_response({"ok": False, "error": "Unknown support action"}, status=400)

    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return miniapp_json_response({"ok": False, "error": f"Support action failed: {error}"}, status=500)


async def miniapp_profile_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    init_data = ""
    if request.method == "POST":
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        init_data = str(payload.get("initData") or "").strip()

    if not init_data:
        init_data = str(request.headers.get("X-Telegram-Init-Data") or "").strip()

    if not init_data:
        return miniapp_json_response(
            {"ok": False, "error": "Missing Telegram init data"},
            status=400,
        )

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = telegram_user["id"]
    upsert_user_from_telegram(telegram_user)

    username_raw = (telegram_user.get("username") or "").strip().lstrip("@")
    username_value = f"@{username_raw}" if username_raw else ""
    first_name = (telegram_user.get("first_name") or "").strip()
    last_name = (telegram_user.get("last_name") or "").strip()
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()

    return miniapp_json_response(
        {
            "ok": True,
            "isAdmin": user_id in ADMIN_IDS,
            "user": {
                "id": int(user_id),
                "username": username_value,
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name,
            },
            "stats": get_miniapp_profile_stats(user_id),
            "history": get_miniapp_purchase_history_payload(user_id, limit=20),
        }
    )


async def miniapp_admin_open_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    init_data = str(payload.get("initData") or "").strip()
    if not init_data:
        init_data = str(request.headers.get("X-Telegram-Init-Data") or "").strip()

    if not init_data:
        return miniapp_json_response(
            {"ok": False, "error": "Missing Telegram init data"},
            status=400,
        )

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = int(telegram_user["id"])
    if user_id not in ADMIN_IDS:
        return miniapp_json_response({"ok": False, "error": "Access denied"}, status=403)

    try:
        await bot.send_message(
            user_id,
            build_admin_panel_text(),
            parse_mode="HTML",
            reply_markup=build_admin_panel_keyboard(),
        )
    except Exception as error:
        return miniapp_json_response(
            {"ok": False, "error": f"Failed to open admin panel: {error}"},
            status=500,
        )

    return miniapp_json_response({"ok": True})


async def miniapp_admin_rates_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    init_data = str(payload.get("initData") or "").strip()
    if not init_data:
        init_data = str(request.headers.get("X-Telegram-Init-Data") or "").strip()

    if not init_data:
        return miniapp_json_response(
            {"ok": False, "error": "Missing Telegram init data"},
            status=400,
        )

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = int(telegram_user["id"])
    if user_id not in ADMIN_IDS:
        return miniapp_json_response({"ok": False, "error": "Access denied"}, status=403)

    incoming_rates = payload.get("starRates")
    allowed_ranges = ("50_75", "76_100", "101_250", "251_plus")

    if incoming_rates is not None:
        if not isinstance(incoming_rates, dict):
            return miniapp_json_response({"ok": False, "error": "Invalid starRates payload"}, status=400)

        parsed_rates = {}
        for range_name in allowed_ranges:
            raw_value = incoming_rates.get(range_name)
            try:
                numeric = float(raw_value)
            except (TypeError, ValueError):
                return miniapp_json_response(
                    {"ok": False, "error": f"Invalid rate value for {range_name}"},
                    status=400,
                )

            if numeric <= 0:
                return miniapp_json_response(
                    {"ok": False, "error": f"Rate must be positive for {range_name}"},
                    status=400,
                )
            parsed_rates[range_name] = round(numeric, 3)

        for range_name, rate_value in parsed_rates.items():
            set_star_rate(range_name, rate_value)

        try:
            _admin_log_action(
                admin_user_id=int(user_id),
                admin_username=str(telegram_user.get("username") or "").strip(),
                action="rates_update",
                payload={"starRates": parsed_rates},
                status="success",
            )
        except Exception:
            pass

    current_rates = get_all_star_rates()
    market_rates = await _admin_get_spend_market_rates_payload()
    return miniapp_json_response(
        {
            "ok": True,
            "starRates": {
                "50_75": round(float(current_rates.get("50_75", 1.7)), 3),
                "76_100": round(float(current_rates.get("76_100", 1.6)), 3),
                "101_250": round(float(current_rates.get("101_250", 1.55)), 3),
                "251_plus": round(float(current_rates.get("251_plus", 1.5)), 3),
            },
            "marketRates": market_rates,
        }
    )


def _admin_parse_user_id(raw_value) -> int:
    try:
        user_id = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError("Некорректный ID пользователя")
    if user_id <= 0:
        raise ValueError("ID пользователя должен быть положительным")
    return user_id


def _admin_resolve_user_id(raw_value) -> int:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        raise ValueError("Введите Telegram ID или @username")

    if re.fullmatch(r"\d+", raw_text):
        return _admin_parse_user_id(raw_text)

    username = raw_text.lstrip("@").strip().lower()
    if not username:
        raise ValueError("Некорректный @username")

    cursor.execute(
        """
        SELECT user_id
        FROM users
        WHERE LOWER(REPLACE(TRIM(username), '@', '')) = ?
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (username,),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise ValueError("Пользователь с таким @username не найден")

    return _admin_parse_user_id(row[0])


def _admin_parse_positive_int(raw_value, *, field_name: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"Некорректное поле: {field_name}")
    if value <= 0:
        raise ValueError(f"{field_name} должно быть больше нуля")
    return value


def _admin_parse_non_negative_int(raw_value, *, field_name: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"Некорректное поле: {field_name}")
    if value < 0:
        raise ValueError(f"{field_name} не может быть отрицательным")
    return value


def _admin_get_stars_total() -> int:
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _admin_get_premium_months_total() -> int:
    cursor.execute("SELECT total FROM total_premium_months WHERE id = 1")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _admin_get_star_rates_payload() -> dict:
    rates = get_all_star_rates()
    return {
        "50_75": round(float(rates.get("50_75", 1.7)), 3),
        "76_100": round(float(rates.get("76_100", 1.6)), 3),
        "101_250": round(float(rates.get("101_250", 1.55)), 3),
        "251_plus": round(float(rates.get("251_plus", 1.5)), 3),
    }


def _admin_get_admins_payload() -> list:
    items = []
    for admin_id in ADMIN_IDS:
        cursor.execute(
            """
            SELECT username, first_name, last_name
            FROM users
            WHERE user_id = ?
            """,
            (int(admin_id),),
        )
        row = cursor.fetchone()
        username = str(row[0] or "").strip() if row else ""
        first_name = str(row[1] or "").strip() if row else ""
        last_name = str(row[2] or "").strip() if row else ""
        full_name = " ".join(part for part in [first_name, last_name] if part).strip()
        items.append(
            {
                "id": int(admin_id),
                "username": username,
                "fullName": full_name,
                "isOwner": int(admin_id) == int(OWNER_ADMIN_ID),
            }
        )
    return items


def _admin_log_serialize_value(value, *, depth: int = 0):
    if depth >= 3:
        return "<...>"
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        clean = value.strip()
        return clean[:240] + ("..." if len(clean) > 240 else "")
    if isinstance(value, (list, tuple)):
        limited = list(value)[:12]
        return [_admin_log_serialize_value(item, depth=depth + 1) for item in limited]
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text in {"initData", "imageBase64"}:
                if key_text == "imageBase64":
                    size = len(str(item or ""))
                    sanitized[key_text] = f"<omitted:{size} chars>"
                continue
            sanitized[key_text] = _admin_log_serialize_value(item, depth=depth + 1)
        return sanitized
    return str(value)[:240]


def _admin_log_action(
    *,
    admin_user_id: int,
    admin_username: str,
    action: str,
    payload: dict,
    status: str,
    error_text: str = "",
) -> None:
    safe_payload = _admin_log_serialize_value(payload or {})
    cursor.execute(
        """
        INSERT INTO admin_action_logs
            (admin_user_id, admin_username, action, payload, status, error_text)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(admin_user_id),
            str(admin_username or "").strip(),
            str(action or "").strip(),
            json.dumps(safe_payload, ensure_ascii=False),
            str(status or "success").strip().lower(),
            str(error_text or "").strip(),
        ),
    )
    conn.commit()


def _admin_build_broadcast_keyboard(raw_buttons):
    if not raw_buttons:
        return None

    if not isinstance(raw_buttons, list):
        raise ValueError("Кнопки должны быть списком")

    inline_rows = []
    for item in raw_buttons:
        if not isinstance(item, dict):
            raise ValueError("Некорректный формат кнопки")
        button_text = str(item.get("text") or "").strip()
        button_url = str(item.get("url") or "").strip()
        if not button_text or not button_url:
            raise ValueError("У каждой кнопки должны быть текст и ссылка")
        if not re.match(r"^(https?://|tg://)", button_url, flags=re.IGNORECASE):
            raise ValueError("Ссылка кнопки должна начинаться с http://, https:// или tg://")
        inline_rows.append([InlineKeyboardButton(text=button_text, url=button_url)])

    if not inline_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=inline_rows)


async def miniapp_admin_action_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    init_data = str(payload.get("initData") or "").strip()
    if not init_data:
        init_data = str(request.headers.get("X-Telegram-Init-Data") or "").strip()
    if not init_data:
        return miniapp_json_response({"ok": False, "error": "Missing Telegram init data"}, status=400)

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    admin_user_id = int(telegram_user["id"])
    if admin_user_id not in ADMIN_IDS:
        return miniapp_json_response({"ok": False, "error": "Access denied"}, status=403)

    action = str(payload.get("action") or "").strip()
    action_payload = payload.get("payload")
    if not isinstance(action_payload, dict):
        action_payload = {}
    action_status = "success"
    action_error = ""

    try:
        if action == "stats_users":
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = int(cursor.fetchone()[0] or 0)
            return miniapp_json_response({"ok": True, "data": {"totalUsers": total_users}})

        if action == "stats_users_list":
            query = str(action_payload.get("query") or "").strip()
            normalized_query = query.lstrip("@").strip()
            try:
                page = int(action_payload.get("page", 1))
            except (TypeError, ValueError):
                page = 1
            try:
                page_size = int(action_payload.get("limit", 8))
            except (TypeError, ValueError):
                page_size = 8

            page = max(1, page)
            page_size = max(1, min(page_size, 8))

            where_clause = ""
            where_params = []
            if query:
                if normalized_query:
                    lowered = normalized_query.lower()
                    like_value = f"%{lowered}%"
                    if normalized_query.isdigit():
                        where_clause = "WHERE user_id = ? OR lower(COALESCE(username, '')) LIKE ?"
                        where_params = [int(normalized_query), like_value]
                    else:
                        where_clause = "WHERE lower(COALESCE(username, '')) LIKE ?"
                        where_params = [like_value]

            cursor.execute(
                f"SELECT COUNT(*) FROM users {where_clause}",
                tuple(where_params),
            )
            total_users = int(cursor.fetchone()[0] or 0)
            total_pages = max(1, (total_users + page_size - 1) // page_size)
            page = min(page, total_pages)
            offset = (page - 1) * page_size

            cursor.execute(
                f"""
                SELECT user_id, username, first_name, last_name
                FROM users
                {where_clause}
                ORDER BY user_id DESC
                LIMIT ? OFFSET ?
                """,
                tuple(where_params + [page_size, offset]),
            )
            rows = cursor.fetchall()
            users = []
            for user_id, username, first_name, last_name in rows:
                full_name = " ".join(
                    part
                    for part in (str(first_name or "").strip(), str(last_name or "").strip())
                    if part
                ).strip()
                users.append(
                    {
                        "id": int(user_id or 0),
                        "username": str(username or "").strip(),
                        "fullName": full_name,
                    }
                )

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "items": users,
                        "page": page,
                        "pageSize": page_size,
                        "total": total_users,
                        "totalPages": total_pages,
                        "query": query,
                    },
                }
            )

        if action == "stats_user_profile":
            user_id = _admin_resolve_user_id(action_payload.get("userId"))
            try:
                history_limit = int(action_payload.get("historyLimit", 40))
            except (TypeError, ValueError):
                history_limit = 40
            history_limit = max(1, min(history_limit, 80))

            cursor.execute(
                """
                SELECT user_id, username, first_name, last_name
                FROM users
                WHERE user_id = ?
                LIMIT 1
                """,
                (user_id,),
            )
            user_row = cursor.fetchone()
            if not user_row:
                raise ValueError("Пользователь не найден")

            row_user_id, row_username, row_first_name, row_last_name = user_row
            full_name = " ".join(
                part
                for part in (str(row_first_name or "").strip(), str(row_last_name or "").strip())
                if part
            ).strip()
            user_payload = {
                "id": int(row_user_id or 0),
                "username": str(row_username or "").strip(),
                "fullName": full_name,
            }

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "user": user_payload,
                        "stats": get_miniapp_profile_stats(user_id),
                        "history": get_miniapp_purchase_history_payload(user_id, limit=history_limit),
                    },
                }
            )

        if action == "operations_list":
            try:
                page = int(action_payload.get("page", 1))
            except (TypeError, ValueError):
                page = 1
            try:
                limit = int(action_payload.get("limit", 8))
            except (TypeError, ValueError):
                limit = 8
            query = str(action_payload.get("query") or "").strip()
            payload_data = get_admin_operations_payload(page=page, limit=limit, query=query)
            return miniapp_json_response({"ok": True, "data": payload_data})

        if action == "support_chats_list":
            try:
                page = int(action_payload.get("page", 1))
            except (TypeError, ValueError):
                page = 1
            try:
                limit = int(action_payload.get("limit", 12))
            except (TypeError, ValueError):
                limit = 12
            query = str(action_payload.get("query") or "").strip()
            payload_data = _support_get_admin_chats_payload(page=page, limit=limit, query=query)
            return miniapp_json_response({"ok": True, "data": payload_data})

        if action == "support_chat_open":
            chat_id = _admin_parse_positive_int(action_payload.get("chatId"), field_name="chatId")
            try:
                messages_limit = int(action_payload.get("messagesLimit", 160))
            except (TypeError, ValueError):
                messages_limit = 160
            payload_data = _support_get_admin_chat_payload(
                chat_id=chat_id,
                messages_limit=messages_limit,
                mark_admin_read=True,
            )
            await _support_clear_admin_notice_for_admin(chat_id=chat_id, admin_user_id=admin_user_id)
            return miniapp_json_response({"ok": True, "data": payload_data})

        if action == "support_chat_send":
            chat_id = _admin_parse_positive_int(action_payload.get("chatId"), field_name="chatId")
            text = _support_normalize_text(action_payload.get("text") or "", max_len=1800)
            image_base64 = str(action_payload.get("imageBase64") or "").strip()
            image_mime = str(action_payload.get("imageMime") or "").strip()
            image_bytes, safe_mime = _support_decode_photo_payload(image_base64, image_mime)
            if not text and not image_bytes:
                raise ValueError("Введите сообщение или добавьте фото")

            chat_row = _support_get_chat_row(chat_id)
            if not chat_row:
                raise ValueError("Чат не найден")

            sender_full_name = " ".join(
                part
                for part in (
                    str(telegram_user.get("first_name") or "").strip(),
                    str(telegram_user.get("last_name") or "").strip(),
                )
                if part
            ).strip()
            message_id = _support_insert_message(
                chat_id=chat_id,
                sender_user_id=int(admin_user_id),
                sender_role="admin",
                sender_username=str(telegram_user.get("username") or ""),
                sender_full_name=sender_full_name or "Администратор",
                text=text,
                photo_blob=image_bytes,
                photo_mime=safe_mime,
            )
            target_user_id = int(chat_row[1] or 0)
            await miniapp_broadcast_event(
                "support_update",
                {
                    "kind": "message",
                    "chatId": int(chat_id),
                    "userId": int(target_user_id),
                    "messageId": int(message_id),
                    "actorRole": "admin",
                },
            )
            payload_data = _support_get_admin_chat_payload(
                chat_id=chat_id,
                messages_limit=160,
                mark_admin_read=False,
            )
            return miniapp_json_response({"ok": True, "data": payload_data})

        if action == "support_chat_rename":
            chat_id = _admin_parse_positive_int(action_payload.get("chatId"), field_name="chatId")
            title = str(action_payload.get("title") or "")
            chat_payload = _support_rename_chat(chat_id, title)
            await miniapp_broadcast_event(
                "support_update",
                {
                    "kind": "chat_renamed",
                    "chatId": int(chat_payload.get("id") or 0),
                    "userId": int(chat_payload.get("userId") or 0),
                    "title": str(chat_payload.get("title") or ""),
                },
            )
            return miniapp_json_response({"ok": True, "data": {"chat": chat_payload}})

        if action == "support_chat_delete":
            chat_id = _admin_parse_positive_int(action_payload.get("chatId"), field_name="chatId")
            deleted_payload = _support_delete_chat(chat_id)
            await miniapp_broadcast_event(
                "support_update",
                {
                    "kind": "chat_deleted",
                    "chatId": int(deleted_payload.get("chatId") or 0),
                    "userId": int(deleted_payload.get("userId") or 0),
                },
            )
            return miniapp_json_response({"ok": True, "data": {"deleted": True, **deleted_payload}})

        if action == "admin_logs_list":
            try:
                page = int(action_payload.get("page", 1))
            except (TypeError, ValueError):
                page = 1
            try:
                limit = int(action_payload.get("limit", 20))
            except (TypeError, ValueError):
                limit = 20

            page = max(1, page)
            limit = max(1, min(limit, 50))

            cursor.execute("SELECT COUNT(*) FROM admin_action_logs")
            total_items = int(cursor.fetchone()[0] or 0)
            total_pages = max(1, (total_items + limit - 1) // limit)
            page = min(page, total_pages)
            offset = (page - 1) * limit

            cursor.execute(
                """
                SELECT id, admin_user_id, admin_username, action, payload, status, error_text, created_at
                FROM admin_action_logs
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
            rows = cursor.fetchall()
            items = []
            for row in rows:
                raw_payload = str(row[4] or "").strip()
                details = ""
                if raw_payload:
                    try:
                        payload_obj = json.loads(raw_payload)
                        details = json.dumps(payload_obj, ensure_ascii=False)
                    except Exception:
                        details = raw_payload
                if len(details) > 280:
                    details = details[:280] + "..."
                items.append(
                    {
                        "id": int(row[0] or 0),
                        "adminUserId": int(row[1] or 0),
                        "adminUsername": str(row[2] or "").strip(),
                        "action": str(row[3] or "").strip(),
                        "details": details,
                        "status": str(row[5] or "success").strip().lower(),
                        "errorText": str(row[6] or "").strip(),
                        "createdAt": str(row[7] or ""),
                    }
                )

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "items": items,
                        "page": page,
                        "pageSize": limit,
                        "total": total_items,
                        "totalPages": total_pages,
                    },
                }
            )

        if action == "admins_list":
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "items": _admin_get_admins_payload(),
                        "ownerId": int(OWNER_ADMIN_ID),
                        "canManage": int(admin_user_id) == int(OWNER_ADMIN_ID),
                    },
                }
            )

        if action == "admin_add":
            if int(admin_user_id) != int(OWNER_ADMIN_ID):
                raise PermissionError("Только владелец может добавлять админов")

            user_id = _admin_resolve_user_id(action_payload.get("userId"))
            changed = False
            if user_id not in ADMIN_IDS:
                ADMIN_IDS.append(user_id)
                _save_admin_ids_to_settings()
                changed = True

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "changed": changed,
                        "userId": user_id,
                        "items": _admin_get_admins_payload(),
                    },
                }
            )

        if action == "admin_remove":
            if int(admin_user_id) != int(OWNER_ADMIN_ID):
                raise PermissionError("Только владелец может удалять админов")

            user_id = _admin_resolve_user_id(action_payload.get("userId"))
            if user_id == int(OWNER_ADMIN_ID):
                raise ValueError("Нельзя удалить владельца")

            if user_id not in ADMIN_IDS:
                raise ValueError("Пользователь не является админом")

            ADMIN_IDS[:] = [admin_id for admin_id in ADMIN_IDS if int(admin_id) != user_id]
            _save_admin_ids_to_settings()

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "changed": True,
                        "userId": user_id,
                        "items": _admin_get_admins_payload(),
                    },
                }
            )

        if action == "stats_sales":
            today_date = app_today_str()
            cursor.execute(
                """
                SELECT item_type, SUM(amount), SUM(cost)
                FROM purchases
                WHERE date(created_at) = ?
                GROUP BY item_type
                """,
                (today_date,),
            )
            rows = cursor.fetchall()

            star_cost = get_star_cost()
            total_revenue = 0.0
            total_cost_price = 0.0
            items = []
            for item_type, total_amount, total_cost in rows:
                amount_value = int(total_amount or 0)
                revenue_value = float(total_cost or 0.0)
                total_revenue += revenue_value

                if item_type == "stars":
                    cost_price = amount_value * float(star_cost)
                elif item_type == "premium":
                    cost_price = 0.0
                    cursor.execute(
                        """
                        SELECT amount FROM purchases
                        WHERE item_type='premium' AND date(created_at) = ?
                        """,
                        (today_date,),
                    )
                    premium_rows = cursor.fetchall()
                    months_map = {3: 1000, 6: 1325, 12: 2400}
                    for (months_value,) in premium_rows:
                        cost_price += float(months_map.get(int(months_value or 0), 0))
                else:
                    cost_price = 0.0

                total_cost_price += cost_price
                items.append(
                    {
                        "type": str(item_type or "").strip().lower() or "stars",
                        "amount": amount_value,
                        "revenue": round(revenue_value, 2),
                        "costPrice": round(cost_price, 2),
                    }
                )

            total_revenue = round(total_revenue * 0.97, 2)
            total_cost_price = round(total_cost_price, 2)
            profit = round(total_revenue - total_cost_price, 2)

            cursor.execute(
                """
                SELECT username, SUM(cost) as total_spent
                FROM purchases
                WHERE date(created_at) = ?
                GROUP BY user_id
                ORDER BY total_spent DESC
                LIMIT 5
                """,
                (today_date,),
            )
            top_buyers_rows = cursor.fetchall()
            top_buyers = [
                {
                    "username": (str(row[0]).strip() if row[0] else "—"),
                    "totalSpent": round(float(row[1] or 0.0), 2),
                }
                for row in top_buyers_rows
            ]

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "items": items,
                        "totalRevenue": total_revenue,
                        "totalCostPrice": total_cost_price,
                        "profit": profit,
                        "topBuyers": top_buyers,
                    },
                }
            )

        if action == "stats_clear":
            threshold_24h = app_datetime_threshold_str(days=1)
            cursor.execute(
                """
                SELECT COUNT(*) FROM purchases
                WHERE datetime(created_at) >= datetime(?)
                """,
                (threshold_24h,),
            )
            count = int(cursor.fetchone()[0] or 0)
            if count > 0:
                cursor.execute(
                    """
                    DELETE FROM purchases
                    WHERE datetime(created_at) >= datetime(?)
                    """,
                    (threshold_24h,),
                )
                conn.commit()
            return miniapp_json_response({"ok": True, "data": {"deletedCount": count}})

        if action == "promo_list":
            cursor.execute(
                """
                SELECT code, discount_percent, min_stars, expires_at, max_uses, uses_count, condition, target,
                       max_uses_per_user, max_free_stars, effect_type, effect_value
                FROM promo_codes
                ORDER BY code ASC
                """,
            )
            rows = cursor.fetchall()
            promos = []
            today = app_today()
            for row in rows:
                effect_type, effect_value = _normalize_promo_effect(
                    row[10], row[11], row[1], row[9]
                )
                expires_at_text = str(row[3] or "")
                is_expired = False
                if expires_at_text:
                    try:
                        is_expired = datetime.datetime.strptime(expires_at_text, "%Y-%m-%d").date() < today
                    except ValueError:
                        is_expired = False
                max_uses_value = int(row[4] or 0)
                uses_count_value = int(row[5] or 0)
                is_limit_reached = max_uses_value > 0 and uses_count_value >= max_uses_value
                remaining_value = max(max_uses_value - uses_count_value, 0)

                if is_expired:
                    status_value = "expired"
                elif is_limit_reached:
                    status_value = "limit"
                else:
                    status_value = "active"

                promos.append(
                    {
                        "code": str(row[0] or ""),
                        "discount": int(row[1] or 0),
                        "minStars": int(row[2] or 0),
                        "expiresAt": expires_at_text,
                        "maxUses": max_uses_value,
                        "usesCount": uses_count_value,
                        "remainingUses": remaining_value,
                        "status": status_value,
                        "condition": str(row[6] or "all"),
                        "target": str(row[7] or "stars"),
                        "maxUsesPerUser": int(row[8] or 1),
                        "maxFreeStars": int(row[9] or 0),
                        "effectType": effect_type,
                        "effectValue": int(effect_value or 0),
                    }
                )
            return miniapp_json_response({"ok": True, "data": {"promos": promos}})

        if action == "promo_create":
            code = str(action_payload.get("code") or "").strip().upper()
            if not re.fullmatch(r"[A-Z0-9_]{3,32}", code):
                raise ValueError("Код промокода: 3-32 символа A-Z, 0-9, _")

            effect_type = str(action_payload.get("effectType") or "").strip().lower()
            if effect_type not in {"discount_percent", "free_stars"}:
                legacy_free_stars = int(action_payload.get("maxFreeStars") or 0)
                effect_type = "free_stars" if legacy_free_stars > 0 else "discount_percent"

            raw_effect_value = action_payload.get("effectValue")
            if raw_effect_value is None:
                raw_effect_value = (
                    action_payload.get("maxFreeStars")
                    if effect_type == "free_stars"
                    else action_payload.get("discount")
                )
            effect_value = _admin_parse_positive_int(raw_effect_value, field_name="effectValue")
            if effect_type == "discount_percent" and effect_value > 100:
                raise ValueError("Для скидки значение effectValue должно быть от 1 до 100")

            min_stars = _admin_parse_non_negative_int(action_payload.get("minStars"), field_name="minStars")
            expires_at = str(action_payload.get("expiresAt") or "").strip()
            try:
                datetime.datetime.strptime(expires_at, "%Y-%m-%d")
            except ValueError:
                raise ValueError("Некорректная дата expiresAt (ожидается YYYY-MM-DD)")

            max_uses = _admin_parse_positive_int(action_payload.get("maxUses"), field_name="maxUses")
            max_uses_per_user = _admin_parse_positive_int(action_payload.get("maxUsesPerUser"), field_name="maxUsesPerUser")
            condition = str(action_payload.get("condition") or "all").strip().lower()
            if condition not in {"all", "buyers"}:
                raise ValueError("condition должен быть all или buyers")
            target = str(action_payload.get("target") or "stars").strip().lower()
            if target not in {"stars", "premium", "ton", "all"}:
                raise ValueError("target должен быть stars, premium, ton или all")
            if effect_type == "free_stars" and target not in {"stars", "all"}:
                raise ValueError("Бесплатные звёзды можно применять только к stars или all")

            discount = effect_value if effect_type == "discount_percent" else 100
            max_free_stars = effect_value if effect_type == "free_stars" else 0

            cursor.execute(
                """
                INSERT OR REPLACE INTO promo_codes
                (
                    code, discount_percent, min_stars, expires_at, max_uses, uses_count,
                    condition, max_free_stars, target, max_uses_per_user, effect_type, effect_value
                )
                VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code,
                    discount,
                    min_stars,
                    expires_at,
                    max_uses,
                    condition,
                    max_free_stars,
                    target,
                    max_uses_per_user,
                    effect_type,
                    effect_value,
                ),
            )
            conn.commit()
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "code": code,
                        "effectType": effect_type,
                        "effectValue": effect_value,
                    },
                }
            )

        if action == "promo_delete":
            code = str(action_payload.get("code") or "").strip().upper()
            if not code:
                raise ValueError("Укажите код промокода")
            cursor.execute("DELETE FROM promo_codes WHERE code=?", (code,))
            deleted_count = int(cursor.rowcount or 0)
            cursor.execute("DELETE FROM used_promo WHERE code=?", (code,))
            cursor.execute("DELETE FROM promo_usages WHERE code=?", (code,))
            conn.commit()
            if deleted_count == 0:
                raise ValueError("Промокод не найден")
            return miniapp_json_response({"ok": True, "data": {"code": code, "deletedCount": deleted_count}})

        if action == "referrals_show":
            user_id = _admin_parse_user_id(action_payload.get("userId"))
            cursor.execute(
                "SELECT username FROM users WHERE referred_by=? AND referrals_with_purchase=1",
                (user_id,),
            )
            usernames = [str(row[0]).strip() for row in cursor.fetchall() if row and row[0]]
            return miniapp_json_response({"ok": True, "data": {"userId": user_id, "usernames": usernames}})

        if action == "referrals_reset":
            user_id = _admin_parse_user_id(action_payload.get("userId"))
            cursor.execute(
                "UPDATE users SET referrals_with_purchase=0 WHERE referred_by=? AND referrals_with_purchase=1",
                (user_id,),
            )
            reset_count = int(cursor.rowcount or 0)
            conn.commit()
            return miniapp_json_response({"ok": True, "data": {"userId": user_id, "resetCount": reset_count}})

        if action == "stars_get":
            market_rates = await _admin_get_spend_market_rates_payload()
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "totalStars": _admin_get_stars_total(),
                        "totalPremiumMonths": _admin_get_premium_months_total(),
                        "starRates": _admin_get_star_rates_payload(),
                        "marketRates": market_rates,
                    },
                }
            )

        if action == "procurement_get":
            procurement = await _admin_get_spend_procurement_payload()
            return miniapp_json_response({"ok": True, "data": procurement})

        if action == "stars_update":
            metric = str(action_payload.get("metric") or "stars").strip().lower()
            if metric not in {"stars", "premium"}:
                raise ValueError("metric должен быть stars или premium")
            mode = str(action_payload.get("mode") or "").strip().lower()
            if mode not in {"add", "remove"}:
                raise ValueError("mode должен быть add или remove")
            amount = _admin_parse_positive_int(action_payload.get("amount"), field_name="amount")
            if metric == "stars":
                total_value = _admin_get_stars_total()
            else:
                total_value = _admin_get_premium_months_total()

            if mode == "add":
                new_total = total_value + amount
            else:
                new_total = max(total_value - amount, 0)

            if metric == "stars":
                cursor.execute("UPDATE total_stars SET total = ? WHERE id = 1", (new_total,))
            else:
                cursor.execute("UPDATE total_premium_months SET total = ? WHERE id = 1", (new_total,))
            conn.commit()
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "metric": metric,
                        "totalStars": _admin_get_stars_total(),
                        "totalPremiumMonths": _admin_get_premium_months_total(),
                    },
                }
            )

        if action == "payment_summary":
            try:
                balance = float(await get_pally_balance())
            except Exception:
                balance = 0.0
            cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM payments WHERE status='paid'")
            row = cursor.fetchone() or (0, 0)
            paid_count = int(row[0] or 0)
            paid_amount = round(float(row[1] or 0.0), 2)
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "balance": round(balance, 2),
                        "paidCount": paid_count,
                        "paidAmount": paid_amount,
                    },
                }
            )

        if action == "payment_history":
            limit_raw = action_payload.get("limit", 10)
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                limit = 10
            limit = max(1, min(limit, 100))
            cursor.execute(
                """
                SELECT bill_id, amount, status, description, date, user_id
                FROM payments
                WHERE status='paid'
                ORDER BY date DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            items = []
            for bill_id, amount, status, description, date_value, user_id in rows:
                items.append(
                    {
                        "billId": str(bill_id or ""),
                        "amount": round(float(amount or 0.0), 2),
                        "status": str(status or ""),
                        "description": str(description or ""),
                        "date": format_date(str(date_value or "")),
                        "userId": int(user_id or 0),
                    }
                )
            return miniapp_json_response({"ok": True, "data": {"items": items}})

        if action == "payment_withdraw":
            target_user_id = str(action_payload.get("userId") or "").strip()
            if not target_user_id:
                raise ValueError("Укажите ID для выплаты")
            try:
                amount = float(action_payload.get("amount"))
            except (TypeError, ValueError):
                raise ValueError("Некорректная сумма выплаты")
            if amount <= 0:
                raise ValueError("Сумма выплаты должна быть больше нуля")
            withdraw_result = await initiate_payment_withdraw(amount, target_user_id)
            success = bool(withdraw_result and withdraw_result.get("success"))
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "success": success,
                        "response": withdraw_result or {},
                    },
                }
            )

        if action == "broadcast_send":
            text = str(action_payload.get("text") or "").strip()
            image_base64 = str(action_payload.get("imageBase64") or "").strip()
            image_name = str(action_payload.get("imageName") or "").strip() or "broadcast.jpg"
            keyboard = _admin_build_broadcast_keyboard(action_payload.get("buttons") or [])

            if not text and not image_base64:
                raise ValueError("Добавьте текст и/или фото для рассылки")

            image_bytes = b""
            if image_base64:
                try:
                    image_bytes = base64.b64decode(image_base64, validate=True)
                except Exception:
                    raise ValueError("Некорректное изображение для рассылки")
                if not image_bytes:
                    raise ValueError("Пустое изображение для рассылки")

            cursor.execute("SELECT user_id FROM users")
            users = cursor.fetchall()
            success_count = 0
            fail_count = 0
            for user_row in users:
                try:
                    target_chat_id = int(user_row[0])
                    if image_bytes:
                        photo_file = types.BufferedInputFile(image_bytes, filename=image_name)
                        await bot.send_photo(
                            chat_id=target_chat_id,
                            photo=photo_file,
                            caption=text if text else None,
                            parse_mode="HTML" if text else None,
                            reply_markup=keyboard,
                            disable_web_page_preview=True,
                        )
                    else:
                        await bot.send_message(
                            chat_id=target_chat_id,
                            text=text,
                            parse_mode="HTML",
                            reply_markup=keyboard,
                            disable_web_page_preview=True,
                        )
                    success_count += 1
                    await asyncio.sleep(0.03)
                except Exception:
                    fail_count += 1

            return miniapp_json_response(
                {"ok": True, "data": {"success": success_count, "fail": fail_count}}
            )

        if action == "user_find":
            query = str(action_payload.get("query") or "").strip()
            if not query:
                raise ValueError("Введите username или ID")
            normalized_query = query.lstrip("@")

            if normalized_query.isdigit():
                cursor.execute("SELECT * FROM users WHERE user_id = ?", (int(normalized_query),))
            else:
                cursor.execute("SELECT * FROM users WHERE username = ?", (normalized_query,))
            user_row = cursor.fetchone()
            if not user_row:
                raise ValueError("Пользователь не найден")

            target_user_id = int(user_row[0])
            cursor.execute(
                """
                SELECT item_type, amount, cost, created_at
                FROM purchases
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT 30
                """,
                (target_user_id,),
            )
            purchases_rows = cursor.fetchall()
            purchases = [
                {
                    "itemType": str(row[0] or ""),
                    "amount": int(row[1] or 0),
                    "cost": round(float(row[2] or 0.0), 2),
                    "createdAt": str(row[3] or ""),
                }
                for row in purchases_rows
            ]

            cursor.execute("SELECT code FROM used_promo WHERE user_id = ?", (target_user_id,))
            used_promos = [str(row[0]) for row in cursor.fetchall() if row and row[0]]

            cursor.execute(
                """
                SELECT amount, status, date
                FROM payments
                WHERE user_id = ?
                ORDER BY date DESC
                LIMIT 30
                """,
                (target_user_id,),
            )
            payment_rows = cursor.fetchall()
            payments = [
                {
                    "amount": round(float(row[0] or 0.0), 2),
                    "status": str(row[1] or ""),
                    "date": str(row[2] or ""),
                }
                for row in payment_rows
            ]

            user_payload = {
                "id": target_user_id,
                "username": f"@{user_row[1]}" if user_row[1] else "—",
                "firstName": str(user_row[2] or ""),
                "lastName": str(user_row[3] or ""),
                "refCode": str(user_row[5] or ""),
                "referralsCount": int(user_row[7] or 0),
                "referralsWithPurchase": int(user_row[8] or 0),
            }

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "user": user_payload,
                        "purchases": purchases,
                        "usedPromos": used_promos,
                        "payments": payments,
                    },
                }
            )

        if action == "add_stars":
            user_id = _admin_parse_user_id(action_payload.get("userId"))
            amount = _admin_parse_positive_int(action_payload.get("amount"), field_name="amount")
            cursor.execute(
                """
                INSERT INTO purchases (user_id, username, item_type, amount, cost)
                VALUES (?, ?, 'stars', ?, 0)
                """,
                (user_id, None, amount),
            )
            conn.commit()

            notification_sent = False
            try:
                await bot.send_message(user_id, f"🎁 Вам начислено {amount}⭐️ от администрации!")
                notification_sent = True
            except Exception:
                notification_sent = False

            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "userId": user_id,
                        "amount": amount,
                        "notificationSent": notification_sent,
                    },
                }
            )

        action_status = "error"
        action_error = "Unknown admin action"
        return miniapp_json_response({"ok": False, "error": "Unknown admin action"}, status=400)

    except ValueError as error:
        action_status = "error"
        action_error = str(error)
        return miniapp_json_response({"ok": False, "error": str(error)}, status=400)
    except PermissionError as error:
        action_status = "error"
        action_error = str(error)
        return miniapp_json_response({"ok": False, "error": str(error)}, status=403)
    except Exception as error:
        action_status = "error"
        action_error = str(error)
        return miniapp_json_response({"ok": False, "error": f"Admin action failed: {error}"}, status=500)
    finally:
        try:
            loggable_actions = {
                "admin_add",
                "admin_remove",
                "stats_clear",
                "promo_create",
                "promo_delete",
                "referrals_reset",
                "stars_update",
                "payment_withdraw",
                "broadcast_send",
                "add_stars",
                "support_chat_send",
                "support_chat_rename",
                "support_chat_delete",
            }
            if action in loggable_actions:
                _admin_log_action(
                    admin_user_id=int(admin_user_id),
                    admin_username=str(telegram_user.get("username") or "").strip(),
                    action=action,
                    payload=action_payload,
                    status=action_status,
                    error_text=action_error,
                )
        except Exception:
            pass


async def miniapp_order_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    try:
        payload = await request.json()
    except Exception:
        return miniapp_json_response({"ok": False, "error": "Invalid JSON payload"}, status=400)

    init_data = str(payload.get("initData") or "").strip()
    order_type = str(payload.get("type") or "").strip().lower()
    target_username = normalize_target_username(str(payload.get("username") or ""))
    promo_code = str(payload.get("promoCode") or "").strip().upper()
    preview_raw = payload.get("previewOnly")
    if isinstance(preview_raw, bool):
        preview_only = preview_raw
    elif isinstance(preview_raw, (int, float)):
        preview_only = int(preview_raw) != 0
    else:
        preview_only = str(preview_raw or "").strip().lower() in {"1", "true", "yes", "on"}

    if order_type not in {"stars", "premium"}:
        return miniapp_json_response({"ok": False, "error": "Unknown order type"}, status=400)
    if not target_username and not preview_only:
        return miniapp_json_response({"ok": False, "error": "Введите Telegram username"}, status=400)

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = telegram_user["id"]
    if not target_username and preview_only:
        target_username = normalize_target_username(str(telegram_user.get("username") or f"id{user_id}"))
    if not target_username:
        return miniapp_json_response({"ok": False, "error": "Введите Telegram username"}, status=400)
    buyer_username = telegram_user["username"] or f"id{user_id}"
    upsert_user_from_telegram(telegram_user)
    profile_stats = get_miniapp_profile_stats(user_id)
    profile_level_info = profile_stats.get("levelInfo") if isinstance(profile_stats, dict) else {}
    level_fixed_star_rate = None
    try:
        candidate_fixed_rate = float((profile_level_info or {}).get("fixedStarRateRub"))
        if candidate_fixed_rate > 0:
            level_fixed_star_rate = candidate_fixed_rate
    except (TypeError, ValueError):
        level_fixed_star_rate = None

    if order_type == "stars":
        try:
            amount = int(payload.get("amount"))
        except (TypeError, ValueError):
            return miniapp_json_response({"ok": False, "error": "Количество звёзд должно быть числом"}, status=400)
        if amount < 50 or amount > 10000:
            return miniapp_json_response({"ok": False, "error": "Количество звёзд должно быть от 50 до 10000"}, status=400)
        stars_rate = level_fixed_star_rate if level_fixed_star_rate is not None else get_star_rate_for_range(amount)
        base_rub = round(amount * float(stars_rate), 2)
        amount_label = f"{amount}⭐️"
        promo_target = "stars"
    else:
        months = parse_months_value(str(payload.get("amount") or ""))
        if months not in PREMIUM_PRICES_RUB:
            return miniapp_json_response({"ok": False, "error": "Выберите пакет Premium"}, status=400)
        amount = months
        base_rub = float(PREMIUM_PRICES_RUB[months])
        amount_label = f"{months} мес. Premium"
        promo_target = "premium"

    discount_percent = 0
    final_rub = base_rub
    promo_effect_type = ""
    promo_effect_value = 0
    promo_applied = False
    promo_preview_error = ""
    if promo_code:
        promo_result = await apply_promo_code(
            user_id=user_id,
            promo_code=promo_code,
            target_type=promo_target,
            stars_amount=amount if promo_target == "stars" else 0,
        )
        if not promo_result.get("ok"):
            promo_preview_error = str(promo_result.get("error") or "Промокод недоступен")
            if preview_only:
                preview_usd = round(base_rub / MINIAPP_USD_RUB_RATE, 2)
                return miniapp_json_response(
                    {
                        "ok": True,
                        "preview": True,
                        "price": {
                            "rub": round(base_rub, 2),
                            "usd": preview_usd,
                        },
                        "promo": {
                            "code": promo_code,
                            "discount": 0,
                            "error": promo_preview_error,
                            "effectType": "",
                            "effectValue": 0,
                        },
                    }
                )
            return miniapp_json_response(
                {"ok": False, "error": promo_result.get("error") or "Промокод недоступен"},
                status=400,
            )
        discount_percent = int(promo_result.get("discount", 0))
        final_rub = round(base_rub * (100 - discount_percent) / 100, 2)
        promo_effect_type = str(promo_result.get("effectType") or "")
        promo_effect_value = int(promo_result.get("effectValue") or 0)
        promo_applied = True

    final_usd = round(final_rub / MINIAPP_USD_RUB_RATE, 2)
    if preview_only:
        return miniapp_json_response(
            {
                "ok": True,
                "preview": True,
                "price": {
                    "rub": round(final_rub, 2),
                    "usd": final_usd,
                },
                "promo": {
                    "code": promo_code,
                    "discount": discount_percent,
                    "error": promo_preview_error,
                    "effectType": promo_effect_type,
                    "effectValue": promo_effect_value,
                },
            }
        )

    payment_method = str(payload.get("paymentMethod") or FK_DEFAULT_PAYMENT_SYSTEM or "44").strip()
    if payment_method not in {"36", "43", "44"}:
        payment_method = str(FK_DEFAULT_PAYMENT_SYSTEM or "44").strip() or "44"

    try:
        history_id = add_miniapp_purchase_history(
            user_id=user_id,
            buyer_username=telegram_user.get("username") or "",
            buyer_first_name=telegram_user.get("first_name") or "",
            buyer_last_name=telegram_user.get("last_name") or "",
            target_username=target_username,
            item_type=order_type,
            amount=amount,
            price_rub=round(final_rub, 2),
            price_usd=final_usd,
            promo_code=promo_code,
            promo_discount=discount_percent,
            promo_error="",
            status="pending",
            counters_applied=0,
            source="api",
        )
    except Exception as error:
        logging.error("Failed to save miniapp purchase history: %s", error)
        error_details = re.sub(r"\s+", " ", str(error or "")).strip()
        if error_details:
            error_details = error_details[:180]
        error_message = "Ошибка базы данных при сохранении заявки."
        if error_details:
            error_message = f"{error_message} {error_details}"
        return miniapp_json_response(
            {"ok": False, "error": error_message, "errorCode": "MAPP-ORD-DB-001"},
            status=500,
        )

    operation_created_at = ""
    try:
        cursor.execute(
            "SELECT created_at FROM miniapp_purchase_history WHERE id = ? LIMIT 1",
            (int(history_id),),
        )
        created_row = cursor.fetchone()
        operation_created_at = str((created_row or [""])[0] or "").strip()
    except Exception:
        operation_created_at = ""
    if not operation_created_at:
        operation_created_at = app_now_iso()
    operation_id = build_operation_id("miniapp", history_id, operation_created_at)
    fk_payment_id = f"miniapp-{int(history_id)}"
    fk_email = f"{int(user_id)}@telegram.org"
    fk_client_ip = extract_request_ip(request)
    fk_result = await create_freekassa_order(
        payment_id=fk_payment_id,
        amount_rub=round(final_rub, 2),
        email=fk_email,
        client_ip=fk_client_ip,
        payment_system=payment_method,
    )
    if not fk_result.get("ok"):
        fk_error_code = str(fk_result.get("errorCode") or "FK-API-000").strip().upper()
        fk_error_text = str(fk_result.get("error") or "Не удалось создать счёт").strip()
        fk_stage = str(fk_result.get("stage") or "provider").strip().lower()
        fk_status = int(fk_result.get("statusCode") or 0)
        fk_debug = format_miniapp_fulfill_error_text(
            fk_error_code,
            fk_stage,
            fk_status,
            fk_error_text,
        )
        set_miniapp_purchase_history_error(history_id, fk_debug, status="error")
        logging.warning(
            "FreeKassa create order failed. history_id=%s code=%s stage=%s status=%s error=%s",
            history_id,
            fk_error_code,
            fk_stage,
            fk_status,
            fk_error_text,
        )
        return miniapp_json_response(
            {
                "ok": False,
                "error": fk_error_text,
                "errorCode": fk_error_code,
                "operation": {
                    "id": operation_id,
                    "historyId": int(history_id),
                    "source": "miniapp",
                    "channel": "freekassa_api",
                    "status": "error",
                    "createdAt": operation_created_at,
                },
            },
            status=502,
        )

    fk_payment_url = str(fk_result.get("paymentUrl") or "").strip()
    fk_order_id = str(fk_result.get("orderId") or "").strip()
    payment_description = (
        f"miniapp payment, history_id={int(history_id)}, type={order_type}, "
        f"amount={amount}, target=@{target_username}, provider=freekassa"
    )
    save_payment(
        int(user_id),
        fk_payment_id,
        round(final_rub, 2),
        "pending",
        payment_description,
    )
    set_miniapp_purchase_history_error(history_id, "", status="pending")

    return miniapp_json_response(
        {
            "ok": True,
            "message": "Счёт создан, перейдите к оплате",
            "price": {
                "rub": round(final_rub, 2),
                "usd": final_usd,
            },
            "promo": {
                "code": promo_code,
                "discount": discount_percent,
                "error": "",
                "effectType": promo_effect_type,
                "effectValue": promo_effect_value,
            },
            "payment": {
                "provider": "freekassa",
                "paymentId": fk_payment_id,
                "orderId": fk_order_id,
                "url": fk_payment_url,
                "method": payment_method,
            },
            "operation": {
                "id": operation_id,
                "historyId": int(history_id),
                "source": "miniapp",
                "channel": "freekassa_api",
                "status": "pending",
                "createdAt": operation_created_at,
                "buyerUserId": int(user_id),
                "buyerUsername": str(telegram_user.get("username") or ""),
                "buyerFullName": " ".join(
                    part
                    for part in (
                        str(telegram_user.get("first_name") or "").strip(),
                        str(telegram_user.get("last_name") or "").strip(),
                    )
                    if part
                ).strip(),
                "targetUsername": str(target_username or ""),
                "itemType": str(order_type or ""),
                "amount": int(amount or 0),
            },
        }
    )


def _get_first_payload_value(payload: dict, *keys: str) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        raw_value = payload.get(key)
        if raw_value is None:
            continue
        safe_value = str(raw_value).strip()
        if safe_value:
            return safe_value
    return ""


def _safe_parse_amount_value(raw_value) -> float:
    safe_text = str(raw_value or "").strip().replace(",", ".")
    try:
        amount_value = round(float(safe_text), 2)
    except (TypeError, ValueError):
        amount_value = 0.0
    return amount_value


async def miniapp_freekassa_webhook_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204)

    if not is_freekassa_webhook_configured():
        logging.warning("FreeKassa webhook called but config is incomplete")
        return web.Response(text="webhook is disabled", status=503)

    payload = {}
    try:
        if request.content_type and "application/json" in request.content_type.lower():
            payload = await request.json()
        else:
            post_data = await request.post()
            payload = dict(post_data)
    except Exception as error:
        logging.warning("FreeKassa webhook invalid payload: %s", error)
        return web.Response(text="invalid payload", status=400)

    merchant_id = _get_first_payload_value(payload, "MERCHANT_ID", "merchant_id", "merchantId")
    amount_raw = _get_first_payload_value(payload, "AMOUNT", "amount")
    payment_id = _get_first_payload_value(payload, "MERCHANT_ORDER_ID", "merchant_order_id", "paymentId")
    received_sign = _get_first_payload_value(payload, "SIGN", "sign", "P_SIGN", "p_sign")

    if not merchant_id or not amount_raw or not payment_id or not received_sign:
        logging.warning(
            "FreeKassa webhook missing required fields: merchant=%s amount=%s payment=%s sign=%s",
            bool(merchant_id),
            bool(amount_raw),
            bool(payment_id),
            bool(received_sign),
        )
        return web.Response(text="missing params", status=400)

    if FK_WEBHOOK_STRICT_MERCHANT and FK_MERCHANT_ID and merchant_id != str(FK_MERCHANT_ID):
        logging.warning(
            "FreeKassa webhook merchant mismatch: expected=%s got=%s",
            str(FK_MERCHANT_ID),
            merchant_id,
        )
        return web.Response(text="merchant mismatch", status=403)

    expected_sign = build_freekassa_webhook_signature(merchant_id, amount_raw, payment_id)
    if not expected_sign or not hmac.compare_digest(expected_sign.lower(), received_sign.lower()):
        logging.warning(
            "FreeKassa webhook bad signature. payment_id=%s",
            payment_id,
        )
        return web.Response(text="bad sign", status=403)

    history_id = find_history_id_for_payment(payment_id)
    if history_id <= 0:
        logging.warning("FreeKassa webhook history not found. payment_id=%s", payment_id)
        return web.Response(text="history not found", status=404)

    history_details = get_miniapp_purchase_history_details(history_id)
    if not history_details:
        logging.warning("FreeKassa webhook details not found. history_id=%s", history_id)
        return web.Response(text="history not found", status=404)

    user_id = int(history_details.get("userId") or 0)
    amount_value = _safe_parse_amount_value(amount_raw)
    if amount_value <= 0:
        amount_value = round(float(history_details.get("priceRub") or 0.0), 2)

    payment_description = (
        f"miniapp payment paid, history_id={int(history_id)}, provider=freekassa, "
        f"intid={_get_first_payload_value(payload, 'intid', 'INTID')}"
    )
    save_payment(
        user_id,
        str(payment_id),
        amount_value,
        "paid",
        payment_description,
    )

    promo_result = try_confirm_history_promo_after_payment(history_id)
    if not promo_result.get("ok") and str(promo_result.get("error") or "") == "promo_confirm_failed":
        set_miniapp_purchase_history_error(
            history_id,
            "Промокод не активирован после оплаты: достигнут лимит.",
            status="warning",
        )

    fulfill_result = await try_fragment_fulfill_miniapp_history(history_id)
    if not fulfill_result.get("ok"):
        error_text = str(fulfill_result.get("error") or "fragment auto-fulfillment failed").strip()
        error_code = str(fulfill_result.get("errorCode") or "MAF-UNK-000").strip().upper()
        fail_stage = str(fulfill_result.get("stage") or "provider")
        fail_status = int(fulfill_result.get("statusCode") or 0)
        logging.warning(
            "Webhook auto-fulfill failed. history_id=%s code=%s stage=%s status=%s error=%s",
            history_id,
            error_code,
            fail_stage,
            fail_status,
            error_text,
        )
        await miniapp_broadcast_event(
            "purchase_failed",
            {
                "historyId": int(history_id),
                "operationId": str(fulfill_result.get("operationId") or ""),
                "userId": int(user_id),
                "errorCode": error_code,
                "errorText": error_text,
                "stage": fail_stage,
                "statusCode": fail_status,
            },
        )
        if user_id > 0:
            try:
                await bot.send_message(
                    user_id,
                    f"❌ Оплата получена, но выдача не выполнена автоматически.\nКод: {error_code}",
                )
            except Exception:
                pass
        return web.Response(text="YES", status=200)

    finalize_result = finalize_miniapp_purchase_history(history_id)
    operation_id = str(
        finalize_result.get("operationId")
        or build_operation_id(
            "miniapp",
            int(history_details.get("id") or 0),
            str(history_details.get("createdAt") or ""),
        )
    )

    if finalize_result.get("ok"):
        await miniapp_broadcast_event(
            "purchase_finalized",
            {
                "historyId": int(finalize_result.get("historyId") or 0),
                "operationId": operation_id,
                "userId": int(finalize_result.get("userId") or user_id),
                "itemType": str(finalize_result.get("itemType") or history_details.get("itemType") or ""),
                "amount": int(finalize_result.get("amount") or parse_months_value(str(history_details.get("amountRaw") or ""))),
                "totals": finalize_result.get("totals") or {},
            },
        )
    else:
        logging.warning("Finalize after FreeKassa webhook returned not ok. history_id=%s", history_id)

    amount_label = build_history_amount_label(
        str(history_details.get("itemType") or ""),
        str(history_details.get("amountRaw") or ""),
    )
    try:
        await bot.send_message(
            GROUP_CHAT_ID,
            (
                "✅ <b>Оплата подтверждена (FreeKassa)</b>\n"
                f"🆔 <b>ID операции:</b> <code>{escape(operation_id)}</code>\n"
                f"💳 <b>Платёж:</b> <code>{escape(str(payment_id))}</code>\n"
                f"👤 <b>Покупатель:</b> @{escape(str(history_details.get('buyerUsername') or f'id{user_id}'))}\n"
                f"🎯 <b>Аккаунт:</b> @{escape(str(history_details.get('targetUsername') or ''))}\n"
                f"📦 <b>Товар:</b> {escape(amount_label)}\n"
                f"💰 <b>Сумма:</b> {amount_value:.2f}₽"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass

    if user_id > 0:
        try:
            await bot.send_message(
                user_id,
                "✅ Оплата получена, заказ подтверждён и отправлен в обработку.",
            )
        except Exception:
            pass

    return web.Response(text="YES", status=200)


def create_miniapp_api() -> web.Application:
    app = web.Application(client_max_size=10 * 1024 * 1024)
    app.router.add_route("OPTIONS", "/api/miniapp/{tail:.*}", miniapp_config_handler)
    app.router.add_get("/api/miniapp/config", miniapp_config_handler)
    app.router.add_get("/api/miniapp/events", miniapp_events_handler)
    app.router.add_get("/api/miniapp/reviews", miniapp_reviews_handler)
    app.router.add_get("/api/miniapp/reviews/avatar/{review_id}", miniapp_review_avatar_handler)
    app.router.add_post("/api/miniapp/support/action", miniapp_support_action_handler)
    app.router.add_get("/api/miniapp/support/photo/{message_id}", miniapp_support_photo_handler)
    app.router.add_get("/api/miniapp/profile", miniapp_profile_handler)
    app.router.add_post("/api/miniapp/profile", miniapp_profile_handler)
    app.router.add_post("/api/miniapp/admin/open", miniapp_admin_open_handler)
    app.router.add_post("/api/miniapp/admin/rates", miniapp_admin_rates_handler)
    app.router.add_post("/api/miniapp/admin/action", miniapp_admin_action_handler)
    app.router.add_post("/api/miniapp/order", miniapp_order_handler)
    app.router.add_post("/api/miniapp/freekassa/webhook", miniapp_freekassa_webhook_handler)
    return app


@dp.message(F.web_app_data)
async def handle_webapp_data(message: types.Message):
    raw_data = message.web_app_data.data if message.web_app_data else ""
    try:
        payload = json.loads(raw_data or "{}")
    except json.JSONDecodeError:
        await message.answer("Ошибка: некорректные данные miniapp.")
        return

    order_type = str(payload.get("type") or "").strip().lower()
    target_username = normalize_target_username(str(payload.get("username") or ""))
    promo_code = str(payload.get("promoCode") or "").strip().upper()
    user_id = message.from_user.id if message.from_user else 0
    buyer_username = (message.from_user.username or "").strip() if message.from_user else ""
    buyer_first_name = (message.from_user.first_name or "").strip() if message.from_user else ""
    buyer_last_name = (message.from_user.last_name or "").strip() if message.from_user else ""
    if not buyer_username:
        buyer_username = f"id{user_id}"

    if user_id:
        upsert_user_from_telegram(
            {
                "id": user_id,
                "username": (message.from_user.username or "") if message.from_user else "",
                "first_name": buyer_first_name,
                "last_name": buyer_last_name,
            }
        )

    if order_type not in {"stars", "premium"}:
        await message.answer("Ошибка: неизвестный тип заявки.")
        return
    if not target_username:
        await message.answer("Ошибка: введите Telegram username.")
        return

    if order_type == "stars":
        try:
            amount = int(payload.get("amount"))
        except (TypeError, ValueError):
            await message.answer("Ошибка: количество звёзд должно быть числом.")
            return
        if amount < 50 or amount > 10000:
            await message.answer("Ошибка: количество звёзд должно быть от 50 до 10000.")
            return
        base_rub = round(amount * get_star_rate_for_range(amount), 2)
        amount_label = f"{amount}⭐️"
        promo_target = "stars"
    else:
        months = parse_months_value(str(payload.get("amount") or ""))
        if months not in PREMIUM_PRICES_RUB:
            await message.answer("Ошибка: выберите пакет Premium.")
            return
        amount = months
        base_rub = float(PREMIUM_PRICES_RUB[months])
        amount_label = f"{months} мес. Premium"
        promo_target = "premium"

    discount_percent = 0
    final_rub = base_rub
    promo_effect_type = ""
    promo_effect_value = 0
    promo_applied = False
    promo_confirm_error = ""
    promo_preview_error = ""
    if promo_code:
        promo_result = await apply_promo_code(
            user_id=user_id,
            promo_code=promo_code,
            target_type=promo_target,
            stars_amount=amount if promo_target == "stars" else 0,
        )
        if not promo_result.get("ok"):
            await message.answer(promo_result.get("error") or "Промокод недоступен")
            return
        discount_percent = int(promo_result.get("discount", 0))
        final_rub = round(base_rub * (100 - discount_percent) / 100, 2)
        promo_effect_type = str(promo_result.get("effectType") or "")
        promo_effect_value = int(promo_result.get("effectValue") or 0)
        promo_applied = True

    final_usd = round(final_rub / MINIAPP_USD_RUB_RATE, 2)
    try:
        history_id = add_miniapp_purchase_history(
            user_id=user_id,
            buyer_username=(message.from_user.username or "") if message.from_user else "",
            buyer_first_name=buyer_first_name,
            buyer_last_name=buyer_last_name,
            target_username=target_username,
            item_type=order_type,
            amount=amount,
            price_rub=round(final_rub, 2),
            price_usd=final_usd,
            promo_code=promo_code,
            promo_discount=discount_percent,
            promo_error="",
            status="pending",
            counters_applied=0,
            source="sendData",
        )
    except Exception as error:
        await message.answer(f"Ошибка сохранения истории: {error}")
        return

    message_lines = [
        "🧾 <b>Новая заявка из MINIAPP (sendData)</b>",
        f"👤 <b>Покупатель:</b> @{escape(buyer_username)} (ID: <code>{user_id}</code>)",
        f"🎯 <b>Аккаунт:</b> @{escape(target_username)}",
        f"📦 <b>Товар:</b> {escape(amount_label)}",
        f"💳 <b>Сумма:</b> {final_rub:.2f}₽ / {round(final_rub / MINIAPP_USD_RUB_RATE, 2):.2f}$",
    ]

    if promo_code:
        if promo_effect_type == "free_stars":
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (бесплатно {promo_effect_value}⭐️)"
            )
        elif discount_percent > 0:
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (скидка {discount_percent}%)"
            )
        else:
            message_lines.append(f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code>")

    message_lines.append("⚠️ <i>Оплата отключена, отправляется только заявка.</i>")
    message_text_group = "\n".join(message_lines)

    try:
        await bot.send_message(
            GROUP_CHAT_ID,
            message_text_group,
            parse_mode="HTML",
            reply_markup=done_keyboard(history_id),
        )
    except Exception as error:
        try:
            delete_miniapp_purchase_history(history_id)
        except Exception:
            pass
        await message.answer(f"Ошибка отправки в группу: {error}")
        return

    if promo_applied and promo_code:
        if not confirm_promo_usage(promo_code, user_id):
            promo_confirm_error = "Промокод не активирован: лимит исчерпан после отправки заявки."
            logging.warning(
                "Promo activation failed after sendData group notification. user_id=%s code=%s",
                user_id,
                promo_code,
            )
    if promo_confirm_error:
        await message.answer(f"Заявка отправлена в группу ✅\n{promo_confirm_error}")
        return
    await message.answer("Заявка отправлена в группу ✅")

@dp.callback_query(lambda c: c.data.startswith("change_rate_"))
async def admin_change_rate(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("🚫 Нет доступа", show_alert=True)
        return

    range_map = {
        "change_rate_50_75": "50_75",
        "change_rate_76_100": "76_100",
        "change_rate_101_250": "101_250",
        "change_rate_251_plus": "251_plus"
    }

    if callback.data not in range_map:
        await callback.answer("❌ Ошибка: неверная кнопка", show_alert=True)
        return

    range_name = range_map[callback.data]
    user_states[callback.from_user.id] = {"awaiting_rate_change": range_name}

    await callback.message.answer(
        f"✏️ Введите новый курс для диапазона {range_name.replace('_', '-')} (например, 1.6):"
    )

@dp.callback_query(lambda c: c.data == "open_change_rate_menu")
async def open_change_rate_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Выберите диапазон для изменения курса:",
        reply_markup=change_star_rate_kb  # твоя клавиатура с диапазонами
    )


@dp.message(
    lambda m: bool(getattr(m, "chat", None))
    and int(getattr(m.chat, "id", 0) or 0) in ALLOWED_REVIEWS_GROUP_CHAT_IDS
)
async def ingest_review_group_message(message: types.Message):
    raw_text = ""
    if message.text:
        raw_text = message.text
    elif message.caption:
        raw_text = message.caption
    review_text = normalize_review_text(raw_text)
    if not review_text:
        return

    reviewer_user_id = 0
    reviewer_username = ""
    reviewer_first_name = ""
    reviewer_last_name = ""
    avatar_file_id = ""

    forward_origin = getattr(message, "forward_origin", None)
    forward_from = getattr(message, "forward_from", None)
    forward_sender_name = str(getattr(message, "forward_sender_name", "") or "").strip()
    is_forwarded = bool(forward_origin or forward_from or forward_sender_name)

    source_user = None
    source_user_from_forward = getattr(forward_origin, "sender_user", None) if forward_origin else None
    if source_user_from_forward and not getattr(source_user_from_forward, "is_bot", False):
        source_user = source_user_from_forward
    elif forward_from and not getattr(forward_from, "is_bot", False):
        source_user = forward_from

    if source_user:
        reviewer_user_id = int(getattr(source_user, "id", 0) or 0)
        reviewer_username = str(getattr(source_user, "username", "") or "").strip()
        reviewer_first_name = str(getattr(source_user, "first_name", "") or "").strip()
        reviewer_last_name = str(getattr(source_user, "last_name", "") or "").strip()
    elif not is_forwarded and message.from_user and not getattr(message.from_user, "is_bot", False):
        reviewer_user_id = int(getattr(message.from_user, "id", 0) or 0)
        reviewer_username = str(getattr(message.from_user, "username", "") or "").strip()
        reviewer_first_name = str(getattr(message.from_user, "first_name", "") or "").strip()
        reviewer_last_name = str(getattr(message.from_user, "last_name", "") or "").strip()
    else:
        hidden_name = ""
        if forward_origin:
            hidden_name = str(getattr(forward_origin, "sender_user_name", "") or "").strip()
            if not hidden_name:
                hidden_name = str(getattr(forward_origin, "author_signature", "") or "").strip()
        if not hidden_name:
            hidden_name = forward_sender_name
        if not hidden_name and message.sender_chat:
            hidden_name = str(getattr(message.sender_chat, "title", "") or "").strip()
            reviewer_username = str(getattr(message.sender_chat, "username", "") or "").strip()
            reviewer_user_id = int(getattr(message.sender_chat, "id", 0) or 0)
        reviewer_first_name = hidden_name or "Аноним"

    if reviewer_user_id > 0:
        avatar_file_id = await get_user_avatar_file_id(reviewer_user_id)

    message_date_value = message.date or app_now()
    if isinstance(message_date_value, datetime.datetime):
        try:
            message_date_local = to_app_timezone(message_date_value)
        except Exception:
            message_date_local = app_now()
    else:
        message_date_local = app_now()
    created_at = message_date_local.strftime("%Y-%m-%d %H:%M:%S")

    try:
        upsert_miniapp_review(
            chat_id=int(message.chat.id),
            message_id=int(message.message_id or 0),
            reviewer_user_id=int(reviewer_user_id or 0),
            reviewer_username=reviewer_username,
            reviewer_first_name=reviewer_first_name,
            reviewer_last_name=reviewer_last_name,
            review_text=review_text,
            avatar_file_id=avatar_file_id,
            created_at=created_at,
        )
        await miniapp_broadcast_event(
            "reviews_updated",
            {
                "chatId": int(getattr(message.chat, "id", 0) or 0),
                "messageId": int(message.message_id or 0),
            },
        )
    except Exception as error:
        logging.warning("Failed to ingest review message. message_id=%s error=%s", message.message_id, error)


@dp.message()
async def handle_message(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state:
        return  # Нет активных состояний, ничего не делаем

    # Обработка ввода username для покупки Premium
    if state.get('awaiting_premium_username', False):
        username_input = message.text.strip()

        if not re.fullmatch(r'@[A-Za-z0-9_]+', username_input):
            await message.answer("Некорректный формат username. Пример: @example")
            return

        if user_id not in user_premium_data:
            await message.answer("⚠️ Ошибка: данные о подписке не найдены. Попробуйте выбрать тариф заново.")
            return

        # Сохраняем username
        user_premium_data[user_id]['username'] = username_input
        months = user_premium_data[user_id]['months']
        price = user_premium_data[user_id]['price']

        await message.answer(
            f"<b>Вы уверены, что хотите приобрести Telegram Premium на {months} мес. за {price}₽ "
            f"на аккаунт {username_input}?</b>\n\n"
            f"<b>⏲️ Время на оплату 10 минут</b>",
            reply_markup=buy_prem_with_promo,  # используем готовую разметку
            parse_mode="HTML"
        )

        user_states.pop(user_id, None)

    if 'awaiting_stars' in state:
        try:
            stars_input = int(message.text)
            if stars_input < 50 or stars_input > 10000:
                await message.answer("Пожалуйста, введите число от 50 до 10 000.")
                return

            multiplier = get_star_rate_for_range(stars_input)

            cost = round(stars_input * multiplier, 2)
            user_purchase_data[user_id] = {
                'stars': stars_input,
                'cost': cost,
                'username': None
            }

            user_states[user_id] = {'awaiting_username': True}

            photo_url = "https://ibb.co/MyFDq6zx"
            caption = (
                "📛 <b>Теперь укажи username Telegram-аккаунта, куда нужно зачислить звёзды.</b>\n\n"
                "Важно:\n"
                "• <b>Убедись</b>, что твой username корректно указан (например, @example).\n"
                "• <b>Если у тебя нет username</b>, его нужно создать в настройках Telegram.\n"
                "• Звёзды будут зачислены <b>в течение 5-ти минут</b> после оплаты"
            )

            await bot.send_photo(
                chat_id=message.chat.id,
                photo=photo_url,
                caption=caption,
                parse_mode='HTML'
            )

        except ValueError:
            await message.answer("Пожалуйста, введите корректное число.")
        return

    # Обработка ввода username для покупки звезд
    if 'awaiting_username' in state:
        username_input = message.text.strip()
        if re.match(r'^@[A-Za-z0-9_]+$', username_input):
            if user_id in user_purchase_data:
                user_purchase_data[user_id]['username'] = username_input
                data = user_purchase_data[user_id]
                stars_amount = data['stars']
                cost = data['cost']
                target_username = data['username']
                await message.answer(
                    f"<b>Вы уверены, что хотите купить ⭐️{stars_amount} за {cost}₽ на аккаунт {target_username}?</b>\n\n"
                    f"<b>⏲ Время на оплату 10 минут</b>",
                    reply_markup=buy_with_promo,
                    parse_mode="HTML"
                )

                user_states.pop(user_id)
            else:
                await message.answer("Произошла ошибка: данные о покупке не найдены. Попробуйте снова.")
        else:
            await message.answer("Некорректный формат username. Он должен начинаться с '@' и содержать только буквы, цифры и _.")
        return

    # Обработка username при состоянии waiting_username_apply
    if 'waiting_username_apply' in state:
        username_input = message.text.strip()
        if re.match(r'^@[A-Za-z0-9_]+$', username_input):
            message_text_group = f'У {user_id} 10 рефов звезды на вот этот акк {username_input}'
            await bot.send_message(GROUP_CHAT_ID2, message_text_group)

            await message.answer(
                f'Подарок за активных 10 рефералов поступит на аккаунт {username_input}\n'
                f'Пожалуйста ожидайте. С уважением @starslix'
            )
            user_states.pop(user_id)
        else:
            await message.answer(
                "Некорректный формат username. Он должен начинаться с '@' и содержать только буквы, цифры и _."
            )
        return
    if state and state.get("awaiting_promo"):
        promo = message.text.strip().upper()
        user_id = message.from_user.id

        # --- Проверяем наличие данных о покупке ---
        if user_id not in user_purchase_data:
            await message.answer(
                "❌ Ошибка: данные о покупке не найдены.\n"
                "Попробуйте заново выбрать пакет звёзд."
            )
            user_states.pop(user_id, None)
            return

        stars_amount = user_purchase_data[user_id]["stars"]
        old_cost = user_purchase_data[user_id]["cost"]
        target_username = user_purchase_data[user_id].get("username", "")

        # --- Проверяем промокод через apply_promo_code ---
        result = await apply_promo_code(user_id, promo, "stars", stars_amount)

        # --- Если промо не найден / недействителен ---
        if not result["ok"]:
            await message.answer(result["error"])
            user_states.pop(user_id, None)
            return

        # --- Промокод валиден ---
        discount = result["discount"]
        new_cost = round(old_cost * (100 - discount) / 100, 2)

        # --- Если скидка 100% — выдаём звёзды сразу ---
        if discount == 100:
            # Отмечаем использование промо
            confirm_promo_usage(promo, user_id)

            # Записываем покупку в БД
            cursor.execute(
                "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
                (user_id, message.from_user.username or "", "stars", stars_amount, 0)
            )
            conn.commit()

            # Увеличиваем общий счётчик звёзд
            cursor.execute("UPDATE total_stars SET total = total + ? WHERE id = 1", (stars_amount,))
            conn.commit()

            # --- Сообщение в группу ---
            buyer_username = message.from_user.username or "user"
            message_text_group = (
                f"@{buyer_username} - {stars_amount}⭐ - оплата подтверждена для аккаунта @{target_username} (промо {promo})"
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✅ Готово", callback_data="delete_msg")]]
            )
            await bot.send_message(GROUP_CHAT_ID, message_text_group, reply_markup=keyboard)

            # --- Сообщение пользователю ---
            await message.answer(
                f"✅ Промокод <b>{promo}</b> успешно применён!\n"
                f"Вы получили <b>{stars_amount}⭐</b> совершенно бесплатно 🎉\n\n"
                f"Звёзды скоро поступят на ваш аккаунт @{target_username}.",
                parse_mode="HTML"
            )

            user_states.pop(user_id, None)
            user_purchase_data.pop(user_id, None)
            return

        # --- Если скидка меньше 100% — сохраняем промо и предлагаем оплату ---
        user_purchase_data[user_id]["promo_pending"] = {
            "code": promo,
            "discount": discount,
            "new_cost": new_cost
        }

        await message.answer(
            f"✅ Промо-код успешно применён!\n"
            f"Вы уверены, что хотите купить ⭐️{stars_amount}\n"
            f"<s>{old_cost}₽</s> ➝ {new_cost}₽ на аккаунт {user_purchase_data[user_id]['username']}\n\n"
            f"⏲ Время на оплату 10 минут",
            reply_markup=buy_final,
            parse_mode="HTML"
        )

        # --- Очищаем состояние ---
        user_states.pop(user_id, None)
        return

        # --- Обработка промо для Premium ---
        if state.get("awaiting_promo_prem"):
            promo = message.text.strip().upper()


            if user_id not in user_premium_data:

                await message.answer("❌ Ошибка: данные о покупке Premium не найдены.")
                user_states.pop(user_id, None)
                return

            months = user_premium_data[user_id]["months"]
            old_price = user_premium_data[user_id]["price"]
            username = user_premium_data[user_id]["username"]

            result = await apply_promo_code(user_id, promo, "premium")


            if not result["ok"]:

                await message.answer(result["error"])
                user_states.pop(user_id, None)
                return

            # --- Промо 100% ---
            if result["discount"] == 100:
                if not confirm_promo_usage(promo, user_id):
                    await message.answer("❌ Промокод больше недоступен.")
                    user_states.pop(user_id, None)
                    return


                await message.answer(
                    f"✅ Промокод {promo} успешно применён!\n"
                    f"Вы получили Telegram Premium на {months} мес. бесплатно 🎁"
                )

                user_states.pop(user_id, None)
                user_premium_data.pop(user_id, None)
                return

            # --- Промо с частичной скидкой ---
            new_price = round(old_price * (100 - result['discount']) / 100, 2)


            user_premium_data[user_id]['promo_pending'] = {
                'code': promo,
                'discount': result['discount'],
                'new_price': new_price
            }

            await message.answer(
                f"✅ Промо-код успешно применён!\n"
                f"Premium {months} мес.\n"
                f"<s>{old_price}₽</s> ➝ {new_price}₽ на аккаунт {username}\n\n"
                f"⏲ Время на оплату 10 минут",
                reply_markup=buyprem,
                parse_mode="HTML"
            )

            user_states.pop(user_id, None)

            return

    if state and state.get("awaiting_rate_change"):
        try:
            new_rate = float(message.text.replace(",", "."))
            range_name = state["awaiting_rate_change"]
            set_star_rate(range_name, new_rate)
            user_states.pop(user_id, None)
            await message.answer(f"✅ Курс для диапазона {range_name.replace('_', '-')} обновлён: {new_rate}")
        except ValueError:
            await message.answer("❌ Введите корректное число.")

def increment_promo_usage(promo_code: str):
    if not promo_code:
        return
    # Используем глобальное соединение или создаём новое
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code = ?",
            (promo_code,)
        )
        conn.commit()

    except Exception:
        pass
    finally:
        conn.close()




# Обработчик удаления промо
@dp.callback_query(lambda c: c.data and c.data.startswith("delete_promo:"))
async def delete_promo_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        return

    promo_code = callback.data.split(":")[1]
    cursor.execute("DELETE FROM promo_codes WHERE code=?", (promo_code,))
    conn.commit()

    await callback.answer(f"✅ Промо-код {promo_code} удалён.", show_alert=True)
    await callback.message.delete()


@dp.callback_query(lambda c: c.data == "buy_custom")
async def buy_custom_handler(callback: types.CallbackQuery):
    photo_url = "https://ibb.co/MyFDq6zx"
    caption = "Введите количество звезд которое хотите купить (от 50 до 10000):"
    user_id = callback.from_user.id
    user_states[user_id] = {'awaiting_stars': True}
    try:
        await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=caption)
    )
    except ValueError:
        pass



async def check_pally_payment_by_id(payment_id: str) -> bool:
    url = f"{PALLY_API_BASE}/payment/status"
    headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}
    params = {"id": payment_id}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status != 200:
                return False
            data = await resp.json()
            status = data.get("status") or data.get("state")
            return status in ("paid","success","completed")


async def send_new_active_ref_message(user_id):
    try:
        await bot.send_message(
            user_id,
            "У вас новый активный реф!"
        )
    except Exception as e:
        print(f"Не удалось отправить сообщение пользователю {user_id}: {e}")




# Получаем актуальный курс TON к рублю через CoinGecko API
async def get_rub_to_ton_rate():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=rub"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            if "toncoin" in data and "rub" in data["toncoin"]:
                return data["toncoin"]["rub"]  # Курс TON/RUB
            else:
                raise Exception("Не удалось получить курс TON/RUB с CoinGecko")


async def process_payment_confirmation(user_id, target_username, stars, callback):
    cursor = conn.cursor()
    buyer_username = callback.from_user.username or "user"

    payment_id = str((user_purchase_data.get(user_id) or {}).get("pally_payment_id") or "").strip()
    stars_operation_id = f"pally:{payment_id}" if payment_id else f"stars:{user_id}:{int(stars or 0)}"
    fulfill_result = await fragment_fulfill_order(
        item_type="stars",
        target_username=str(target_username or ""),
        amount_value=int(stars or 0),
        operation_id=stars_operation_id,
    )
    if not fulfill_result.get("ok"):
        fail_stage = str(fulfill_result.get("stage") or "unknown")
        fail_status = int(fulfill_result.get("statusCode") or 0)
        error_text = str(fulfill_result.get("error") or "unknown error")
        debug_error_text = f"[stage={fail_stage} status={fail_status}] {error_text}"
        logging.error(
            "Stars auto-fulfillment failed user=%s target=%s stars=%s op=%s error=%s",
            user_id,
            target_username,
            stars,
            stars_operation_id,
            debug_error_text,
        )
        if payment_id:
            try:
                cursor.execute(
                    "UPDATE payments SET status = ?, description = ? WHERE bill_id = ?",
                    ("warning", f"Stars auto-fulfillment failed: {debug_error_text[:190]}", payment_id),
                )
                conn.commit()
            except Exception:
                pass
        await bot.send_message(
            user_id,
            "⚠️ Оплата подтверждена, но автовыдача Stars не выполнена. Поддержка уже уведомлена.",
        )
        try:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠️ Ошибка автовыдачи Stars\nUser: <code>{int(user_id)}</code>\n"
                f"Target: @{escape(str(target_username or ''))}\n"
                f"Stars: <b>{int(stars or 0)}</b>\nError: {escape(debug_error_text)}",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    # --- Сохраняем покупку ---
    final_cost = user_purchase_data.get(user_id, {}).get('amount', 0)
    cursor.execute(
        "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
        (user_id, buyer_username, "stars", stars, final_cost)
    )
    conn.commit()


    # --- Увеличиваем общий счётчик звёзд ---
    cursor.execute("UPDATE total_stars SET total = total + ? WHERE id = 1", (stars,))
    conn.commit()

    # --- Обработка промокода ---
    promo_data = user_purchase_data.get(user_id, {}).get('promo_pending')
    promo_text = ""

    if promo_data:
        promo_code = promo_data.get('code')
        discount = promo_data.get('discount', 0)

        try:
            # увеличиваем uses_count и записываем в used_promo
            confirm_promo_usage(promo_code, user_id)
        except Exception:
            pass

        if discount > 0:
            promo_text = f" (промо {promo_code} — скидка {discount}%)"

    # --- Формируем сообщение в группу (всегда, даже без промо) ---
    message_text_group = (
        f"@{buyer_username} - {stars}⭐ - оплата подтверждена для аккаунта {target_username}{promo_text}"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Готово", callback_data="delete_msg")]
        ]
    )

    await bot.send_message(GROUP_CHAT_ID, message_text_group, reply_markup=keyboard)


    # --- Сообщение пользователю ---
    await bot.send_message(
        user_id,
        f"✅ Оплата подтверждена! Спасибо за покупку!\n"
        f"Звёзды скоро поступят на ваш аккаунт. Ожидайте.\n\n"
        f'🌟 Оценить наш сервис можно <b><a href="https://t.me/+Qkb-Q43fRf40NGFk">ЗДЕСЬ</a></b> — нам будет приятно 💫',
        parse_mode="HTML",
        disable_web_page_preview=True
    )


    # --- Очистка данных пользователя ---
    user_purchase_data.pop(user_id, None)


# ✅ Функция фиксации использования промо
def confirm_promo_usage(promo_code: str, user_id: int) -> bool:
    """Фиксирует активацию промокода с проверкой лимитов в одной транзакции."""
    try:
        promo_code = str(promo_code or "").strip().upper()
        user_id = int(user_id)
        if not promo_code or user_id <= 0:
            return False

        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            """
            SELECT max_uses, uses_count, max_uses_per_user
            FROM promo_codes
            WHERE code = ?
            """,
            (promo_code,),
        )
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return False

        max_uses = int(row[0] or 0)
        uses_count = int(row[1] or 0)
        max_uses_per_user = int(row[2] or 1)
        if max_uses > 0 and uses_count >= max_uses:
            conn.rollback()
            return False

        user_uses = _get_user_promo_uses_count(user_id, promo_code)
        if max_uses_per_user > 0 and user_uses >= max_uses_per_user:
            conn.rollback()
            return False

        cursor.execute(
            "INSERT INTO promo_usages (user_id, code) VALUES (?, ?)",
            (user_id, promo_code),
        )
        cursor.execute(
            "INSERT OR IGNORE INTO used_promo (user_id, code) VALUES (?, ?)",
            (user_id, promo_code),
        )
        cursor.execute(
            """
            UPDATE promo_codes
            SET uses_count = uses_count + 1
            WHERE code = ? AND (? <= 0 OR uses_count < ?)
            """,
            (promo_code, max_uses, max_uses),
        )
        if cursor.rowcount <= 0:
            conn.rollback()
            return False

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False




def save_payment(user_id, bill_id, amount, status, description):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # Проверяем, существует ли уже платёж с таким bill_id
        cursor.execute('SELECT * FROM payments WHERE bill_id=?', (bill_id,))
        existing_payment = cursor.fetchone()

        if existing_payment:
            # Если платёж существует, обновляем его статус
            cursor.execute('''
                UPDATE payments
                SET status=?, amount=?, description=?, date=CURRENT_TIMESTAMP
                WHERE bill_id=?
            ''', (status, amount, description, bill_id))

        else:
            # Если платёж не существует, вставляем новый
            cursor.execute('''
                INSERT INTO payments (user_id, bill_id, amount, status, description, date)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, bill_id, amount, status, description))


        conn.commit()

    except sqlite3.Error as e:
        print(f"Ошибка при сохранении платежа: {e}")

    finally:
        conn.close()



async def check_pally_payment_status(bill_id: str) -> bool:
    url = f"{PALLY_API_BASE}/bill/status"
    headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}
    params = {"id": bill_id}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                return False
            try:
                data = await resp.json()
            except Exception:
                return False

            if str(data.get("success")).lower() == "true":
                status = str(data.get("status")).lower()  # <-- приведение к нижнему регистру

                return status in ("paid", "success", "completed", "overpaid")
            else:
                return False


import datetime

def generate_unique_label(user_id):
    now = datetime.datetime.now()
    label = f"{now.year:02d}{now.month:02d}{now.day:02d}{now.hour:02d}{now.minute:02d}{now.second:02d}{user_id}"
    return label


async def create_pally_payment(amount, description, label):
    url = f"{PALLY_API_BASE}/bill/create"
    payload = {
        "shop_id": PALLY_SHOP_ID,
        "amount": amount,
        "currency": "RUB",
        "description": description,
        "metadata": {"label": label},
        "payer_pays_commission": 0,
        "success_url": "https://t.me/pallytestrobot",
        "fail_url": "https://t.me/pallytestrobot",
        "type": "normal"
    }
    headers = {
        "Authorization": f"Bearer {PALLY_API_TOKEN}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers, timeout=30) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise Exception(f"HTTP ошибка: {resp.status} - {text}")

                data = await resp.json()
                if str(data.get("success")).lower() == "true":
                    payment_id = data.get("bill_id")
                    payment_url = data.get("link_page_url")
                    return payment_id, payment_url
                else:
                    error_message = data.get('message', 'Неизвестная ошибка')
                    raise Exception(f"Ошибка API Pally: {error_message}")
        except Exception as e:
            # Логируем ошибку для диагностики
            logging.error(f"Ошибка при создании платежа через Pally: {e}")
            raise Exception(f"Ошибка при создании платежа, попробуйте позже. {str(e)}")


@dp.callback_query(lambda c: c.data == "pay_sbp")
async def pay_sbp_callback(callback):
    user_id = callback.from_user.id

    if user_id not in user_purchase_data:
        await callback.message.answer("Ошибка: данные о покупке не найдены.")
        return

    # --- Получаем сумму и параметры ---
    if 'promo_pending' in user_purchase_data[user_id]:
        amount_value = user_purchase_data[user_id]['promo_pending']['new_cost']
    else:
        amount_value = user_purchase_data[user_id]['cost']

    stars_amount = user_purchase_data[user_id]['stars']
    target_username = user_purchase_data[user_id]['username']
    label_str = generate_unique_label(user_id)

    # --- Создаем платеж через Pally ---
    try:
        payment_id, payment_url = await create_pally_payment(
            amount_value,
            f"Покупка {stars_amount}⭐ для {target_username}",
            label_str
        )

    except Exception:
        await callback.message.answer("Ошибка при создании платежа, попробуйте позже.")
        return

    # --- Сохраняем данные о платеже сразу с статусом 'pending' ---

    user_purchase_data[user_id]['pally_payment_id'] = payment_id
    user_purchase_data[user_id]['label'] = label_str
    user_purchase_data[user_id]['amount'] = amount_value
    user_purchase_data[user_id]['callback_obj'] = callback

    # --- Сохраняем платёж в базу данных с статусом 'pending' ---
    description = f"Покупка {stars_amount}⭐ для {target_username}"
    save_payment(user_id, payment_id, amount_value, "pending", description)  # Сохраняем как "pending"

    # --- Отправляем кнопку оплаты ---
    pay_button = InlineKeyboardButton(text=f"Оплатить {amount_value}₽", url=payment_url)
    markup = InlineKeyboardMarkup(inline_keyboard=[[pay_button]])
    await callback.message.edit_text(
        "Нажмите кнопку ниже для перехода к оплате. Оплата будет проверяться автоматически.",
        reply_markup=markup
    )

    # --- Проверка оплаты каждые 3 сек до 10 минут ---
    start_time = asyncio.get_event_loop().time()
    payment_confirmed = False
    while True:
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > 600:  # 10 минут
            break

        try:
            if await check_pally_payment_status(payment_id):
                payment_confirmed = True
                break
        except Exception as e:
            print(f"Ошибка проверки статуса платежа: {e}")

        await asyncio.sleep(3)

    # --- Обработка результата ---
    if payment_confirmed:


        # Обновляем платёж на "успешный" после подтверждения
        cursor.execute("UPDATE payments SET status = ? WHERE bill_id = ?", ("paid", payment_id))
        conn.commit()

        await process_payment_confirmation(user_id, target_username, stars_amount, callback)
    else:
        await callback.message.answer("Оплата не была подтверждена за 10 минут. Попробуйте снова.")


@dp.callback_query(lambda c: c.data == "back_first")
async def back_first_callback(callback: types.CallbackQuery):
    await callback.message.delete()

    photo_url = 'https://ibb.co/XrPBvfbS'  # замените на ваш реальный URL

    username = callback.from_user.username or callback.from_user.first_name
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    total_stars = row[0] if (row and row[0] is not None) else 0
    approx_usd = total_stars * 0.013  # примерный курс доллара
    stars_info = f"<b>У нас уже купили:</b> {total_stars:,}⭐️ (~${approx_usd:.2f})".replace(",", " ")
    text3 = (
        f"<b>Добро пожаловать в STARSLIX!</b>\n\n"
        f"<b>Привет, {username}!</b>\n"
        f"<b>{stars_info}</b>\n"
        "<b>Покупай звёзды и Premium, дари подарки, сияй ярче всех!</b>\n\n"
        "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>Соглашение</a></b> | "
        "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>Политика</a></b>\n"
        "<b>Время работы бота: 06:00-00:00 (МСК)</b>"
    )

    await callback.message.answer_photo(
        photo=photo_url,
        caption=text3,
        reply_markup=hll,
        parse_mode='HTML'
    )

@dp.callback_query(lambda c: c.data == "back")
async def back_callback(callback: types.CallbackQuery):
    # URL изображения
    photo_url = 'https://ibb.co/MyFDq6zx'  # замените на ваш реальный URL

    username = callback.from_user.username or callback.from_user.first_name
    caption = "Выбери один из популярных пакетов или укажи своё количество:"
    await callback.message.delete()
    await callback.message.answer_photo(
        photo=photo_url,
        caption=caption,
        reply_markup=get_star_keyboard()
    )

@dp.callback_query(lambda c: c.data == "back_prem")
async def back_callback(callback: types.CallbackQuery):
    # URL изображения
    photo_url = 'https://ibb.co/MyFDq6zx'  # замените на ваш реальный URL

    username = callback.from_user.username or callback.from_user.first_name
    caption = (
        f"<b>💙Telegram Premium💙</b>\n\n"
        f"<b>Выберите срок подписки:</b>"
    )
    await callback.message.delete()
    await callback.message.answer_photo(
        photo=photo_url,
        caption=caption,
        reply_markup=prem,
        parse_mode='HTML'
    )

@dp.callback_query(lambda c: c.data == "back_start")
async def back_start_callback(callback: types.CallbackQuery):
    await callback.message.delete()

    photo_url = 'https://ibb.co/XrPBvfbS'  # замените на ваш реальный URL

    username = callback.from_user.username or callback.from_user.first_name
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    total_stars = row[0] if (row and row[0] is not None) else 0
    approx_usd = total_stars * 0.013  # примерный курс доллара
    stars_info = f"<b>Всего куплено звёзд:</b> {total_stars:,}⭐️ (~${approx_usd:.2f})".replace(",", " ")
    text3 = (
        f"<b>Добро пожаловать в STARSLIX!</b>\n\n"
        f"<b>Привет, {username}!</b>\n"
        f"<b>{stars_info}</b>\n"
        "<b>Покупай звёзды и Premium, дари подарки, сияй ярче всех!</b>\n\n"
        "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>Соглашение</a></b> | "
        "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>Политика</a></b>\n"
        "<b>Время работы: 06:00-00:00 (МСК)</b>"
    )
    await callback.message.answer_photo(
        photo=photo_url,
        caption=text3,
        reply_markup=hll,
        parse_mode='HTML'
    )

@dp.callback_query(lambda c: c.data == "check_subscriptionhelp")
async def handle_check_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("Ошибка при проверке подписки. Попробуйте позже.", show_alert=True)
        return

    if is_subscribed:
        await callback.answer("Спасибо! Вы успешно подписались.\U00002764", show_alert=True)
        # Продолжайте выполнение логики
        await help_callback(callback)
    else:
        await callback.answer("Вы еще не подписались на канал.\U0001F61E", show_alert=True)
@dp.callback_query(lambda c: c.data == "help")
async def help_callback(callback: types.CallbackQuery):
    # Проверка подписки
    user_id = callback.from_user.id
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # Обработка ошибок (например, если бот не в состоянии проверить)
        await callback.message.answer("Ошибка проверки подписки. Попробуйте позже.")
        return

    if not is_subscribed:
        # Пользователь не подписан — удаляем сообщение и просим подписаться
        await callback.message.delete()

        # Отправляем сообщение с кнопкой "Подписаться" и "Проверить"
        subscribe_button = InlineKeyboardButton(text="Подписаться", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="✅ Готово", callback_data="check_subscriptionhelp")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "Пожалуйста, подпишитесь на наш канал, чтобы продолжить.",
            reply_markup=keyboard
        )
        return


    text = (
        '❓ Часто задаваемые вопросы (FAQ)\n\n'
        '1. Что такое Telegram-звезды и зачем их покупать?\n'
        'Telegram-звезды — это виртуальные подарки, которые можно отправлять друзьям и близким в Telegram. '
        'Наш бот помогает легко и быстро приобрести звезды, чтобы делать приятные подарки или украшать свой профиль.\n\n'
        '2. Что такое "активный реферал"?\n'
        'Активный реферал — это ваш реферал, который сделал хотя бы одну покупку через бота.\n\n'
        '3. Как я могу получить бонус за рефералов?\n'
        'Получить бонус можно только при наличии 10 или более активных рефералов. '
        'Для этого необходимо оставить заявку на получение бонуса — свяжитесь с поддержкой или воспользуйтесь формой ниже.\n\n'
        '4. Какие преимущества у активных пользователей?\n'
        'Активные пользователи получают дополнительные бонусы, скидки и эксклюзивные предложения. '
        'Также они могут участвовать в специальных акциях и конкурсах.\n\n'
        '5. Как купить звезды через бота?\n'
        'Просто выберите нужный пакет звезд, следуйте инструкциям и оплатите удобным способом. '
        'После оплаты звезды автоматически зачисляются на ваш аккаунт.\n\n'
        '6. Могу ли я использовать реферальную систему без приглашения друзей?\n'
        'Да! Вы можете просто покупать звезды и пользоваться всеми функциями бота без участия в реферальной системе.\n\n'
        'Если у вас есть дополнительные вопросы — нажмите на кнопку "Задать вопрос".'
    )
    # Удаляем сообщение с кнопкой (если нужно)
    await callback.message.delete()
    # Отправляем сообщение с FAQ
    await callback.message.answer(text=text, reply_markup=help)

def parse_callback_history_id(callback_data: str, prefix: str):
    data = str(callback_data or "").strip()
    marker = f"{prefix}:"
    if not data.startswith(marker):
        return None
    raw_value = data[len(marker):].strip()
    if not raw_value.isdigit():
        return None
    history_id = int(raw_value)
    return history_id if history_id > 0 else None


def confirm_keyboard(history_id=None):
    confirm_data = "confirm_delete"
    cancel_data = "cancel_delete"
    if history_id is not None:
        confirm_data = f"confirm_delete:{int(history_id)}"
        cancel_data = f"cancel_delete:{int(history_id)}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=confirm_data),
                InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_data)
            ]
        ]
    )


def done_keyboard(history_id=None):
    delete_data = "delete_msg"
    if history_id is not None:
        delete_data = f"delete_msg:{int(history_id)}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Готово", callback_data=delete_data)]
        ]
    )


async def safe_callback_answer(callback: types.CallbackQuery, text: str, show_alert: bool = False):
    try:
        await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest:
        pass
    except Exception:
        pass

@dp.callback_query(lambda c: c.data == "delete_msg" or c.data.startswith("delete_msg:"))
async def ask_delete_confirmation(callback: types.CallbackQuery):
    history_id = parse_callback_history_id(callback.data, "delete_msg")
    try:
        await callback.message.edit_reply_markup(reply_markup=confirm_keyboard(history_id))
    except Exception:
        pass
    await safe_callback_answer(callback, "Вы уверены, что хотите удалить сообщение?")


@dp.callback_query(lambda c: c.data == "confirm_delete" or c.data.startswith("confirm_delete:"))
async def confirm_delete_callback(callback: types.CallbackQuery):
    history_id = parse_callback_history_id(callback.data, "confirm_delete")
    logging.warning(
        "Admin confirm callback received. data=%s history_id=%s admin_id=%s",
        str(callback.data or ""),
        history_id,
        int(getattr(callback.from_user, "id", 0) or 0),
    )

    if history_id is not None:
        try:
            fulfill_result = await try_fragment_fulfill_miniapp_history(history_id)
            if not fulfill_result.get("ok"):
                error_text = str(fulfill_result.get("error") or "fragment auto-fulfillment failed")
                error_code = str(fulfill_result.get("errorCode") or "MAF-UNK-000")
                fail_stage = str(fulfill_result.get("stage") or "unknown")
                fail_status = int(fulfill_result.get("statusCode") or 0)
                logging.warning(
                    "Failed to auto-fulfill miniapp purchase. id=%s code=%s stage=%s status=%s error=%s",
                    history_id,
                    error_code,
                    fail_stage,
                    fail_status,
                    error_text,
                )
                purchase_row = get_miniapp_purchase_history_row(history_id)
                if purchase_row:
                    await miniapp_broadcast_event(
                        "purchase_failed",
                        {
                            "historyId": int(history_id),
                            "operationId": str(fulfill_result.get("operationId") or ""),
                            "userId": int(purchase_row.get("userId") or 0),
                            "errorCode": error_code,
                            "errorText": error_text,
                            "stage": fail_stage,
                            "statusCode": fail_status,
                        },
                    )
                try:
                    await callback.message.edit_reply_markup(reply_markup=done_keyboard(history_id))
                except Exception:
                    pass
                await safe_callback_answer(
                    callback,
                    f"Auto-fulfill failed [{error_code}]: {error_text}",
                    show_alert=True,
                )
                return

            finalize_result = finalize_miniapp_purchase_history(history_id)
            if finalize_result.get("ok"):
                await miniapp_broadcast_event(
                    "purchase_finalized",
                    {
                        "historyId": int(finalize_result.get("historyId") or 0),
                        "operationId": str(finalize_result.get("operationId") or ""),
                        "userId": int(finalize_result.get("userId") or 0),
                        "itemType": str(finalize_result.get("itemType") or ""),
                        "amount": int(finalize_result.get("amount") or 0),
                        "totals": finalize_result.get("totals") or {},
                    },
                )
            else:
                logging.warning(
                    "Finalize returned not ok. history_id=%s result=%s",
                    history_id,
                    finalize_result,
                )
        except Exception as error:
            logging.warning("Failed to finalize miniapp history entry. id=%s error=%s", history_id, error)
            error_code = "MAF-CB-500"
            error_text = str(error or "confirm callback failed")
            fail_debug = format_miniapp_fulfill_error_text(error_code, "callback", 0, error_text)
            try:
                set_miniapp_purchase_history_error(history_id, fail_debug, status="error")
                purchase_row = get_miniapp_purchase_history_row(history_id)
                if purchase_row:
                    await miniapp_broadcast_event(
                        "purchase_failed",
                        {
                            "historyId": int(history_id),
                            "operationId": build_operation_id(
                                "miniapp",
                                int(purchase_row.get("id") or 0),
                                str(purchase_row.get("createdAt") or ""),
                            ),
                            "userId": int(purchase_row.get("userId") or 0),
                            "errorCode": error_code,
                            "errorText": error_text,
                            "stage": "callback",
                            "statusCode": 0,
                        },
                    )
            except Exception:
                pass
    else:
        logging.warning(
            "confirm_delete callback without history id. data=%s admin_id=%s",
            str(callback.data or ""),
            int(getattr(callback.from_user, "id", 0) or 0),
        )

    try:
        await callback.message.delete()
        await safe_callback_answer(callback, "Message deleted", show_alert=True)
    except Exception:
        await safe_callback_answer(callback, "Delete failed", show_alert=True)


@dp.callback_query(lambda c: c.data == "cancel_delete" or c.data.startswith("cancel_delete:"))
async def cancel_delete_callback(callback: types.CallbackQuery):
    history_id = parse_callback_history_id(callback.data, "cancel_delete")
    try:
        await callback.message.edit_reply_markup(reply_markup=done_keyboard(history_id))
        await safe_callback_answer(callback, "Удаление отменено ❌", show_alert=True)
    except Exception:
        await safe_callback_answer(callback, "Ошибка", show_alert=True)


async def run_bot_and_miniapp() -> None:
    init_db()
    logging.warning(
        "Fragment config loaded: auto=%s raw_auto=%s base=%s seed=%s key=%s",
        bool(FRAGMENT_API_AUTO_FULFILL),
        str(os.getenv("FRAGMENT_API_AUTO_FULFILL") or ""),
        bool(FRAGMENT_API_BASE_URL),
        bool(FRAGMENT_API_SEED),
        bool(FRAGMENT_API_KEY),
    )

    miniapp_runner = web.AppRunner(create_miniapp_api())
    await miniapp_runner.setup()
    miniapp_site = web.TCPSite(miniapp_runner, MINIAPP_API_HOST, MINIAPP_API_PORT)
    await miniapp_site.start()

    try:
        await dp.start_polling(bot)
    finally:
        await miniapp_runner.cleanup()


if __name__ == '__main__':
    asyncio.run(run_bot_and_miniapp())




