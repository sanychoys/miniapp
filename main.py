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
from datetime import datetime
import base64
from urllib.parse import parse_qsl
from html import escape


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
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file()

API_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ADMIN_ID = 382257126
ADMIN_IDS = [OWNER_ADMIN_ID, 7545158182, 6342564240] # owner, support, nikita
GROUP_CHAT_ID = -1002812420141
GROUP_CHAT_ID2 = -1003001456496
CHANNEL_ID = "@starslixx"

if not API_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()


PALLY_API_TOKEN = os.getenv("PALLY_API_TOKEN")      # РїРѕР»СѓС‡РёС‚Рµ РІ Р»РёС‡РЅРѕРј РєР°Р±РёРЅРµС‚Рµ pally
PALLY_SHOP_ID = os.getenv("PALLY_SHOP_ID")          # shop_id РјР°РіР°Р·РёРЅР° РІ pally
PALLY_API_BASE = "https://pal24.pro/api/v1"          # РёСЃРїРѕР»СЊР·СѓР№С‚Рµ С‚РѕС‚ Р¶Рµ РґРѕРјРµРЅ, С‡С‚Рѕ РІ РІР°С€РµРј Р°РєРєР°СѓРЅС‚Рµ / РІ РґРѕРєРµ
PALLY_REDIRECT_SUCCESS = "https://t.me/RuTelegramStars_bot?start=payment_success"
PALLY_REDIRECT_FAIL = "https://t.me/RuTelegramStars_bot?start=payment_fail"

if not PALLY_API_TOKEN:
    raise RuntimeError("Missing PALLY_API_TOKEN environment variable")

if not PALLY_SHOP_ID:
    raise RuntimeError("Missing PALLY_SHOP_ID environment variable")

MINIAPP_API_HOST = os.getenv("MINIAPP_API_HOST", "0.0.0.0")
MINIAPP_API_PORT = int(os.getenv("MINIAPP_API_PORT", "8080"))
MINIAPP_USD_RUB_RATE = float(os.getenv("MINIAPP_USD_RUB_RATE", "76.5"))
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


def init_db():
    global conn, cursor
    conn = sqlite3.connect('database.db')
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

    # РЎРѕР·РґР°РЅРёРµ РЅРѕРІРѕР№ С‚Р°Р±Р»РёС†С‹ СЃ РЅСѓР¶РЅРѕР№ СЃС‚СЂСѓРєС‚СѓСЂРѕР№
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

    # РўР°Р±Р»РёС†Р° СЂРµС„РµСЂР°Р»СЊРЅС‹С… РєРѕРґРѕРІ
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS ref_codes (
               code TEXT PRIMARY KEY,
               user_id INTEGER,
               ref_count INTEGER DEFAULT 0
           )
       ''')

    # РўР°Р±Р»РёС†Р° РїСЂРѕРґСѓРєС‚РѕРІ
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

    # Р”РѕР±Р°РІР»СЏРµРј РЅРѕРІС‹Р№ СЃС‚РѕР»Р±РµС†, РµСЃР»Рё РµРіРѕ РЅРµС‚
    try:
        cursor.execute("ALTER TABLE promo_codes ADD COLUMN max_uses_per_user INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass  # СѓР¶Рµ РґРѕР±Р°РІР»РµРЅ
    try:
        cursor.execute("ALTER TABLE promo_codes ADD COLUMN effect_type TEXT DEFAULT 'discount_percent'")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE promo_codes ADD COLUMN effect_value INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    # РўР°Р±Р»РёС†Р° РёСЃРїРѕР»СЊР·РѕРІР°РЅРЅС‹С… РїСЂРѕРјРѕРєРѕРґРѕРІ
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

    # РўР°Р±Р»РёС†Р° РїРѕРєСѓРїРѕРє
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS purchases (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               user_id INTEGER,
               username TEXT,
               item_type TEXT, -- "stars" РёР»Рё "premium"
               amount INTEGER, -- С‡РёСЃР»Рѕ Р·РІС‘Р·Рґ РёР»Рё РјРµСЃСЏС†РµРІ РїСЂРµРјРёСѓРјР°
               cost REAL,      -- СЃРєРѕР»СЊРєРѕ Р·Р°РїР»Р°С‚РёР» РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )
       ''')
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
        [InlineKeyboardButton(text="Р’РІРµСЃС‚Рё РєРѕР»РёС‡РµСЃС‚РІРѕ Р·РІС‘Р·Рґ", callback_data="buy_custom")]
    ])

    for i in range(0, len(stars), 2):
        row = []
        for s in stars[i:i + 2]:
            row.append(InlineKeyboardButton(text=f"в­ђпёЏ{s} | {calc_price(s)}в‚Ѕ", callback_data=f"{s}stars"))
        keyboard.inline_keyboard.append(row)

    keyboard.inline_keyboard.append([InlineKeyboardButton(text="в¬…пёЏРќР°Р·Р°Рґ", callback_data="back_first")])
    return keyboard
#

#conn = sqlite3.connect('database.db')
#cursor = conn.cursor()

# Р’С‹РїРѕР»РЅРµРЅРёРµ SQL-Р·Р°РїСЂРѕСЃР° РґР»СЏ РїРѕР»СѓС‡РµРЅРёСЏ РІСЃРµС… РґР°РЅРЅС‹С… РёР· С‚Р°Р±Р»РёС†С‹ users
#cursor.execute('SELECT * FROM users')
#rows = cursor.fetchall()

# Р’С‹РІРѕРґ СЂРµР·СѓР»СЊС‚Р°С‚Р° РІ РєРѕРЅСЃРѕР»СЊ
#for row in rows:
#    print(row)

# Р—Р°РєСЂС‹С‚РёРµ СЃРѕРµРґРёРЅРµРЅРёСЏ
#conn.close()

# Р“Р»РѕР±Р»СЊРЅС‹Рµ СЃР»РѕРІР°СЂРё РґР»СЏ С…СЂР°РЅРµРЅРёСЏ СЃРѕСЃС‚РѕСЏРЅРёСЏ Рё РїР»Р°С‚РµР¶РµР№
user_payments = {}  # user_id: amount
user_states = {}  # user_id: {'awaiting_stars': bool}
# Р’ РЅР°С‡Р°Р»Рµ С„Р°Р№Р»Р° РёР»Рё РїРµСЂРµРґ С„СѓРЅРєС†РёРµР№, РіРґРµ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ
user_purchase_data = {}
user_premium_data = {}

async def check_subscription(user_id):
    # РџСЂРѕРІРµСЂРєР° РїРѕРґРїРёСЃРєРё С‡РµСЂРµР· get_chat_member
    chat_member = await bot.get_chat_member(CHANNEL_ID, user_id)
    return chat_member.status in ['member', 'administrator', 'creator']

def generate_ref_code(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


@dp.message(CommandStart())
async def handle_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ"
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    photo_url = "https://ibb.co/XrPBvfbS"           #https://ibb.co/sdvyxnmq

    # РџРѕР»СѓС‡Р°РµРј С‚РµРєСЃС‚ Рё СЂРµС„РµСЂР°Р»СЊРЅС‹Р№ РєРѕРґ, РµСЃР»Рё РµСЃС‚СЊ
    text = message.text or ""
    parts = text.split(maxsplit=1)
    referral_code = parts[1].strip() if len(parts) > 1 else None

    # РџСЂРѕРІРµСЂРєР° СЃСѓС‰РµСЃС‚РІРѕРІР°РЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
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

    # РћР±СЂР°Р±РѕС‚РєР° СЂРµС„РµСЂР°Р»Р°
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

    # Р§РёС‚Р°РµРј РѕР±С‰РµРµ РєРѕР»РёС‡РµСЃС‚РІРѕ РїСЂРѕРґР°РЅРЅС‹С… Р·РІС‘Р·Рґ РёР· С‚Р°Р±Р»РёС†С‹ total_stars (С‡С‚РѕР±С‹ СЃРѕРІРїР°РґР°Р»Рѕ СЃ Р°РґРјРёРЅРєРѕР№)
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    total_stars = row[0] if (row and row[0] is not None) else 0
    approx_usd = total_stars * 0.013  # РїСЂРёРјРµСЂРЅС‹Р№ РєСѓСЂСЃ РґРѕР»Р»Р°СЂР°
    stars_info = f"<b>РЈ РЅР°СЃ СѓР¶Рµ РєСѓРїРёР»Рё:</b> {total_stars:,}в­ђпёЏ (~${approx_usd:.2f})".replace(",", " ")

    # --- РўРµРєСЃС‚ 1: РЅРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ СЃ СЂРµС„РµСЂР°Р»РѕРј ---
    if referral_code and inviter:
        text1 = (
            f"<b>Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ STARSLIX!</b>\n\n"
            f"<b>РџСЂРёРІРµС‚, {username}!</b>\n"
            f"<b>{stars_info}</b>\n"
            "<b>РџРѕРєСѓРїР°Р№ Р·РІС‘Р·РґС‹ Рё Premium, РґР°СЂРё РїРѕРґР°СЂРєРё, СЃРёСЏР№ СЏСЂС‡Рµ РІСЃРµС…!</b>\n\n"
            "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>РЎРѕРіР»Р°С€РµРЅРёРµ</a></b> | "
            "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>РџРѕР»РёС‚РёРєР°</a></b>\n"
            "<b>Р’СЂРµРјСЏ СЂР°Р±РѕС‚С‹: 06:00-00:00 (РњРЎРљ)</b>"
        )
        await message.answer_photo(photo=photo_url, caption=text1, reply_markup=hll, parse_mode='HTML')
        return

    # --- РўРµРєСЃС‚ 2: СѓР¶Рµ Р±С‹Р» Р·Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅ ---
    if user_record:
        text2 = (
            f"<b>Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ STARSLIX!</b>\n\n"
            f"<b>РџСЂРёРІРµС‚, {username}!</b>\n"
            f"<b>{stars_info}</b>\n"
            "<b>РџРѕРєСѓРїР°Р№ Р·РІС‘Р·РґС‹ Рё Premium, РґР°СЂРё РїРѕРґР°СЂРєРё, СЃРёСЏР№ СЏСЂС‡Рµ РІСЃРµС…!</b>\n\n"
            "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>РЎРѕРіР»Р°С€РµРЅРёРµ</a></b> | "
            "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>РџРѕР»РёС‚РёРєР°</a></b>\n"
            "<b>Р’СЂРµРјСЏ СЂР°Р±РѕС‚С‹: 06:00-00:00 (РњРЎРљ)</b>"
        )
        await message.answer_photo(photo=photo_url, caption=text2, reply_markup=hll, parse_mode='HTML')
        return

    # --- РўРµРєСЃС‚ 3: РѕР±С‹С‡РЅС‹Р№ СЃС‚Р°СЂС‚ Р±РµР· СЂРµС„РµСЂР°Р»Р° ---
    text3 = (
        f"<b>Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ STARSLIX!</b>\n\n"
        f"<b>РџСЂРёРІРµС‚, {username}!</b>\n"
        f"<b>{stars_info}</b>\n"
        "<b>РџРѕРєСѓРїР°Р№ Р·РІС‘Р·РґС‹ Рё Premium, РґР°СЂРё РїРѕРґР°СЂРєРё, СЃРёСЏР№ СЏСЂС‡Рµ РІСЃРµС…!</b>\n\n"
        "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>РЎРѕРіР»Р°С€РµРЅРёРµ</a></b> | "
        "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>РџРѕР»РёС‚РёРєР°</a></b>\n"
        "<b>Р’СЂРµРјСЏ СЂР°Р±РѕС‚С‹: 06:00-00:00 (РњРЎРљ)</b>"
    )
    await message.answer_photo(photo=photo_url, caption=text3, reply_markup=hll, parse_mode='HTML')




@dp.callback_query(lambda c: c.data == "check_subscription")
async def handle_check_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("РћС€РёР±РєР° РїСЂРё РїСЂРѕРІРµСЂРєРµ РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.", show_alert=True)
        return
    if is_subscribed:
        await callback.answer("РЎРїР°СЃРёР±Рѕ! Р’С‹ СѓСЃРїРµС€РЅРѕ РїРѕРґРїРёСЃР°Р»РёСЃСЊ.\U00002764", show_alert=True)
        await ref_system(callback)
    else:
        await callback.answer("Р’С‹ РµС‰Рµ РЅРµ РїРѕРґРїРёСЃР°Р»РёСЃСЊ РЅР° РєР°РЅР°Р».\U0001F61E", show_alert=True)

@dp.callback_query(lambda c: c.data == "ref_system")
async def ref_system(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    # РџСЂРѕРІРµСЂРєР° РїРѕРґРїРёСЃРєРё
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # РћР±СЂР°Р±РѕС‚РєР° РѕС€РёР±РѕРє (РЅР°РїСЂРёРјРµСЂ, РµСЃР»Рё Р±РѕС‚ РЅРµ РІ СЃРѕСЃС‚РѕСЏРЅРёРё РїСЂРѕРІРµСЂРёС‚СЊ)
        await callback.message.answer("РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
        return
    if not is_subscribed:
        # РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РїРѕРґРїРёСЃР°РЅ вЂ” СѓРґР°Р»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ Рё РїСЂРѕСЃРёРј РїРѕРґРїРёСЃР°С‚СЊСЃСЏ
        await callback.message.delete()

        # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№ "РџРѕРґРїРёСЃР°С‚СЊСЃСЏ" Рё "РџСЂРѕРІРµСЂРёС‚СЊ"
        subscribe_button = InlineKeyboardButton(text="РџРѕРґРїРёСЃР°С‚СЊСЃСЏ", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="check_subscription")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РїРѕРґРїРёС€РёС‚РµСЃСЊ РЅР° РЅР°С€ РєР°РЅР°Р», С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ.",
            reply_markup=keyboard
        )
        return


    # РџРѕР»СѓС‡Р°РµРј user's ref_code
    cursor.execute(
        'SELECT ref_code FROM users WHERE user_id=?',
        (user_id,)
    )
    row = cursor.fetchone()

    # РРЅРёС†РёР°Р»РёР·Р°С†РёСЏ РїРµСЂРµРјРµРЅРЅС‹С…
    message_text = ""
    photo_url = ""
    keyboard = None

    if row and row[0]:
        ref_code = row[0]

        # РџРѕРґСЃС‡РёС‚С‹РІР°РµРј РѕР±С‰РµРµ РєРѕР»РёС‡РµСЃС‚РІРѕ СЂРµС„РµСЂР°Р»РѕРІ
        cursor.execute(
            'SELECT COUNT(*) FROM users WHERE referred_by=?',
            (user_id,)
        )
        total_referrals = cursor.fetchone()[0]

        # РџРѕРґСЃС‡РёС‚С‹РІР°РµРј Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ
        cursor.execute(
            'SELECT COUNT(*) FROM users WHERE referred_by=? AND referrals_with_purchase=1',
            (user_id,)
        )
        active_referrals_count = cursor.fetchone()[0]

        ref_link = f"https://t.me/starslixbot?start={ref_code}"

        message_text = (
            f"рџ‘Ґ <b>Р—Р°СЂР°Р±Р°С‚С‹РІР°Р№ Р·РІС‘Р·РґС‹ СЃ РґСЂСѓР·СЊСЏРјРё!</b>\n\n"
            f"РџСЂРёРіР»Р°С€Р°Р№ РґСЂСѓР·РµР№ РІ РЅР°С€ Р±РѕС‚ Рё РїРѕР»СѓС‡Р°Р№ С‰РµРґСЂС‹Рµ Р±РѕРЅСѓСЃС‹! Р­С‚Рѕ РїСЂРѕСЃС‚Рѕ!\n\n"
            f"рџЋЃ<b> РЈСЃР»РѕРІРёСЏ СЂРµС„РµСЂР°Р»СЊРЅРѕР№ СЃРёСЃС‚РµРјС‹:</b>\n"
            f"Р—Р° РєР°Р¶РґС‹Рµ <b>10</b> Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ (С‚РµС…, РєС‚Рѕ РєСѓРїРёР» Р·РІС‘Р·РґС‹ С‡РµСЂРµР· С‚РІРѕСЋ СЃСЃС‹Р»РєСѓ), <b>С‚С‹ РїРѕР»СѓС‡Р°РµС€СЊ РЅР°РіСЂР°РґСѓ РІ 100 Р·РІС‘Р·Рґ РЅР° СЃРІРѕР№ СЃС‡С‘С‚!</b>\n\n"
            f"<b>РљР°Рє СЌС‚Рѕ СЂР°Р±РѕС‚Р°РµС‚:</b>\n"
            f"1. Р”РµР»РёС€СЊСЃСЏ СЃРІРѕРµР№ СѓРЅРёРєР°Р»СЊРЅРѕР№ СЂРµС„РµСЂР°Р»СЊРЅРѕР№ СЃСЃС‹Р»РєРѕР№ СЃ РґСЂСѓР·СЊСЏРјРё.\n"
            f"2. РўРІРѕРё РґСЂСѓР·СЊСЏ РїРµСЂРµС…РѕРґСЏС‚ РїРѕ СЃСЃС‹Р»РєРµ Рё РїРѕРєСѓРїР°СЋС‚ Р·РІС‘Р·РґС‹.\n"
            f"3. РљР°Рє С‚РѕР»СЊРєРѕ Сѓ С‚РµР±СЏ РЅР°Р±РёСЂР°РµС‚СЃСЏ 10 Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ, РЅР°Р¶РјРё РєРЅРѕРїРєСѓ 'Р—Р° РїРѕРґР°СЂРєРѕРј'.\n"
            f"4. РњС‹ РїСЂРѕРІРµСЂСЏРµРј Р°РєС‚РёРІРЅРѕСЃС‚СЊ Рё РјРіРЅРѕРІРµРЅРЅРѕ РЅР°С‡РёСЃР»СЏРµРј С‚РµР±Рµ 100 Р·РІС‘Р·Рґ!\n\n"
            f"<b>РўРІРѕСЏ РїРµСЂСЃРѕРЅР°Р»СЊРЅР°СЏ СЃСЃС‹Р»РєР°:</b>\n<b><code>{ref_link}</code></b>\n\n"
            f"<b>РЎС‚Р°С‚РёСЃС‚РёРєР°:</b>\nрџ“Љ <b>РўРІРѕРёС… СЂРµС„РµСЂР°Р»РѕРІ:</b> {total_referrals}\nрџЋЃ <b>Р“РѕС‚РѕРІРѕ Рє РЅР°РіСЂР°РґРµ:</b> {active_referrals_count}/10"
        )

        photo_url = "https://ibb.co/XrPBvfbS"

        keyboard_buttons = [
            [InlineKeyboardButton(text="РњРѕРё Р°РєС‚РёРІРЅС‹Рµ СЂРµС„РµСЂР°Р»С‹", callback_data="show_referrals")]
        ]

        if active_referrals_count >= 10:
            keyboard_buttons.append([InlineKeyboardButton(text="Р—Р° РїРѕРґР°СЂРєРѕРј", callback_data="apply")])

        keyboard_buttons.append([InlineKeyboardButton(text="в¬…пёЏРќР°Р·Р°Рґ", callback_data="back_first")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

    else:
        message_text = "РџСЂРѕС„РёР»СЊ РЅРµ РЅР°Р№РґРµРЅ РёР»Рё Сѓ РІР°СЃ РЅРµС‚ СЂРµС„РµСЂР°Р»СЊРЅРѕРіРѕ РєРѕРґР°."
        photo_url = "https://ibb.co/XrPBvfbS"

        # РЎРѕР·РґР°РµРј РєР»Р°РІРёР°С‚СѓСЂСѓ СЃ РєРЅРѕРїРєР°РјРё
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="РњРѕРё СЂРµС„РµСЂР°Р»С‹", callback_data="show_referrals")],
            [InlineKeyboardButton(text="в¬…пёЏРќР°Р·Р°Рґ", callback_data="back_start")]
        ])

    # РЎРѕР·РґР°РµРј InputMediaPhoto СЃ URL
    media = InputMediaPhoto(
        media=photo_url,
        caption=message_text,
        parse_mode='HTML'
    )

    # РћР±РЅРѕРІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ С„РѕС‚Рѕ Рё РєР»Р°РІРёР°С‚СѓСЂРѕР№
    await callback.message.edit_media(media)
    await callback.message.edit_reply_markup(reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "show_referrals")
async def show_referrals(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    # РС‰РµРј С‚РѕР»СЊРєРѕ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ (referrals_with_purchase=1)
    cursor.execute(
        'SELECT username FROM users WHERE referred_by=? AND user_id != ? AND referrals_with_purchase=1',
        (user_id, user_id)
    )
    rows = cursor.fetchall()

    if rows:
        usernames_list = [row[0] for row in rows if row[0]]
        if usernames_list:
            usernames = "\n".join(f"@{username}" for username in usernames_list)
            message_text = f"Р’Р°С€Рё Р°РєС‚РёРІРЅС‹Рµ СЂРµС„РµСЂР°Р»С‹:\n{usernames}"
        else:
            message_text = "РЈ РІР°СЃ РїРѕРєР° РЅРµС‚ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ."
    else:
        message_text = "РЈ РІР°СЃ РїРѕРєР° РЅРµС‚ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ."

    await callback.message.answer(message_text)

@dp.callback_query(lambda c: c.data == "check_subscriptionstar")
async def handle_check_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("РћС€РёР±РєР° РїСЂРё РїСЂРѕРІРµСЂРєРµ РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.", show_alert=True)
        return

    if is_subscribed:
        await callback.answer("РЎРїР°СЃРёР±Рѕ! Р’С‹ СѓСЃРїРµС€РЅРѕ РїРѕРґРїРёСЃР°Р»РёСЃСЊ.\U00002764", show_alert=True)
        # РџСЂРѕРґРѕР»Р¶Р°Р№С‚Рµ РІС‹РїРѕР»РЅРµРЅРёРµ Р»РѕРіРёРєРё
        await buy_stars_callback(callback)
    else:
        await callback.answer("Р’С‹ РµС‰Рµ РЅРµ РїРѕРґРїРёСЃР°Р»РёСЃСЊ РЅР° РєР°РЅР°Р».\U0001F61E", show_alert=True)
# РћР±СЂР°Р±РѕС‚РєР° РЅР°Р¶Р°С‚РёСЏ "РєСѓРїРёС‚СЊ Р·РІРµР·РґС‹"

@dp.callback_query(lambda c: c.data == "apply")
async def handle_apply(callback: types.CallbackQuery):
    # РџРѕР»СѓС‡Р°РµРј username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, РєРѕС‚РѕСЂС‹Р№ РЅР°Р¶Р°Р» РєРЅРѕРїРєСѓ
    username = callback.from_user.username
    user_id = callback.from_user.id
    # РћР±РЅРѕРІР»СЏРµРј СЃРѕСЃС‚РѕСЏРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    user_states[user_id] = {'waiting_username_apply': True}
    # РЈРґР°Р»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№
    await callback.message.delete()
    # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ
    await callback.message.answer("РљСѓРґР° РѕС‚РїСЂР°РІРёС‚СЊ Р·РІРµР·РґС‹? Р’РІРµРґРёС‚Рµ username РїРѕР»СѓС‡Р°С‚РµР»СЏ (РЅР°РїСЂРёРјРµСЂ, @username).")




@dp.message(Command("stats"))
async def admin_commands(message: types.Message):
    user_id = message.from_user.id

    if user_id not in ADMIN_IDS:
        return

    # РЎС‡РёС‚Р°РµРј РєРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РІ Р‘Р”
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    await message.answer(f"рџ‘Ґ Р’СЃРµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{total_users}</b>", parse_mode="HTML")



@dp.message(lambda message: message.text and (message.text.startswith("ref_") or message.text.startswith("delref_")))
async def admin_commands(message: types.Message):
    user_id = message.from_user.id


    if user_id not in ADMIN_IDS:
        await message.answer("РЈ РІР°СЃ РЅРµС‚ РґРѕСЃС‚СѓРїР° Рє СЌС‚РѕР№ РєРѕРјР°РЅРґРµ.")

        return

    command = message.text.strip()


    if command.startswith("ref_"):
        target_id_str = command[4:]
        action = "ref"

    elif command.startswith("delref_"):
        target_id_str = command[7:]
        action = "delref"

    else:
        await message.answer("РќРµРєРѕСЂСЂРµРєС‚РЅР°СЏ РєРѕРјР°РЅРґР°.")

        return

    if not target_id_str.isdigit():
        await message.answer("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")

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
                await message.answer(f"РђРєС‚РёРІРЅС‹Рµ СЂРµС„РµСЂР°Р»С‹ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {target_id}:\n{usernames}")
            else:
                await message.answer(f"РЈ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {target_id} РїРѕРєР° РЅРµС‚ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ.")
        else:
            await message.answer(f"РЈ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {target_id} РїРѕРєР° РЅРµС‚ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ.")
    elif action == "delref":

        # РћР±РЅРѕРІР»СЏРµРј СЃС‚Р°С‚СѓСЃ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ РЅР° РЅРµР°РєС‚РёРІРЅС‹С…
        cursor.execute(
                'UPDATE users SET referrals_with_purchase=0 WHERE referred_by=? AND referrals_with_purchase=1',
                (target_id,)
        )
        conn.commit()

        await message.answer(f"Р’СЃРµ Р°РєС‚РёРІРЅС‹Рµ СЂРµС„РµСЂР°Р»С‹ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {target_id} СѓСЃРїРµС€РЅРѕ СЃР±СЂРѕС€РµРЅС‹.")


@dp.callback_query(lambda c: c.data == "check_subscriptionprem")
async def handle_check_subscriptionprem(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        await callback.answer("РћС€РёР±РєР° РїСЂРё РїСЂРѕРІРµСЂРєРµ РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.", show_alert=True)
        return

    if is_subscribed:
        await callback.answer("РЎРїР°СЃРёР±Рѕ! Р’С‹ СѓСЃРїРµС€РЅРѕ РїРѕРґРїРёСЃР°Р»РёСЃСЊ.\U00002764", show_alert=True)
        # РџСЂРѕРґРѕР»Р¶Р°Р№С‚Рµ РІС‹РїРѕР»РЅРµРЅРёРµ Р»РѕРіРёРєРё
        await premium_handler(callback)
    else:
        await callback.answer("Р’С‹ РµС‰Рµ РЅРµ РїРѕРґРїРёСЃР°Р»РёСЃСЊ РЅР° РєР°РЅР°Р».\U0001F61E", show_alert=True)


@dp.callback_query(lambda c: c.data == "premium")
async def premium_handler(callback: types.CallbackQuery):
    # РџСЂРѕРІРµСЂРєР° РїРѕРґРїРёСЃРєРё
    user_id = callback.from_user.id
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # РћР±СЂР°Р±РѕС‚РєР° РѕС€РёР±РѕРє (РЅР°РїСЂРёРјРµСЂ, РµСЃР»Рё Р±РѕС‚ РЅРµ РІ СЃРѕСЃС‚РѕСЏРЅРёРё РїСЂРѕРІРµСЂРёС‚СЊ)
        await callback.message.answer("РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
        return

    if not is_subscribed:
        # РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РїРѕРґРїРёСЃР°РЅ вЂ” СѓРґР°Р»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ Рё РїСЂРѕСЃРёРј РїРѕРґРїРёСЃР°С‚СЊСЃСЏ
        await callback.message.delete()

        # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№ "РџРѕРґРїРёСЃР°С‚СЊСЃСЏ" Рё "РџСЂРѕРІРµСЂРёС‚СЊ"
        subscribe_button = InlineKeyboardButton(text="РџРѕРґРїРёСЃР°С‚СЊСЃСЏ", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="check_subscriptionprem")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РїРѕРґРїРёС€РёС‚РµСЃСЊ РЅР° РЅР°С€ РєР°РЅР°Р», С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ.",
            reply_markup=keyboard
        )
        return



    photo_url = "https://ibb.co/MyFDq6zx"  # Р·Р°РјРµРЅРёС‚Рµ РЅР° РЅСѓР¶РЅСѓСЋ СЃСЃС‹Р»РєСѓ
    caption = (
        f"<b>рџ’™Telegram Premiumрџ’™</b>\n\n"
        f"<b>Р’С‹Р±РµСЂРёС‚Рµ СЃСЂРѕРє РїРѕРґРїРёСЃРєРё:</b>"
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
        await callback.answer("РћС€РёР±РєР° РІС‹Р±РѕСЂР° СЃСЂРѕРєР° РїРѕРґРїРёСЃРєРё.", show_alert=True)
        return

    # РЎРѕС…СЂР°РЅСЏРµРј РґР°РЅРЅС‹Рµ Рѕ РїРѕРєСѓРїРєРµ Premium
    user_premium_data[user_id] = {
        'months': months,
        'price': price,
        'username': None
    }

    # РџСЂРѕСЃРёРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РІРІРµСЃС‚Рё username
    photo_url = "https://ibb.co/MyFDq6zx"
    caption = (
        "рџ“› <b>РўРµРїРµСЂСЊ СѓРєР°Р¶Рё username Telegram-Р°РєРєР°СѓРЅС‚Р°, РєСѓРґР° РЅСѓР¶РЅРѕ РїСЂРёРѕР±СЂРµСЃС‚Рё Premium.</b>\n\n"
        "Р’Р°Р¶РЅРѕ:\n"
        "вЂў <b>РЈР±РµРґРёСЃСЊ</b>, С‡С‚Рѕ С‚РІРѕР№ username РєРѕСЂСЂРµРєС‚РЅРѕ СѓРєР°Р·Р°РЅ (РЅР°РїСЂРёРјРµСЂ, `@example`).\n"
        "вЂў <b>Р•СЃР»Рё Сѓ С‚РµР±СЏ РЅРµС‚ username</b>, РµРіРѕ РЅСѓР¶РЅРѕ СЃРѕР·РґР°С‚СЊ РІ РЅР°СЃС‚СЂРѕР№РєР°С… Telegram.\n"
        "вЂў Premium Р±СѓРґРµС‚ Р·Р°С‡РёСЃР»РµРЅ <b>РІ С‚РµС‡РµРЅРёРµ 5-С‚Рё РјРёРЅСѓС‚</b> РїРѕСЃР»Рµ РѕРїР»Р°С‚С‹"
    )

    await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=caption, parse_mode='HTML')
    )

    user_states[user_id] = {'awaiting_premium_username': True}
    await callback.answer()


# Р“РµРЅРµСЂР°С†РёСЏ СѓРЅРёРєР°Р»СЊРЅРѕРіРѕ РЅРѕРјРµСЂР° РїР»Р°С‚РµР¶Р°
def generate_unique_label_prem(user_id):
    now = datetime.datetime.now()
    return f"{now.year:02d}{now.month:02d}{now.day:02d}{now.hour:02d}{now.minute:02d}{now.second:02d}{user_id}"


@dp.callback_query(lambda c: c.data == "pay_prem")
async def pay_pally_prem_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in user_premium_data:
        await callback.message.answer(
            "вљ пёЏ РћС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРєСѓРїРєРµ РЅРµ РЅР°Р№РґРµРЅС‹. РџРѕРїСЂРѕР±СѓР№С‚Рµ Р·Р°РЅРѕРІРѕ РІС‹Р±СЂР°С‚СЊ С‚Р°СЂРёС„ Рё РІРІРµСЃС‚Рё username."
        )
        return

    data = user_premium_data[user_id]
    months = data.get("months")
    price = data.get("price")
    target_username = data.get("username")

    if not months or not price:
        await callback.message.answer("РћС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРґРїРёСЃРєРµ РЅРµ РЅР°Р№РґРµРЅС‹. РџРѕРїСЂРѕР±СѓР№С‚Рµ РІС‹Р±СЂР°С‚СЊ С‚Р°СЂРёС„ Р·Р°РЅРѕРІРѕ.")
        return

    if not target_username:
        user_states[user_id] = {'awaiting_premium_username': True}
        await callback.message.answer(
            "вќ— РџРѕР¶Р°Р»СѓР№СЃС‚Р°, СЃРЅР°С‡Р°Р»Р° СѓРєР°Р¶Рё username РґР»СЏ Р°РєС‚РёРІР°С†РёРё Premium."
        )
        return

    # РџСЂРёРјРµРЅСЏРµРј РїСЂРѕРјРѕ, РµСЃР»Рё РµСЃС‚СЊ
    amount_value = data.get("promo_pending", {}).get("new_price", price)

    label_str = generate_unique_label_prem(user_id)
    target_username = data.get("username")
    if not target_username:
        user_states[user_id] = {'awaiting_premium_username': True}
        await callback.message.answer(
            "вќ— РџРѕР¶Р°Р»СѓР№СЃС‚Р°, СЃРЅР°С‡Р°Р»Р° СѓРєР°Р¶Рё username РґР»СЏ Р°РєС‚РёРІР°С†РёРё Premium."
        )
        return

    try:
        payment_id, payment_url = await create_pally_payment(
            amount_value,
            f"РџРѕРєСѓРїРєР° Premium {months} РјРµСЃ. РґР»СЏ {target_username}",
            label_str
        )
    except Exception as e:
        print(f"РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ РїР»Р°С‚РµР¶Р°: {e}")
        await callback.message.answer("РћС€РёР±РєР° РїСЂРё СЃРѕР·РґР°РЅРёРё РїР»Р°С‚РµР¶Р°. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
        return

    # РЎРѕС…СЂР°РЅСЏРµРј ID РїР»Р°С‚РµР¶Р° Рё СЃСѓРјРјСѓ
    user_premium_data[user_id]["pally_payment_id"] = payment_id
    user_premium_data[user_id]["label"] = label_str
    user_premium_data[user_id]["amount"] = amount_value

    # РћС‚РїСЂР°РІР»СЏРµРј РєРЅРѕРїРєСѓ РѕРїР»Р°С‚С‹
    pay_button = InlineKeyboardButton(text=f"рџ’і РћРїР»Р°С‚РёС‚СЊ {amount_value}в‚Ѕ", url=payment_url)
    markup = InlineKeyboardMarkup(inline_keyboard=[[pay_button]])

    await callback.message.edit_text(
        "РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ РґР»СЏ РїРµСЂРµС…РѕРґР° Рє РѕРїР»Р°С‚Рµ. РћРїР»Р°С‚Р° Р±СѓРґРµС‚ РїСЂРѕРІРµСЂСЏС‚СЊСЃСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё.",
        reply_markup=markup
    )

    # РџСЂРѕРІРµСЂСЏРµРј РѕРїР»Р°С‚Сѓ 10 РјРёРЅСѓС‚
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
            print(f"РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё СЃС‚Р°С‚СѓСЃР° РїР»Р°С‚РµР¶Р° Premium: {e}")

        await asyncio.sleep(3)

    if payment_confirmed:
        await process_payment_confirmationprem(user_id, target_username, months, callback)
    else:
        await callback.message.answer("РћРїР»Р°С‚Р° РЅРµ Р±С‹Р»Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР° Р·Р° 10 РјРёРЅСѓС‚. РџРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅРѕРІР°.")


# РћР±СЂР°Р±РѕС‚РєР° СѓСЃРїРµС€РЅРѕР№ РѕРїР»Р°С‚С‹ Premium
async def process_payment_confirmationprem(user_id, target_username, months, callback):
    """
    РџРѕР»РЅР°СЏ РѕР±СЂР°Р±РѕС‚РєР° РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ Premium:
    - РћР±РЅРѕРІР»СЏРµС‚ СЃС‚Р°С‚СѓСЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    - РЎРѕС…СЂР°РЅСЏРµС‚ РїРѕРєСѓРїРєСѓ
    - РЈРІРµРґРѕРјР»СЏРµС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё РіСЂСѓРїРїСѓ
    """
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

    # РџСЂРѕРјРѕ-РєРѕРґ
    final_price = user_premium_data[user_id]["price"]
    if "promo_pending" in user_premium_data[user_id]:
        promo_data = user_premium_data[user_id].pop("promo_pending")
        final_price = promo_data["new_price"]
        promo_code_used = promo_data["code"]
        confirm_promo_usage(promo_code_used, user_id)

    # РЎРѕС…СЂР°РЅСЏРµРј РїРѕРєСѓРїРєСѓ
    buyer_username = callback.from_user.username or callback.from_user.first_name
    cursor.execute(
        "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
        (user_id, target_username, "premium", months, final_price)
    )
    conn.commit()

    # РЈРІРµРґРѕРјР»СЏРµРј РіСЂСѓРїРїСѓ
    message_text_group = (
        f"@{buyer_username} вЂ” РѕРїР»Р°С‚Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР° РґР»СЏ Р°РєРєР°СѓРЅС‚Р° {target_username} PREMIUM РЅР° {months} РјРµСЃ."
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="delete_msg")]]
    )
    await bot.send_message(GROUP_CHAT_ID, message_text_group, reply_markup=keyboard)

    # РЈРІРµРґРѕРјР»СЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    await bot.send_message(
        user_id,
        "вњ… РћРїР»Р°С‚Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР°! РЎРїР°СЃРёР±Рѕ Р·Р° РїРѕРєСѓРїРєСѓ!\n"
        f"Premium СЃРєРѕСЂРѕ РїРѕСЃС‚СѓРїРёС‚ РЅР° РІР°С€ Р°РєРєР°СѓРЅС‚ {target_username}.\n"
        f'рџ’¬ <a href="https://t.me/+Qkb-Q43fRf40NGFk">РћС†РµРЅРёС‚СЊ РЅР°С€ СЃРµСЂРІРёСЃ</a>',
        parse_mode="HTML",
        disable_web_page_preview=True
    )

    # РћС‡РёС‰Р°РµРј РґР°РЅРЅС‹Рµ
    user_premium_data.pop(user_id, None)


@dp.callback_query(lambda c: c.data == "buy_stars")
async def buy_stars_callback(callback: types.CallbackQuery):
    # РџСЂРѕРІРµСЂРєР° РїРѕРґРїРёСЃРєРё
    user_id = callback.from_user.id
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # РћР±СЂР°Р±РѕС‚РєР° РѕС€РёР±РѕРє (РЅР°РїСЂРёРјРµСЂ, РµСЃР»Рё Р±РѕС‚ РЅРµ РІ СЃРѕСЃС‚РѕСЏРЅРёРё РїСЂРѕРІРµСЂРёС‚СЊ)
        await callback.message.answer("РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
        return

    if not is_subscribed:
        # РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РїРѕРґРїРёСЃР°РЅ вЂ” СѓРґР°Р»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ Рё РїСЂРѕСЃРёРј РїРѕРґРїРёСЃР°С‚СЊСЃСЏ
        await callback.message.delete()

        # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№ "РџРѕРґРїРёСЃР°С‚СЊСЃСЏ" Рё "РџСЂРѕРІРµСЂРёС‚СЊ"
        subscribe_button = InlineKeyboardButton(text="РџРѕРґРїРёСЃР°С‚СЊСЃСЏ", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="check_subscriptionstar")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РїРѕРґРїРёС€РёС‚РµСЃСЊ РЅР° РЅР°С€ РєР°РЅР°Р», С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ.",
            reply_markup=keyboard
        )
        return

    photo_url = "https://ibb.co/MyFDq6zx"
    caption = 'Р’С‹Р±РµСЂРё РѕРґРёРЅ РёР· РїРѕРїСѓР»СЏСЂРЅС‹С… РїР°РєРµС‚РѕРІ РёР»Рё СѓРєР°Р¶Рё СЃРІРѕС‘ РєРѕР»РёС‡РµСЃС‚РІРѕ:'
    await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=caption),
        reply_markup=get_star_keyboard()
    )


@dp.callback_query(lambda c: c.data.endswith("stars"))
async def select_stars(callback: types.CallbackQuery):
    await callback.answer()  # РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ РѕС‚РІРµС‡Р°РµРј РЅР° callback, С‡С‚РѕР±С‹ СѓР±СЂР°С‚СЊ "С‡Р°СЃРёРєРё"
    photo_url = "https://ibb.co/MyFDq6zx"
    caption = (
        "рџ“› <b>РўРµРїРµСЂСЊ СѓРєР°Р¶Рё username Telegram-Р°РєРєР°СѓРЅС‚Р°, РєСѓРґР° РЅСѓР¶РЅРѕ Р·Р°С‡РёСЃР»РёС‚СЊ Р·РІС‘Р·РґС‹.</b>\n\n"
        "Р’Р°Р¶РЅРѕ:\n"
        "вЂў <b>РЈР±РµРґРёСЃСЊ</b>, С‡С‚Рѕ С‚РІРѕР№ username РєРѕСЂСЂРµРєС‚РЅРѕ СѓРєР°Р·Р°РЅ (РЅР°РїСЂРёРјРµСЂ, @example).\n"
        "вЂў <b>Р•СЃР»Рё Сѓ С‚РµР±СЏ РЅРµС‚ username</b>, РµРіРѕ РЅСѓР¶РЅРѕ СЃРѕР·РґР°С‚СЊ РІ РЅР°СЃС‚СЂРѕР№РєР°С… Telegram.\n"
        "вЂў Р—РІС‘Р·РґС‹ Р±СѓРґСѓС‚ Р·Р°С‡РёСЃР»РµРЅС‹ <b>РІ С‚РµС‡РµРЅРёРµ 5-С‚Рё РјРёРЅСѓС‚</b> РїРѕСЃР»Рµ РѕРїР»Р°С‚С‹"
    )
    data_str = callback.data[:-5]  # СѓР±РёСЂР°РµРј "stars"

    try:
        stars_amount = int(data_str)  # РЎРєРѕР»СЊРєРѕ Р·РІРµР·Рґ РІС‹Р±СЂР°Р» РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ
        multiplier = get_star_rate_for_range(stars_amount)  # <-- РґРёРЅР°РјРёС‡РµСЃРєРёР№ РєСѓСЂСЃ РёР· Р±Р°Р·С‹
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
        await callback.message.answer("РџСЂРѕРёР·РѕС€Р»Р° РѕС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ РІС‹Р±РѕСЂР°. РџРѕРїСЂРѕР±СѓР№С‚Рµ РµС‰С‘ СЂР°Р·.")



def build_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°", callback_data="admin_stats_menu"),
         InlineKeyboardButton(text="рџЋџ РџСЂРѕРјРѕ-РєРѕРґС‹", callback_data="admin_promo_menu")],

        [InlineKeyboardButton(text="рџ‘Ґ Р РµС„РµСЂР°Р»С‹", callback_data="admin_ref_menu"),
         InlineKeyboardButton(text="в­ђ РџСЂРѕРґР°РЅРЅС‹Рµ Р·РІС‘Р·РґС‹", callback_data="admin_stars_total")],

        [InlineKeyboardButton(text="рџ“ў Р Р°СЃСЃС‹Р»РєР°", callback_data="broadcast_start"),
         InlineKeyboardButton(text="рџ’і РџР»Р°С‚С‘Р¶РєР°", callback_data="payment_menu")],
        [InlineKeyboardButton(text="рџ”Ќ РќР°Р№С‚Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ", callback_data="admin_find_user")],
        [InlineKeyboardButton(text="вћ• РќР°С‡РёСЃР»РёС‚СЊ Р·РІС‘Р·РґС‹", callback_data="cad")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="back_first")]
    ])


def build_admin_panel_text() -> str:
    return (
        "вљ™пёЏ <b>РђРґРјРёРЅ-РїР°РЅРµР»СЊ</b>\n\n"
        "Р’С‹Р±РµСЂРёС‚Рµ РЅСѓР¶РЅС‹Р№ СЂР°Р·РґРµР»:"
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
        "Р’РІРµРґРёС‚Рµ <b>ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ</b>, РєРѕС‚РѕСЂРѕРјСѓ РЅСѓР¶РЅРѕ РЅР°С‡РёСЃР»РёС‚СЊ Р·РІС‘Р·РґС‹:", parse_mode="HTML"
    )

@dp.message(AddStarsForm.waiting_user_id)
async def admin_add_stars_get_user(message: types.Message, state: FSMContext):

    if not message.text.isdigit():

        await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ РєРѕСЂСЂРµРєС‚РЅС‹Р№ С‡РёСЃР»РѕРІРѕР№ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
        return

    await state.update_data(user_id=int(message.text))
    await state.set_state(AddStarsForm.waiting_amount)

    await message.answer("РўРµРїРµСЂСЊ РІРІРµРґРёС‚Рµ <b>РєРѕР»РёС‡РµСЃС‚РІРѕ Р·РІС‘Р·Рґ</b> РґР»СЏ РЅР°С‡РёСЃР»РµРЅРёСЏ:", parse_mode="HTML")


@dp.message(AddStarsForm.waiting_amount)
async def admin_add_stars_confirm(message: types.Message, state: FSMContext):

    data = await state.get_data()
    user_id = data.get("user_id")


    if not message.text.isdigit():

        await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ (РєРѕР»РёС‡РµСЃС‚РІРѕ Р·РІС‘Р·Рґ).")
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

        await message.answer("вќЊ РџСЂРѕРёР·РѕС€Р»Р° РѕС€РёР±РєР° РїСЂРё РґРѕР±Р°РІР»РµРЅРёРё РІ Р±Р°Р·Сѓ РґР°РЅРЅС‹С….")
        return

    await message.answer(f"вњ… РџРѕР»СЊР·РѕРІР°С‚РµР»СЋ <b>{user_id}</b> РЅР°С‡РёСЃР»РµРЅРѕ <b>{stars_to_add}в­ђпёЏ</b>.", parse_mode="HTML")

    # РџС‹С‚Р°РµРјСЃСЏ СѓРІРµРґРѕРјРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    try:
        await bot.send_message(user_id, f"рџЋЃ Р’Р°Рј РЅР°С‡РёСЃР»РµРЅРѕ {stars_to_add}в­ђпёЏ РѕС‚ Р°РґРјРёРЅРёСЃС‚СЂР°С†РёРё!")

    except Exception as e:

        await message.answer("вљ пёЏ РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ СѓРІРµРґРѕРјР»РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ (РІРѕР·РјРѕР¶РЅРѕ, Р±РѕС‚ Р·Р°Р±Р»РѕРєРёСЂРѕРІР°РЅ).")





# === 1. РђРґРјРёРЅ РЅР°Р¶РёРјР°РµС‚ РєРЅРѕРїРєСѓ "РќР°Р№С‚Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ" ===
@dp.callback_query(lambda c: c.data == "admin_find_user")
async def ask_username_or_id(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("рџљ« РќРµС‚ РґРѕСЃС‚СѓРїР°", show_alert=True)
        return

    user_states[callback.from_user.id] = {"awaiting_user_lookup": True}
    await callback.message.answer("рџ§ѕ Р’РІРµРґРёС‚Рµ username (РЅР°РїСЂРёРјРµСЂ: @example) РёР»Рё ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ:")


# === 2. РџРѕРёСЃРє РїРѕ username РёР»Рё ID ===
@dp.message(lambda m: user_states.get(m.from_user.id, {}).get("awaiting_user_lookup"))
async def lookup_user_data(message: types.Message):
    query = message.text.strip().lstrip("@")
    user_states.pop(message.from_user.id, None)

    # РћРїСЂРµРґРµР»СЏРµРј вЂ” СЌС‚Рѕ ID РёР»Рё username
    if query.isdigit():
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (int(query),))
    else:
        cursor.execute("SELECT * FROM users WHERE username = ?", (query,))
    user_row = cursor.fetchone()

    if not user_row:
        await message.answer("вќЊ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ РІ Р±Р°Р·Рµ.")
        return

    user_id = user_row[0]

    # --- РџРѕРєСѓРїРєРё ---
    cursor.execute("SELECT item_type, amount, cost, created_at FROM purchases WHERE user_id = ?", (user_id,))
    purchases = cursor.fetchall()

    # --- РџСЂРѕРјРѕРєРѕРґС‹ ---
    cursor.execute("SELECT code FROM used_promo WHERE user_id = ?", (user_id,))
    used_promos = [row[0] for row in cursor.fetchall()]

    # --- РџР»Р°С‚РµР¶Рё ---
    cursor.execute("SELECT amount, status, date FROM payments WHERE user_id = ?", (user_id,))
    payments = cursor.fetchall()

    # --- Р¤РѕСЂРјРёСЂСѓРµРј РѕС‚РІРµС‚ ---
    text = f"<b>рџ”Ћ РРЅС„РѕСЂРјР°С†РёСЏ Рѕ РїРѕР»СЊР·РѕРІР°С‚РµР»Рµ:</b>\n\n"
    text += f"рџ‘¤ <b>ID:</b> <code>{user_row[0]}</code>\n"
    text += f"рџЄЄ <b>Username:</b> @{user_row[1] or 'вЂ”'}\n"
    text += f"рџ§· <b>РРјСЏ:</b> {user_row[2] or '-'} {user_row[3] or ''}\n"
    text += f"рџ‘Ґ <b>РџСЂРёРіР»Р°СЃРёР»:</b> {user_row[4] or 'вЂ”'}\n"
    text += f"рџ”— <b>Р РµС„. РєРѕРґ:</b> {user_row[5] or 'вЂ”'}\n"
    text += f"рџ‘¤ <b>Р РµС„РµСЂР°Р»РѕРІ:</b> {user_row[7]} (СЃ РїРѕРєСѓРїРєРѕР№: {user_row[8]})\n\n"

    # РџРѕРєСѓРїРєРё
    if purchases:
        text += "<b>рџ›’ РџРѕРєСѓРїРєРё:</b>\n"
        for p in purchases:
            text += f"вЂў {p[0]} вЂ” {p[1]} С€С‚. Р·Р° {p[2]}в‚Ѕ ({p[3]})\n"
    else:
        text += "рџ›’ РќРµС‚ РїРѕРєСѓРїРѕРє.\n"

    # РџСЂРѕРјРѕРєРѕРґС‹
    if used_promos:
        text += "\n<b>рџЋџ РСЃРїРѕР»СЊР·РѕРІР°РЅРЅС‹Рµ РїСЂРѕРјРѕРєРѕРґС‹:</b>\n" + ", ".join(used_promos)
    else:
        text += "\nрџЋџ РџСЂРѕРјРѕРєРѕРґС‹ РЅРµ РёСЃРїРѕР»СЊР·РѕРІР°Р».\n"

    # РџР»Р°С‚РµР¶Рё
    if payments:
        text += "\n<b>рџ’і РџР»Р°С‚РµР¶Рё:</b>\n"
        for pay in payments:
            text += f"вЂў {pay[0]}в‚Ѕ вЂ” {pay[1]} ({pay[2]})\n"
    else:
        text += "\nрџ’і РџР»Р°С‚РµР¶РµР№ РЅРµС‚.\n"

    await message.answer(text, parse_mode="HTML")







@dp.callback_query(lambda c: c.data == "payment_menu")
async def payment_menu(callback: types.CallbackQuery):
    await callback.answer()  # РџРѕРґС‚РІРµСЂР¶РґР°РµРј callback

    # Р—РґРµСЃСЊ РґРѕР±Р°РІРёРј РєСЂР°СЃРёРІРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєР°РјРё РґР»СЏ РґРµР№СЃС‚РІРёР№.
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџ’і РЎРґРµР»Р°С‚СЊ РІС‹РїР»Р°С‚Сѓ", callback_data="payment_withdraw")],
        [InlineKeyboardButton(text="рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="payment_refresh")],
        [InlineKeyboardButton(text="рџ“њ РСЃС‚РѕСЂРёСЏ РїРѕРїРѕР»РЅРµРЅРёР№", callback_data="payment_history")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
    ])

    # РџРѕРєР°Р¶РµРј Р±Р°Р»Р°РЅСЃ СЃСЂР°Р·Сѓ РїСЂРё РѕС‚РєСЂС‹С‚РёРё РјРµРЅСЋ
    balance = await get_pally_balance()
    await callback.message.edit_text(
        f"рџ’і <b>РџР»Р°С‚С‘Р¶РєР°</b>\n\n"
        f"Р’Р°С€ С‚РµРєСѓС‰РёР№ Р±Р°Р»Р°РЅСЃ: <b>{balance:.2f} в‚Ѕ</b>\n\n"
        f"Р§С‚Рѕ Р±С‹ РІС‹ С…РѕС‚РµР»Рё СЃРґРµР»Р°С‚СЊ РґР°Р»СЊС€Рµ?",
        parse_mode="HTML",
        reply_markup=keyboard
    )


async def initiate_payment_withdraw(amount: float, user_id: str):
    """
    Р—Р°РїСЂРѕСЃ РЅР° РІС‹РїРѕР»РЅРµРЅРёРµ РІС‹РїР»Р°С‚С‹ С‡РµСЂРµР· Pally API.
    """
    url = f"{PALLY_API_BASE}/merchant/withdraw"  # РџСЂРёРјРµСЂРЅС‹Р№ endpoint РґР»СЏ Р·Р°РїСЂРѕСЃР°
    headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}

    data = {
        "user_id": user_id,
        "amount": amount
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            response_data = await resp.json()
            if response_data.get("success"):
                return response_data  # Р’РѕР·РІСЂР°С‰Р°РµРј СѓСЃРїРµС€РЅС‹Р№ РѕС‚РІРµС‚
            else:
                return None  # Р’РѕР·РІСЂР°С‰Р°РµРј None, РµСЃР»Рё РЅРµСѓРґР°С‡РЅРѕ

async def get_pally_balance():
    url = f"{PALLY_API_BASE}/merchant/balance"
    headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()


            if str(data.get("success")).lower() == "true":
                # РёС‰РµРј Р±Р°Р»Р°РЅСЃ РІ RUB
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
        f"рџ’° Р‘Р°Р»Р°РЅСЃ Р°РєРєР°СѓРЅС‚Р°: <b>{balance:.2f} в‚Ѕ</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="payment_menu")]
        ])
    )


@dp.callback_query(lambda c: c.data == "payment_refresh")
async def payment_refresh(callback: types.CallbackQuery):
    balance = await get_pally_balance()  # РџРѕР»СѓС‡Р°РµРј РѕР±РЅРѕРІР»РµРЅРЅС‹Р№ Р±Р°Р»Р°РЅСЃ
    await callback.message.edit_text(
        f"рџ”„ РћР±РЅРѕРІР»РµРЅРѕ!\n\n"
        f"Р’Р°С€ С‚РµРєСѓС‰РёР№ Р±Р°Р»Р°РЅСЃ: <b>{balance:.2f} в‚Ѕ</b>\n\n"
        f"Р§С‚Рѕ Р±С‹ РІС‹ С…РѕС‚РµР»Рё СЃРґРµР»Р°С‚СЊ РґР°Р»СЊС€Рµ?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="рџ’і РЎРґРµР»Р°С‚СЊ РІС‹РїР»Р°С‚Сѓ", callback_data="payment_withdraw")],
            [InlineKeyboardButton(text="рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="payment_refresh")],
            [InlineKeyboardButton(text="рџ“њ РСЃС‚РѕСЂРёСЏ РїРѕРїРѕР»РЅРµРЅРёР№", callback_data="payment_history")],
            [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
        ])
    )


def get_payment_history():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    try:
        cursor.execute(
            'SELECT bill_id, amount, status, description, date, user_id FROM payments WHERE status="paid" ORDER BY date DESC LIMIT 10',
        )
        rows = cursor.fetchall()

        if rows:
            history_message = "<b>рџ“њ РСЃС‚РѕСЂРёСЏ РїРѕРїРѕР»РЅРµРЅРёР№:</b>\n\n"
            for idx, row in enumerate(rows, start=1):
                bill_id, amount, status, description, date, user_id = row
                status_emoji = "вњ… Success"  # РЎС‚Р°С‚СѓСЃ "paid" РІСЃРµРіРґР° Р±СѓРґРµС‚ Success
                formatted_date = format_date(date)  # РџСЂРµРѕР±СЂР°Р·СѓРµРј РґР°С‚Сѓ СЃ СѓС‡С‘С‚РѕРј С‡Р°СЃРѕРІРѕРіРѕ РїРѕСЏСЃР°

                # РџРѕР»СѓС‡Р°РµРј РёРјСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, РµСЃР»Рё РЅРµРѕР±С…РѕРґРёРјРѕ (РјРѕР¶РЅРѕ РёР·РІР»РµС‡СЊ РµРіРѕ РёР· Р±Р°Р·С‹ РґР°РЅРЅС‹С…)
                history_message += f"<b>рџ”№ в„–{idx} рџ§ѕ Bill ID:</b> `{bill_id}`\n"
                history_message += f"<b>рџ’° РЎСѓРјРјР°:</b> {amount}в‚Ѕ\n"
                history_message += f"<b>рџ”„ РЎС‚Р°С‚СѓСЃ:</b> {status_emoji}\n"
                history_message += f"<b>рџ“„ РћРїРёСЃР°РЅРёРµ:</b> {description}\n"
                history_message += f"<b>рџ“… Р”Р°С‚Р°:</b> {formatted_date}\n"
                history_message += f"<b>рџ‘¤ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ:</b> {user_id}\n"  # Р•СЃР»Рё РЅРµРѕР±С…РѕРґРёРјРѕ, РїРѕРєР°Р·С‹РІР°РµРј user_id
                history_message += "<b>---------------------------------</b>\n"

            history_message += "<b>рџ“ќ РџРѕРєР°Р·Р°РЅС‹ РїРѕСЃР»РµРґРЅРёРµ 10 РїРѕРїРѕР»РЅРµРЅРёР№.</b>"
        else:
            history_message = "<b>вљ пёЏ РќРµС‚ РёСЃС‚РѕСЂРёРё РїРѕРїРѕР»РЅРµРЅРёР№.</b>"

        return history_message

    except sqlite3.Error as e:
        logging.error(f"РћС€РёР±РєР° РїСЂРё РїРѕР»СѓС‡РµРЅРёРё РёСЃС‚РѕСЂРёРё РїР»Р°С‚РµР¶РµР№: {e}")
        return "<b>вљ пёЏ РћС€РёР±РєР° РїСЂРё РїРѕР»СѓС‡РµРЅРёРё РёСЃС‚РѕСЂРёРё РїР»Р°С‚РµР¶РµР№.</b>"

    finally:
        conn.close()




def debug_check_paid_payments(user_id):
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Р—Р°РїСЂРѕСЃ РґР»СЏ РїРѕР»СѓС‡РµРЅРёСЏ С‚РѕР»СЊРєРѕ РѕРїР»Р°С‡РµРЅРЅС‹С… РїР»Р°С‚РµР¶РµР№
    cursor.execute(
        'SELECT bill_id, amount, status, description, date FROM payments WHERE user_id=? AND status="paid" ORDER BY date DESC',
        (user_id,)
    )
    rows = cursor.fetchall()

    conn.close()





async def get_pally_bill_info():
        url = f"{PALLY_API_BASE}/bill/payments"
        headers = {"Authorization": f"Bearer {PALLY_API_TOKEN}"}
        params = {"id": bill_id, "per_page": 10}  # РћРіСЂР°РЅРёС‡РёРІР°РµРј РґРѕ 10 Р·Р°РїРёСЃРµР№ РґР»СЏ С‚РµСЃС‚Р°

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("success") and "data" in data:
                            return data["data"]
                        else:
                            logging.error(f"РћС€РёР±РєР° РІ РѕС‚РІРµС‚Рµ РѕС‚ Pally: {data.get('message')}")
                            return []
                    else:
                        logging.error(f"РћС€РёР±РєР° Р·Р°РїСЂРѕСЃР°: {resp.status}")
                        return []
            except Exception as e:
                logging.error(f"РћС€РёР±РєР° РїСЂРё Р·Р°РїСЂРѕСЃРµ РёСЃС‚РѕСЂРёРё РїР»Р°С‚РµР¶РµР№: {e}")
                return []


from datetime import datetime
from dateutil import parser
from datetime import timedelta

def format_date(date_str):
    try:
        # РСЃРїРѕР»СЊР·СѓРµРј parser.parse РґР»СЏ РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёСЏ СЃС‚СЂРѕРєРё РІ РѕР±СЉРµРєС‚ datetime
        original_date = parser.parse(date_str)

        # РџРѕР»СѓС‡Р°РµРј С‡Р°СЃРѕРІРѕР№ РїРѕСЏСЃ РЎР°РјР°СЂС‹ (UTC+4)
        samara_tz = pytz.timezone('Europe/Samara')

        # РџСЂРµРѕР±СЂР°Р·СѓРµРј РІСЂРµРјСЏ РІ С‡Р°СЃРѕРІРѕР№ РїРѕСЏСЃ РЎР°РјР°СЂС‹
        samara_time = original_date.astimezone(samara_tz)

        # Р”РѕР±Р°РІР»СЏРµРј 3 С‡Р°СЃР°
        samara_time_plus_3 = samara_time + timedelta(hours=3)

        # Р’РѕР·РІСЂР°С‰Р°РµРј РѕС‚С„РѕСЂРјР°С‚РёСЂРѕРІР°РЅРЅСѓСЋ РґР°С‚Сѓ РІ РЅСѓР¶РЅРѕРј С„РѕСЂРјР°С‚Рµ
        return samara_time_plus_3.strftime("%Y-%m-%d  %H:%M:%S")  # РџСЂРёРјРµСЂ: 2025-10-31 20:47:20
    except ValueError as e:
        print(f"РћС€РёР±РєР° РїСЂРё С„РѕСЂРјР°С‚РёСЂРѕРІР°РЅРёРё РґР°С‚С‹: {e}")
        return date_str  # Р’РѕР·РІСЂР°С‰Р°РµРј РёСЃС…РѕРґРЅСѓСЋ СЃС‚СЂРѕРєСѓ, РµСЃР»Рё РѕС€РёР±РєР°



@dp.callback_query(lambda c: c.data == "payment_history")
async def history_payments_callback(callback: types.CallbackQuery):
    # РЈР±РёСЂР°РµРј РїРµСЂРµРґР°С‡Сѓ user_id, С‚Р°Рє РєР°Рє Р·Р°РїСЂРѕСЃ С‚РµРїРµСЂСЊ РЅРµ Р·Р°РІРёСЃРёС‚ РѕС‚ РЅРµРіРѕ
    history_message = get_payment_history()

    # РЎРѕР·РґР°РµРј РєРЅРѕРїРєРё "РќР°Р·Р°Рґ" Рё "РћР±РЅРѕРІРёС‚СЊ"
    back_button = InlineKeyboardButton(text="рџ”™ РќР°Р·Р°Рґ", callback_data="back_to_main")
    refresh_button = InlineKeyboardButton(text="рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="payment_history")
    markup = InlineKeyboardMarkup(inline_keyboard=[[back_button, refresh_button]])

    try:
        await callback.message.edit_text(history_message, parse_mode="HTML", reply_markup=markup)

    except sqlite3.Error as e:
        logging.error(f"РћС€РёР±РєР° РїСЂРё РїРѕР»СѓС‡РµРЅРёРё РёСЃС‚РѕСЂРёРё РїР»Р°С‚РµР¶РµР№: {e}")
        await callback.message.answer(f"<b>вљ пёЏ РћС€РёР±РєР° РїСЂРё РїРѕР»СѓС‡РµРЅРёРё РёСЃС‚РѕСЂРёРё РїР»Р°С‚РµР¶РµР№: {e}</b>", parse_mode="HTML")


@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await callback.answer()  # РџРѕРґС‚РІРµСЂР¶РґР°РµРј callback

    # Р—РґРµСЃСЊ РґРѕР±Р°РІРёРј РєСЂР°СЃРёРІРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєР°РјРё РґР»СЏ РґРµР№СЃС‚РІРёР№.
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџ’і РЎРґРµР»Р°С‚СЊ РІС‹РїР»Р°С‚Сѓ", callback_data="payment_withdraw")],
        [InlineKeyboardButton(text="рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="payment_refresh")],
        [InlineKeyboardButton(text="рџ“њ РСЃС‚РѕСЂРёСЏ РїРѕРїРѕР»РЅРµРЅРёР№", callback_data="payment_history")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
    ])

    # РџРѕРєР°Р¶РµРј Р±Р°Р»Р°РЅСЃ СЃСЂР°Р·Сѓ РїСЂРё РѕС‚РєСЂС‹С‚РёРё РјРµРЅСЋ
    balance = await get_pally_balance()
    await callback.message.edit_text(
        f"рџ’і <b>РџР»Р°С‚С‘Р¶РєР°</b>\n\n"
        f"Р’Р°С€ С‚РµРєСѓС‰РёР№ Р±Р°Р»Р°РЅСЃ: <b>{balance:.2f} в‚Ѕ</b>\n\n"
        f"Р§С‚Рѕ Р±С‹ РІС‹ С…РѕС‚РµР»Рё СЃРґРµР»Р°С‚СЊ РґР°Р»СЊС€Рµ?",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "payment_history")
async def update_payment_history(callback: types.CallbackQuery):
    # РџРµСЂРµР·Р°РїСѓСЃРєР°РµРј С„СѓРЅРєС†РёСЋ РґР»СЏ РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё
    user_id = callback.from_user.id
    history_message = get_payment_history(user_id)

    # РЎРѕР·РґР°РµРј РєРЅРѕРїРєРё "РќР°Р·Р°Рґ" Рё "РћР±РЅРѕРІРёС‚СЊ"
    back_button = InlineKeyboardButton(text="рџ”™ РќР°Р·Р°Рґ", callback_data="back_to_main")
    refresh_button = InlineKeyboardButton(text="рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="payment_history")
    markup = InlineKeyboardMarkup(inline_keyboard=[[back_button, refresh_button]])

    await callback.message.edit_text(history_message, parse_mode="HTML", reply_markup=markup)



@dp.callback_query(lambda c: c.data == "admin_stats_menu")
async def admin_stats_menu(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџ‘Ґ РџРѕР»СЊР·РѕРІР°С‚РµР»Рё", callback_data="stats_users")],
        [InlineKeyboardButton(text="рџ’ё РџСЂРѕРґР°Р¶Рё", callback_data="stats_sales")],
        [InlineKeyboardButton(text="рџ”„ РћС‡РёСЃС‚РёС‚СЊ СЃС‚Р°С‚РёСЃС‚РёРєСѓ РїСЂРѕРґР°Р¶", callback_data="stats_clear")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
    ])

    await callback.message.edit_text(
        "рџ“Љ <b>РЎС‚Р°С‚РёСЃС‚РёРєР°</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РЅСѓР¶РЅС‹Р№ РїСѓРЅРєС‚:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# рџ‘¤ РљРѕР»-РІРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№
@dp.callback_query(lambda c: c.data == "stats_users")
async def stats_users(callback: types.CallbackQuery):
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    await callback.message.answer(f"рџ‘Ґ Р’СЃРµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{total_users}</b>", parse_mode="HTML")


def get_star_cost():
    """РџРѕР»СѓС‡РёС‚СЊ С‚РµРєСѓС‰РёР№ РєСѓСЂСЃ Р·РІРµР·РґС‹"""
    cursor.execute("SELECT value FROM settings WHERE key='star_cost'")
    row = cursor.fetchone()
    return float(row[0]) if row else 1.33


def set_star_cost(new_cost: float):
    """РћР±РЅРѕРІРёС‚СЊ РєСѓСЂСЃ Р·РІРµР·РґС‹"""
    cursor.execute("UPDATE settings SET value=? WHERE key='star_cost'", (new_cost,))
    conn.commit()



@dp.callback_query(lambda c: c.data == "stats_sales")
async def stats_sales(callback: types.CallbackQuery):
    cursor.execute("""
        SELECT item_type, SUM(amount), SUM(cost)
        FROM purchases
        WHERE date(created_at) = date('now', 'localtime')
        GROUP BY item_type
    """)
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
                f"в­ђ Р—РІС‘Р·РґС‹: {total_amount}С€С‚ вЂ” {total_cost:.2f}в‚Ѕ (СЃРµР±РµСЃС‚РѕРёРјРѕСЃС‚СЊ {cost_price:.2f}в‚Ѕ, РєСѓСЂСЃ {star_cost}в‚Ѕ)"
            )

        elif item_type == "premium":
            cost_price = 0
            cursor.execute("""
                SELECT amount, cost FROM purchases
                WHERE item_type='premium' AND date(created_at) = date('now', 'localtime')
            """)
            premiums = cursor.fetchall()
            months_map = {3: 1000, 6: 1325, 12: 2400}
            for months, cost in premiums:
                cost_price += months_map.get(months, 0)
            total_cost_price += cost_price
            total_items_text.append(
                f"рџ’™ Premium: {total_amount} РјРµСЃ вЂ” {total_cost:.2f}в‚Ѕ (СЃРµР±РµСЃС‚РѕРёРјРѕСЃС‚СЊ {cost_price:.2f}в‚Ѕ)"
            )

    total_revenue *= 0.97
    profit = total_revenue - total_cost_price

    # РўРѕРї 5 РїРѕРєСѓРїР°С‚РµР»РµР№
    cursor.execute("""
        SELECT username, SUM(cost) as total_spent
        FROM purchases
        WHERE date(created_at) = date('now', 'localtime')
        GROUP BY user_id
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    top_buyers = cursor.fetchall()
    top_text = "\n".join([f"{i+1}. {row[0]} вЂ” {row[1]:.2f}в‚Ѕ" for i, row in enumerate(top_buyers)]) or "РќРµС‚ РїРѕРєСѓРїРѕРє"

    text = (
        f"рџ“Љ <b>РЎС‚Р°С‚РёСЃС‚РёРєР° Р·Р° СЃРµРіРѕРґРЅСЏ:</b>\n\n"
        + "\n".join(total_items_text) + "\n\n"
        f"рџ’° РћР±С‰Р°СЏ РІС‹СЂСѓС‡РєР°: {total_revenue:.2f}в‚Ѕ\n"
        f"рџ“‰ РЎРµР±РµСЃС‚РѕРёРјРѕСЃС‚СЊ: {total_cost_price:.2f}в‚Ѕ\n"
        f"вњ… Р§РёСЃС‚Р°СЏ РїСЂРёР±С‹Р»СЊ: {profit:.2f}в‚Ѕ\n\n"
        f"рџЏ† РўРѕРї РїРѕРєСѓРїР°С‚РµР»РµР№:\n{top_text}"
    )

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="вљ™пёЏ РР·РјРµРЅРёС‚СЊ РєСѓСЂСЃ Р·РІРµР·РґС‹", callback_data="change_star_cost")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_stats_menu")]
    ])

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)


class AdminState(StatesGroup):
    awaiting_new_star_cost = State()


@dp.callback_query(lambda c: c.data == "change_star_cost")
async def change_star_cost(callback: types.CallbackQuery, state: FSMContext):
    current_cost = get_star_cost()
    await callback.message.edit_text(
        f"рџ’± РўРµРєСѓС‰РёР№ РєСѓСЂСЃ Р·Р° 1в­ђ: <b>{current_cost}в‚Ѕ</b>\n\n"
        f"Р’РІРµРґРёС‚Рµ РЅРѕРІС‹Р№ РєСѓСЂСЃ (РЅР°РїСЂРёРјРµСЂ: <code>1.5</code>):",
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
        await message.answer(f"вњ… РљСѓСЂСЃ СѓСЃРїРµС€РЅРѕ РѕР±РЅРѕРІР»С‘РЅ: 1в­ђ = {new_cost}в‚Ѕ")

    except ValueError:
        await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ РєРѕСЂСЂРµРєС‚РЅРѕРµ С‡РёСЃР»Рѕ (РЅР°РїСЂРёРјРµСЂ: 1.5)")

    await state.clear()

@dp.callback_query(lambda c: c.data == "stats_clear")
async def stats_clear(callback: types.CallbackQuery):
    # РџСЂРѕРІРµСЂСЏРµРј, РµСЃС‚СЊ Р»Рё РїСЂРѕРґР°Р¶Рё Р·Р° РїРѕСЃР»РµРґРЅРёРµ 24 С‡Р°СЃР°
    cursor.execute("""
        SELECT COUNT(*) FROM purchases
        WHERE created_at >= datetime('now', '-1 day')
    """)
    count = cursor.fetchone()[0]

    if count == 0:
        await callback.message.edit_text(
            "вќЊ Р—Р° РїРѕСЃР»РµРґРЅРёРµ 24 С‡Р°СЃР° РїСЂРѕРґР°Р¶ РЅРµ Р±С‹Р»Рѕ.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_stats_menu")]
            ])
        )
        return

    # РЈРґР°Р»СЏРµРј РїСЂРѕРґР°Р¶Рё Р·Р° РїРѕСЃР»РµРґРЅРёРµ 24 С‡Р°СЃР°
    cursor.execute("""
        DELETE FROM purchases
        WHERE created_at >= datetime('now', '-1 day')
    """)
    conn.commit()

    await callback.message.edit_text(
        "рџ—‘ РЎС‚Р°С‚РёСЃС‚РёРєР° РїСЂРѕРґР°Р¶ Р·Р° РїРѕСЃР»РµРґРЅРёРµ 24 С‡Р°СЃР° СѓСЃРїРµС€РЅРѕ РѕС‡РёС‰РµРЅР°.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_stats_menu")]
        ])
    )


@dp.callback_query(lambda c: c.data == "leaders")
async def show_leaders(callback: types.CallbackQuery):
    viewer_id = callback.from_user.id

    # Р’С‹С‡РёСЃР»СЏРµРј РЅР°С‡Р°Р»Рѕ РЅРµРґРµР»Рё (РїРѕРЅРµРґРµР»СЊРЅРёРє)
    today = datetime.date.today()
    start_of_week = today - datetime.timedelta(days=today.weekday())  # РїРѕРЅРµРґРµР»СЊРЅРёРє
    start_of_week_str = start_of_week.strftime("%Y-%m-%d")

    # РќР°С‡Р°Р»Рѕ РјРµСЃСЏС†Р°
    start_of_month = today.replace(day=1)
    start_of_month_str = start_of_month.strftime("%Y-%m-%d")

    # РўРѕРї Р·Р° РЅРµРґРµР»СЋ
    cursor.execute("""
        SELECT user_id, SUM(amount) as total_stars
        FROM purchases
        WHERE item_type = 'stars' AND created_at >= ?
        GROUP BY user_id
        ORDER BY total_stars DESC
        LIMIT 5
    """, (start_of_week_str,))
    weekly = cursor.fetchall()

    # РўРѕРї Р·Р° РјРµСЃСЏС†
    cursor.execute("""
        SELECT user_id, SUM(amount) as total_stars
        FROM purchases
        WHERE item_type = 'stars' AND created_at >= ?
        GROUP BY user_id
        ORDER BY total_stars DESC
        LIMIT 5
    """, (start_of_month_str,))
    monthly = cursor.fetchall()

    # Р¤СѓРЅРєС†РёСЏ РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
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
                    return f"@{username[:3]}вЂ¦{short_hash}"
                elif first_name:
                    return f"{first_name[:2]}вЂ¦{short_hash}"
        return f"ID{str(user_id)[-3:]}вЂ¦{short_hash}"

    def format_list(rows):
        if not rows:
            return "вЂ” РїРѕРєР° РЅРµС‚ РґР°РЅРЅС‹С…"
        return "\n".join([f"{i+1}. {get_user_display(r[0], viewer_id)} вЂ” {int(r[1])}в­ђпёЏ"
                          for i, r in enumerate(rows)])

    text = (
        "рџЏ† <b>Р›РёРґРµСЂС‹ РїРѕ Р·РІС‘Р·РґР°Рј</b>\n"
        "<i>РџРѕС‡РµРјСѓ РІРёРґРЅРѕ С‚РѕР»СЊРєРѕ РјРѕР№ СЋР·?\n</i>"
        "<b>РљР°Р¶РґС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РІРёРґРёС‚ РїРѕР»РЅРѕС†РµРЅРЅРѕ С‚РѕР»СЊРєРѕ СЃРІРѕР№ СЋР·вќ—пёЏ</b>\n\n"
        f"рџ“… <b>Р—Р° РЅРµРґРµР»СЋ:</b>\n{format_list(weekly)}\n\n"
        f"рџ—“ <b>Р—Р° РјРµСЃСЏС†:</b>\n{format_list(monthly)}"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="back_first")]
    ])

    photo_url = "https://ibb.co/MyFDq6zx"
    await callback.message.edit_media(
        InputMediaPhoto(media=photo_url, caption=text, parse_mode="HTML"),
        reply_markup=keyboard
    )

@dp.message(Command("my_rank"))
async def myrank(message: types.Message):
    user_id = message.from_user.id
    today = datetime.date.today()
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

    text = "<b>рџ“Љ Р’Р°С€Рµ РјРµСЃС‚Рѕ РІ СЂРµР№С‚РёРЅРіРµ</b>\n\n"

    if week_pos:
        text += f"рџ“… <b>Р—Р° РЅРµРґРµР»СЋ:</b>\nрџЏ… РњРµСЃС‚Рѕ: <b>{week_pos}</b>\nв­ђпёЏ Р—РІС‘Р·РґС‹: <b>{week_stars}</b>\n\n"
    else:
        text += "рџ“… <b>Р—Р° РЅРµРґРµР»СЋ:</b>\nРќРµС‚ Р·РІС‘Р·Рґ Р·Р° СЌС‚РѕС‚ РїРµСЂРёРѕРґ.\n\n"

    if month_pos:
        text += f"рџ—“ <b>Р—Р° РјРµСЃСЏС†:</b>\nрџЏ… РњРµСЃС‚Рѕ: <b>{month_pos}</b>\nв­ђпёЏ Р—РІС‘Р·РґС‹: <b>{month_stars}</b>\n"
    else:
        text += "рџ—“ <b>Р—Р° РјРµСЃСЏС†:</b>\nРќРµС‚ Р·РІС‘Р·Рґ Р·Р° СЌС‚РѕС‚ РїРµСЂРёРѕРґ.\n"

    await message.answer(text, parse_mode="HTML")


# === FSM РґР»СЏ РїРѕС€Р°РіРѕРІРѕРіРѕ СЃРѕР·РґР°РЅРёСЏ РїСЂРѕРјРѕ ===
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

# --- Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ РїСЂРѕРјРѕРєРѕРґРѕРІ ---
@dp.callback_query(lambda c: c.data == "admin_promo_menu")
async def admin_promo_menu(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="вћ• РЎРѕР·РґР°С‚СЊ РїСЂРѕРјРѕ", callback_data="admin_promo_create")],
        [InlineKeyboardButton(text="рџ“‹ РђРєС‚РёРІРЅС‹Рµ РїСЂРѕРјРѕ", callback_data="admin_promo_list")],
        [InlineKeyboardButton(text="рџ—‘ РЈРґР°Р»РёС‚СЊ РїСЂРѕРјРѕ", callback_data="admin_promo_delete")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
    ])
    await callback.message.edit_text(
        "рџЋџ <b>РџСЂРѕРјРѕ-РєРѕРґС‹</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# --- 1. РќР°С‡Р°Р»Рѕ СЃРѕР·РґР°РЅРёСЏ РїСЂРѕРјРѕРєРѕРґР° ---
@dp.callback_query(lambda c: c.data == "admin_promo_create")
async def start_promo_form(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ РїСЂРѕРјРѕ-РєРѕРґР° (РЅР°РїСЂРёРјРµСЂ: NEWYEAR2026):")
    await state.set_state(PromoForm.code)

# --- 2. РЎРєРёРґРєР° ---
@dp.message(PromoForm.code)
async def promo_code_step(message: types.Message, state: FSMContext):
    await state.update_data(code=message.text.upper())
    await message.answer("Р’РІРµРґРёС‚Рµ СЂР°Р·РјРµСЂ СЃРєРёРґРєРё РІ % (РЅР°РїСЂРёРјРµСЂ: 10):")
    await state.set_state(PromoForm.discount)

# --- 3. РњРёРЅРёРјСѓРј Р·РІС‘Р·Рґ ---
@dp.message(PromoForm.discount)
async def promo_discount_step(message: types.Message, state: FSMContext):
    try:
        discount = int(message.text)
    except:
        return await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ РґР»СЏ СЃРєРёРґРєРё.")
    await state.update_data(discount=discount)
    await message.answer("Р’РІРµРґРёС‚Рµ РјРёРЅРёРјР°Р»СЊРЅРѕРµ РєРѕР»РёС‡РµСЃС‚РІРѕ Р·РІС‘Р·Рґ РґР»СЏ Р°РєС‚РёРІР°С†РёРё РїСЂРѕРјРѕ:")
    await state.set_state(PromoForm.min_stars)

# --- 4. РЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ ---
@dp.message(PromoForm.min_stars)
async def promo_min_stars_step(message: types.Message, state: FSMContext):
    try:
        min_stars = int(message.text)
    except:
        return await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ.")
    await state.update_data(min_stars=min_stars)
    await message.answer("Р’РІРµРґРёС‚Рµ РґР°С‚Сѓ РѕРєРѕРЅС‡Р°РЅРёСЏ РїСЂРѕРјРѕ (РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD):")
    await state.set_state(PromoForm.expires_at)

# --- 5. РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»РёС‡РµСЃС‚РІРѕ Р°РєС‚РёРІР°С†РёР№ ---
@dp.message(PromoForm.expires_at)
async def promo_expires_step(message: types.Message, state: FSMContext):
    await state.update_data(expires_at=message.text)
    await message.answer("Р’РІРµРґРёС‚Рµ РѕР±С‰РµРµ РєРѕР»РёС‡РµСЃС‚РІРѕ Р°РєС‚РёРІР°С†РёР№ РїСЂРѕРјРѕ (РЅР°РїСЂРёРјРµСЂ: 100):")
    await state.set_state(PromoForm.max_uses)

# --- 6. РњР°РєСЃРёРјСѓРј Р°РєС‚РёРІР°С†РёР№ РЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ ---
@dp.message(PromoForm.max_uses)
async def promo_max_uses_step(message: types.Message, state: FSMContext):
    try:
        max_uses = int(message.text)
    except:
        return await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ.")
    await state.update_data(max_uses=max_uses)
    await message.answer("Р’РІРµРґРёС‚Рµ, СЃРєРѕР»СЊРєРѕ СЂР°Р· РѕРґРёРЅ С‡РµР»РѕРІРµРє РјРѕР¶РµС‚ Р°РєС‚РёРІРёСЂРѕРІР°С‚СЊ СЌС‚РѕС‚ РїСЂРѕРјРѕ (РЅР°РїСЂРёРјРµСЂ: 1 РёР»Рё 3):")
    await state.set_state(PromoForm.max_uses_per_user)

# --- 7. РЈСЃР»РѕРІРёРµ (РєРЅРѕРїРєР°РјРё) ---
@dp.message(PromoForm.max_uses_per_user)
async def promo_condition_step(message: types.Message, state: FSMContext):
    try:
        per_user = int(message.text)
    except:
        return await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ.")
    await state.update_data(max_uses_per_user=per_user)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџЋџ Р’СЃРµРј", callback_data="cond_all"),
         InlineKeyboardButton(text="рџ’° РўРѕР»СЊРєРѕ РїРѕРєСѓРїР°С‚РµР»СЏРј", callback_data="cond_buyers")]
    ])
    await message.answer("Р’С‹Р±РµСЂРёС‚Рµ СѓСЃР»РѕРІРёРµ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ:", reply_markup=kb)
    await state.set_state(PromoForm.condition)

@dp.callback_query(lambda c: c.data.startswith("cond_"))
async def promo_condition_choice(callback: types.CallbackQuery, state: FSMContext):
    condition = "all" if callback.data == "cond_all" else "buyers"
    await state.update_data(condition=condition)
    await callback.message.answer("Р’РІРµРґРёС‚Рµ РјР°РєСЃ. Р±РµСЃРїР»Р°С‚РЅС‹С… Р·РІС‘Р·Рґ (РёР»Рё 0):")
    await state.set_state(PromoForm.max_free_stars)

# --- 8. РњР°РєСЃ. Р±РµСЃРїР»Р°С‚РЅС‹Рµ Р·РІС‘Р·РґС‹ ---
@dp.message(PromoForm.max_free_stars)
async def promo_max_free_step(message: types.Message, state: FSMContext):
    try:
        max_free = int(message.text)
    except:
        return await message.answer("вљ пёЏ Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ.")
    await state.update_data(max_free_stars=max_free)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="в­ђпёЏ Р—РІС‘Р·РґС‹", callback_data="target_stars"),
         InlineKeyboardButton(text="рџ’Ћ Premium", callback_data="target_premium"),
         InlineKeyboardButton(text="рџЋЃ Р’СЃС‘", callback_data="target_all")]
    ])
    await message.answer("Р’С‹Р±РµСЂРёС‚Рµ, Рє С‡РµРјСѓ РїСЂРёРјРµРЅРёРј РїСЂРѕРјРѕРєРѕРґ:", reply_markup=kb)
    await state.set_state(PromoForm.target)

# --- 9. РўРёРї РїСЂРѕРјРѕ (Р·РІС‘Р·РґС‹/РїСЂРµРјРёСѓРј/РІСЃС‘) ---
@dp.callback_query(lambda c: c.data.startswith("target_"))
async def promo_target_choice(callback: types.CallbackQuery, state: FSMContext):
    target = callback.data.replace("target_", "")
    await state.update_data(target=target)
    data = await state.get_data()

    text = (
        f"рџЋџ <b>РџСЂРѕРІРµСЂСЊ РґР°РЅРЅС‹Рµ РїРµСЂРµРґ СЃРѕС…СЂР°РЅРµРЅРёРµРј:</b>\n\n"
        f"рџ”ё РљРѕРґ: <code>{data['code']}</code>\n"
        f"рџ’° РЎРєРёРґРєР°: {data['discount']}%\n"
        f"в­ђ РњРёРЅ. Р·РІС‘Р·Рґ: {data['min_stars']}\n"
        f"рџ“… РСЃС‚РµРєР°РµС‚: {data['expires_at']}\n"
        f"рџ”ў Р’СЃРµРіРѕ Р°РєС‚РёРІР°С†РёР№: {data['max_uses']}\n"
        f"рџ‘¤ РќР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {data['max_uses_per_user']}\n"
        f"рџЋЇ РЈСЃР»РѕРІРёРµ: {data['condition']}\n"
        f"рџЊџ РњР°РєСЃ. Р±РµСЃРїР»Р°С‚РЅС‹С… Р·РІС‘Р·Рґ: {data['max_free_stars']}\n"
        f"рџЋЇ РџСЂРёРјРµРЅСЏРµС‚СЃСЏ Рє: {data['target']}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="вњ… РџРѕРґС‚РІРµСЂРґРёС‚СЊ", callback_data="promo_save")],
        [InlineKeyboardButton(text="вќЊ РћС‚РјРµРЅР°", callback_data="promo_cancel")]
    ])
    await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(PromoForm.confirm)

# --- 10. РЎРѕС…СЂР°РЅРµРЅРёРµ РІ Р‘Р” ---
@dp.callback_query(lambda c: c.data in ["promo_save", "promo_cancel"])
async def promo_finish(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "promo_cancel":
        await state.clear()
        return await callback.message.answer("вќЊ РЎРѕР·РґР°РЅРёРµ РїСЂРѕРјРѕРєРѕРґР° РѕС‚РјРµРЅРµРЅРѕ.")

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
    await callback.message.answer(f"вњ… РџСЂРѕРјРѕ <b>{data['code']}</b> СѓСЃРїРµС€РЅРѕ СЃРѕР·РґР°РЅ!", parse_mode="HTML")

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
        return await callback.message.answer("вќЊ РђРєС‚РёРІРЅС‹С… РїСЂРѕРјРѕ-РєРѕРґРѕРІ РЅРµС‚.")

    text = "рџ“Љ <b>РђРєС‚РёРІРЅС‹Рµ РїСЂРѕРјРѕ-РєРѕРґС‹:</b>\n\n"
    for row in rows:
        code, discount, min_stars, expires_at, max_uses, uses_count, condition = row
        text += (
            f"<b>РљРѕРґ:</b> <code>{code}</code>\n"
            f"<b>РЎРєРёРґРєР°:</b> {discount}%\n"
            f"<b>РњРёРЅ. Р·РІС‘Р·Рґ:</b> {min_stars}\n"
            f"<b>Р”РµР№СЃС‚РІСѓРµС‚ РґРѕ:</b> {expires_at}\n"
            f"<b>РњР°РєСЃ. Р°РєС‚РёРІР°С†РёР№:</b> {max_uses}\n"
            f"<b>РСЃРїРѕР»СЊР·РѕРІР°РЅРѕ:</b> {uses_count}\n"
            f"<b>РЈСЃР»РѕРІРёРµ:</b> {condition}\n\n"
        )

    # РџРѕРєР°Р·С‹РІР°РµРј РІ С‚РѕРј Р¶Рµ СЃРѕРѕР±С‰РµРЅРёРё, С‡С‚РѕР±С‹ РЅРµ РїР»РѕРґРёС‚СЊ РЅРѕРІС‹Рµ
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_promo_menu")]
                                     ]))


class DeletePromoForm(StatesGroup):
    code = State()

@dp.callback_query(lambda c: c.data == "admin_promo_delete")
async def start_delete_promo(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return

    await callback.message.answer("Р’РІРµРґРёС‚Рµ РєРѕРґ РїСЂРѕРјРѕ-РєРѕРґР°, РєРѕС‚РѕСЂС‹Р№ РЅСѓР¶РЅРѕ СѓРґР°Р»РёС‚СЊ:")
    await state.set_state(DeletePromoForm.code)

@dp.message(DeletePromoForm.code)
async def delete_promo_step(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    code = message.text.upper()
    cursor.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
    if cursor.fetchone() is None:
        await message.answer(f"вќЊ РџСЂРѕРјРѕ-РєРѕРґ <b>{code}</b> РЅРµ РЅР°Р№РґРµРЅ.", parse_mode="HTML")
        await state.clear()
        return

    cursor.execute("DELETE FROM promo_codes WHERE code=?", (code,))
    conn.commit()
    await message.answer(f"вњ… РџСЂРѕРјРѕ-РєРѕРґ <b>{code}</b> СѓСЃРїРµС€РЅРѕ СѓРґР°Р»С‘РЅ.", parse_mode="HTML")
    await state.clear()


class ReferralsForm(StatesGroup):
    show = State()
    reset = State()

@dp.callback_query(lambda c: c.data == "admin_ref_menu")
async def admin_ref_menu(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџ“‹ РџРѕРєР°Р·Р°С‚СЊ СЂРµС„РµСЂР°Р»РѕРІ", callback_data="ref_show")],
        [InlineKeyboardButton(text="в™»пёЏ РЎР±СЂРѕСЃРёС‚СЊ СЂРµС„РµСЂР°Р»РѕРІ", callback_data="ref_reset")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
    ])

    await callback.message.edit_text(
        "рџ‘Ґ <b>Р РµС„РµСЂР°Р»СЊРЅР°СЏ СЃРёСЃС‚РµРјР°</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "ref_show")
async def admin_ref_show(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Р’РІРµРґРёС‚Рµ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, С‡С‚РѕР±С‹ РїРѕРєР°Р·Р°С‚СЊ РµРіРѕ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ:")
    await state.set_state(ReferralsForm.show)

@dp.message(ReferralsForm.show)
async def process_show_referrals(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("вќЊ РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ ID. Р’РІРµРґРёС‚Рµ С‚РѕР»СЊРєРѕ С‡РёСЃР»Рѕ.")
        return

    user_id = int(message.text)
    cursor.execute(
        'SELECT username FROM users WHERE referred_by=? AND referrals_with_purchase=1',
        (user_id,)
    )
    rows = cursor.fetchall()
    await state.clear()

    if not rows:
        await message.answer(f"вќЊ РЈ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id} РЅРµС‚ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ.")
        return

    usernames = "\n".join(f"@{row[0]}" for row in rows if row[0])
    await message.answer(f"рџ“‹ РђРєС‚РёРІРЅС‹Рµ СЂРµС„РµСЂР°Р»С‹ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}:\n{usernames}")


@dp.callback_query(lambda c: c.data == "ref_reset")
async def admin_ref_reset(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Р’РІРµРґРёС‚Рµ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, С‡С‚РѕР±С‹ СЃР±СЂРѕСЃРёС‚СЊ РµРіРѕ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ:")
    await state.set_state(ReferralsForm.reset)

@dp.message(ReferralsForm.reset)
async def process_reset_referrals(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("вќЊ РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ ID. Р’РІРµРґРёС‚Рµ С‚РѕР»СЊРєРѕ С‡РёСЃР»Рѕ.")
        return

    user_id = int(message.text)
    cursor.execute(
        'UPDATE users SET referrals_with_purchase=0 WHERE referred_by=? AND referrals_with_purchase=1',
        (user_id,)
    )
    conn.commit()
    await state.clear()
    await message.answer(f"в™»пёЏ Р’СЃРµ Р°РєС‚РёРІРЅС‹Рµ СЂРµС„РµСЂР°Р»С‹ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id} СѓСЃРїРµС€РЅРѕ СЃР±СЂРѕС€РµРЅС‹.")


@dp.callback_query(lambda c: c.data == "admin_back_main")
async def admin_back_main(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°", callback_data="admin_stats_menu"),
         InlineKeyboardButton(text="рџЋџ РџСЂРѕРјРѕ-РєРѕРґС‹", callback_data="admin_promo_menu")],

        [InlineKeyboardButton(text="рџ‘Ґ Р РµС„РµСЂР°Р»С‹", callback_data="admin_ref_menu"),
         InlineKeyboardButton(text="в­ђ РџСЂРѕРґР°РЅРЅС‹Рµ Р·РІС‘Р·РґС‹", callback_data="admin_stars_total")],

        [InlineKeyboardButton(text="рџ“ў Р Р°СЃСЃС‹Р»РєР°", callback_data="broadcast_start"),
         InlineKeyboardButton(text="рџ’і РџР»Р°С‚С‘Р¶РєР°", callback_data="payment_menu")],
        [InlineKeyboardButton(text="рџ”Ќ РќР°Р№С‚Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ", callback_data="admin_find_user")],
        [InlineKeyboardButton(text="вћ• РќР°С‡РёСЃР»РёС‚СЊ Р·РІС‘Р·РґС‹", callback_data="cad")],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="back_first")]
    ])

    text = (
        "вљ™пёЏ <b>РђРґРјРёРЅ-РїР°РЅРµР»СЊ</b>\n\n"
        "Р’С‹Р±РµСЂРёС‚Рµ РЅСѓР¶РЅС‹Р№ СЂР°Р·РґРµР»:"
    )

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

# --- РЎР»РѕРІР°СЂСЊ РґР»СЏ РѕС‚СЃР»РµР¶РёРІР°РЅРёСЏ РґРµР№СЃС‚РІРёР№ ---
pending_action = {}  # {user_id: "add" | "remove"}

# --- Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ Р·РІС‘Р·Рґ ---
@dp.callback_query(lambda c: c.data == "admin_stars_total")
async def admin_stars_total(callback: types.CallbackQuery):
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    total_stars = cursor.fetchone()[0] or 0

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="вћ• Р”РѕР±Р°РІРёС‚СЊ", callback_data="admin_stars_add"),
            InlineKeyboardButton(text="вћ– РЈР±СЂР°С‚СЊ", callback_data="admin_stars_remove")
        ],
        [
            InlineKeyboardButton(text='Р¦Р•РќРђ РќРђ Р—Р’Р•Р—Р”Р«', callback_data='open_change_rate_menu')
        ],
        [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_back_main")]
    ])

    await callback.message.edit_text(
        f"в­ђ <b>Р’СЃРµРіРѕ РїСЂРѕРґР°РЅРѕ Р·РІС‘Р·Рґ:</b> {total_stars:,}".replace(",", " "),
        parse_mode="HTML",
        reply_markup=keyboard
    )



# --- РћР±СЂР°Р±РѕС‚РєР° РЅР°Р¶Р°С‚РёР№ ---
@dp.callback_query(lambda c: c.data == "admin_stars_add")
async def admin_stars_add(callback: types.CallbackQuery):
    pending_action[callback.from_user.id] = "add"
    await callback.message.answer("Р’РІРµРґРёС‚Рµ, СЃРєРѕР»СЊРєРѕ Р·РІС‘Р·Рґ РґРѕР±Р°РІРёС‚СЊ:")

@dp.callback_query(lambda c: c.data == "admin_stars_remove")
async def admin_stars_remove(callback: types.CallbackQuery):
    pending_action[callback.from_user.id] = "remove"
    await callback.message.answer("Р’РІРµРґРёС‚Рµ, СЃРєРѕР»СЊРєРѕ Р·РІС‘Р·Рґ СѓР±СЂР°С‚СЊ:")

# --- РћР±СЂР°Р±РѕС‚РєР° С‡РёСЃРµР» ---
@dp.message(lambda m: bool(m.text) and m.text.isdigit() and m.from_user.id in pending_action)
async def handle_stars_edit(message: types.Message):
    action = pending_action.get(message.from_user.id)
    stars_value = int(message.text)

    # РџСЂРѕРІРµСЂСЏРµРј С‚РµРєСѓС‰РµРµ Р·РЅР°С‡РµРЅРёРµ
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    total_stars = cursor.fetchone()[0] or 0

    # РћР±РЅРѕРІР»СЏРµРј РІ Р·Р°РІРёСЃРёРјРѕСЃС‚Рё РѕС‚ РґРµР№СЃС‚РІРёСЏ
    if action == "add":
        new_total = total_stars + stars_value
        msg_action = f"вњ… Р”РѕР±Р°РІР»РµРЅРѕ {stars_value}в­ђ"
    elif action == "remove":
        new_total = max(total_stars - stars_value, 0)
        msg_action = f"вњ… РЈР±СЂР°РЅРѕ {stars_value}в­ђ"
    else:
        return

    cursor.execute("UPDATE total_stars SET total = ? WHERE id = 1", (new_total,))
    conn.commit()

    # РћС‚РїСЂР°РІР»СЏРµРј СЂРµР·СѓР»СЊС‚Р°С‚
    await message.answer(
        f"{msg_action}\n"
        f"РўРµРїРµСЂСЊ РІСЃРµРіРѕ РїСЂРѕРґР°РЅРѕ: <b>{new_total:,}</b>в­ђ".replace(",", " "),
        parse_mode="HTML"
    )

    # РЈР±РёСЂР°РµРј РёР· СЃР»РѕРІР°СЂСЏ
    pending_action.pop(message.from_user.id, None)




# --- РљРЅРѕРїРєР° "Р Р°СЃСЃС‹Р»РєР°" ---
@dp.callback_query(lambda c: c.data == "broadcast_start")
async def broadcast_start(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return

    user_id = callback.from_user.id
    user_states[user_id] = {"awaiting_broadcast": True}

    await callback.message.answer(
        "рџ“ў РћС‚РїСЂР°РІСЊ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё.\n\n"
        "РњРѕР¶РЅРѕ РѕС‚РїСЂР°РІРёС‚СЊ:\n"
        "вЂў РїСЂРѕСЃС‚Рѕ С‚РµРєСЃС‚ рџ’¬\n"
        "вЂў С„РѕС‚Рѕ рџ“ё\n"
        "вЂў С„РѕС‚Рѕ СЃ РїРѕРґРїРёСЃСЊСЋ рџ–ј\n\n"
        "РџРѕСЃР»Рµ СЌС‚РѕРіРѕ СЏ СЃРїСЂРѕС€Сѓ вЂ” РґРѕР±Р°РІРёС‚СЊ Р»Рё РєРЅРѕРїРєРё."
    )



# --- РџСЂРёС‘Рј С‚РµРєСЃС‚Р° РёР»Рё С„РѕС‚Рѕ (СЃ РїРѕРґРїРёСЃСЊСЋ РёР»Рё Р±РµР·) ---
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
        return await message.answer("вљ пёЏ РџРѕРґРґРµСЂР¶РёРІР°СЋС‚СЃСЏ С‚РѕР»СЊРєРѕ С‚РµРєСЃС‚ Рё С„РѕС‚Рѕ.")

    user_states[user_id]["broadcast_content"] = content
    user_states[user_id]["awaiting_broadcast"] = False
    user_states[user_id]["awaiting_buttons"] = True

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="вћ• Р”РѕР±Р°РІРёС‚СЊ РєРЅРѕРїРєРё", callback_data="broadcast_add_buttons")],
        [InlineKeyboardButton(text="рџљЂ РћС‚РїСЂР°РІРёС‚СЊ Р±РµР· РєРЅРѕРїРѕРє", callback_data="broadcast_send_now")],
        [InlineKeyboardButton(text="вќЊ РћС‚РјРµРЅР°", callback_data="admin_back_main")]
    ])

    await message.answer("РҐРѕС‡РµС€СЊ РґРѕР±Р°РІРёС‚СЊ РєРЅРѕРїРєРё Рє СЃРѕРѕР±С‰РµРЅРёСЋ?", reply_markup=kb)



@dp.callback_query(lambda c: c.data == "broadcast_add_buttons")
async def broadcast_add_buttons(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    state = user_states.get(user_id)
    if not state:
        return await callback.answer("РЎРµСЃСЃРёСЏ СѓСЃС‚Р°СЂРµР»Р°. РќР°С‡РЅРё Р·Р°РЅРѕРІРѕ.")

    state["awaiting_buttons_text"] = True
    state["awaiting_buttons"] = False

    await callback.message.answer(
        "рџ“І РћС‚РїСЂР°РІСЊ РєРЅРѕРїРєРё РІ С„РѕСЂРјР°С‚Рµ:\n\n"
        "РўРµРєСЃС‚ РєРЅРѕРїРєРё - https://СЃСЃС‹Р»РєР°\n"
        "РњРѕР¶РЅРѕ РЅРµСЃРєРѕР»СЊРєРѕ, РїРѕ РѕРґРЅРѕР№ РЅР° СЃС‚СЂРѕРєСѓ.\n"
        "РџСЂРёРјРµСЂ:\n"
        "РќР°С€ СЃР°Р№С‚ - https://example.com\n"
        "РљР°РЅР°Р» - https://t.me/example\n\n"
        "РџРѕСЃР»Рµ СЌС‚РѕРіРѕ СЏ РѕС‚РїСЂР°РІР»СЋ СЂР°СЃСЃС‹Р»РєСѓ."
    )

@dp.message(lambda m: m.from_user.id in ADMIN_IDS and user_states.get(m.from_user.id, {}).get("awaiting_buttons_text"))
async def broadcast_buttons_step(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state:
        return await message.answer("РЎРµСЃСЃРёСЏ СѓСЃС‚Р°СЂРµР»Р°.")

    # РЎРїРёСЃРѕРє СЂСЏРґРѕРІ РєРЅРѕРїРѕРє
    inline_keyboard = []

    # РїР°СЂСЃРёРј РєРЅРѕРїРєРё
    for line in message.text.splitlines():
        if " - " in line:
            text, url = line.split(" - ", 1)
            inline_keyboard.append([InlineKeyboardButton(text=text.strip(), url=url.strip())])

    # СЃРѕР·РґР°С‘Рј РєР»Р°РІРёР°С‚СѓСЂСѓ
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
                    disable_web_page_preview=True  # в†ђ РґРѕР±Р°РІР»РµРЅРѕ
                )
            else:
                await bot.send_message(
                    chat_id=user[0],
                    text=content["text"],
                    parse_mode="HTML",
                    reply_markup=keyboard,
                    disable_web_page_preview=True  # в†ђ РґРѕР±Р°РІР»РµРЅРѕ
                )
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            fail += 1

    await bot.send_message(
        user_id,
        f"рџ“ў Р Р°СЃСЃС‹Р»РєР° Р·Р°РІРµСЂС€РµРЅР°!\nвњ… РЈСЃРїРµС€РЅРѕ: {success}\nвќЊ РћС€РёР±РєР°: {fail}"
    )
    user_states.pop(user_id, None)



import datetime  # РµСЃР»Рё Сѓ С‚РµР±СЏ С‚Р°Рє

@dp.callback_query(lambda c: c.data == "enter_promo")
async def ask_promo(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_states[user_id] = {"awaiting_promo": True}
    await callback.message.answer("Р’РІРµРґРёС‚Рµ РІР°С€ РїСЂРѕРјРѕ-РєРѕРґ:")

@dp.callback_query(lambda c: c.data == "enter_promo_prem")
async def ask_promo_prem(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_states[user_id] = {"awaiting_promo_prem": True}
    await callback.message.answer("Р’РІРµРґРёС‚Рµ РІР°С€ РїСЂРѕРјРѕ-РєРѕРґ РґР»СЏ Premium:")


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
        return {"ok": False, "discount": 0, "error": "вќЊ РЈРєР°Р¶РёС‚Рµ РїСЂРѕРјРѕ-РєРѕРґ."}

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
        return {"ok": False, "discount": 0, "error": "вќЊ РќРµРІРµСЂРЅС‹Р№ РїСЂРѕРјРѕ-РєРѕРґ."}

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
        return {"ok": False, "discount": 0, "error": "вќЊ РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ С‚РёРї Р·Р°РєР°Р·Р° РґР»СЏ РїСЂРѕРјРѕРєРѕРґР°."}
    if target not in (target_type, "all"):
        return {"ok": False, "discount": 0, "error": f"вќЊ Р­С‚РѕС‚ РїСЂРѕРјРѕ-РєРѕРґ РЅРµР»СЊР·СЏ РїСЂРёРјРµРЅРёС‚СЊ РґР»СЏ {target_type}."}

    # РџСЂРѕРІРµСЂРєР° РґР°С‚С‹ РёСЃС‚РµС‡РµРЅРёСЏ
    if expires_at:
        try:
            exp_date = datetime.datetime.strptime(str(expires_at), "%Y-%m-%d").date()
            if datetime.date.today() > exp_date:
                return {"ok": False, "discount": 0, "error": "вќЊ РЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ РїСЂРѕРјРѕ-РєРѕРґР° РёСЃС‚С‘Рє."}
        except Exception:
            return {"ok": False, "discount": 0, "error": "вќЊ РћС€РёР±РєР° С„РѕСЂРјР°С‚Р° РґР°С‚С‹ РІ РїСЂРѕРјРѕ-РєРѕРґРµ."}

    max_uses = int(max_uses or 0)
    uses_count = int(uses_count or 0)
    if max_uses > 0 and uses_count >= max_uses:
        return {"ok": False, "discount": 0, "error": "вќЊ РџСЂРѕРјРѕ-РєРѕРґ Р±РѕР»СЊС€Рµ РЅРµРґРѕСЃС‚СѓРїРµРЅ."}

    max_uses_per_user = int(max_uses_per_user or 1)
    used_by_user = _get_user_promo_uses_count(user_id, promo_code)
    if max_uses_per_user > 0 and used_by_user >= max_uses_per_user:
        return {
            "ok": False,
            "discount": 0,
            "error": f"вќЊ Р­С‚РѕС‚ РїСЂРѕРјРѕ-РєРѕРґ РјРѕР¶РЅРѕ Р°РєС‚РёРІРёСЂРѕРІР°С‚СЊ РЅРµ Р±РѕР»РµРµ {max_uses_per_user} СЂР°Р·(Р°) РЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.",
        }

    condition = str(condition or "all").strip().lower()
    if condition == "buyers" and not _user_has_any_purchases(user_id):
        return {"ok": False, "discount": 0, "error": "вќЊ Р­С‚РѕС‚ РїСЂРѕРјРѕ-РєРѕРґ РґРѕСЃС‚СѓРїРµРЅ С‚РѕР»СЊРєРѕ РїРѕРєСѓРїР°С‚РµР»СЏРј."}

    min_stars = int(min_stars or 0)
    if target_type == "stars" and min_stars > 0 and int(stars_amount or 0) < min_stars:
        return {
            "ok": False,
            "discount": 0,
            "error": f"вќЊ Р”Р»СЏ СЌС‚РѕРіРѕ РїСЂРѕРјРѕРєРѕРґР° РЅСѓР¶РЅРѕ РјРёРЅРёРјСѓРј {min_stars}в­ђпёЏ.",
        }

    effect_type, effect_value = _normalize_promo_effect(
        raw_effect_type, raw_effect_value, discount_percent, max_free_stars
    )

    if effect_type == "free_stars":
        if target_type != "stars":
            return {"ok": False, "discount": 0, "error": "вќЊ Р‘РµСЃРїР»Р°С‚РЅС‹Рµ Р·РІС‘Р·РґС‹ СЂР°Р±РѕС‚Р°СЋС‚ С‚РѕР»СЊРєРѕ РґР»СЏ РїРѕРєСѓРїРєРё Р·РІС‘Р·Рґ."}
        if effect_value <= 0:
            return {"ok": False, "discount": 0, "error": "вќЊ РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ Р±РµСЃРїР»Р°С‚РЅС‹С… Р·РІС‘Р·Рґ РІ РїСЂРѕРјРѕРєРѕРґРµ."}
        if int(stars_amount or 0) != effect_value:
            return {
                "ok": False,
                "discount": 0,
                "error": f"вќЊ Р­С‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ РІС‹РґР°С‘С‚ Р±РµСЃРїР»Р°С‚РЅРѕ С‚РѕР»СЊРєРѕ {effect_value}в­ђпёЏ.",
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
        return {"ok": False, "discount": 0, "error": "вќЊ РќРµРєРѕСЂСЂРµРєС‚РЅР°СЏ СЃРєРёРґРєР° РІ РїСЂРѕРјРѕРєРѕРґРµ."}

    return {
        "ok": True,
        "discount": discount_value,
        "effectType": "discount_percent",
        "effectValue": discount_value,
        "error": "",
    }




change_star_rate_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="50вЂ“75 в­ђ", callback_data="change_rate_50_75")],
    [InlineKeyboardButton(text="76вЂ“100 в­ђ", callback_data="change_rate_76_100")],
    [InlineKeyboardButton(text="101вЂ“250 в­ђ", callback_data="change_rate_101_250")],
    [InlineKeyboardButton(text="251+ в­ђ", callback_data="change_rate_251_plus")],
    [InlineKeyboardButton(text="в¬…пёЏ РќР°Р·Р°Рґ", callback_data="admin_stars_total")]
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
    source: str = "api",
) -> None:
    cursor.execute(
        '''
        INSERT INTO miniapp_purchase_history (
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
            source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            int(user_id),
            (buyer_username or "").strip(),
            (buyer_first_name or "").strip(),
            (buyer_last_name or "").strip(),
            (target_username or "").strip(),
            (item_type or "").strip(),
            str(amount),
            float(price_rub),
            float(price_usd),
            (promo_code or "").strip().upper(),
            int(promo_discount or 0),
            (promo_error or "").strip(),
            (source or "api").strip(),
        ),
    )
    conn.commit()


def build_history_amount_label(item_type: str, amount_value: str) -> str:
    clean_amount = (amount_value or "").strip()
    if item_type == "premium":
        digits = re.sub(r"[^0-9]", "", clean_amount)
        return f"{digits} РјРµСЃ." if digits else "вЂ”"
    return clean_amount or "вЂ”"


def build_history_price_label(price_usd: float, price_rub: float) -> str:
    rub_value = float(price_rub)
    usd_value = float(price_usd)
    rub_text = f"{int(rub_value)}в‚Ѕ" if rub_value.is_integer() else f"{rub_value:.2f}в‚Ѕ"
    return f"{usd_value:.2f}$/{rub_text}"


def get_miniapp_purchase_history_payload(user_id: int, limit: int = 20) -> list:
    cursor.execute(
        '''
        SELECT item_type, amount, price_usd, price_rub, created_at
        FROM miniapp_purchase_history
        WHERE user_id=?
        ORDER BY id DESC
        LIMIT ?
        ''',
        (int(user_id), int(limit)),
    )
    rows = cursor.fetchall()

    if not rows:
        cursor.execute(
            '''
            SELECT item_type, amount, cost, created_at
            FROM purchases
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT ?
            ''',
            (int(user_id), int(limit)),
        )
        fallback_rows = cursor.fetchall()
        rows = []
        for item_type, amount, cost, created_at in fallback_rows:
            price_rub = float(cost or 0.0)
            price_usd = round(price_rub / MINIAPP_USD_RUB_RATE, 2) if MINIAPP_USD_RUB_RATE > 0 else 0.0
            rows.append((item_type, amount, price_usd, price_rub, created_at))

    payload = []
    for item_type, amount, price_usd, price_rub, created_at in rows:
        normalized_type = str(item_type or "").strip().lower()
        if normalized_type not in {"stars", "premium", "ton"}:
            normalized_type = "stars"
        payload.append(
            {
                "type": normalized_type,
                "main": build_history_amount_label(normalized_type, str(amount)),
                "price": build_history_price_label(float(price_usd), float(price_rub)),
                "createdAt": created_at,
            }
        )
    return payload


def build_miniapp_config_payload() -> dict:
    star_amounts = [100, 500, 1000]
    star_rates = get_all_star_rates()
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
    return miniapp_json_response(
        {
            "ok": True,
            "starRates": {
                "50_75": round(float(current_rates.get("50_75", 1.7)), 3),
                "76_100": round(float(current_rates.get("76_100", 1.6)), 3),
                "101_250": round(float(current_rates.get("101_250", 1.55)), 3),
                "251_plus": round(float(current_rates.get("251_plus", 1.5)), 3),
            },
        }
    )


def _admin_parse_user_id(raw_value) -> int:
    try:
        user_id = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ")
    if user_id <= 0:
        raise ValueError("ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РїРѕР»РѕР¶РёС‚РµР»СЊРЅС‹Рј")
    return user_id


def _admin_parse_positive_int(raw_value, *, field_name: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ РїРѕР»Рµ: {field_name}")
    if value <= 0:
        raise ValueError(f"{field_name} РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ Р±РѕР»СЊС€Рµ РЅСѓР»СЏ")
    return value


def _admin_parse_non_negative_int(raw_value, *, field_name: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ РїРѕР»Рµ: {field_name}")
    if value < 0:
        raise ValueError(f"{field_name} РЅРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ РѕС‚СЂРёС†Р°С‚РµР»СЊРЅС‹Рј")
    return value


def _admin_get_stars_total() -> int:
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
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
        raise ValueError("РљРЅРѕРїРєРё РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ СЃРїРёСЃРєРѕРј")

    inline_rows = []
    for item in raw_buttons:
        if not isinstance(item, dict):
            raise ValueError("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ С„РѕСЂРјР°С‚ РєРЅРѕРїРєРё")
        button_text = str(item.get("text") or "").strip()
        button_url = str(item.get("url") or "").strip()
        if not button_text or not button_url:
            raise ValueError("РЈ РєР°Р¶РґРѕР№ РєРЅРѕРїРєРё РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ С‚РµРєСЃС‚ Рё СЃСЃС‹Р»РєР°")
        if not re.match(r"^(https?://|tg://)", button_url, flags=re.IGNORECASE):
            raise ValueError("РЎСЃС‹Р»РєР° РєРЅРѕРїРєРё РґРѕР»Р¶РЅР° РЅР°С‡РёРЅР°С‚СЊСЃСЏ СЃ http://, https:// РёР»Рё tg://")
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
                raise PermissionError("РўРѕР»СЊРєРѕ РІР»Р°РґРµР»РµС† РјРѕР¶РµС‚ РґРѕР±Р°РІР»СЏС‚СЊ Р°РґРјРёРЅРѕРІ")

            user_id = _admin_parse_user_id(action_payload.get("userId"))
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
                raise PermissionError("РўРѕР»СЊРєРѕ РІР»Р°РґРµР»РµС† РјРѕР¶РµС‚ СѓРґР°Р»СЏС‚СЊ Р°РґРјРёРЅРѕРІ")

            user_id = _admin_parse_user_id(action_payload.get("userId"))
            if user_id == int(OWNER_ADMIN_ID):
                raise ValueError("РќРµР»СЊР·СЏ СѓРґР°Р»РёС‚СЊ РІР»Р°РґРµР»СЊС†Р°")

            if user_id not in ADMIN_IDS:
                raise ValueError("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ СЏРІР»СЏРµС‚СЃСЏ Р°РґРјРёРЅРѕРј")

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
            cursor.execute(
                """
                SELECT item_type, SUM(amount), SUM(cost)
                FROM purchases
                WHERE date(created_at) = date('now', 'localtime')
                GROUP BY item_type
                """
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
                        WHERE item_type='premium' AND date(created_at) = date('now', 'localtime')
                        """
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
                WHERE date(created_at) = date('now', 'localtime')
                GROUP BY user_id
                ORDER BY total_spent DESC
                LIMIT 5
                """
            )
            top_buyers_rows = cursor.fetchall()
            top_buyers = [
                {
                    "username": (str(row[0]).strip() if row[0] else "вЂ”"),
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
            cursor.execute(
                """
                SELECT COUNT(*) FROM purchases
                WHERE created_at >= datetime('now', '-1 day')
                """
            )
            count = int(cursor.fetchone()[0] or 0)
            if count > 0:
                cursor.execute(
                    """
                    DELETE FROM purchases
                    WHERE created_at >= datetime('now', '-1 day')
                    """
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
            today = datetime.date.today()
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
                raise ValueError("РљРѕРґ РїСЂРѕРјРѕРєРѕРґР°: 3-32 СЃРёРјРІРѕР»Р° A-Z, 0-9, _")

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
                raise ValueError("Р”Р»СЏ СЃРєРёРґРєРё Р·РЅР°С‡РµРЅРёРµ effectValue РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ РѕС‚ 1 РґРѕ 100")

            min_stars = _admin_parse_non_negative_int(action_payload.get("minStars"), field_name="minStars")
            expires_at = str(action_payload.get("expiresAt") or "").strip()
            try:
                datetime.datetime.strptime(expires_at, "%Y-%m-%d")
            except ValueError:
                raise ValueError("РќРµРєРѕСЂСЂРµРєС‚РЅР°СЏ РґР°С‚Р° expiresAt (РѕР¶РёРґР°РµС‚СЃСЏ YYYY-MM-DD)")

            max_uses = _admin_parse_positive_int(action_payload.get("maxUses"), field_name="maxUses")
            max_uses_per_user = _admin_parse_positive_int(action_payload.get("maxUsesPerUser"), field_name="maxUsesPerUser")
            condition = str(action_payload.get("condition") or "all").strip().lower()
            if condition not in {"all", "buyers"}:
                raise ValueError("condition РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ all РёР»Рё buyers")
            target = str(action_payload.get("target") or "stars").strip().lower()
            if target not in {"stars", "premium", "ton", "all"}:
                raise ValueError("target РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ stars, premium, ton РёР»Рё all")
            if effect_type == "free_stars" and target not in {"stars", "all"}:
                raise ValueError("Р‘РµСЃРїР»Р°С‚РЅС‹Рµ Р·РІС‘Р·РґС‹ РјРѕР¶РЅРѕ РїСЂРёРјРµРЅСЏС‚СЊ С‚РѕР»СЊРєРѕ Рє stars РёР»Рё all")

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
                raise ValueError("РЈРєР°Р¶РёС‚Рµ РєРѕРґ РїСЂРѕРјРѕРєРѕРґР°")
            cursor.execute("DELETE FROM promo_codes WHERE code=?", (code,))
            deleted_count = int(cursor.rowcount or 0)
            cursor.execute("DELETE FROM used_promo WHERE code=?", (code,))
            cursor.execute("DELETE FROM promo_usages WHERE code=?", (code,))
            conn.commit()
            if deleted_count == 0:
                raise ValueError("РџСЂРѕРјРѕРєРѕРґ РЅРµ РЅР°Р№РґРµРЅ")
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
            return miniapp_json_response(
                {
                    "ok": True,
                    "data": {
                        "totalStars": _admin_get_stars_total(),
                        "starRates": _admin_get_star_rates_payload(),
                    },
                }
            )

        if action == "stars_update":
            mode = str(action_payload.get("mode") or "").strip().lower()
            if mode not in {"add", "remove"}:
                raise ValueError("mode РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ add РёР»Рё remove")
            amount = _admin_parse_positive_int(action_payload.get("amount"), field_name="amount")
            total_stars = _admin_get_stars_total()
            if mode == "add":
                new_total = total_stars + amount
            else:
                new_total = max(total_stars - amount, 0)
            cursor.execute("UPDATE total_stars SET total = ? WHERE id = 1", (new_total,))
            conn.commit()
            return miniapp_json_response({"ok": True, "data": {"totalStars": int(new_total)}})

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
                raise ValueError("РЈРєР°Р¶РёС‚Рµ ID РґР»СЏ РІС‹РїР»Р°С‚С‹")
            try:
                amount = float(action_payload.get("amount"))
            except (TypeError, ValueError):
                raise ValueError("РќРµРєРѕСЂСЂРµРєС‚РЅР°СЏ СЃСѓРјРјР° РІС‹РїР»Р°С‚С‹")
            if amount <= 0:
                raise ValueError("РЎСѓРјРјР° РІС‹РїР»Р°С‚С‹ РґРѕР»Р¶РЅР° Р±С‹С‚СЊ Р±РѕР»СЊС€Рµ РЅСѓР»СЏ")
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
                raise ValueError("Р”РѕР±Р°РІСЊС‚Рµ С‚РµРєСЃС‚ Рё/РёР»Рё С„РѕС‚Рѕ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё")

            image_bytes = b""
            if image_base64:
                try:
                    image_bytes = base64.b64decode(image_base64, validate=True)
                except Exception:
                    raise ValueError("РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ РёР·РѕР±СЂР°Р¶РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё")
                if not image_bytes:
                    raise ValueError("РџСѓСЃС‚РѕРµ РёР·РѕР±СЂР°Р¶РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё")

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
                raise ValueError("Р’РІРµРґРёС‚Рµ username РёР»Рё ID")
            normalized_query = query.lstrip("@")

            if normalized_query.isdigit():
                cursor.execute("SELECT * FROM users WHERE user_id = ?", (int(normalized_query),))
            else:
                cursor.execute("SELECT * FROM users WHERE username = ?", (normalized_query,))
            user_row = cursor.fetchone()
            if not user_row:
                raise ValueError("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ")

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
                "username": f"@{user_row[1]}" if user_row[1] else "вЂ”",
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
                await bot.send_message(user_id, f"рџЋЃ Р’Р°Рј РЅР°С‡РёСЃР»РµРЅРѕ {amount}в­ђпёЏ РѕС‚ Р°РґРјРёРЅРёСЃС‚СЂР°С†РёРё!")
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

    if order_type not in {"stars", "premium"}:
        return miniapp_json_response({"ok": False, "error": "Unknown order type"}, status=400)
    if not target_username:
        return miniapp_json_response({"ok": False, "error": "Введите Telegram username"}, status=400)

    try:
        telegram_user = parse_telegram_init_data(init_data)
    except ValueError as error:
        return miniapp_json_response({"ok": False, "error": str(error)}, status=401)

    user_id = telegram_user["id"]
    buyer_username = telegram_user["username"] or f"id{user_id}"
    upsert_user_from_telegram(telegram_user)

    if order_type == "stars":
        try:
            amount = int(payload.get("amount"))
        except (TypeError, ValueError):
            return miniapp_json_response({"ok": False, "error": "Количество звёзд должно быть числом"}, status=400)
        if amount < 50 or amount > 10000:
            return miniapp_json_response({"ok": False, "error": "Количество звёзд должно быть от 50 до 10000"}, status=400)
        base_rub = round(amount * get_star_rate_for_range(amount), 2)
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
    promo_error = ""
    promo_effect_type = ""
    promo_effect_value = 0
    promo_applied = False

    if promo_code:
        promo_result = await apply_promo_code(
            user_id=user_id,
            promo_code=promo_code,
            target_type=promo_target,
            stars_amount=amount if promo_target == "stars" else 0,
        )
        if not promo_result.get("ok"):
            promo_error = promo_result.get("error") or "Промокод недоступен"
        else:
            discount_percent = int(promo_result.get("discount", 0))
            final_rub = round(base_rub * (100 - discount_percent) / 100, 2)
            promo_effect_type = str(promo_result.get("effectType") or "")
            promo_effect_value = int(promo_result.get("effectValue") or 0)
            promo_applied = True

    message_lines = [
        "🧾 <b>Новая заявка из MINIAPP (тест без оплаты)</b>",
        f"👤 <b>Покупатель:</b> @{escape(buyer_username)} (ID: <code>{user_id}</code>)",
        f"🎯 <b>Аккаунт:</b> @{escape(target_username)}",
        f"📦 <b>Товар:</b> {escape(amount_label)}",
        f"💳 <b>Сумма:</b> {final_rub:.2f}₽",
    ]

    if promo_code:
        if promo_error:
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (не применён: {escape(promo_error)})"
            )
        elif promo_effect_type == "free_stars":
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (бесплатно {promo_effect_value}⭐️)"
            )
        elif discount_percent > 0:
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (скидка {discount_percent}%)"
            )
        else:
            message_lines.append(f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code>")

    message_lines.append("⚠️ <i>Оплата не проводится: отправлена только заявка.</i>")
    message_text_group = "\n".join(message_lines)

    try:
        await bot.send_message(
            GROUP_CHAT_ID,
            message_text_group,
            parse_mode="HTML",
            reply_markup=done_keyboard(),
        )
    except Exception as error:
        return miniapp_json_response(
            {"ok": False, "error": f"Не удалось отправить заявку в группу: {error}"},
            status=500,
        )

    if promo_applied and promo_code:
        if not confirm_promo_usage(promo_code, user_id):
            promo_error = "Промокод не активирован: лимит исчерпан после отправки заявки."
            logging.warning(
                "Promo activation failed after group notification. user_id=%s code=%s",
                user_id,
                promo_code,
            )

    final_usd = round(final_rub / MINIAPP_USD_RUB_RATE, 2)
    try:
        add_miniapp_purchase_history(
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
            promo_error=promo_error,
            source="api",
        )
    except Exception as error:
        return miniapp_json_response(
            {"ok": False, "error": f"Failed to save purchase history: {error}"},
            status=500,
        )

    return miniapp_json_response(
        {
            "ok": True,
            "message": "Заявка отправлена в группу",
            "price": {
                "rub": round(final_rub, 2),
                "usd": final_usd,
            },
            "promo": {
                "code": promo_code,
                "discount": discount_percent,
                "error": promo_error,
                "effectType": promo_effect_type,
                "effectValue": promo_effect_value,
            },
        }
    )


def create_miniapp_api() -> web.Application:
    app = web.Application()
    app.router.add_route("OPTIONS", "/api/miniapp/{tail:.*}", miniapp_config_handler)
    app.router.add_get("/api/miniapp/config", miniapp_config_handler)
    app.router.add_get("/api/miniapp/profile", miniapp_profile_handler)
    app.router.add_post("/api/miniapp/profile", miniapp_profile_handler)
    app.router.add_post("/api/miniapp/admin/open", miniapp_admin_open_handler)
    app.router.add_post("/api/miniapp/admin/rates", miniapp_admin_rates_handler)
    app.router.add_post("/api/miniapp/admin/action", miniapp_admin_action_handler)
    app.router.add_post("/api/miniapp/order", miniapp_order_handler)
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
    promo_error = ""
    promo_effect_type = ""
    promo_effect_value = 0
    promo_applied = False

    if promo_code:
        promo_result = await apply_promo_code(
            user_id=user_id,
            promo_code=promo_code,
            target_type=promo_target,
            stars_amount=amount if promo_target == "stars" else 0,
        )
        if not promo_result.get("ok"):
            promo_error = promo_result.get("error") or "Промокод недоступен"
        else:
            discount_percent = int(promo_result.get("discount", 0))
            final_rub = round(base_rub * (100 - discount_percent) / 100, 2)
            promo_effect_type = str(promo_result.get("effectType") or "")
            promo_effect_value = int(promo_result.get("effectValue") or 0)
            promo_applied = True

    message_lines = [
        "🧾 <b>Новая заявка из MINIAPP (sendData)</b>",
        f"👤 <b>Покупатель:</b> @{escape(buyer_username)} (ID: <code>{user_id}</code>)",
        f"🎯 <b>Аккаунт:</b> @{escape(target_username)}",
        f"📦 <b>Товар:</b> {escape(amount_label)}",
        f"💳 <b>Сумма:</b> {final_rub:.2f}₽ / {round(final_rub / MINIAPP_USD_RUB_RATE, 2):.2f}$",
    ]

    if promo_code:
        if promo_error:
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (не применён: {escape(promo_error)})"
            )
        elif promo_effect_type == "free_stars":
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
            reply_markup=done_keyboard(),
        )
    except Exception as error:
        await message.answer(f"Ошибка отправки в группу: {error}")
        return

    if promo_applied and promo_code:
        if not confirm_promo_usage(promo_code, user_id):
            promo_error = "Промокод не активирован: лимит исчерпан после отправки заявки."
            logging.warning(
                "Promo activation failed after sendData group notification. user_id=%s code=%s",
                user_id,
                promo_code,
            )

    final_usd = round(final_rub / MINIAPP_USD_RUB_RATE, 2)
    try:
        add_miniapp_purchase_history(
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
            promo_error=promo_error,
            source="sendData",
        )
    except Exception as error:
        await message.answer(f"Ошибка сохранения истории: {error}")
        return

    await message.answer("Заявка отправлена в группу ✅")

@dp.callback_query(lambda c: c.data.startswith("change_rate_"))
async def admin_change_rate(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("рџљ« РќРµС‚ РґРѕСЃС‚СѓРїР°", show_alert=True)
        return

    range_map = {
        "change_rate_50_75": "50_75",
        "change_rate_76_100": "76_100",
        "change_rate_101_250": "101_250",
        "change_rate_251_plus": "251_plus"
    }

    if callback.data not in range_map:
        await callback.answer("вќЊ РћС€РёР±РєР°: РЅРµРІРµСЂРЅР°СЏ РєРЅРѕРїРєР°", show_alert=True)
        return

    range_name = range_map[callback.data]
    user_states[callback.from_user.id] = {"awaiting_rate_change": range_name}

    await callback.message.answer(
        f"вњЏпёЏ Р’РІРµРґРёС‚Рµ РЅРѕРІС‹Р№ РєСѓСЂСЃ РґР»СЏ РґРёР°РїР°Р·РѕРЅР° {range_name.replace('_', '-')} (РЅР°РїСЂРёРјРµСЂ, 1.6):"
    )

@dp.callback_query(lambda c: c.data == "open_change_rate_menu")
async def open_change_rate_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Р’С‹Р±РµСЂРёС‚Рµ РґРёР°РїР°Р·РѕРЅ РґР»СЏ РёР·РјРµРЅРµРЅРёСЏ РєСѓСЂСЃР°:",
        reply_markup=change_star_rate_kb  # С‚РІРѕСЏ РєР»Р°РІРёР°С‚СѓСЂР° СЃ РґРёР°РїР°Р·РѕРЅР°РјРё
    )


@dp.message()
async def handle_message(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state:
        return  # РќРµС‚ Р°РєС‚РёРІРЅС‹С… СЃРѕСЃС‚РѕСЏРЅРёР№, РЅРёС‡РµРіРѕ РЅРµ РґРµР»Р°РµРј

    # РћР±СЂР°Р±РѕС‚РєР° РІРІРѕРґР° username РґР»СЏ РїРѕРєСѓРїРєРё Premium
    if state.get('awaiting_premium_username', False):
        username_input = message.text.strip()

        if not re.fullmatch(r'@[A-Za-z0-9_]+', username_input):
            await message.answer("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ С„РѕСЂРјР°С‚ username. РџСЂРёРјРµСЂ: @example")
            return

        if user_id not in user_premium_data:
            await message.answer("вљ пёЏ РћС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРґРїРёСЃРєРµ РЅРµ РЅР°Р№РґРµРЅС‹. РџРѕРїСЂРѕР±СѓР№С‚Рµ РІС‹Р±СЂР°С‚СЊ С‚Р°СЂРёС„ Р·Р°РЅРѕРІРѕ.")
            return

        # РЎРѕС…СЂР°РЅСЏРµРј username
        user_premium_data[user_id]['username'] = username_input
        months = user_premium_data[user_id]['months']
        price = user_premium_data[user_id]['price']

        await message.answer(
            f"<b>Р’С‹ СѓРІРµСЂРµРЅС‹, С‡С‚Рѕ С…РѕС‚РёС‚Рµ РїСЂРёРѕР±СЂРµСЃС‚Рё Telegram Premium РЅР° {months} РјРµСЃ. Р·Р° {price}в‚Ѕ "
            f"РЅР° Р°РєРєР°СѓРЅС‚ {username_input}?</b>\n\n"
            f"<b>вЏІпёЏ Р’СЂРµРјСЏ РЅР° РѕРїР»Р°С‚Сѓ 10 РјРёРЅСѓС‚</b>",
            reply_markup=buy_prem_with_promo,  # РёСЃРїРѕР»СЊР·СѓРµРј РіРѕС‚РѕРІСѓСЋ СЂР°Р·РјРµС‚РєСѓ
            parse_mode="HTML"
        )

        user_states.pop(user_id, None)

    if 'awaiting_stars' in state:
        try:
            stars_input = int(message.text)
            if stars_input < 50 or stars_input > 10000:
                await message.answer("РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РІРІРµРґРёС‚Рµ С‡РёСЃР»Рѕ РѕС‚ 50 РґРѕ 10 000.")
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
                "рџ“› <b>РўРµРїРµСЂСЊ СѓРєР°Р¶Рё username Telegram-Р°РєРєР°СѓРЅС‚Р°, РєСѓРґР° РЅСѓР¶РЅРѕ Р·Р°С‡РёСЃР»РёС‚СЊ Р·РІС‘Р·РґС‹.</b>\n\n"
                "Р’Р°Р¶РЅРѕ:\n"
                "вЂў <b>РЈР±РµРґРёСЃСЊ</b>, С‡С‚Рѕ С‚РІРѕР№ username РєРѕСЂСЂРµРєС‚РЅРѕ СѓРєР°Р·Р°РЅ (РЅР°РїСЂРёРјРµСЂ, @example).\n"
                "вЂў <b>Р•СЃР»Рё Сѓ С‚РµР±СЏ РЅРµС‚ username</b>, РµРіРѕ РЅСѓР¶РЅРѕ СЃРѕР·РґР°С‚СЊ РІ РЅР°СЃС‚СЂРѕР№РєР°С… Telegram.\n"
                "вЂў Р—РІС‘Р·РґС‹ Р±СѓРґСѓС‚ Р·Р°С‡РёСЃР»РµРЅС‹ <b>РІ С‚РµС‡РµРЅРёРµ 5-С‚Рё РјРёРЅСѓС‚</b> РїРѕСЃР»Рµ РѕРїР»Р°С‚С‹"
            )

            await bot.send_photo(
                chat_id=message.chat.id,
                photo=photo_url,
                caption=caption,
                parse_mode='HTML'
            )

        except ValueError:
            await message.answer("РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РІРІРµРґРёС‚Рµ РєРѕСЂСЂРµРєС‚РЅРѕРµ С‡РёСЃР»Рѕ.")
        return

    # РћР±СЂР°Р±РѕС‚РєР° РІРІРѕРґР° username РґР»СЏ РїРѕРєСѓРїРєРё Р·РІРµР·Рґ
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
                    f"<b>Р’С‹ СѓРІРµСЂРµРЅС‹, С‡С‚Рѕ С…РѕС‚РёС‚Рµ РєСѓРїРёС‚СЊ в­ђпёЏ{stars_amount} Р·Р° {cost}в‚Ѕ РЅР° Р°РєРєР°СѓРЅС‚ {target_username}?</b>\n\n"
                    f"<b>вЏІ Р’СЂРµРјСЏ РЅР° РѕРїР»Р°С‚Сѓ 10 РјРёРЅСѓС‚</b>",
                    reply_markup=buy_with_promo,
                    parse_mode="HTML"
                )

                user_states.pop(user_id)
            else:
                await message.answer("РџСЂРѕРёР·РѕС€Р»Р° РѕС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРєСѓРїРєРµ РЅРµ РЅР°Р№РґРµРЅС‹. РџРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅРѕРІР°.")
        else:
            await message.answer("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ С„РѕСЂРјР°С‚ username. РћРЅ РґРѕР»Р¶РµРЅ РЅР°С‡РёРЅР°С‚СЊСЃСЏ СЃ '@' Рё СЃРѕРґРµСЂР¶Р°С‚СЊ С‚РѕР»СЊРєРѕ Р±СѓРєРІС‹, С†РёС„СЂС‹ Рё _.")
        return

    # РћР±СЂР°Р±РѕС‚РєР° username РїСЂРё СЃРѕСЃС‚РѕСЏРЅРёРё waiting_username_apply
    if 'waiting_username_apply' in state:
        username_input = message.text.strip()
        if re.match(r'^@[A-Za-z0-9_]+$', username_input):
            message_text_group = f'РЈ {user_id} 10 СЂРµС„РѕРІ Р·РІРµР·РґС‹ РЅР° РІРѕС‚ СЌС‚РѕС‚ Р°РєРє {username_input}'
            await bot.send_message(GROUP_CHAT_ID2, message_text_group)

            await message.answer(
                f'РџРѕРґР°СЂРѕРє Р·Р° Р°РєС‚РёРІРЅС‹С… 10 СЂРµС„РµСЂР°Р»РѕРІ РїРѕСЃС‚СѓРїРёС‚ РЅР° Р°РєРєР°СѓРЅС‚ {username_input}\n'
                f'РџРѕР¶Р°Р»СѓР№СЃС‚Р° РѕР¶РёРґР°Р№С‚Рµ. РЎ СѓРІР°Р¶РµРЅРёРµРј @starslix'
            )
            user_states.pop(user_id)
        else:
            await message.answer(
                "РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ С„РѕСЂРјР°С‚ username. РћРЅ РґРѕР»Р¶РµРЅ РЅР°С‡РёРЅР°С‚СЊСЃСЏ СЃ '@' Рё СЃРѕРґРµСЂР¶Р°С‚СЊ С‚РѕР»СЊРєРѕ Р±СѓРєРІС‹, С†РёС„СЂС‹ Рё _."
            )
        return
    if state and state.get("awaiting_promo"):
        promo = message.text.strip().upper()
        user_id = message.from_user.id

        # --- РџСЂРѕРІРµСЂСЏРµРј РЅР°Р»РёС‡РёРµ РґР°РЅРЅС‹С… Рѕ РїРѕРєСѓРїРєРµ ---
        if user_id not in user_purchase_data:
            await message.answer(
                "вќЊ РћС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРєСѓРїРєРµ РЅРµ РЅР°Р№РґРµРЅС‹.\n"
                "РџРѕРїСЂРѕР±СѓР№С‚Рµ Р·Р°РЅРѕРІРѕ РІС‹Р±СЂР°С‚СЊ РїР°РєРµС‚ Р·РІС‘Р·Рґ."
            )
            user_states.pop(user_id, None)
            return

        stars_amount = user_purchase_data[user_id]["stars"]
        old_cost = user_purchase_data[user_id]["cost"]
        target_username = user_purchase_data[user_id].get("username", "")

        # --- РџСЂРѕРІРµСЂСЏРµРј РїСЂРѕРјРѕРєРѕРґ С‡РµСЂРµР· apply_promo_code ---
        result = await apply_promo_code(user_id, promo, "stars", stars_amount)

        # --- Р•СЃР»Рё РїСЂРѕРјРѕ РЅРµ РЅР°Р№РґРµРЅ / РЅРµРґРµР№СЃС‚РІРёС‚РµР»РµРЅ ---
        if not result["ok"]:
            await message.answer(result["error"])
            user_states.pop(user_id, None)
            return

        # --- РџСЂРѕРјРѕРєРѕРґ РІР°Р»РёРґРµРЅ ---
        discount = result["discount"]
        new_cost = round(old_cost * (100 - discount) / 100, 2)

        # --- Р•СЃР»Рё СЃРєРёРґРєР° 100% вЂ” РІС‹РґР°С‘Рј Р·РІС‘Р·РґС‹ СЃСЂР°Р·Сѓ ---
        if discount == 100:
            # РћС‚РјРµС‡Р°РµРј РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ РїСЂРѕРјРѕ
            confirm_promo_usage(promo, user_id)

            # Р—Р°РїРёСЃС‹РІР°РµРј РїРѕРєСѓРїРєСѓ РІ Р‘Р”
            cursor.execute(
                "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
                (user_id, message.from_user.username or "", "stars", stars_amount, 0)
            )
            conn.commit()

            # РЈРІРµР»РёС‡РёРІР°РµРј РѕР±С‰РёР№ СЃС‡С‘С‚С‡РёРє Р·РІС‘Р·Рґ
            cursor.execute("UPDATE total_stars SET total = total + ? WHERE id = 1", (stars_amount,))
            conn.commit()

            # --- РЎРѕРѕР±С‰РµРЅРёРµ РІ РіСЂСѓРїРїСѓ ---
            buyer_username = message.from_user.username or "user"
            message_text_group = (
                f"@{buyer_username} - {stars_amount}в­ђ - РѕРїР»Р°С‚Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР° РґР»СЏ Р°РєРєР°СѓРЅС‚Р° @{target_username} (РїСЂРѕРјРѕ {promo})"
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="delete_msg")]]
            )
            await bot.send_message(GROUP_CHAT_ID, message_text_group, reply_markup=keyboard)

            # --- РЎРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ ---
            await message.answer(
                f"вњ… РџСЂРѕРјРѕРєРѕРґ <b>{promo}</b> СѓСЃРїРµС€РЅРѕ РїСЂРёРјРµРЅС‘РЅ!\n"
                f"Р’С‹ РїРѕР»СѓС‡РёР»Рё <b>{stars_amount}в­ђ</b> СЃРѕРІРµСЂС€РµРЅРЅРѕ Р±РµСЃРїР»Р°С‚РЅРѕ рџЋ‰\n\n"
                f"Р—РІС‘Р·РґС‹ СЃРєРѕСЂРѕ РїРѕСЃС‚СѓРїСЏС‚ РЅР° РІР°С€ Р°РєРєР°СѓРЅС‚ @{target_username}.",
                parse_mode="HTML"
            )

            user_states.pop(user_id, None)
            user_purchase_data.pop(user_id, None)
            return

        # --- Р•СЃР»Рё СЃРєРёРґРєР° РјРµРЅСЊС€Рµ 100% вЂ” СЃРѕС…СЂР°РЅСЏРµРј РїСЂРѕРјРѕ Рё РїСЂРµРґР»Р°РіР°РµРј РѕРїР»Р°С‚Сѓ ---
        user_purchase_data[user_id]["promo_pending"] = {
            "code": promo,
            "discount": discount,
            "new_cost": new_cost
        }

        await message.answer(
            f"вњ… РџСЂРѕРјРѕ-РєРѕРґ СѓСЃРїРµС€РЅРѕ РїСЂРёРјРµРЅС‘РЅ!\n"
            f"Р’С‹ СѓРІРµСЂРµРЅС‹, С‡С‚Рѕ С…РѕС‚РёС‚Рµ РєСѓРїРёС‚СЊ в­ђпёЏ{stars_amount}\n"
            f"<s>{old_cost}в‚Ѕ</s> вћќ {new_cost}в‚Ѕ РЅР° Р°РєРєР°СѓРЅС‚ {user_purchase_data[user_id]['username']}\n\n"
            f"вЏІ Р’СЂРµРјСЏ РЅР° РѕРїР»Р°С‚Сѓ 10 РјРёРЅСѓС‚",
            reply_markup=buy_final,
            parse_mode="HTML"
        )

        # --- РћС‡РёС‰Р°РµРј СЃРѕСЃС‚РѕСЏРЅРёРµ ---
        user_states.pop(user_id, None)
        return

        # --- РћР±СЂР°Р±РѕС‚РєР° РїСЂРѕРјРѕ РґР»СЏ Premium ---
        if state.get("awaiting_promo_prem"):
            promo = message.text.strip().upper()


            if user_id not in user_premium_data:

                await message.answer("вќЊ РћС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРєСѓРїРєРµ Premium РЅРµ РЅР°Р№РґРµРЅС‹.")
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

            # --- РџСЂРѕРјРѕ 100% ---
            if result["discount"] == 100:
                if not confirm_promo_usage(promo, user_id):
                    await message.answer("❌ Промокод больше недоступен.")
                    user_states.pop(user_id, None)
                    return


                await message.answer(
                    f"вњ… РџСЂРѕРјРѕРєРѕРґ {promo} СѓСЃРїРµС€РЅРѕ РїСЂРёРјРµРЅС‘РЅ!\n"
                    f"Р’С‹ РїРѕР»СѓС‡РёР»Рё Telegram Premium РЅР° {months} РјРµСЃ. Р±РµСЃРїР»Р°С‚РЅРѕ рџЋЃ"
                )

                user_states.pop(user_id, None)
                user_premium_data.pop(user_id, None)
                return

            # --- РџСЂРѕРјРѕ СЃ С‡Р°СЃС‚РёС‡РЅРѕР№ СЃРєРёРґРєРѕР№ ---
            new_price = round(old_price * (100 - result['discount']) / 100, 2)


            user_premium_data[user_id]['promo_pending'] = {
                'code': promo,
                'discount': result['discount'],
                'new_price': new_price
            }

            await message.answer(
                f"вњ… РџСЂРѕРјРѕ-РєРѕРґ СѓСЃРїРµС€РЅРѕ РїСЂРёРјРµРЅС‘РЅ!\n"
                f"Premium {months} РјРµСЃ.\n"
                f"<s>{old_price}в‚Ѕ</s> вћќ {new_price}в‚Ѕ РЅР° Р°РєРєР°СѓРЅС‚ {username}\n\n"
                f"вЏІ Р’СЂРµРјСЏ РЅР° РѕРїР»Р°С‚Сѓ 10 РјРёРЅСѓС‚",
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
            await message.answer(f"вњ… РљСѓСЂСЃ РґР»СЏ РґРёР°РїР°Р·РѕРЅР° {range_name.replace('_', '-')} РѕР±РЅРѕРІР»С‘РЅ: {new_rate}")
        except ValueError:
            await message.answer("вќЊ Р’РІРµРґРёС‚Рµ РєРѕСЂСЂРµРєС‚РЅРѕРµ С‡РёСЃР»Рѕ.")

def increment_promo_usage(promo_code: str):
    if not promo_code:
        return
    # РСЃРїРѕР»СЊР·СѓРµРј РіР»РѕР±Р°Р»СЊРЅРѕРµ СЃРѕРµРґРёРЅРµРЅРёРµ РёР»Рё СЃРѕР·РґР°С‘Рј РЅРѕРІРѕРµ
    conn = sqlite3.connect('database.db')  # <-- Р·Р°РјРµРЅР° get_conn()
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




# РћР±СЂР°Р±РѕС‚С‡РёРє СѓРґР°Р»РµРЅРёСЏ РїСЂРѕРјРѕ
@dp.callback_query(lambda c: c.data and c.data.startswith("delete_promo:"))
async def delete_promo_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        return

    promo_code = callback.data.split(":")[1]
    cursor.execute("DELETE FROM promo_codes WHERE code=?", (promo_code,))
    conn.commit()

    await callback.answer(f"вњ… РџСЂРѕРјРѕ-РєРѕРґ {promo_code} СѓРґР°Р»С‘РЅ.", show_alert=True)
    await callback.message.delete()


@dp.callback_query(lambda c: c.data == "buy_custom")
async def buy_custom_handler(callback: types.CallbackQuery):
    photo_url = "https://ibb.co/MyFDq6zx"
    caption = "Р’РІРµРґРёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ Р·РІРµР·Рґ РєРѕС‚РѕСЂРѕРµ С…РѕС‚РёС‚Рµ РєСѓРїРёС‚СЊ (РѕС‚ 50 РґРѕ 10000):"
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
            "РЈ РІР°СЃ РЅРѕРІС‹Р№ Р°РєС‚РёРІРЅС‹Р№ СЂРµС„!"
        )
    except Exception as e:
        print(f"РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {user_id}: {e}")





import aiohttp

# РџРѕР»СѓС‡Р°РµРј Р°РєС‚СѓР°Р»СЊРЅС‹Р№ РєСѓСЂСЃ TON Рє СЂСѓР±Р»СЋ С‡РµСЂРµР· CoinGecko API
async def get_rub_to_ton_rate():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=rub"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            if "toncoin" in data and "rub" in data["toncoin"]:
                return data["toncoin"]["rub"]  # РљСѓСЂСЃ TON/RUB
            else:
                raise Exception("РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ РєСѓСЂСЃ TON/RUB СЃ CoinGecko")


async def process_payment_confirmation(user_id, target_username, stars, callback):
    cursor = conn.cursor()
    buyer_username = callback.from_user.username or "user"

    # --- РЎРѕС…СЂР°РЅСЏРµРј РїРѕРєСѓРїРєСѓ ---
    final_cost = user_purchase_data.get(user_id, {}).get('amount', 0)
    cursor.execute(
        "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
        (user_id, buyer_username, "stars", stars, final_cost)
    )
    conn.commit()


    # --- РЈРІРµР»РёС‡РёРІР°РµРј РѕР±С‰РёР№ СЃС‡С‘С‚С‡РёРє Р·РІС‘Р·Рґ ---
    cursor.execute("UPDATE total_stars SET total = total + ? WHERE id = 1", (stars,))
    conn.commit()

    # --- РћР±СЂР°Р±РѕС‚РєР° РїСЂРѕРјРѕРєРѕРґР° ---
    promo_data = user_purchase_data.get(user_id, {}).get('promo_pending')
    promo_text = ""

    if promo_data:
        promo_code = promo_data.get('code')
        discount = promo_data.get('discount', 0)

        try:
            # СѓРІРµР»РёС‡РёРІР°РµРј uses_count Рё Р·Р°РїРёСЃС‹РІР°РµРј РІ used_promo
            confirm_promo_usage(promo_code, user_id)
        except Exception:
            pass

        if discount > 0:
            promo_text = f" (РїСЂРѕРјРѕ {promo_code} вЂ” СЃРєРёРґРєР° {discount}%)"

    # --- Р¤РѕСЂРјРёСЂСѓРµРј СЃРѕРѕР±С‰РµРЅРёРµ РІ РіСЂСѓРїРїСѓ (РІСЃРµРіРґР°, РґР°Р¶Рµ Р±РµР· РїСЂРѕРјРѕ) ---
    message_text_group = (
        f"@{buyer_username} - {stars}в­ђ - РѕРїР»Р°С‚Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР° РґР»СЏ Р°РєРєР°СѓРЅС‚Р° {target_username}{promo_text}"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="delete_msg")]
        ]
    )

    await bot.send_message(GROUP_CHAT_ID, message_text_group, reply_markup=keyboard)


    # --- РЎРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ ---
    await bot.send_message(
        user_id,
        f"вњ… РћРїР»Р°С‚Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР°! РЎРїР°СЃРёР±Рѕ Р·Р° РїРѕРєСѓРїРєСѓ!\n"
        f"Р—РІС‘Р·РґС‹ СЃРєРѕСЂРѕ РїРѕСЃС‚СѓРїСЏС‚ РЅР° РІР°С€ Р°РєРєР°СѓРЅС‚. РћР¶РёРґР°Р№С‚Рµ.\n\n"
        f'рџЊџ РћС†РµРЅРёС‚СЊ РЅР°С€ СЃРµСЂРІРёСЃ РјРѕР¶РЅРѕ <b><a href="https://t.me/+Qkb-Q43fRf40NGFk">Р—Р”Р•РЎР¬</a></b> вЂ” РЅР°Рј Р±СѓРґРµС‚ РїСЂРёСЏС‚РЅРѕ рџ’«',
        parse_mode="HTML",
        disable_web_page_preview=True
    )


    # --- РћС‡РёСЃС‚РєР° РґР°РЅРЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ ---
    user_purchase_data.pop(user_id, None)


# вњ… Р¤СѓРЅРєС†РёСЏ С„РёРєСЃР°С†РёРё РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ РїСЂРѕРјРѕ
def confirm_promo_usage(promo_code: str, user_id: int) -> bool:
    """Р¤РёРєСЃРёСЂСѓРµС‚ Р°РєС‚РёРІР°С†РёСЋ РїСЂРѕРјРѕРєРѕРґР° СЃ РїСЂРѕРІРµСЂРєРѕР№ Р»РёРјРёС‚РѕРІ РІ РѕРґРЅРѕР№ С‚СЂР°РЅР·Р°РєС†РёРё."""
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
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    try:
        # РџСЂРѕРІРµСЂСЏРµРј, СЃСѓС‰РµСЃС‚РІСѓРµС‚ Р»Рё СѓР¶Рµ РїР»Р°С‚С‘Р¶ СЃ С‚Р°РєРёРј bill_id
        cursor.execute('SELECT * FROM payments WHERE bill_id=?', (bill_id,))
        existing_payment = cursor.fetchone()

        if existing_payment:
            # Р•СЃР»Рё РїР»Р°С‚С‘Р¶ СЃСѓС‰РµСЃС‚РІСѓРµС‚, РѕР±РЅРѕРІР»СЏРµРј РµРіРѕ СЃС‚Р°С‚СѓСЃ
            cursor.execute('''
                UPDATE payments
                SET status=?, amount=?, description=?, date=CURRENT_TIMESTAMP
                WHERE bill_id=?
            ''', (status, amount, description, bill_id))

        else:
            # Р•СЃР»Рё РїР»Р°С‚С‘Р¶ РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚, РІСЃС‚Р°РІР»СЏРµРј РЅРѕРІС‹Р№
            cursor.execute('''
                INSERT INTO payments (user_id, bill_id, amount, status, description, date)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, bill_id, amount, status, description))


        conn.commit()

    except sqlite3.Error as e:
        print(f"РћС€РёР±РєР° РїСЂРё СЃРѕС…СЂР°РЅРµРЅРёРё РїР»Р°С‚РµР¶Р°: {e}")

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
                status = str(data.get("status")).lower()  # <-- РїСЂРёРІРµРґРµРЅРёРµ Рє РЅРёР¶РЅРµРјСѓ СЂРµРіРёСЃС‚СЂСѓ

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
                    raise Exception(f"HTTP РѕС€РёР±РєР°: {resp.status} - {text}")

                data = await resp.json()
                if str(data.get("success")).lower() == "true":
                    payment_id = data.get("bill_id")
                    payment_url = data.get("link_page_url")
                    return payment_id, payment_url
                else:
                    error_message = data.get('message', 'РќРµРёР·РІРµСЃС‚РЅР°СЏ РѕС€РёР±РєР°')
                    raise Exception(f"РћС€РёР±РєР° API Pally: {error_message}")
        except Exception as e:
            # Р›РѕРіРёСЂСѓРµРј РѕС€РёР±РєСѓ РґР»СЏ РґРёР°РіРЅРѕСЃС‚РёРєРё
            logging.error(f"РћС€РёР±РєР° РїСЂРё СЃРѕР·РґР°РЅРёРё РїР»Р°С‚РµР¶Р° С‡РµСЂРµР· Pally: {e}")
            raise Exception(f"РћС€РёР±РєР° РїСЂРё СЃРѕР·РґР°РЅРёРё РїР»Р°С‚РµР¶Р°, РїРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ. {str(e)}")


@dp.callback_query(lambda c: c.data == "pay_sbp")
async def pay_sbp_callback(callback):
    user_id = callback.from_user.id

    if user_id not in user_purchase_data:
        await callback.message.answer("РћС€РёР±РєР°: РґР°РЅРЅС‹Рµ Рѕ РїРѕРєСѓРїРєРµ РЅРµ РЅР°Р№РґРµРЅС‹.")
        return

    # --- РџРѕР»СѓС‡Р°РµРј СЃСѓРјРјСѓ Рё РїР°СЂР°РјРµС‚СЂС‹ ---
    if 'promo_pending' in user_purchase_data[user_id]:
        amount_value = user_purchase_data[user_id]['promo_pending']['new_cost']
    else:
        amount_value = user_purchase_data[user_id]['cost']

    stars_amount = user_purchase_data[user_id]['stars']
    target_username = user_purchase_data[user_id]['username']
    label_str = generate_unique_label(user_id)

    # --- РЎРѕР·РґР°РµРј РїР»Р°С‚РµР¶ С‡РµСЂРµР· Pally ---
    try:
        payment_id, payment_url = await create_pally_payment(
            amount_value,
            f"РџРѕРєСѓРїРєР° {stars_amount}в­ђ РґР»СЏ {target_username}",
            label_str
        )

    except Exception:
        await callback.message.answer("РћС€РёР±РєР° РїСЂРё СЃРѕР·РґР°РЅРёРё РїР»Р°С‚РµР¶Р°, РїРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
        return

    # --- РЎРѕС…СЂР°РЅСЏРµРј РґР°РЅРЅС‹Рµ Рѕ РїР»Р°С‚РµР¶Рµ СЃСЂР°Р·Сѓ СЃ СЃС‚Р°С‚СѓСЃРѕРј 'pending' ---

    user_purchase_data[user_id]['pally_payment_id'] = payment_id
    user_purchase_data[user_id]['label'] = label_str
    user_purchase_data[user_id]['amount'] = amount_value
    user_purchase_data[user_id]['callback_obj'] = callback

    # --- РЎРѕС…СЂР°РЅСЏРµРј РїР»Р°С‚С‘Р¶ РІ Р±Р°Р·Сѓ РґР°РЅРЅС‹С… СЃ СЃС‚Р°С‚СѓСЃРѕРј 'pending' ---
    description = f"РџРѕРєСѓРїРєР° {stars_amount}в­ђ РґР»СЏ {target_username}"
    save_payment(user_id, payment_id, amount_value, "pending", description)  # РЎРѕС…СЂР°РЅСЏРµРј РєР°Рє "pending"

    # --- РћС‚РїСЂР°РІР»СЏРµРј РєРЅРѕРїРєСѓ РѕРїР»Р°С‚С‹ ---
    pay_button = InlineKeyboardButton(text=f"РћРїР»Р°С‚РёС‚СЊ {amount_value}в‚Ѕ", url=payment_url)
    markup = InlineKeyboardMarkup(inline_keyboard=[[pay_button]])
    await callback.message.edit_text(
        "РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ РґР»СЏ РїРµСЂРµС…РѕРґР° Рє РѕРїР»Р°С‚Рµ. РћРїР»Р°С‚Р° Р±СѓРґРµС‚ РїСЂРѕРІРµСЂСЏС‚СЊСЃСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё.",
        reply_markup=markup
    )

    # --- РџСЂРѕРІРµСЂРєР° РѕРїР»Р°С‚С‹ РєР°Р¶РґС‹Рµ 3 СЃРµРє РґРѕ 10 РјРёРЅСѓС‚ ---
    start_time = asyncio.get_event_loop().time()
    payment_confirmed = False
    while True:
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > 600:  # 10 РјРёРЅСѓС‚
            break

        try:
            if await check_pally_payment_status(payment_id):
                payment_confirmed = True
                break
        except Exception as e:
            print(f"РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё СЃС‚Р°С‚СѓСЃР° РїР»Р°С‚РµР¶Р°: {e}")

        await asyncio.sleep(3)

    # --- РћР±СЂР°Р±РѕС‚РєР° СЂРµР·СѓР»СЊС‚Р°С‚Р° ---
    if payment_confirmed:


        # РћР±РЅРѕРІР»СЏРµРј РїР»Р°С‚С‘Р¶ РЅР° "СѓСЃРїРµС€РЅС‹Р№" РїРѕСЃР»Рµ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ
        cursor.execute("UPDATE payments SET status = ? WHERE bill_id = ?", ("paid", payment_id))
        conn.commit()

        await process_payment_confirmation(user_id, target_username, stars_amount, callback)
    else:
        await callback.message.answer("РћРїР»Р°С‚Р° РЅРµ Р±С‹Р»Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР° Р·Р° 10 РјРёРЅСѓС‚. РџРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅРѕРІР°.")


@dp.callback_query(lambda c: c.data == "back_first")
async def back_first_callback(callback: types.CallbackQuery):
    await callback.message.delete()

    photo_url = 'https://ibb.co/XrPBvfbS'  # Р·Р°РјРµРЅРёС‚Рµ РЅР° РІР°С€ СЂРµР°Р»СЊРЅС‹Р№ URL

    username = callback.from_user.username or callback.from_user.first_name
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    total_stars = row[0] if (row and row[0] is not None) else 0
    approx_usd = total_stars * 0.013  # РїСЂРёРјРµСЂРЅС‹Р№ РєСѓСЂСЃ РґРѕР»Р»Р°СЂР°
    stars_info = f"<b>РЈ РЅР°СЃ СѓР¶Рµ РєСѓРїРёР»Рё:</b> {total_stars:,}в­ђпёЏ (~${approx_usd:.2f})".replace(",", " ")
    text3 = (
        f"<b>Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ STARSLIX!</b>\n\n"
        f"<b>РџСЂРёРІРµС‚, {username}!</b>\n"
        f"<b>{stars_info}</b>\n"
        "<b>РџРѕРєСѓРїР°Р№ Р·РІС‘Р·РґС‹ Рё Premium, РґР°СЂРё РїРѕРґР°СЂРєРё, СЃРёСЏР№ СЏСЂС‡Рµ РІСЃРµС…!</b>\n\n"
        "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>РЎРѕРіР»Р°С€РµРЅРёРµ</a></b> | "
        "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>РџРѕР»РёС‚РёРєР°</a></b>\n"
        "<b>Р’СЂРµРјСЏ СЂР°Р±РѕС‚С‹ Р±РѕС‚Р°: 06:00-00:00 (РњРЎРљ)</b>"
    )

    await callback.message.answer_photo(
        photo=photo_url,
        caption=text3,
        reply_markup=hll,
        parse_mode='HTML'
    )

@dp.callback_query(lambda c: c.data == "back")
async def back_callback(callback: types.CallbackQuery):
    # URL РёР·РѕР±СЂР°Р¶РµРЅРёСЏ
    photo_url = 'https://ibb.co/MyFDq6zx'  # Р·Р°РјРµРЅРёС‚Рµ РЅР° РІР°С€ СЂРµР°Р»СЊРЅС‹Р№ URL

    username = callback.from_user.username or callback.from_user.first_name
    caption = "Р’С‹Р±РµСЂРё РѕРґРёРЅ РёР· РїРѕРїСѓР»СЏСЂРЅС‹С… РїР°РєРµС‚РѕРІ РёР»Рё СѓРєР°Р¶Рё СЃРІРѕС‘ РєРѕР»РёС‡РµСЃС‚РІРѕ:"
    await callback.message.delete()
    await callback.message.answer_photo(
        photo=photo_url,
        caption=caption,
        reply_markup=get_star_keyboard()
    )

@dp.callback_query(lambda c: c.data == "back_prem")
async def back_callback(callback: types.CallbackQuery):
    # URL РёР·РѕР±СЂР°Р¶РµРЅРёСЏ
    photo_url = 'https://ibb.co/MyFDq6zx'  # Р·Р°РјРµРЅРёС‚Рµ РЅР° РІР°С€ СЂРµР°Р»СЊРЅС‹Р№ URL

    username = callback.from_user.username or callback.from_user.first_name
    caption = (
        f"<b>рџ’™Telegram Premiumрџ’™</b>\n\n"
        f"<b>Р’С‹Р±РµСЂРёС‚Рµ СЃСЂРѕРє РїРѕРґРїРёСЃРєРё:</b>"
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

    photo_url = 'https://ibb.co/XrPBvfbS'  # Р·Р°РјРµРЅРёС‚Рµ РЅР° РІР°С€ СЂРµР°Р»СЊРЅС‹Р№ URL

    username = callback.from_user.username or callback.from_user.first_name
    cursor.execute("SELECT total FROM total_stars WHERE id = 1")
    row = cursor.fetchone()
    total_stars = row[0] if (row and row[0] is not None) else 0
    approx_usd = total_stars * 0.013  # РїСЂРёРјРµСЂРЅС‹Р№ РєСѓСЂСЃ РґРѕР»Р»Р°СЂР°
    stars_info = f"<b>Р’СЃРµРіРѕ РєСѓРїР»РµРЅРѕ Р·РІС‘Р·Рґ:</b> {total_stars:,}в­ђпёЏ (~${approx_usd:.2f})".replace(",", " ")
    text3 = (
        f"<b>Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ STARSLIX!</b>\n\n"
        f"<b>РџСЂРёРІРµС‚, {username}!</b>\n"
        f"<b>{stars_info}</b>\n"
        "<b>РџРѕРєСѓРїР°Р№ Р·РІС‘Р·РґС‹ Рё Premium, РґР°СЂРё РїРѕРґР°СЂРєРё, СЃРёСЏР№ СЏСЂС‡Рµ РІСЃРµС…!</b>\n\n"
        "<b><a href='https://telegra.ph/Polzovatelskoe-soglashenie-07-12-16'>РЎРѕРіР»Р°С€РµРЅРёРµ</a></b> | "
        "<b><a href='https://telegra.ph/Politika-Konfidencialnosti-07-12-24'>РџРѕР»РёС‚РёРєР°</a></b>\n"
        "<b>Р’СЂРµРјСЏ СЂР°Р±РѕС‚С‹: 06:00-00:00 (РњРЎРљ)</b>"
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
        await callback.answer("РћС€РёР±РєР° РїСЂРё РїСЂРѕРІРµСЂРєРµ РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.", show_alert=True)
        return

    if is_subscribed:
        await callback.answer("РЎРїР°СЃРёР±Рѕ! Р’С‹ СѓСЃРїРµС€РЅРѕ РїРѕРґРїРёСЃР°Р»РёСЃСЊ.\U00002764", show_alert=True)
        # РџСЂРѕРґРѕР»Р¶Р°Р№С‚Рµ РІС‹РїРѕР»РЅРµРЅРёРµ Р»РѕРіРёРєРё
        await help_callback(callback)
    else:
        await callback.answer("Р’С‹ РµС‰Рµ РЅРµ РїРѕРґРїРёСЃР°Р»РёСЃСЊ РЅР° РєР°РЅР°Р».\U0001F61E", show_alert=True)
@dp.callback_query(lambda c: c.data == "help")
async def help_callback(callback: types.CallbackQuery):
    # РџСЂРѕРІРµСЂРєР° РїРѕРґРїРёСЃРєРё
    user_id = callback.from_user.id
    is_subscribed = False
    try:
        is_subscribed = await check_subscription(user_id)
    except Exception as e:
        # РћР±СЂР°Р±РѕС‚РєР° РѕС€РёР±РѕРє (РЅР°РїСЂРёРјРµСЂ, РµСЃР»Рё Р±РѕС‚ РЅРµ РІ СЃРѕСЃС‚РѕСЏРЅРёРё РїСЂРѕРІРµСЂРёС‚СЊ)
        await callback.message.answer("РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїРѕРґРїРёСЃРєРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
        return

    if not is_subscribed:
        # РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РїРѕРґРїРёСЃР°РЅ вЂ” СѓРґР°Р»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ Рё РїСЂРѕСЃРёРј РїРѕРґРїРёСЃР°С‚СЊСЃСЏ
        await callback.message.delete()

        # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№ "РџРѕРґРїРёСЃР°С‚СЊСЃСЏ" Рё "РџСЂРѕРІРµСЂРёС‚СЊ"
        subscribe_button = InlineKeyboardButton(text="РџРѕРґРїРёСЃР°С‚СЊСЃСЏ", url=f"https://t.me/{CHANNEL_ID.lstrip('@')}")
        check_button = InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="check_subscriptionhelp")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[subscribe_button], [check_button]])

        await callback.message.answer(
            "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РїРѕРґРїРёС€РёС‚РµСЃСЊ РЅР° РЅР°С€ РєР°РЅР°Р», С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ.",
            reply_markup=keyboard
        )
        return


    text = (
        'вќ“ Р§Р°СЃС‚Рѕ Р·Р°РґР°РІР°РµРјС‹Рµ РІРѕРїСЂРѕСЃС‹ (FAQ)\n\n'
        '1. Р§С‚Рѕ С‚Р°РєРѕРµ Telegram-Р·РІРµР·РґС‹ Рё Р·Р°С‡РµРј РёС… РїРѕРєСѓРїР°С‚СЊ?\n'
        'Telegram-Р·РІРµР·РґС‹ вЂ” СЌС‚Рѕ РІРёСЂС‚СѓР°Р»СЊРЅС‹Рµ РїРѕРґР°СЂРєРё, РєРѕС‚РѕСЂС‹Рµ РјРѕР¶РЅРѕ РѕС‚РїСЂР°РІР»СЏС‚СЊ РґСЂСѓР·СЊСЏРј Рё Р±Р»РёР·РєРёРј РІ Telegram. '
        'РќР°С€ Р±РѕС‚ РїРѕРјРѕРіР°РµС‚ Р»РµРіРєРѕ Рё Р±С‹СЃС‚СЂРѕ РїСЂРёРѕР±СЂРµСЃС‚Рё Р·РІРµР·РґС‹, С‡С‚РѕР±С‹ РґРµР»Р°С‚СЊ РїСЂРёСЏС‚РЅС‹Рµ РїРѕРґР°СЂРєРё РёР»Рё СѓРєСЂР°С€Р°С‚СЊ СЃРІРѕР№ РїСЂРѕС„РёР»СЊ.\n\n'
        '2. Р§С‚Рѕ С‚Р°РєРѕРµ "Р°РєС‚РёРІРЅС‹Р№ СЂРµС„РµСЂР°Р»"?\n'
        'РђРєС‚РёРІРЅС‹Р№ СЂРµС„РµСЂР°Р» вЂ” СЌС‚Рѕ РІР°С€ СЂРµС„РµСЂР°Р», РєРѕС‚РѕСЂС‹Р№ СЃРґРµР»Р°Р» С…РѕС‚СЏ Р±С‹ РѕРґРЅСѓ РїРѕРєСѓРїРєСѓ С‡РµСЂРµР· Р±РѕС‚Р°.\n\n'
        '3. РљР°Рє СЏ РјРѕРіСѓ РїРѕР»СѓС‡РёС‚СЊ Р±РѕРЅСѓСЃ Р·Р° СЂРµС„РµСЂР°Р»РѕРІ?\n'
        'РџРѕР»СѓС‡РёС‚СЊ Р±РѕРЅСѓСЃ РјРѕР¶РЅРѕ С‚РѕР»СЊРєРѕ РїСЂРё РЅР°Р»РёС‡РёРё 10 РёР»Рё Р±РѕР»РµРµ Р°РєС‚РёРІРЅС‹С… СЂРµС„РµСЂР°Р»РѕРІ. '
        'Р”Р»СЏ СЌС‚РѕРіРѕ РЅРµРѕР±С…РѕРґРёРјРѕ РѕСЃС‚Р°РІРёС‚СЊ Р·Р°СЏРІРєСѓ РЅР° РїРѕР»СѓС‡РµРЅРёРµ Р±РѕРЅСѓСЃР° вЂ” СЃРІСЏР¶РёС‚РµСЃСЊ СЃ РїРѕРґРґРµСЂР¶РєРѕР№ РёР»Рё РІРѕСЃРїРѕР»СЊР·СѓР№С‚РµСЃСЊ С„РѕСЂРјРѕР№ РЅРёР¶Рµ.\n\n'
        '4. РљР°РєРёРµ РїСЂРµРёРјСѓС‰РµСЃС‚РІР° Сѓ Р°РєС‚РёРІРЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№?\n'
        'РђРєС‚РёРІРЅС‹Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»Рё РїРѕР»СѓС‡Р°СЋС‚ РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ Р±РѕРЅСѓСЃС‹, СЃРєРёРґРєРё Рё СЌРєСЃРєР»СЋР·РёРІРЅС‹Рµ РїСЂРµРґР»РѕР¶РµРЅРёСЏ. '
        'РўР°РєР¶Рµ РѕРЅРё РјРѕРіСѓС‚ СѓС‡Р°СЃС‚РІРѕРІР°С‚СЊ РІ СЃРїРµС†РёР°Р»СЊРЅС‹С… Р°РєС†РёСЏС… Рё РєРѕРЅРєСѓСЂСЃР°С….\n\n'
        '5. РљР°Рє РєСѓРїРёС‚СЊ Р·РІРµР·РґС‹ С‡РµСЂРµР· Р±РѕС‚Р°?\n'
        'РџСЂРѕСЃС‚Рѕ РІС‹Р±РµСЂРёС‚Рµ РЅСѓР¶РЅС‹Р№ РїР°РєРµС‚ Р·РІРµР·Рґ, СЃР»РµРґСѓР№С‚Рµ РёРЅСЃС‚СЂСѓРєС†РёСЏРј Рё РѕРїР»Р°С‚РёС‚Рµ СѓРґРѕР±РЅС‹Рј СЃРїРѕСЃРѕР±РѕРј. '
        'РџРѕСЃР»Рµ РѕРїР»Р°С‚С‹ Р·РІРµР·РґС‹ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё Р·Р°С‡РёСЃР»СЏСЋС‚СЃСЏ РЅР° РІР°С€ Р°РєРєР°СѓРЅС‚.\n\n'
        '6. РњРѕРіСѓ Р»Рё СЏ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ СЂРµС„РµСЂР°Р»СЊРЅСѓСЋ СЃРёСЃС‚РµРјСѓ Р±РµР· РїСЂРёРіР»Р°С€РµРЅРёСЏ РґСЂСѓР·РµР№?\n'
        'Р”Р°! Р’С‹ РјРѕР¶РµС‚Рµ РїСЂРѕСЃС‚Рѕ РїРѕРєСѓРїР°С‚СЊ Р·РІРµР·РґС‹ Рё РїРѕР»СЊР·РѕРІР°С‚СЊСЃСЏ РІСЃРµРјРё С„СѓРЅРєС†РёСЏРјРё Р±РѕС‚Р° Р±РµР· СѓС‡Р°СЃС‚РёСЏ РІ СЂРµС„РµСЂР°Р»СЊРЅРѕР№ СЃРёСЃС‚РµРјРµ.\n\n'
        'Р•СЃР»Рё Сѓ РІР°СЃ РµСЃС‚СЊ РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ РІРѕРїСЂРѕСЃС‹ вЂ” РЅР°Р¶РјРёС‚Рµ РЅР° РєРЅРѕРїРєСѓ "Р—Р°РґР°С‚СЊ РІРѕРїСЂРѕСЃ".'
    )
    # РЈРґР°Р»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№ (РµСЃР»Рё РЅСѓР¶РЅРѕ)
    await callback.message.delete()
    # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ FAQ
    await callback.message.answer(text=text, reply_markup=help)

def confirm_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="вњ… Р”Р°, СѓРґР°Р»РёС‚СЊ", callback_data="confirm_delete"),
                InlineKeyboardButton(text="вќЊ РћС‚РјРµРЅР°", callback_data="cancel_delete")
            ]
        ]
    )
def done_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="вњ… Р“РѕС‚РѕРІРѕ", callback_data="delete_msg")]
        ]
    )


async def safe_callback_answer(callback: types.CallbackQuery, text: str, show_alert: bool = False):
    try:
        await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest:
        pass
    except Exception:
        pass

@dp.callback_query(lambda c: c.data == "delete_msg")
async def ask_delete_confirmation(callback: types.CallbackQuery):
    try:
        await callback.message.edit_reply_markup(reply_markup=confirm_keyboard())
    except Exception:
        pass
    await safe_callback_answer(callback, "Р’С‹ СѓРІРµСЂРµРЅС‹, С‡С‚Рѕ С…РѕС‚РёС‚Рµ СѓРґР°Р»РёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ?")


@dp.callback_query(lambda c: c.data == "confirm_delete")
async def confirm_delete_callback(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
        await safe_callback_answer(callback, "РЎРѕРѕР±С‰РµРЅРёРµ СѓРґР°Р»РµРЅРѕ вњ… ", show_alert=True)
    except Exception:
        await safe_callback_answer(callback, "РћС€РёР±РєР° СѓРґР°Р»РµРЅРёСЏ", show_alert=True)


@dp.callback_query(lambda c: c.data == "cancel_delete")
async def cancel_delete_callback(callback: types.CallbackQuery):
    try:
        await callback.message.edit_reply_markup(reply_markup=done_keyboard())
        await safe_callback_answer(callback, "РЈРґР°Р»РµРЅРёРµ РѕС‚РјРµРЅРµРЅРѕ вќЊ", show_alert=True)
    except Exception:
        await safe_callback_answer(callback, "РћС€РёР±РєР°", show_alert=True)


async def run_bot_and_miniapp() -> None:
    init_db()

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


