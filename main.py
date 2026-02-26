from logging import exception
import re
import asyncio
import sqlite3
import os
from aiogram import Bot, types, Dispatcher, Router
from aiogram import F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
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
ADMIN_IDS = [7545158182, 382257126, 6342564240] #я, саппорт, никита
GROUP_CHAT_ID = -1002812420141
GROUP_CHAT_ID2 = -1003001456496
CHANNEL_ID = "@starslixx"

if not API_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()


PALLY_API_TOKEN = os.getenv("PALLY_API_TOKEN")      # получите в личном кабинете pally
PALLY_SHOP_ID = os.getenv("PALLY_SHOP_ID")          # shop_id магазина в pally
PALLY_API_BASE = "https://pal24.pro/api/v1"          # используйте тот же домен, что в вашем аккаунте / в доке
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

    # Таблица использованных промокодов
    cursor.execute('''
           CREATE TABLE IF NOT EXISTS used_promo (
               user_id INTEGER,
               code TEXT,
               PRIMARY KEY (user_id, code)
           )
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

    # Инициализация переменных
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

    # Ищем только активных рефералов (referrals_with_purchase=1)
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
        cursor.execute("UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code=?", (promo_code_used,))
        cursor.execute("INSERT OR IGNORE INTO used_promo (user_id, code) VALUES (?, ?)", (user_id, promo_code_used))
        conn.commit()

    # Сохраняем покупку
    buyer_username = callback.from_user.username or callback.from_user.first_name
    cursor.execute(
        "INSERT INTO purchases (user_id, username, item_type, amount, cost) VALUES (?, ?, ?, ?, ?)",
        (user_id, target_username, "premium", months, final_price)
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



@dp.message(Command("ad"))
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
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

    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

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
    conn = sqlite3.connect('database.db')
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
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Запрос для получения только оплаченных платежей
    cursor.execute(
        'SELECT bill_id, amount, status, description, date FROM payments WHERE user_id=? AND status="paid" ORDER BY date DESC',
        (user_id,)
    )
    rows = cursor.fetchall()

    conn.close()





async def get_pally_bill_info():
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


from datetime import datetime
from dateutil import parser
from datetime import timedelta

def format_date(date_str):
    try:
        # Используем parser.parse для преобразования строки в объект datetime
        original_date = parser.parse(date_str)

        # Получаем часовой пояс Самары (UTC+4)
        samara_tz = pytz.timezone('Europe/Samara')

        # Преобразуем время в часовой пояс Самары
        samara_time = original_date.astimezone(samara_tz)

        # Добавляем 3 часа
        samara_time_plus_3 = samara_time + timedelta(hours=3)

        # Возвращаем отформатированную дату в нужном формате
        return samara_time_plus_3.strftime("%Y-%m-%d  %H:%M:%S")  # Пример: 2025-10-31 20:47:20
    except ValueError as e:
        print(f"Ошибка при форматировании даты: {e}")
        return date_str  # Возвращаем исходную строку, если ошибка



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
                f"⭐ Звёзды: {total_amount}шт — {total_cost:.2f}₽ (себестоимость {cost_price:.2f}₽, курс {star_cost}₽)"
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
                f"💙 Premium: {total_amount} мес — {total_cost:.2f}₽ (себестоимость {cost_price:.2f}₽)"
            )

    total_revenue *= 0.97
    profit = total_revenue - total_cost_price

    # Топ 5 покупателей
    cursor.execute("""
        SELECT username, SUM(cost) as total_spent
        FROM purchases
        WHERE date(created_at) = date('now', 'localtime')
        GROUP BY user_id
        ORDER BY total_spent DESC
        LIMIT 5
    """)
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
    # Проверяем, есть ли продажи за последние 24 часа
    cursor.execute("""
        SELECT COUNT(*) FROM purchases
        WHERE created_at >= datetime('now', '-1 day')
    """)
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
        WHERE created_at >= datetime('now', '-1 day')
    """)
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
    today = datetime.date.today()
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
@dp.message(lambda m: m.text.isdigit() and m.from_user.id in pending_action)
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


async def apply_promo_code(user_id: int, promo_code: str, target_type: str, stars_amount: int = 0) -> dict:
    """
    Универсальная функция применения промокода.
    target_type: "stars" или "premium"
    stars_amount: количество звёзд (только для покупок stars)
    Возвращает dict:
        ok: bool
        discount: int
        error: str
    """
    promo_code = promo_code.upper()

    cursor.execute(
        """
        SELECT discount_percent, min_stars, expires_at, max_uses, uses_count,
               condition, max_free_stars, target
        FROM promo_codes WHERE code=?
        """,
        (promo_code,)
    )
    row = cursor.fetchone()

    if not row:
        return {"ok": False, "discount": 0, "error": "❌ Неверный промо-код."}

    discount, min_stars, expires_at, max_uses, uses_count, condition, max_free_stars, target = row

    # Проверка типа товара
    if target not in (target_type, "all"):
        return {"ok": False, "discount": 0, "error": f"❌ Этот промо-код нельзя применить для {target_type}."}

    # Проверка, использовал ли уже пользователь
    cursor.execute("SELECT 1 FROM used_promo WHERE user_id=? AND code=?", (user_id, promo_code))
    if cursor.fetchone():
        return {"ok": False, "discount": 0, "error": "❌ Вы уже использовали этот промо-код."}

    # Проверка даты истечения
    if expires_at:
        try:
            exp_date = datetime.datetime.strptime(expires_at, "%Y-%m-%d").date()
            if datetime.date.today() > exp_date:
                return {"ok": False, "discount": 0, "error": "❌ Срок действия промо-кода истёк."}
        except Exception:
            return {"ok": False, "discount": 0, "error": "❌ Ошибка формата даты в промо-коде."}

    # Проверка лимита использования
    if max_uses and uses_count >= max_uses:
        return {"ok": False, "discount": 0, "error": "❌ Промо-код больше недоступен."}

    # Проверка минимального количества звёзд
    if target_type == "stars" and stars_amount < min_stars:
        return {
            "ok": False,
            "discount": 0,
            "error": f"❌ Для использования этого промо-кода необходимо выбрать минимум {min_stars}⭐️."
        }

    # Проверка max_free_stars для 100% скидки
    if discount == 100 and max_free_stars and stars_amount != max_free_stars:
        return {
            "ok": False,
            "discount": 0,
            "error": f"❌ Этот промо-код работает только на {max_free_stars}⭐️."
        }

    # ✅ Проверка на покупку за последние 7 дней — только для 100% промо
    if discount == 100:
        seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
        cursor.execute(
            "SELECT COUNT(*) FROM purchases WHERE user_id=? AND datetime(created_at) >= datetime(?)",
            (user_id, seven_days_ago)
        )
        recent_purchases = cursor.fetchone()[0]

        if recent_purchases == 0:
            return {
                "ok": False,
                "discount": 0,
                "error": "❌ Бесплатные промо-коды доступны только пользователям, совершавшим покупку за последние 7 дней."
            }

    return {"ok": True, "discount": discount, "error": ""}




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


def build_miniapp_config_payload() -> dict:
    star_amounts = [100, 500, 1000]
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
        "stars": stars,
        "premium": premium,
    }


async def miniapp_config_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)
    return miniapp_json_response(build_miniapp_config_payload())


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

    if order_type == "stars":
        try:
            amount = int(payload.get("amount"))
        except (TypeError, ValueError):
            return miniapp_json_response(
                {"ok": False, "error": "Количество звёзд должно быть числом"},
                status=400,
            )

        if amount < 50 or amount > 10000:
            return miniapp_json_response(
                {"ok": False, "error": "Количество звёзд должно быть от 50 до 10000"},
                status=400,
            )

        base_rub = round(amount * get_star_rate_for_range(amount), 2)
        amount_label = f"{amount}⭐"
        promo_target = "stars"
    else:
        months = parse_months_value(str(payload.get("amount") or ""))
        if months not in PREMIUM_PRICES_RUB:
            return miniapp_json_response(
                {"ok": False, "error": "Выберите пакет Premium"},
                status=400,
            )
        amount = months
        base_rub = float(PREMIUM_PRICES_RUB[months])
        amount_label = f"{months} мес. Premium"
        promo_target = "premium"

    discount_percent = 0
    final_rub = base_rub
    promo_error = ""

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
        elif discount_percent > 0:
            message_lines.append(
                f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (скидка {discount_percent}%)"
            )
        else:
            message_lines.append(f"🎟 <b>Промокод:</b> <code>{escape(promo_code)}</code> (0%)")

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

    return miniapp_json_response(
        {
            "ok": True,
            "message": "Заявка отправлена в группу",
            "price": {
                "rub": round(final_rub, 2),
                "usd": round(final_rub / MINIAPP_USD_RUB_RATE, 2),
            },
            "promo": {
                "code": promo_code,
                "discount": discount_percent,
                "error": promo_error,
            },
        }
    )


def create_miniapp_api() -> web.Application:
    app = web.Application()
    app.router.add_route("OPTIONS", "/api/miniapp/{tail:.*}", miniapp_config_handler)
    app.router.add_get("/api/miniapp/config", miniapp_config_handler)
    app.router.add_post("/api/miniapp/order", miniapp_order_handler)
    return app

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
                cursor.execute("INSERT OR IGNORE INTO used_promo (user_id, code) VALUES (?, ?)", (user_id, promo))
                cursor.execute("UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code=?", (promo,))
                conn.commit()


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
    conn = sqlite3.connect('database.db')  # <-- замена get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code = ?",
            (promo_code,)
        )
        conn.commit()

    except Exception as e:
        print(f"[DEBUG] Ошибка increment_promo_usage: {e}")
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





import aiohttp

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
        except Exception as e:
            print(f"[ERROR] Ошибка при фиксации промокода {promo_code}: {e}")
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
    """
    Фиксирует использование промо-кода:
    - добавляет запись в used_promo
    - увеличивает uses_count в promo_codes
    Возвращает True, если успешно, иначе False.
    """
    try:
        promo_code = promo_code.upper()

        # Добавляем запись о применении промо
        cursor.execute(
            "INSERT INTO used_promo (user_id, code, used_at) VALUES (?, ?, datetime('now'))",
            (user_id, promo_code)
        )

        # Увеличиваем uses_count в promo_codes
        cursor.execute(
            "UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code = ?",
            (promo_code,)
        )

        conn.commit()
        print(f"[INFO] Промо {promo_code} успешно зафиксирован для user_id={user_id}")
        return True

    except Exception as e:
        print(f"[ERROR] Ошибка при фиксации промокода {promo_code}: {e}")
        conn.rollback()
        return False




def save_payment(user_id, bill_id, amount, status, description):
    conn = sqlite3.connect('database.db')
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

def confirm_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data="confirm_delete"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_delete")
            ]
        ]
    )
def done_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Готово", callback_data="delete_msg")]
        ]
    )

@dp.callback_query(lambda c: c.data == "delete_msg")
async def ask_delete_confirmation(callback: types.CallbackQuery):
    try:
        await callback.message.edit_reply_markup(reply_markup=confirm_keyboard())
        await callback.answer("Вы уверены, что хотите удалить сообщение?")
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@dp.callback_query(lambda c: c.data == "confirm_delete")
async def confirm_delete_callback(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
        await callback.answer("Сообщение удалено ✅ ", show_alert=True)
    except Exception as e:
        await callback.answer(f"Ошибка удаления: {e}", show_alert=True)


@dp.callback_query(lambda c: c.data == "cancel_delete")
async def cancel_delete_callback(callback: types.CallbackQuery):
    try:
        await callback.message.edit_reply_markup(reply_markup=done_keyboard())
        await callback.answer("Удаление отменено ❌", show_alert=True)
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


async def run_bot_and_miniapp() -> None:
    init_db()

    miniapp_runner = web.AppRunner(create_miniapp_api())
    await miniapp_runner.setup()
    miniapp_site = web.TCPSite(miniapp_runner, MINIAPP_API_HOST, MINIAPP_API_PORT)
    await miniapp_site.start()

    print(f"[MINIAPP API] http://{MINIAPP_API_HOST}:{MINIAPP_API_PORT}")

    try:
        await dp.start_polling(bot)
    finally:
        await miniapp_runner.cleanup()


if __name__ == '__main__':
    asyncio.run(run_bot_and_miniapp())
