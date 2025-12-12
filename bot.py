#!/usr/bin/env python3
import asyncio
import hashlib
import logging
import re
import sqlite3
import time
import requests
import os
import json

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramRetryAfter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Переменные окружения ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
NOTIFY_DELAY = int(os.getenv("NOTIFY_DELAY", "2"))

DB_ORDERS = "orders.db"
DB_SUBS = "subs.db"

MAX_COLS = 25
MAX_MESSAGE_LENGTH = 4000

# --- Флаг тихого старта ---
FIRST_RUN = True  # на первом проходе заполняем orders, но НЕ кладем в pending

# --- Инициализация баз ---
def init_db_orders():
    conn = sqlite3.connect(DB_ORDERS)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            row_index INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            line TEXT NOT NULL,
            updated_at REAL DEFAULT (strftime('%s','now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pending (
            row_index INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            line TEXT NOT NULL,
            ts REAL NOT NULL,
            is_new BOOLEAN DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()

def init_db_subs():
    conn = sqlite3.connect(DB_SUBS)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

# --- Работа с Google Sheets ---
def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SHEET_ID)
    ws = doc.get_worksheet(0)
    return ws

# --- Вспомогательные функции ---
def make_line(row):
    # нормализация строки (без пустых ячеек, чистые пробелы)
    parts = []
    for x in row[:MAX_COLS]:
        s = (x or "").strip()
        if s:
            parts.append(s)
    return " | ".join(parts)

def make_hash(line):
    return hashlib.sha256(line.encode()).hexdigest()

def is_url(text):
    return re.match(r"^https?://", text or "")

def shorten_clck(long_url):
    try:
        r = requests.get("https://clck.ru/--", params={"url": long_url}, timeout=7)
        if r.status_code == 200:
            return r.text.strip()
        return f"Ошибка: {r.status_code}"
    except Exception as e:
        return f"Ошибка: {e}"

def add_subscriber(chat_id):
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO subscribers(chat_id) VALUES(?)", (chat_id,))
    conn.commit()
    conn.close()

def remove_subscriber(chat_id):
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def get_subscribers():
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("SELECT chat_id FROM subscribers")
    subs = [row[0] for row in c.fetchall()]
    conn.close()
    return subs

async def send_safe(bot: Bot, chat_id: int, text: str):
    try:
        if len(text) > MAX_MESSAGE_LENGTH:
            text = text[:MAX_MESSAGE_LENGTH]
        await bot.send_message(chat_id, text)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await send_safe(bot, chat_id, text)
    except Exception as e:
        logger.error(f"send_safe error: {e}")

# --- Основной цикл опроса + тихий старт ---
async def poll_loop(bot: Bot):
    global FIRST_RUN
    while True:
        try:
            ws = get_sheet()
            rows = ws.get_all_values()
            conn = sqlite3.connect(DB_ORDERS)
            c = conn.cursor()

            for idx, row in enumerate(rows, start=1):
                # пропускаем пустые строки
                if not any(row):
                    continue
                line = make_line(row)
                if not line:
                    continue
                h = make_hash(line)

                c.execute("SELECT hash FROM orders WHERE row_index=?", (idx,))
                res = c.fetchone()

                if res is None:
                    # новая строка: всегда записываем в orders
                    c.execute("INSERT INTO orders(row_index, hash, line) VALUES(?,?,?)", (idx, h, line))
                    # тихий старт: не кладем в pending на первом проходе
                    if not FIRST_RUN:
                        c.execute(
                            "INSERT OR REPLACE INTO pending(row_index, hash, line, ts, is_new) VALUES(?,?,?,?,1)",
                            (idx, h, line, time.time())
                        )
                else:
                    # строка существовала; если изменился хеш — считаем обновлением
                    if res[0] != h:
                        c.execute(
                            "UPDATE orders SET hash=?, line=?, updated_at=strftime('%s','now') WHERE row_index=?",
                            (h, line, idx)
                        )
                        c.execute(
                            "INSERT OR REPLACE INTO pending(row_index, hash, line, ts, is_new) VALUES(?,?,?,?,0)",
                            (idx, h, line, time.time())
                        )

            conn.commit()
            conn.close()

            # рассылка готовых уведомлений
            await notify_subscribers(bot)

        except Exception as e:
            logger.error(f"poll_loop error: {e}")

        # после первого цикла снимаем флаг тихого старта
        if FIRST_RUN:
            FIRST_RUN = False

        await asyncio.sleep(POLL_INTERVAL)

async def notify_subscribers(bot: Bot):
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("SELECT row_index, line, is_new FROM pending WHERE ts <= ?", (time.time() - NOTIFY_DELAY,))
    rows = c.fetchall()
    for row_index, line, is_new in rows:
        msg = ("🆕 Новый заказ:\n" + line) if is_new else ("♻ Обновлён заказ:\n" + line)
        subs = get_subscribers()
        for chat_id in subs:
            await send_safe(bot, chat_id, msg)
        c.execute("DELETE FROM pending WHERE row_index=?", (row_index,))
    conn.commit()
    conn.close()

# --- Основной запуск ---
async def main():
    init_db_orders()
    init_db_subs()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    sub_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Подписаться на рассылку")],
            [KeyboardButton(text="Отписаться от рассылки")]
        ],
        resize_keyboard=True
    )

    @dp.message(Command("start"))
    async def cmd_start(msg: types.Message):
        await msg.answer("Привет! Я умею сокращать ссылки и рассылать заказы.", reply_markup=sub_kb)

    @dp.message()
    async def sub_buttons(msg: types.Message):
        text = (msg.text or "").strip()
        if not text:
            return

        if text == "Подписаться на рассылку":
            add_subscriber(msg.from_user.id)
            await msg.answer("✅ Вы подписаны!", reply_markup=sub_kb)
            return

        if text == "Отписаться от рассылки":
            remove_subscriber(msg.from_user.id)
            await msg.answer("❌ Вы отписались.", reply_markup=sub_kb)
            return

        if is_url(text):
            short = shorten_clck(text)
            if short.startswith("http"):
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Открыть короткую ссылку", url=short)]]
                )
                await msg.answer(f"🔗 Короткая ссылка: {short}", reply_markup=kb)
            else:
                await msg.answer(f"⚠ Не удалось сократить ссылку.\nОтвет: {short}")
            return

    asyncio.create_task(poll_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
