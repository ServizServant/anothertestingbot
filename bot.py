#!/usr/bin/env python3
import asyncio
import hashlib
import logging
import re
import sqlite3
import time
import requests
import os

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramRetryAfter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Читаем переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
NOTIFY_DELAY = int(os.getenv("NOTIFY_DELAY", "2"))

DB_ORDERS = "orders.db"
DB_SUBS = "subs.db"

MAX_COLS = 25
MAX_MESSAGE_LENGTH = 4000

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

# --- остальные функции работы с БД и Google Sheets остаются такими же, как в локальной версии ---
# Важно: для авторизации в Google Sheets используем JSON из переменной окружения

def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    # JSON ключ хранится в переменной окружения
    import json
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SHEET_ID)
    ws = doc.get_worksheet(0)
    return ws

# --- poll_loop, send_safe, make_hash, make_line и т.д. остаются без изменений ---

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
