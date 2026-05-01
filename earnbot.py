import asyncio
import json
import logging
import os
import random
import sqlite3
import string
import time
from contextlib import closing
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# =========================================================
#   CONFIGURATION
# =========================================================
BOT_TOKEN = "8688447065:AAE5E2l4qbgm0jwLW9bIxS3olFIfzvvuDBc"
ADMIN_ID = 8502686983

# Database path
DB_PATH = "earnbot.db"

# IMPORTANT: Bot must be ADMIN in these channels to check members
REQUIRED_CHANNELS = [
    {"chat_id": "@NeroxaOfficial", "title": "Neroxa Official", "url": "https://t.me/NeroxaOfficial"},
    {"chat_id": "@NeroxaUpdate", "title": "Neroxa Update", "url": "https://t.me/NeroxaUpdate"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("earnbot")

# =========================================================
#   DATABASE
# =========================================================
def db():
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def db_init():
    with closing(db()) as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id      INTEGER PRIMARY KEY,
            username     TEXT,
            first_name   TEXT,
            lang         TEXT DEFAULT 'en',
            coins        INTEGER DEFAULT 0,
            stars        INTEGER DEFAULT 0,
            xp           INTEGER DEFAULT 0,
            level        INTEGER DEFAULT 1,
            energy       INTEGER DEFAULT 100,
            energy_ts    INTEGER DEFAULT 0,
            join_date    TEXT,
            last_active  TEXT,
            referrer_id  INTEGER,
            ref_count    INTEGER DEFAULT 0,
            vip          INTEGER DEFAULT 0,
            vip_until    TEXT,
            banned       INTEGER DEFAULT 0,
            streak       INTEGER DEFAULT 0,
            last_daily   TEXT,
            tasks_done   INTEGER DEFAULT 0,
            verified     INTEGER DEFAULT 0,
            total_earned_coins INTEGER DEFAULT 0,
            total_earned_stars INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS tasks(
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            kind         TEXT NOT NULL,
            title        TEXT NOT NULL,
            url          TEXT,
            chat_id      TEXT,
            coin_reward  INTEGER DEFAULT 0,
            star_reward  INTEGER DEFAULT 0,
            xp_reward    INTEGER DEFAULT 0,
            energy_cost  INTEGER DEFAULT 1,
            cooldown_s   INTEGER DEFAULT 0,
            daily_limit  INTEGER DEFAULT 1,
            min_level    INTEGER DEFAULT 1,
            active       INTEGER DEFAULT 1,
            clicks       INTEGER DEFAULT 0,
            vip_only     INTEGER DEFAULT 0,
            description  TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS completed_tasks(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            task_id   INTEGER,
            ts        INTEGER,
            day       TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_ct_user ON completed_tasks(user_id);
        CREATE INDEX IF NOT EXISTS ix_ct_day  ON completed_tasks(day);

        CREATE TABLE IF NOT EXISTS referrals(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ref_id    INTEGER,
            user_id   INTEGER UNIQUE,
            ts        INTEGER
        );

        CREATE TABLE IF NOT EXISTS transactions(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            amount    INTEGER,
            currency  TEXT,
            note      TEXT,
            ts        INTEGER
        );

        CREATE TABLE IF NOT EXISTS withdrawals(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            method    TEXT,
            number    TEXT,
            amount    INTEGER,
            coins     INTEGER,
            status    TEXT DEFAULT 'pending',
            ts        INTEGER,
            admin_note TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS shop_items(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT,
            kind      TEXT,
            price     INTEGER,
            payload   TEXT,
            active    INTEGER DEFAULT 1,
            description TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS settings(
            key   TEXT PRIMARY KEY,
            value TEXT,
            description TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS user_joined_channels(
            user_id INTEGER,
            channel_id TEXT,
            joined_at TEXT,
            PRIMARY KEY(user_id, channel_id)
        );
        """)

        # Default settings
        defaults = {
            "max_energy": ("100", "Maximum energy for regular users"),
            "energy_regen_min": ("5", "Minutes to regenerate 1 energy"),
            "vip_max_energy": ("200", "Maximum energy for VIP users"),
            "daily_coin": ("50", "Daily bonus coins"),
            "daily_star": ("1", "Daily bonus stars"),
            "streak_bonus": ("10", "Streak bonus per day"),
            "mystery_min": ("10", "Minimum mystery box reward"),
            "mystery_max": ("200", "Maximum mystery box reward"),
            "min_withdraw": ("1000", "Minimum withdrawal amount in coins"),
            "coin_per_taka": ("100", "Coins per 1 taka"),
            "withdraw_fee_pct": ("5", "Withdrawal fee percentage"),
            "daily_withdraw_limit": ("1", "Daily withdrawal limit per user"),
            "ref_coin": ("100", "Referral coin reward"),
            "ref_star": ("1", "Referral star reward"),
            "ref_xp": ("20", "Referral XP reward"),
            "xp_per_level": ("200", "XP required per level"),
            "spin_cost": ("5", "Spin cost in stars"),
            "vip_price_stars": ("100", "VIP price in stars"),
            "vip_days": ("30", "VIP duration in days"),
            "vip_daily_bonus": ("100", "VIP daily bonus"),
        }
        for k, (v, desc) in defaults.items():
            con.execute("INSERT OR IGNORE INTO settings(key,value,description) VALUES(?,?,?)", (k, v, desc))

        # Seed shop
        if con.execute("SELECT COUNT(*) FROM shop_items").fetchone()[0] == 0:
            con.executemany(
                "INSERT INTO shop_items(name,kind,price,payload,description) VALUES(?,?,?,?,?)",
                [
                    ("⚡ Energy Refill (100)", "energy", 5, "100", "Restores 100 energy instantly"),
                    ("🎡 Spin Ticket x5", "spin", 3, "5", "Get 5 lucky spin tickets"),
                    ("👑 VIP 30 Days", "vip", 100, "30", "Get VIP access for 30 days"),
                    ("🚀 VIP 7 Days", "vip", 50, "7", "Get VIP access for 7 days"),
                ],
            )
        
        # Add sample tasks
        con.execute("""
            INSERT OR IGNORE INTO tasks(id, kind, title, url, chat_id, coin_reward, star_reward, xp_reward, energy_cost, cooldown_s, daily_limit, min_level, active, vip_only, description)
            VALUES 
            (1, 'channel', 'Join VIP Channel', 'https://t.me/NeroxaOfficial', '@NeroxaOfficial', 500, 5, 100, 5, 0, 3, 1, 1, 1, 'Join our official channel for updates'),
            (2, 'shortlink', 'Visit Premium Link', 'https://example.com', NULL, 300, 3, 50, 3, 300, 5, 1, 1, 1, 'Visit our premium partner site'),
            (3, 'sponsor', 'Watch Sponsor', 'https://example.com', NULL, 200, 2, 30, 2, 600, 3, 1, 1, 1, 'Watch sponsored content')
        """)

def s_get(key: str, default: str = "") -> str:
    with closing(db()) as con:
        row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

def s_geti(key: str, default: int = 0) -> int:
    try: 
        return int(s_get(key, str(default)))
    except: 
        return default

def s_set(key: str, value):
    with closing(db()) as con:
        con.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

# =========================================================
#   I18N
# =========================================================
DIV  = "━━━━━━━━━━━━━━━━━━"
SDIV = "─── ◆ ───"

T = {
    "en": {
        "welcome":     f"╔══════════════════╗\n   ✨ <b>PREMIUM EARN BOT</b> ✨\n╚══════════════════╝\n\n🔐 <b>Verification Required</b>\n💎 Join all channels below to unlock!\n\n{SDIV}",
        "joined":      "✅ <b>Verified Successfully!</b>\n💎 Welcome to the Premium Club.",
        "not_joined":  "❌ <b>Access Denied</b>\nPlease join all required channels first.\n\n⚠️ Make sure you have joined:\n• @NeroxaOfficial\n• @NeroxaUpdate\n\nThen click the button below.",
        "select_lang": f"╔══════════════════╗\n   🌍 <b>SELECT LANGUAGE</b>   \n╚══════════════════╝\n\n🌐 Please choose your language:\n🇧🇩 আপনার ভাষা নির্বাচন করুন:\n{SDIV}",
        "lang_set":    "✅ Language saved!",
        "menu":        f"╔══════════════════╗\n   🏠 <b>MAIN MENU</b>   \n╚══════════════════╝\n\n💎 Hello <b>{{name}}</b>!\n💰 Coins: <b>{{coins}}</b>   ⭐ Stars: <b>{{stars}}</b>\n⚡ Energy: <b>{{energy}}</b>   🏆 Lv.<b>{{lvl}}</b>\n👑 VIP: <b>{{vip_status}}</b>\n\n{SDIV}\n✨ Choose an option below:",
        "profile":     f"╔══════════════════╗\n   👤 <b>MY PROFILE</b>   \n╚══════════════════╝\n\n🆔 <b>ID:</b> <code>{{id}}</code>\n👤 <b>Name:</b> {{name}}\n\n{SDIV}\n💰 <b>Coins:</b>     {{coins}}\n⭐ <b>Stars:</b>     {{stars}}\n⚡ <b>Energy:</b>    {{energy}}/{{maxe}}\n🏆 <b>Level:</b>     {{lvl}}\n📊 <b>XP:</b>        {{xp}} / {{nxt}}\n🔥 <b>Streak:</b>    {{streak}} days\n👥 <b>Referrals:</b> {{ref}}\n👑 <b>VIP:</b>       {{vip}}\n{SDIV}",
        "earn":        f"╔══════════════════╗\n   💰 <b>EARNING HUB</b>   \n╚══════════════════╝\n\n✨ Pick your favourite way to earn!\n{SDIV}",
        "vip_only":    "👑 <b>VIP Only</b>\nThis feature is only available for VIP members!\n💎 Buy VIP from Shop →",
        "no_energy":   "⚡ Not enough energy! Recharge soon.",
        "cooldown":    "⏱ This task is on cooldown.",
        "daily_done":  "✅ Daily limit reached for this task.",
        "task_ok":     "🎉 <b>Task Completed!</b>\n💰 +{c}   ⭐ +{s}   🏆 +{x}",
        "back":        "⬅️ Back",
        "banned":      "🚫 You are banned from this bot.",
        "ref_ok":      "🎁 Referral reward credited!",
        "self_ref":    "❌ You cannot refer yourself.",
        "wallet":      f"╔══════════════════╗\n   👛 <b>MY WALLET</b>   \n╚══════════════════╝\n\n💰 <b>Coins:</b>  {{c}}\n⭐ <b>Stars:</b>  {{s}}\n🏆 <b>XP:</b>     {{x}}\n{SDIV}",
    },
    "bn": {
        "welcome":     f"╔══════════════════╗\n   ✨ <b>প্রিমিয়াম আর্ন বট</b> ✨\n╚══════════════════╝\n\n🔐 <b>ভেরিফিকেশন প্রয়োজন</b>\n💎 সব চ্যানেল জয়েন করুন!\n\n{SDIV}",
        "joined":      "✅ <b>সফলভাবে ভেরিফাইড!</b>\n💎 প্রিমিয়াম ক্লাবে স্বাগতম।",
        "not_joined":  "❌ <b>অ্যাক্সেস ডিনাইড</b>\nদয়া করে সব চ্যানেল জয়েন করুন।",
        "select_lang": f"╔══════════════════╗\n   🌍 <b>ভাষা নির্বাচন</b>   \n╚══════════════════╝\n\n🌐 আপনার ভাষা নির্বাচন করুন:\n{SDIV}",
        "lang_set":    "✅ ভাষা সংরক্ষণ করা হয়েছে!",
        "menu":        f"╔══════════════════╗\n   🏠 <b>প্রধান মেনু</b>   \n╚══════════════════╝\n\n💎 হ্যালো <b>{{name}}</b>!\n💰 কয়েন: <b>{{coins}}</b>   ⭐ স্টার: <b>{{stars}}</b>\n⚡ এনার্জি: <b>{{energy}}</b>   🏆 Lv.<b>{{lvl}}</b>\n👑 VIP: <b>{{vip_status}}</b>\n\n{SDIV}\n✨ একটি অপশন বাছুন:",
        "profile":     f"╔══════════════════╗\n   👤 <b>আমার প্রোফাইল</b>   \n╚══════════════════╝\n\n🆔 <b>আইডি:</b> <code>{{id}}</code>\n👤 <b>নাম:</b> {{name}}\n\n{SDIV}\n💰 <b>কয়েন:</b>    {{coins}}\n⭐ <b>স্টার:</b>     {{stars}}\n⚡ <b>এনার্জি:</b>   {{energy}}/{{maxe}}\n🏆 <b>লেভেল:</b>    {{lvl}}\n📊 <b>XP:</b>       {{xp}} / {{nxt}}\n🔥 <b>স্ট্রিক:</b>    {{streak}} দিন\n👥 <b>রেফার:</b>     {{ref}}\n👑 <b>VIP:</b>      {{vip}}\n{SDIV}",
        "earn":        f"╔══════════════════╗\n   💰 <b>আর্নিং হাব</b>   \n╚══════════════════╝\n\n✨ আপনার পছন্দের উপায় বাছুন!\n{SDIV}",
        "vip_only":    "👑 <b>শুধু VIP</b>\nএই ফিচার শুধু VIP সদস্যদের জন্য!\n💎 শপ থেকে VIP কিনুন →",
        "no_energy":   "⚡ পর্যাপ্ত এনার্জি নেই!",
        "cooldown":    "⏱ এই টাস্ক কুলডাউনে আছে।",
        "daily_done":  "✅ আজকের লিমিট শেষ।",
        "task_ok":     "🎉 <b>টাস্ক সম্পন্ন!</b>\n💰 +{c}   ⭐ +{s}   🏆 +{x}",
        "back":        "⬅️ পিছনে",
        "banned":      "🚫 আপনি ব্যান হয়েছেন।",
        "ref_ok":      "🎁 রেফার রিওয়ার্ড পেয়েছেন!",
        "self_ref":    "❌ নিজেকে রেফার করা যাবে না।",
        "wallet":      f"╔══════════════════╗\n   👛 <b>আমার ওয়ালেট</b>   \n╚══════════════════╝\n\n💰 <b>কয়েন:</b> {{c}}\n⭐ <b>স্টার:</b> {{s}}\n🏆 <b>XP:</b>    {{x}}\n{SDIV}",
    },
}

def tr(lang: str, key: str, **kw) -> str:
    s = T.get(lang, T["en"]).get(key) or T["en"].get(key, key)
    return s.format(**kw) if kw else s

# =========================================================
#   USER UTILITIES
# =========================================================
def now_ts() -> int: 
    return int(time.time())

def today() -> str:  
    return datetime.utcnow().strftime("%Y-%m-%d")

def get_user(uid: int) -> Optional[sqlite3.Row]:
    with closing(db()) as con:
        return con.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

def upsert_user(msg_or_cb) -> sqlite3.Row:
    u = msg_or_cb.from_user
    with closing(db()) as con:
        existing = con.execute("SELECT user_id FROM users WHERE user_id=?", (u.id,)).fetchone()
        if not existing:
            con.execute(
                "INSERT INTO users(user_id,username,first_name,join_date,last_active,energy,energy_ts) "
                "VALUES(?,?,?,?,?,?,?)",
                (u.id, u.username or "", u.first_name or "", today(), today(),
                 s_geti("max_energy", 100), now_ts()),
            )
        else:
            con.execute(
                "UPDATE users SET username=?, first_name=?, last_active=? WHERE user_id=?",
                (u.username or "", u.first_name or "", today(), u.id),
            )
    return get_user(u.id)

def add_balance(uid: int, coins=0, stars=0, xp=0, note=""):
    with closing(db()) as con:
        con.execute(
            "UPDATE users SET coins=coins+?, stars=stars+?, xp=xp+?, total_earned_coins=total_earned_coins+?, total_earned_stars=total_earned_stars+? WHERE user_id=?",
            (coins, stars, xp, coins, stars, uid),
        )
        if coins or stars:
            con.execute(
                "INSERT INTO transactions(user_id,amount,currency,note,ts) VALUES(?,?,?,?,?)",
                (uid, coins if coins else stars, "coin" if coins else "star", note, now_ts()),
            )
        u = con.execute("SELECT xp, level FROM users WHERE user_id=?", (uid,)).fetchone()
        per = s_geti("xp_per_level", 200)
        new_level = max(1, (u["xp"] // per) + 1)
        if new_level != u["level"]:
            con.execute("UPDATE users SET level=? WHERE user_id=?", (new_level, uid))

def regen_energy(uid: int):
    u = get_user(uid)
    if not u: 
        return
    max_e = s_geti("vip_max_energy", 200) if u["vip"] else s_geti("max_energy", 100)
    if u["energy"] >= max_e: 
        return
    mins = max(1, s_geti("energy_regen_min", 5))
    current_ts = u["energy_ts"] or now_ts()
    elapsed = (now_ts() - current_ts) // (mins * 60)
    if elapsed > 0:
        new_e = min(max_e, u["energy"] + int(elapsed))
        with closing(db()) as con:
            con.execute("UPDATE users SET energy=?, energy_ts=? WHERE user_id=?",
                        (new_e, now_ts(), uid))

def consume_energy(uid: int, amount: int) -> bool:
    regen_energy(uid)
    u = get_user(uid)
    if not u or u["energy"] < amount: 
        return False
    with closing(db()) as con:
        con.execute("UPDATE users SET energy=energy-? WHERE user_id=?", (amount, uid))
    return True

def is_admin(uid: int) -> bool: 
    return uid == ADMIN_ID

def is_vip(uid: int) -> bool:
    u = get_user(uid)
    if not u:
        return False
    if u["vip"] == 1 and u["vip_until"]:
        if u["vip_until"] >= today():
            return True
        else:
            with closing(db()) as con:
                con.execute("UPDATE users SET vip=0, vip_until=NULL WHERE user_id=?", (uid,))
            return False
    return u["vip"] == 1

# =========================================================
#   IMPROVED FORCE-JOIN VERIFICATION
# =========================================================
async def check_joined(bot: Bot, uid: int) -> tuple[bool, list]:
    """Check if user has joined all required channels"""
    missing = []
    
    for channel in REQUIRED_CHANNELS:
        try:
            # Try to get chat member
            member = await bot.get_chat_member(
                chat_id=channel["chat_id"], 
                user_id=uid
            )
            
            # Check member status
            if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
                # User has joined
                continue
            else:
                # User hasn't joined or is restricted
                missing.append(channel)
                
        except TelegramBadRequest as e:
            # Bot might not be admin or channel doesn't exist
            log.error(f"Cannot check channel {channel['chat_id']}: {e}")
            # If we can't verify, consider it as missing
            missing.append(channel)
            
        except Exception as e:
            log.error(f"Unexpected error checking {channel['chat_id']}: {e}")
            missing.append(channel)
    
    return len(missing) == 0, missing

async def force_join_middleware(bot: Bot, uid: int, lang: str) -> tuple[bool, Optional[InlineKeyboardMarkup]]:
    """Middleware to check force join before allowing access"""
    if is_admin(uid):
        return True, None
    
    ok, missing = await check_joined(bot, uid)
    
    if not ok:
        keyboard = join_kb(missing, lang)
        return False, keyboard
    
    return True, None

def join_kb(missing, lang="en") -> InlineKeyboardMarkup:
    """Create keyboard with join buttons"""
    rows = []
    
    for channel in missing:
        rows.append([
            InlineKeyboardButton(
                text=f"📢 {tr(lang, 'join_channel', title=channel['title'])}",
                url=channel["url"]
            )
        ])
    
    rows.append([
        InlineKeyboardButton(
            text="✅ " + tr(lang, "check_joined"),
            callback_data="check_join"
        )
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================
#   KEYBOARDS
# =========================================================
def main_menu_kb(lang="en", admin=False, vip=False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 ᴘʀᴏғɪʟᴇ",  callback_data="profile"),
         InlineKeyboardButton(text="👛 ᴡᴀʟʟᴇᴛ",   callback_data="wallet")],
        [InlineKeyboardButton(text="💰 ᴇᴀʀɴ ᴄᴏɪɴ", callback_data="earn"),
         InlineKeyboardButton(text="🎮 ɢᴀᴍᴇ ᴢᴏɴᴇ", callback_data="games")],
        [InlineKeyboardButton(text="🎁 ᴅᴀɪʟʏ ʙᴏɴᴜs", callback_data="daily"),
         InlineKeyboardButton(text="🛍 sʜᴏᴘ",        callback_data="shop")],
        [InlineKeyboardButton(text="👥 ʀᴇғᴇʀʀᴀʟ",  callback_data="ref"),
         InlineKeyboardButton(text="💸 ᴡɪᴛʜᴅʀᴀᴡ",   callback_data="withdraw")],
        [InlineKeyboardButton(text="🏆 ʟᴇᴀᴅᴇʀʙᴏᴀʀᴅ", callback_data="lb"),
         InlineKeyboardButton(text="👑 ᴠɪᴘ ᴄʟᴜʙ",     callback_data="vip")],
        [InlineKeyboardButton(text="🌍 ʟᴀɴɢᴜᴀɢᴇ", callback_data="lang"),
         InlineKeyboardButton(text="💬 sᴜᴘᴘᴏʀᴛ",   callback_data="support")],
    ]
    if admin:
        rows.append([InlineKeyboardButton(text="👑 ＡＤＭＩＮ ＰＡＮＥＬ", callback_data="admin")])
    if not vip:
        rows.insert(2, [InlineKeyboardButton(text="👑 ɢᴇᴛ ᴠɪᴘ", callback_data="vip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_kb(cb="menu", label="⬅️ Back") -> InlineKeyboardMarkup:
    if cb == "menu":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=cb),
         InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]
    ])

def menu_text(user, lang="en") -> str:
    vip_status = "✅ Active" if user["vip"] else "❌ No"
    return tr(lang, "menu",
              name=user["first_name"] or user["username"] or "User",
              coins=user["coins"], stars=user["stars"],
              energy=user["energy"], lvl=user["level"],
              vip_status=vip_status)

# =========================================================
#   LANGUAGE SELECTION STATE
# =========================================================
class LangState(StatesGroup):
    waiting = State()

# =========================================================
#   ADD MISSING TRANSLATIONS
# =========================================================
# Add these to the T dictionary
T["en"]["join_channel"] = "Join • {title}"
T["en"]["check_joined"] = "✅ I have joined"
T["en"]["verifying"] = "🔍 Verifying your membership..."

T["bn"]["join_channel"] = "জয়েন • {title}"
T["bn"]["check_joined"] = "✅ আমি জয়েন করেছি"
T["bn"]["verifying"] = "🔍 আপনার সদস্যপদ যাচাই করা হচ্ছে..."

# =========================================================
#   ROUTERS
# =========================================================
router = Router()
admin_router = Router()

# ---------- /start ----------
@router.message(CommandStart())
async def start_handler(msg: Message, state: FSMContext, bot: Bot):
    await state.clear()
    
    user = upsert_user(msg)
    
    # Handle referral
    args = msg.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        ref_id = int(args[1].split("_")[1])
        if ref_id != msg.from_user.id:
            with closing(db()) as con:
                existing = con.execute("SELECT referrer_id FROM users WHERE user_id=?", (msg.from_user.id,)).fetchone()
                if not existing or not existing["referrer_id"]:
                    con.execute("UPDATE users SET referrer_id=? WHERE user_id=?", (ref_id, msg.from_user.id))
                    con.execute("UPDATE users SET ref_count=ref_count+1 WHERE user_id=?", (ref_id,))
                    add_balance(ref_id, coins=s_geti("ref_coin", 100), stars=s_geti("ref_star", 1), xp=s_geti("ref_xp", 20), note="Referral reward")
                    try:
                        await bot.send_message(ref_id, "🎁 You got a referral reward!")
                    except:
                        pass
    
    if not user["lang"] or user["lang"] not in ["en", "bn"]:
        await state.set_state(LangState.waiting)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🇬🇧 English", callback_data="lang_en"),
             InlineKeyboardButton(text="🇧🇩 বাংলা", callback_data="lang_bn")]
        ])
        await msg.answer(tr("en", "select_lang"), reply_markup=kb)
        return
    
    if user["banned"]:
        return await msg.answer(tr(user["lang"], "banned"))
    
    # Check force join
    ok, missing = await check_joined(bot, msg.from_user.id)
    if missing and not is_admin(msg.from_user.id):
        welcome_text = tr(user["lang"], "welcome")
        return await msg.answer(welcome_text, reply_markup=join_kb(missing, user["lang"]))
    
    user = get_user(msg.from_user.id)
    await msg.answer(menu_text(user, user["lang"]),
                     reply_markup=main_menu_kb(user["lang"], is_admin(msg.from_user.id), is_vip(msg.from_user.id)))

@router.callback_query(F.data.startswith("lang_"))
async def set_language(cb: CallbackQuery, state: FSMContext):
    lang = cb.data.split("_")[1]
    with closing(db()) as con:
        con.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, cb.from_user.id))
    await state.clear()
    await cb.answer(tr(lang, "lang_set"), show_alert=True)
    
    # Check force join
    ok, missing = await check_joined(cb.bot, cb.from_user.id)
    if missing and not is_admin(cb.from_user.id):
        return await cb.message.edit_text(tr(lang, "welcome"),
                                          reply_markup=join_kb(missing, lang))
    
    user = get_user(cb.from_user.id)
    await cb.message.edit_text(menu_text(user, lang),
                               reply_markup=main_menu_kb(lang, is_admin(cb.from_user.id), is_vip(cb.from_user.id)))

@router.callback_query(F.data == "check_join")
async def check_join_cb(cb: CallbackQuery, bot: Bot):
    """Handle check join button click"""
    await cb.answer(tr("en", "verifying"), show_alert=False)
    
    user = upsert_user(cb)
    lang = user["lang"] or "en"
    
    ok, missing = await check_joined(bot, cb.from_user.id)
    
    if not ok:
        # Still missing channels
        text = tr(lang, "not_joined")
        return await cb.message.edit_text(text, reply_markup=join_kb(missing, lang))
    
    # All channels joined
    user = get_user(cb.from_user.id)
    
    # Update verified status
    with closing(db()) as con:
        con.execute("UPDATE users SET verified=1 WHERE user_id=?", (cb.from_user.id,))
    
    text = tr(lang, "joined") + "\n\n" + menu_text(user, lang)
    await cb.message.edit_text(
        text,
        reply_markup=main_menu_kb(lang, is_admin(cb.from_user.id), is_vip(cb.from_user.id))
    )

@router.callback_query(F.data == "menu")
async def menu_cb(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    user = upsert_user(cb)
    regen_energy(cb.from_user.id)
    user = get_user(cb.from_user.id)
    
    # Check force join again
    ok, missing = await check_joined(cb.bot, cb.from_user.id)
    if missing and not is_admin(cb.from_user.id):
        return await cb.message.edit_text(
            tr(user["lang"], "not_joined"),
            reply_markup=join_kb(missing, user["lang"])
        )
    
    text = menu_text(user, user["lang"])
    kb = main_menu_kb(user["lang"], is_admin(cb.from_user.id), is_vip(cb.from_user.id))
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb)
    await cb.answer()

# ---------- Profile ----------
@router.callback_query(F.data == "profile")
async def profile_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    regen_energy(cb.from_user.id)
    user = get_user(cb.from_user.id)
    per = s_geti("xp_per_level", 200)
    nxt = user["level"] * per
    max_e = s_geti("vip_max_energy", 200) if user["vip"] else s_geti("max_energy", 100)
    vip_status = "Active 👑" if user["vip"] else "No"
    text = tr(user["lang"], "profile",
              id=user["user_id"], name=user["first_name"] or user["username"] or "-",
              coins=user["coins"], stars=user["stars"], energy=user["energy"], maxe=max_e,
              lvl=user["level"], xp=user["xp"], nxt=nxt,
              streak=user["streak"], ref=user["ref_count"],
              vip=vip_status)
    await cb.message.edit_text(text, reply_markup=back_kb())
    await cb.answer()

# ---------- Wallet ----------
@router.callback_query(F.data == "wallet")
async def wallet_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    with closing(db()) as con:
        txs = con.execute(
            "SELECT * FROM transactions WHERE user_id=? ORDER BY ts DESC LIMIT 6",
            (cb.from_user.id,)).fetchall()
        wd = con.execute(
            "SELECT * FROM withdrawals WHERE user_id=? ORDER BY ts DESC LIMIT 4",
            (cb.from_user.id,)).fetchall()
    txt = tr(user["lang"], "wallet", c=user["coins"], s=user["stars"], x=user["xp"])
    if txs:
        txt += "\n\n📒 <b>Recent</b>\n" + "\n".join(
            f"• {t['note'] or t['currency']}: {'+' if t['amount']>=0 else ''}{t['amount']}"
            for t in txs)
    if wd:
        txt += "\n\n💸 <b>Withdrawals</b>\n" + "\n".join(
            f"• {w['method']} {w['amount']}৳ — {w['status']}" for w in wd)
    await cb.message.edit_text(txt, reply_markup=back_kb())
    await cb.answer()

# ---------- Earn Center (VIP only) ----------
@router.callback_query(F.data == "earn")
async def earn_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    # Check if user is VIP
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    rows = [
        [InlineKeyboardButton(text="📢 ᴄʜᴀɴɴᴇʟ ᴊᴏɪɴ",   callback_data="t_kind_channel"),
         InlineKeyboardButton(text="🔗 sʜᴏʀᴛʟɪɴᴋ",      callback_data="t_kind_shortlink")],
        [InlineKeyboardButton(text="📣 sᴘᴏɴsᴏʀᴇᴅ",     callback_data="t_kind_sponsor"),
         InlineKeyboardButton(text="🎥 ᴡᴀᴛᴄʜ ᴀᴅs",      callback_data="t_kind_ad")],
        [InlineKeyboardButton(text="🏠 Main Menu",       callback_data="menu")],
    ]
    await cb.message.edit_text(tr(user["lang"], "earn"),
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

# ---------- Task list (VIP only) ----------
@router.callback_query(F.data.startswith("t_kind_"))
async def task_list_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    kind = cb.data.split("_", 2)[2]
    with closing(db()) as con:
        tasks = con.execute(
            "SELECT * FROM tasks WHERE kind=? AND active=1 AND min_level<=? AND vip_only=1",
            (kind, user["level"])).fetchall()
    if not tasks:
        return await cb.answer("No active tasks. Check back soon!", show_alert=True)
    rows = []
    for t in tasks:
        rows.append([InlineKeyboardButton(
            text=f"{t['title']} • +{t['coin_reward']}💰 +{t['star_reward']}⭐",
            callback_data=f"task_{t['id']}")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="earn")])
    await cb.message.edit_text(f"🎯 <b>{kind.title()} Tasks (VIP)</b>",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("task_"))
async def task_open(cb: CallbackQuery, bot: Bot):
    user = upsert_user(cb)
    
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    tid = int(cb.data.split("_")[1])
    with closing(db()) as con:
        t = con.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
    if not t: 
        return await cb.answer("Task not found.", show_alert=True)

    with closing(db()) as con:
        done_today = con.execute(
            "SELECT COUNT(*) FROM completed_tasks WHERE user_id=? AND task_id=? AND day=?",
            (cb.from_user.id, tid, today())).fetchone()[0]
        last = con.execute(
            "SELECT MAX(ts) FROM completed_tasks WHERE user_id=? AND task_id=?",
            (cb.from_user.id, tid)).fetchone()[0] or 0
    if done_today >= t["daily_limit"]:
        return await cb.answer(tr(user["lang"], "daily_done"), show_alert=True)
    if t["cooldown_s"] and now_ts() - last < t["cooldown_s"]:
        return await cb.answer(tr(user["lang"], "cooldown"), show_alert=True)

    if not consume_energy(cb.from_user.id, t["energy_cost"]):
        return await cb.answer(tr(user["lang"], "no_energy"), show_alert=True)

    add_balance(cb.from_user.id,
                coins=t["coin_reward"], stars=t["star_reward"], xp=t["xp_reward"],
                note=f"Task #{tid}")
    with closing(db()) as con:
        con.execute(
            "INSERT INTO completed_tasks(user_id,task_id,ts,day) VALUES(?,?,?,?)",
            (cb.from_user.id, tid, now_ts(), today()))
        con.execute("UPDATE users SET tasks_done=tasks_done+1 WHERE user_id=?",
                    (cb.from_user.id,))
        con.execute("UPDATE tasks SET clicks=clicks+1 WHERE id=?", (tid,))
    await cb.answer(tr(user["lang"], "task_ok",
                       c=t["coin_reward"], s=t["star_reward"], x=t["xp_reward"]),
                    show_alert=True)
    await earn_cb(cb)

# ---------- Daily / Streak / Mystery ----------
@router.callback_query(F.data == "daily")
async def daily_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    rows = [
        [InlineKeyboardButton(text="🎁 ᴄʟᴀɪᴍ ᴅᴀɪʟʏ ʙᴏɴᴜs", callback_data="daily_claim")],
        [InlineKeyboardButton(text="📦 ᴍʏsᴛᴇʀʏ ʙᴏx",       callback_data="daily_box"),
         InlineKeyboardButton(text="💎 ᴛʀᴇᴀsᴜʀᴇ ᴄʜᴇsᴛ",    callback_data="daily_chest")],
        [InlineKeyboardButton(text="🏠 Main Menu",          callback_data="menu")],
    ]
    text = (f"╔══════════════════╗\n   🎁 <b>DAILY REWARDS</b>   \n╚══════════════════╝\n\n"
            f"🔥 <b>Current Streak:</b> {user['streak']} days\n"
            f"📅 <b>Last Claim:</b> {user['last_daily'] or 'never'}\n"
            f"🎯 <b>Next Bonus:</b> +{s_geti('daily_coin',50)+s_geti('streak_bonus',10)*user['streak']}💰\n"
            f"{SDIV}\n✨ Keep your streak alive for bigger rewards!")
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "daily_claim")
async def daily_claim(cb: CallbackQuery):
    user = upsert_user(cb)
    if user["last_daily"] == today():
        return await cb.answer("⏳ Already claimed today. Come back tomorrow!", show_alert=True)
    yest = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    streak = user["streak"] + 1 if user["last_daily"] == yest else 1
    base = s_geti("daily_coin", 50)
    bonus = s_geti("streak_bonus", 10) * (streak - 1)
    coins = base + bonus
    if user["vip"]: 
        coins += s_geti("vip_daily_bonus", 100)
    stars = s_geti("daily_star", 1)
    add_balance(cb.from_user.id, coins=coins, stars=stars, xp=10, note="Daily bonus")
    with closing(db()) as con:
        con.execute("UPDATE users SET streak=?, last_daily=? WHERE user_id=?",
                    (streak, today(), cb.from_user.id))
    await cb.answer(f"🎉 +{coins}💰  +{stars}⭐\n🔥 Streak: {streak}", show_alert=True)
    await daily_cb(cb)

@router.callback_query(F.data == "daily_box")
async def daily_box(cb: CallbackQuery):
    upsert_user(cb)
    if not consume_energy(cb.from_user.id, 5):
        return await cb.answer("⚡ Need 5 energy.", show_alert=True)
    win = random.randint(s_geti("mystery_min", 10), s_geti("mystery_max", 200))
    add_balance(cb.from_user.id, coins=win, note="Mystery box")
    await cb.answer(f"🎁 You found {win}💰!", show_alert=True)
    await daily_cb(cb)

@router.callback_query(F.data == "daily_chest")
async def daily_chest(cb: CallbackQuery):
    upsert_user(cb)
    if not consume_energy(cb.from_user.id, 10):
        return await cb.answer("⚡ Need 10 energy.", show_alert=True)
    coin = random.randint(50, 500)
    star = random.choice([0, 0, 0, 1, 2])
    add_balance(cb.from_user.id, coins=coin, stars=star, note="Chest")
    await cb.answer(f"💰 Chest: +{coin}💰 +{star}⭐", show_alert=True)
    await daily_cb(cb)

# ---------- Games ----------
@router.callback_query(F.data == "games")
async def games_cb(cb: CallbackQuery):
    upsert_user(cb)
    vip = is_vip(cb.from_user.id)
    
    if not vip:
        rows = [[InlineKeyboardButton(text="👑 GET VIP TO PLAY", callback_data="vip")]]
    else:
        rows = [
            [InlineKeyboardButton(text="🎡 LUCKY SPIN",     callback_data="g_spin")],
            [InlineKeyboardButton(text="🎲 DICE ROLL",      callback_data="g_dice"),
             InlineKeyboardButton(text="🪙 COIN FLIP",      callback_data="g_flip")],
            [InlineKeyboardButton(text="🎮 TAP GAME",       callback_data="g_tap")],
        ]
    rows.append([InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")])
    
    text = (f"╔══════════════════╗\n   🎮 <b>GAME ZONE</b>   \n╚══════════════════╝\n\n"
            f"✨ {'VIP only' if not vip else 'Pick a game and win big rewards!'}\n{SDIV}")
    await cb.message.edit_text(text,
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "g_spin")
async def g_spin(cb: CallbackQuery):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    cost = s_geti("spin_cost", 5)
    if user["stars"] < cost:
        return await cb.answer(f"Need {cost}⭐ to spin.", show_alert=True)
    with closing(db()) as con:
        con.execute("UPDATE users SET stars=stars-? WHERE user_id=?", (cost, cb.from_user.id))
    prizes = [10, 25, 50, 100, 200, 500, 1000, 0]
    win = random.choice(prizes)
    if win: 
        add_balance(cb.from_user.id, coins=win, note="Spin")
    await cb.answer(f"🎡 Spin result: +{win}💰" if win else "🎡 No win, try again!",
                    show_alert=True)
    await games_cb(cb)

@router.callback_query(F.data == "g_dice")
async def g_dice(cb: CallbackQuery, bot: Bot):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if not consume_energy(cb.from_user.id, 2):
        return await cb.answer("⚡ Need 2 energy.", show_alert=True)
    msg = await bot.send_dice(cb.from_user.id, emoji="🎲")
    await asyncio.sleep(3)
    val = msg.dice.value
    win = val * 10
    add_balance(cb.from_user.id, coins=win, note="Dice")
    await bot.send_message(cb.from_user.id, f"🎲 You rolled {val} → +{win}💰")
    await cb.answer()

@router.callback_query(F.data == "g_flip")
async def g_flip(cb: CallbackQuery):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if not consume_energy(cb.from_user.id, 1):
        return await cb.answer("⚡ Need 1 energy.", show_alert=True)
    win = random.choice([0, 20, 0, 50, 0, 100])
    if win: 
        add_balance(cb.from_user.id, coins=win, note="Coin flip")
    await cb.answer(f"🪙 +{win}💰" if win else "🪙 Tails! No reward.", show_alert=True)

class TapState(StatesGroup): 
    playing = State()

@router.callback_query(F.data == "g_tap")
async def g_tap(cb: CallbackQuery, state: FSMContext):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if not consume_energy(cb.from_user.id, 3):
        return await cb.answer("⚡ Need 3 energy.", show_alert=True)
    await state.set_state(TapState.playing)
    await state.update_data(taps=0, ends=now_ts() + 15)
    rows = [[InlineKeyboardButton(text="👆 TAP! (0)", callback_data="tap_hit")],
            [InlineKeyboardButton(text="✅ Finish",   callback_data="tap_end")]]
    await cb.message.edit_text("🎮 <b>Tap Game</b> — tap as fast as you can in 15s!\n\n💰 Each tap = 2 coins!",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "tap_hit", TapState.playing)
async def tap_hit(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if now_ts() > data.get("ends", 0):
        return await tap_end(cb, state)
    taps = data["taps"] + 1
    await state.update_data(taps=taps)
    rows = [[InlineKeyboardButton(text=f"👆 TAP! ({taps})", callback_data="tap_hit")],
            [InlineKeyboardButton(text="✅ Finish",          callback_data="tap_end")]]
    try:
        await cb.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except TelegramBadRequest: 
        pass
    await cb.answer()

@router.callback_query(F.data == "tap_end", TapState.playing)
async def tap_end(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    taps = data.get("taps", 0)
    win = taps * 2
    add_balance(cb.from_user.id, coins=win, xp=taps, note="Tap game")
    await state.clear()
    await cb.message.edit_text(f"🎮 Game Over!\n\n👆 Total Taps: <b>{taps}</b>\n💰 Reward: +{win} coins!",
                               reply_markup=back_kb("games"))
    await cb.answer()

# ---------- Referral ----------
@router.callback_query(F.data == "ref")
async def ref_cb(cb: CallbackQuery, bot: Bot):
    user = upsert_user(cb)
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{cb.from_user.id}"
    text = (f"╔══════════════════╗\n   👥 <b>REFERRAL PROGRAM</b>\n╚══════════════════╝\n\n"
            f"🔗 <b>Your Invite Link:</b>\n<code>{link}</code>\n\n"
            f"{SDIV}\n"
            f"👥 <b>Total Referrals:</b>  {user['ref_count']}\n"
            f"💰 <b>Per Referral:</b>     {s_geti('ref_coin',100)} coins\n"
            f"⭐ <b>Per Referral:</b>     {s_geti('ref_star',1)} stars\n"
            f"🏆 <b>Per Referral:</b>     {s_geti('ref_xp',20)} XP\n"
            f"{SDIV}\n✨ Share your link and earn forever!")
    rows = [
        [InlineKeyboardButton(text="📤 Share Link",
                              url=f"https://t.me/share/url?url={link}&text=💎 Join the Premium Earn Bot!")],
        [InlineKeyboardButton(text="🏆 Leaderboard", callback_data="lb"),
         InlineKeyboardButton(text="🏠 Main Menu",   callback_data="menu")],
    ]
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "lb")
async def lb_cb(cb: CallbackQuery):
    upsert_user(cb)
    with closing(db()) as con:
        top = con.execute(
            "SELECT user_id, first_name, ref_count, coins FROM users "
            "ORDER BY ref_count DESC, coins DESC LIMIT 10").fetchall()
    txt = f"╔══════════════════╗\n   🏆 <b>TOP REFERRERS</b>   \n╚══════════════════╝\n\n"
    medals = ["🥇","🥈","🥉"] + ["🎖"]*7
    if not top:
        txt += "No referrers yet — be the first!"
    for i, u in enumerate(top):
        name = (u['first_name'] or str(u['user_id']))[:16]
        txt += f"{medals[i]} <b>{name}</b> — {u['ref_count']} refs · {u['coins']}💰\n"
    txt += f"\n{SDIV}\n✨ Climb the ranks and win prestige!"
    await cb.message.edit_text(txt, reply_markup=back_kb("ref"))
    await cb.answer()

# ---------- Shop ----------
@router.callback_query(F.data == "shop")
async def shop_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    with closing(db()) as con:
        items = con.execute("SELECT * FROM shop_items WHERE active=1").fetchall()
    rows = [[InlineKeyboardButton(text=f"🛍 {it['name']}  ·  {it['price']}⭐",
                                  callback_data=f"buy_{it['id']}")] for it in items]
    rows.append([InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")])
    text = (f"╔══════════════════╗\n   🛍 <b>PREMIUM SHOP</b>   \n╚══════════════════╝\n\n"
            f"⭐ Your Stars: <b>{user['stars']}</b>\n"
            f"{SDIV}\n✨ Tap an item to purchase:")
    await cb.message.edit_text(text,
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("buy_"))
async def buy_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    iid = int(cb.data.split("_")[1])
    with closing(db()) as con:
        it = con.execute("SELECT * FROM shop_items WHERE id=?", (iid,)).fetchone()
    if not it: 
        return await cb.answer("Item missing.", show_alert=True)
    if user["stars"] < it["price"]:
        return await cb.answer("⭐ Not enough stars.", show_alert=True)
    with closing(db()) as con:
        con.execute("UPDATE users SET stars=stars-? WHERE user_id=?",
                    (it["price"], cb.from_user.id))
    if it["kind"] == "energy":
        add_e = int(it["payload"] or 100)
        with closing(db()) as con:
            con.execute("UPDATE users SET energy=energy+? WHERE user_id=?",
                        (add_e, cb.from_user.id))
        await cb.answer(f"⚡ +{add_e} energy!", show_alert=True)
    elif it["kind"] == "vip":
        days = int(it["payload"] or 30)
        until = (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d")
        with closing(db()) as con:
            con.execute("UPDATE users SET vip=1, vip_until=? WHERE user_id=?",
                        (until, cb.from_user.id))
        await cb.answer(f"👑 VIP activated for {days} days!", show_alert=True)
    elif it["kind"] == "spin":
        add_balance(cb.from_user.id, stars=int(it["payload"] or 5), note="Spin tickets")
        await cb.answer("🎡 Spin tickets credited!", show_alert=True)
    else:
        await cb.answer("✅ Purchase complete.", show_alert=True)
    await shop_cb(cb)

# ---------- VIP ----------
@router.callback_query(F.data == "vip")
async def vip_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    vip_active = is_vip(cb.from_user.id)
    status = ('🟢 Active until ' + user['vip_until']) if vip_active else '🔴 Inactive'
    text = (f"╔══════════════════╗\n   👑 <b>VIP CLUB</b>   \n╚══════════════════╝\n\n"
            f"💎 <b>Status:</b> {status}\n"
            f"{SDIV}\n"
            f"<b>✨ VIP Benefits:</b>\n"
            f"  • 🚀 Access to ALL earning tasks\n"
            f"  • 💎 Premium tasks with higher rewards\n"
            f"  • 🎮 Exclusive games\n"
            f"  • ⚡ {s_geti('vip_max_energy',200)} max energy\n"
            f"  • 🎁 +{s_geti('vip_daily_bonus',100)}💰 daily bonus\n"
            f"  • 👑 VIP badge\n\n"
            f"{SDIV}\n💳 <b>Price:</b> {s_geti('vip_price_stars',100)}⭐ / {s_geti('vip_days',30)} days")
    rows = [[InlineKeyboardButton(text="💎 Buy VIP Now",  callback_data="shop")],
            [InlineKeyboardButton(text="🏠 Main Menu",     callback_data="menu")]]
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

# ---------- Withdraw ----------
class WithdrawState(StatesGroup):
    method = State()
    amount = State()
    number = State()
    confirm = State()

@router.callback_query(F.data == "withdraw")
async def withdraw_cb(cb: CallbackQuery, state: FSMContext):
    user = upsert_user(cb)
    minw = s_geti("min_withdraw", 1000)
    rate = s_geti("coin_per_taka", 100)
    text = (f"╔══════════════════╗\n   💸 <b>WITHDRAW</b>   \n╚══════════════════╝\n\n"
            f"💰 <b>Your Coins:</b>     {user['coins']}\n"
            f"💵 <b>Available:</b>      {user['coins']//rate}৳\n"
            f"{SDIV}\n"
            f"🔁 <b>Rate:</b>           {rate} coins = 1৳\n"
            f"⬇️ <b>Minimum:</b>        {minw} coins\n"
            f"📅 <b>Daily Limit:</b>    {s_geti('daily_withdraw_limit',1)} request\n"
            f"💳 <b>Fee:</b>            {s_geti('withdraw_fee_pct',5)}%\n"
            f"{SDIV}\n✨ Choose payment method below:")
    rows = [[InlineKeyboardButton(text="📲 ʙᴋᴀsʜ",  callback_data="wd_bkash"),
             InlineKeyboardButton(text="📲 ɴᴀɢᴀᴅ",  callback_data="wd_nagad")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]]
    await state.set_state(WithdrawState.method)
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.in_({"wd_bkash", "wd_nagad"}), WithdrawState.method)
async def wd_method(cb: CallbackQuery, state: FSMContext):
    method = "Bkash" if cb.data == "wd_bkash" else "Nagad"
    await state.update_data(method=method)
    await state.set_state(WithdrawState.amount)
    await cb.message.edit_text(f"💸 <b>{method}</b>\nEnter amount in <b>coins</b>:",
                               reply_markup=back_kb("withdraw"))
    await cb.answer()

@router.message(WithdrawState.amount)
async def wd_amount(msg: Message, state: FSMContext):
    try: 
        amt = int((msg.text or "").strip())
    except: 
        return await msg.answer("❌ Invalid number.")
    minw = s_geti("min_withdraw", 1000)
    user = get_user(msg.from_user.id)
    if amt < minw: 
        return await msg.answer(f"❌ Minimum {minw} coins.")
    if amt > user["coins"]: 
        return await msg.answer("❌ Not enough coins.")
    with closing(db()) as con:
        cnt = con.execute(
            "SELECT COUNT(*) FROM withdrawals WHERE user_id=? AND date(ts,'unixepoch')=date('now')",
            (msg.from_user.id,)).fetchone()[0]
    if cnt >= s_geti("daily_withdraw_limit", 1):
        return await msg.answer("❌ Daily withdraw limit reached.")
    await state.update_data(amount=amt)
    await state.set_state(WithdrawState.number)
    await msg.answer("📱 Send your payment number:")

@router.message(WithdrawState.number)
async def wd_number(msg: Message, state: FSMContext):
    num = (msg.text or "").strip()
    if not (10 <= len(num) <= 14):
        return await msg.answer("❌ Invalid number.")
    await state.update_data(number=num)
    data = await state.get_data()
    rate = s_geti("coin_per_taka", 100)
    fee = s_geti("withdraw_fee_pct", 5)
    coins = data["amount"]
    taka = (coins // rate)
    taka_net = taka - (taka * fee // 100)
    await state.set_state(WithdrawState.confirm)
    text = (f"📝 <b>Confirm Withdraw</b>\n\n"
            f"Method: {data['method']}\nNumber: <code>{num}</code>\n"
            f"Coins: {coins}\nGross: {taka}৳\nFee {fee}%: {taka-taka_net}৳\n"
            f"Net: <b>{taka_net}৳</b>")
    rows = [[InlineKeyboardButton(text="✅ Submit",  callback_data="wd_submit"),
             InlineKeyboardButton(text="❌ Cancel",  callback_data="menu")]]
    await msg.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data == "wd_submit", WithdrawState.confirm)
async def wd_submit(cb: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    rate = s_geti("coin_per_taka", 100)
    coins = data["amount"]
    taka = coins // rate
    with closing(db()) as con:
        con.execute("UPDATE users SET coins=coins-? WHERE user_id=?", (coins, cb.from_user.id))
        cur = con.execute(
            "INSERT INTO withdrawals(user_id,method,number,amount,coins,ts) VALUES(?,?,?,?,?,?)",
            (cb.from_user.id, data["method"], data["number"], taka, coins, now_ts()))
        wid = cur.lastrowid
    await state.clear()
    await cb.message.edit_text("✅ Withdraw request submitted. Awaiting admin review.",
                               reply_markup=back_kb())
    try:
        await bot.send_message(
            ADMIN_ID,
            f"💸 <b>NEW WITHDRAW</b>\nUser: <code>{cb.from_user.id}</code>\n"
            f"Method: {data['method']} {data['number']}\n"
            f"Amount: {taka}৳ ({coins} coins)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve", callback_data=f"wapp_{wid}"),
                 InlineKeyboardButton(text="❌ Reject",  callback_data=f"wrej_{wid}")]
            ]))
    except Exception: 
        pass
    await cb.answer()

# ---------- Language ----------
@router.callback_query(F.data == "lang")
async def lang_cb(cb: CallbackQuery):
    upsert_user(cb)
    rows = [[InlineKeyboardButton(text="🇬🇧 English", callback_data="setlang_en"),
             InlineKeyboardButton(text="🇧🇩 বাংলা",  callback_data="setlang_bn")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]]
    text = (f"╔══════════════════╗\n   🌍 <b>LANGUAGE</b>   \n╚══════════════════╝\n\n"
            f"🌐 Choose your preferred language\n"
            f"আপনার পছন্দের ভাষা বেছে নিন\n{SDIV}")
    await cb.message.edit_text(text,
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("setlang_"))
async def setlang_cb(cb: CallbackQuery, state: FSMContext):
    lang = cb.data.split("_")[1]
    with closing(db()) as con:
        con.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, cb.from_user.id))
    await cb.answer(tr(lang, "lang_set"), show_alert=True)
    user = get_user(cb.from_user.id)
    await state.clear()
    text = menu_text(user, user["lang"])
    kb = main_menu_kb(user["lang"], is_admin(cb.from_user.id), is_vip(cb.from_user.id))
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb)

# ---------- Support ----------
@router.callback_query(F.data == "support")
async def support_cb(cb: CallbackQuery):
    upsert_user(cb)
    text = (f"╔══════════════════╗\n   💬 <b>SUPPORT</b>   \n╚══════════════════╝\n\n"
            f"💎 Need help? We're here for you!\n{SDIV}\n"
            f"👤 <b>Admin ID:</b> <code>{ADMIN_ID}</code>\n"
            f"📩 Send a direct message to admin for any issue.\n"
            f"⏱ Average reply time: under 24 hours.\n{SDIV}")
    await cb.message.edit_text(text, reply_markup=back_kb())
    await cb.answer()

# =========================================================
#   STARTUP
# =========================================================
async def main():
    db_init()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    log.info("🚀 Premium Earn Bot starting...")
    log.info(f"👑 Admin ID: {ADMIN_ID}")
    log.info("📢 Required Channels:")
    for ch in REQUIRED_CHANNELS:
        log.info(f"   - {ch['title']}: {ch['chat_id']}")
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Stopped.")
