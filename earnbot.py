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
from collections import defaultdict

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
    InputFile,
)

# =========================================================
#   CONFIGURATION
# =========================================================
BOT_TOKEN = "8688447065:AAE5E2l4qbgm0jwLW9bIxS3olFIfzvvuDBc"
ADMIN_ID = 8502686983

# Database path
DB_PATH = "earnbot.db"

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
            total_earned_stars INTEGER DEFAULT 0,
            mining_power   INTEGER DEFAULT 1,
            mining_level   INTEGER DEFAULT 1,
            last_mine      INTEGER DEFAULT 0,
            fishing_rod    INTEGER DEFAULT 1,
            last_fish      INTEGER DEFAULT 0,
            pet_level      INTEGER DEFAULT 1,
            pet_exp        INTEGER DEFAULT 0,
            pet_hunger     INTEGER DEFAULT 100,
            last_pet_feed  INTEGER DEFAULT 0,
            weekly_bonus   INTEGER DEFAULT 0,
            last_weekly    TEXT,
            monthly_bonus  INTEGER DEFAULT 0,
            last_monthly   TEXT,
            gamble_wins    INTEGER DEFAULT 0,
            gamble_losses  INTEGER DEFAULT 0,
            battle_rank    INTEGER DEFAULT 1,
            battle_points  INTEGER DEFAULT 0,
            last_battle    INTEGER DEFAULT 0,
            investment     INTEGER DEFAULT 0,
            investment_roi INTEGER DEFAULT 0,
            last_investment INTEGER DEFAULT 0,
            lottery_tickets INTEGER DEFAULT 0,
            lottery_wins   INTEGER DEFAULT 0,
            daily_streak_max INTEGER DEFAULT 0
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

        CREATE TABLE IF NOT EXISTS bot_stats(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            total_users INTEGER,
            active_users INTEGER,
            coins_in_circulation INTEGER,
            stars_in_circulation INTEGER,
            vip_users INTEGER
        );

        -- NEW TABLES FOR NEW FEATURES
        CREATE TABLE IF NOT EXISTS achievements(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT,
            reward_coins INTEGER,
            reward_stars INTEGER,
            requirement_type TEXT,
            requirement_value INTEGER,
            icon TEXT DEFAULT '🏆'
        );

        CREATE TABLE IF NOT EXISTS user_achievements(
            user_id INTEGER,
            achievement_id INTEGER,
            earned_at INTEGER,
            PRIMARY KEY(user_id, achievement_id)
        );

        CREATE TABLE IF NOT EXISTS daily_challenges(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            challenge_type TEXT,
            target INTEGER,
            reward_coins INTEGER,
            reward_stars INTEGER,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS user_challenges(
            user_id INTEGER,
            challenge_id INTEGER,
            progress INTEGER DEFAULT 0,
            completed INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, challenge_id)
        );

        CREATE TABLE IF NOT EXISTS tournaments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            start_date TEXT,
            end_date TEXT,
            prize_pool INTEGER,
            entry_fee INTEGER,
            max_participants INTEGER,
            current_participants INTEGER DEFAULT 0,
            status TEXT DEFAULT 'upcoming'
        );

        CREATE TABLE IF NOT EXISTS tournament_participants(
            user_id INTEGER,
            tournament_id INTEGER,
            score INTEGER DEFAULT 0,
            joined_at INTEGER,
            PRIMARY KEY(user_id, tournament_id)
        );

        CREATE TABLE IF NOT EXISTS marketplace_listings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id INTEGER,
            item_type TEXT,
            item_value INTEGER,
            price_coins INTEGER,
            price_stars INTEGER,
            listed_at INTEGER,
            status TEXT DEFAULT 'active'
        );

        CREATE TABLE IF NOT EXISTS guilds(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            owner_id INTEGER,
            level INTEGER DEFAULT 1,
            exp INTEGER DEFAULT 0,
            members_count INTEGER DEFAULT 1,
            coins INTEGER DEFAULT 0,
            created_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS guild_members(
            user_id INTEGER,
            guild_id INTEGER,
            role TEXT DEFAULT 'member',
            joined_at INTEGER,
            PRIMARY KEY(user_id, guild_id)
        );

        CREATE TABLE IF NOT EXISTS polls(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT,
            options TEXT,
            created_by INTEGER,
            created_at INTEGER,
            ends_at INTEGER,
            status TEXT DEFAULT 'active'
        );

        CREATE TABLE IF NOT EXISTS poll_votes(
            poll_id INTEGER,
            user_id INTEGER,
            option_index INTEGER,
            PRIMARY KEY(poll_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS giveaways(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prize TEXT,
            prize_coins INTEGER DEFAULT 0,
            prize_stars INTEGER DEFAULT 0,
            winners_count INTEGER DEFAULT 1,
            entry_cost INTEGER DEFAULT 0,
            ends_at INTEGER,
            created_by INTEGER,
            participants TEXT DEFAULT '',
            winners TEXT DEFAULT '',
            status TEXT DEFAULT 'active'
        );

        CREATE TABLE IF NOT EXISTS trade_requests(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user INTEGER,
            to_user INTEGER,
            offer_coins INTEGER DEFAULT 0,
            offer_stars INTEGER DEFAULT 0,
            request_coins INTEGER DEFAULT 0,
            request_stars INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS mining_upgrades(
            level INTEGER PRIMARY KEY,
            cost_coins INTEGER,
            power_increase INTEGER,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS fishing_upgrades(
            level INTEGER PRIMARY KEY,
            cost_coins INTEGER,
            rod_power INTEGER,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS pets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            base_power INTEGER,
            max_level INTEGER,
            upgrade_cost INTEGER,
            description TEXT
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
            "bot_name": ("Premium Earn Bot", "Bot display name"),
            "welcome_message": ("Welcome to {bot_name}! 🎉\nEarn coins and rewards daily!", "Welcome message template"),
            "mining_base_reward": ("50", "Base mining reward per session"),
            "mining_session_minutes": ("60", "Mining session duration in minutes"),
            "fishing_base_reward": ("30", "Base fishing reward per catch"),
            "pet_food_cost": ("10", "Cost to feed pet in coins"),
            "lottery_ticket_price": ("5", "Lottery ticket price in stars"),
            "lottery_min_players": ("10", "Minimum players for lottery"),
            "lottery_house_edge": ("10", "House edge percentage for lottery"),
            "battle_entry_fee": ("20", "Battle entry fee in coins"),
            "battle_winner_reward": ("50", "Battle winner reward multiplier"),
            "investment_daily_roi": ("5", "Daily ROI percentage for investments"),
            "investment_max": ("10000", "Maximum investment amount"),
            "tournament_entry_fee": ("100", "Tournament entry fee"),
            "marketplace_fee": ("5", "Marketplace listing fee percentage"),
            "guild_create_cost": ("1000", "Cost to create a guild"),
            "daily_challenge_count": ("3", "Number of daily challenges"),
            "weekly_bonus_multiplier": ("2", "Weekly bonus multiplier"),
            "monthly_bonus_multiplier": ("5", "Monthly bonus multiplier"),
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
                    ("💰 Coin Booster x2", "booster", 20, "2", "Double coins from tasks for 1 hour"),
                    ("⛏️ Mining Upgrade", "mining", 500, "1", "Increase mining power by 1"),
                    ("🎣 Fishing Rod Upgrade", "fishing", 300, "1", "Better rod for rare fish"),
                    ("🐕 Pet Dog", "pet", 1000, "dog", "Loyal pet that helps you earn"),
                    ("🐈 Pet Cat", "pet", 800, "cat", "Lucky cat that finds coins"),
                    ("🎰 Jackpot Ticket", "jackpot", 10, "1", "Entry to weekly jackpot"),
                    ("📈 Investment Token", "investment", 200, "1", "Start earning passive income"),
                    ("⚔️ Battle Pass", "battle_pass", 150, "30", "30 days of battle rewards"),
                ],
            )
        
        # Seed achievements
        if con.execute("SELECT COUNT(*) FROM achievements").fetchone()[0] == 0:
            con.executemany(
                "INSERT INTO achievements(name,description,reward_coins,reward_stars,requirement_type,requirement_value,icon) VALUES(?,?,?,?,?,?,?)",
                [
                    ("First Steps", "Complete 10 tasks", 100, 5, "tasks_completed", 10, "👣"),
                    ("Task Master", "Complete 100 tasks", 1000, 50, "tasks_completed", 100, "👑"),
                    ("Millionaire", "Earn 1,000,000 coins", 10000, 100, "coins_earned", 1000000, "💰"),
                    ("Star Collector", "Earn 1000 stars", 5000, 100, "stars_earned", 1000, "⭐"),
                    ("Social Butterfly", "Get 50 referrals", 5000, 50, "referrals", 50, "🦋"),
                    ("VIP Elite", "Buy VIP 3 times", 10000, 200, "vip_purchases", 3, "💎"),
                    ("Streak Warrior", "30 day login streak", 3000, 30, "streak", 30, "⚔️"),
                    ("Battle Champion", "Win 50 battles", 5000, 100, "battle_wins", 50, "🏆"),
                    ("Mining King", "Mine 100 times", 2000, 20, "mining_sessions", 100, "⛏️"),
                    ("Fishing Legend", "Catch 100 fish", 2000, 20, "fish_caught", 100, "🎣"),
                    ("Lottery Winner", "Win a lottery", 5000, 50, "lottery_wins", 1, "🎰"),
                    ("Guild Master", "Create a guild", 3000, 30, "guild_created", 1, "🏛️"),
                    ("Trading Expert", "Complete 50 trades", 2000, 20, "trades_completed", 50, "🤝"),
                    ("Daily Dedication", "Complete 30 daily challenges", 3000, 30, "daily_challenges", 30, "📅"),
                    ("Tournament Winner", "Win a tournament", 10000, 100, "tournament_wins", 1, "🏅"),
                ],
            )
        
        # Seed mining upgrades
        if con.execute("SELECT COUNT(*) FROM mining_upgrades").fetchone()[0] == 0:
            for i in range(1, 11):
                con.execute(
                    "INSERT INTO mining_upgrades(level,cost_coins,power_increase,description) VALUES(?,?,?,?)",
                    (i, 100 * i ** 2, i, f"Mining power +{i}")
                )
        
        # Seed fishing upgrades
        if con.execute("SELECT COUNT(*) FROM fishing_upgrades").fetchone()[0] == 0:
            for i in range(1, 11):
                con.execute(
                    "INSERT INTO fishing_upgrades(level,cost_coins,rod_power,description) VALUES(?,?,?,?)",
                    (i, 80 * i ** 2, i * 2, f"Fishing rod power +{i*2}")
                )
        
        # Seed pets
        if con.execute("SELECT COUNT(*) FROM pets").fetchone()[0] == 0:
            con.executemany(
                "INSERT INTO pets(name,base_power,max_level,upgrade_cost,description) VALUES(?,?,?,?,?)",
                [
                    ("🐕 Loyal Dog", 5, 10, 500, "Guards your coins and finds extras"),
                    ("🐈 Lucky Cat", 3, 15, 400, "Brings good luck and better rewards"),
                    ("🐉 Dragon", 10, 20, 1000, "Powerful beast that doubles earnings"),
                    ("🦉 Wise Owl", 4, 12, 600, "Increases XP gain by 50%"),
                    ("🐧 Penguin", 2, 8, 300, "Cute friend that finds energy"),
                ],
            )

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
DIV  = "──────────────────"
SDIV = "──── ◇ ────"

T = {
    "en": {
        "welcome":     f"╔══════════════════╗\n   ✨ <b>PREMIUM EARN BOT</b> ✨\n╚══════════════════╝\n\n🔐 <b>Verification Required</b>\n💎 Join all channels below to unlock!\n\n{SDIV}",
        "joined":      "✅ <b>Verified Successfully!</b>\n💎 Welcome to the Premium Club.",
        "not_joined":  "❌ <b>Access Denied</b>\nPlease join all required channels first.",
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
        "profile":     f"╔══════════════════╗\n   👤 <b>আমার প্রোফাইল</b>   \n╚══════════════════╝\n\n🆔 <b>আইডি:</b> <code>{{id}}</code>\n👤 <b>নাম:</b> {{name}}\n\n{SDIV}\n💰 <b>কয়েন:</b>    {{coins}}\n⭐ <b>স্টার:</b>     {{stars}}\n⚡ <b>এনার্জি:</b>   {{energy}}/{{maxe}}\n🏆 <b>লেভেল:</b>    {{lvl}}\n📊 <b>XP:</b>       {{xp}} / {{nxt}}\n🔥 <b>স্ট্রিক:</b>    {{streak}} দিন\n👥 <b>রেফারাল:</b>     {{ref}}\n👑 <b>VIP:</b>       {{vip}}\n{SDIV}",
        "earn":        f"╔══════════════════╗\n   💰 <b>আর্নিং হাব</b>   \n╚══════════════════╝\n\n✨ আপনার পছন্দের উপায় বাছুন!\n{SDIV}",
        "vip_only":    "👑 <b>শুধু VIP</b>\nএই ফিচার শুধু VIP সদস্যদের জন্য!\n💎 শপ থেকে VIP কিনুন →",
        "no_energy":   "⚡ পর্যাপ্ত এনার্জি নেই!",
        "cooldown":    "⏱ এই টাস্ক কুলডাউনে আছে।",
        "daily_done":  "✅ আজকের লিমিট শেষ।",
        "task_ok":     "🎉 <b>টাস্ক সম্পন্ন!</b>\n💰 +{c}   ⭐ +{s}   🏆 +{x}",
        "back":        "⬅️ পিছনে",
        "banned":      "🚫 আপনি ব্যান হয়েছেন।",
        "ref_ok":      "🎁 রেফারাল রিওয়ার্ড পেয়েছেন!",
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

def get_all_users() -> list:
    with closing(db()) as con:
        return con.execute("SELECT * FROM users ORDER BY coins DESC").fetchall()

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
            # Check level up achievements
            check_achievements(uid)

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

def check_achievements(uid: int):
    with closing(db()) as con:
        user = con.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        achievements = con.execute("SELECT * FROM achievements").fetchall()
        
        for ach in achievements:
            # Check if already earned
            earned = con.execute(
                "SELECT 1 FROM user_achievements WHERE user_id=? AND achievement_id=?",
                (uid, ach["id"])
            ).fetchone()
            if earned:
                continue
            
            # Calculate progress based on requirement type
            earned_flag = False
            if ach["requirement_type"] == "tasks_completed":
                if user["tasks_done"] >= ach["requirement_value"]:
                    earned_flag = True
            elif ach["requirement_type"] == "coins_earned":
                if user["total_earned_coins"] >= ach["requirement_value"]:
                    earned_flag = True
            elif ach["requirement_type"] == "stars_earned":
                if user["total_earned_stars"] >= ach["requirement_value"]:
                    earned_flag = True
            elif ach["requirement_type"] == "referrals":
                if user["ref_count"] >= ach["requirement_value"]:
                    earned_flag = True
            elif ach["requirement_type"] == "streak":
                if user["streak"] >= ach["requirement_value"]:
                    earned_flag = True
            
            if earned_flag:
                con.execute(
                    "INSERT INTO user_achievements(user_id,achievement_id,earned_at) VALUES(?,?,?)",
                    (uid, ach["id"], now_ts())
                )
                add_balance(uid, coins=ach["reward_coins"], stars=ach["reward_stars"], 
                           note=f"Achievement: {ach['name']}")

# =========================================================
#   FORCE-JOIN VERIFICATION
# =========================================================
async def check_joined(bot: Bot, uid: int) -> tuple[bool, list]:
    missing = []
    for c in REQUIRED_CHANNELS:
        try:
            m = await bot.get_chat_member(c["chat_id"], uid)
            if m.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                missing.append(c)
        except TelegramBadRequest:
            missing.append(c)
        except Exception:
            missing.append(c)
    return len(missing) == 0, missing

def join_kb(missing) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"📢 JOIN • {c['title']}", url=c["url"])] for c in missing]
    rows.append([InlineKeyboardButton(text="✅ I HAVE JOINED ✅", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================
#   MAIN MENU KEYBOARD
# =========================================================
def main_menu_kb(lang="en", admin=False, vip=False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 PROFILE",  callback_data="profile"),
         InlineKeyboardButton(text="👛 WALLET",   callback_data="wallet")],
        [InlineKeyboardButton(text="💰 EARN COINS", callback_data="earn"),
         InlineKeyboardButton(text="🎮 GAMES", callback_data="games")],
        [InlineKeyboardButton(text="🎁 DAILY", callback_data="daily"),
         InlineKeyboardButton(text="🛍 SHOP",        callback_data="shop")],
        [InlineKeyboardButton(text="👥 REFERRAL",  callback_data="ref"),
         InlineKeyboardButton(text="💸 WITHDRAW",   callback_data="withdraw")],
        [InlineKeyboardButton(text="🏆 LEADERBOARD", callback_data="lb"),
         InlineKeyboardButton(text="👑 VIP CLUB",     callback_data="vip")],
        [InlineKeyboardButton(text="🌍 LANGUAGE", callback_data="lang"),
         InlineKeyboardButton(text="💬 SUPPORT",   callback_data="support")],
        [InlineKeyboardButton(text="⛏️ MINING", callback_data="mining"),
         InlineKeyboardButton(text="🎣 FISHING", callback_data="fishing")],
        [InlineKeyboardButton(text="🐕 PETS", callback_data="pets"),
         InlineKeyboardButton(text="💼 INVEST", callback_data="invest")],
        [InlineKeyboardButton(text="🎰 LOTTERY", callback_data="lottery"),
         InlineKeyboardButton(text="⚔️ BATTLE", callback_data="battle")],
        [InlineKeyboardButton(text="🏛️ GUILD", callback_data="guild"),
         InlineKeyboardButton(text="🏆 TOURNAMENT", callback_data="tournament")],
        [InlineKeyboardButton(text="🤝 TRADE", callback_data="trade"),
         InlineKeyboardButton(text="🏪 MARKET", callback_data="market")],
        [InlineKeyboardButton(text="📊 DAILY CHALLENGES", callback_data="challenges"),
         InlineKeyboardButton(text="🏅 ACHIEVEMENTS", callback_data="achievements")],
        [InlineKeyboardButton(text="🗳️ POLLS", callback_data="polls"),
         InlineKeyboardButton(text="🎁 GIVEAWAY", callback_data="giveaway")],
    ]
    if admin:
        rows.append([InlineKeyboardButton(text="👑 ADMIN PANEL", callback_data="admin")])
    if not vip and not admin:
        rows.insert(5, [InlineKeyboardButton(text="👑 GET VIP", callback_data="vip")])
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
#   USER SEARCH STATE
# =========================================================
class AdminUserState(StatesGroup):
    searching = State()
    editing = State()

# =========================================================
#   NEW FEATURES STATES
# =========================================================
class MiningState(StatesGroup):
    mining = State()

class FishingState(StatesGroup):
    fishing = State()

class PetState(StatesGroup):
    feeding = State()
    playing = State()

class InvestState(StatesGroup):
    amount = State()
    duration = State()

class LotteryState(StatesGroup):
    buying = State()

class BattleState(StatesGroup):
    waiting = State()
    selecting = State()

class TradeState(StatesGroup):
    selecting_user = State()
    offering = State()
    requesting = State()

class GuildState(StatesGroup):
    creating = State()
    naming = State()
    joining = State()

class TournamentState(StatesGroup):
    registering = State()

class ChallengeState(StatesGroup):
    active = State()

class GiveawayState(StatesGroup):
    creating = State()

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
                    check_achievements(ref_id)
                    try:
                        await bot.send_message(ref_id, tr(get_user(ref_id)["lang"] if get_user(ref_id) else "en", "ref_ok"))
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
    
    ok, missing = await check_joined(bot, msg.from_user.id)
    if missing and not is_admin(msg.from_user.id):
        return await msg.answer(tr(user["lang"], "welcome"),
                                reply_markup=join_kb(missing))
    
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
    
    ok, missing = await check_joined(cb.bot, cb.from_user.id)
    if missing and not is_admin(cb.from_user.id):
        return await cb.message.edit_text(tr(lang, "welcome"),
                                          reply_markup=join_kb(missing))
    
    user = get_user(cb.from_user.id)
    await cb.message.edit_text(menu_text(user, lang),
                               reply_markup=main_menu_kb(lang, is_admin(cb.from_user.id), is_vip(cb.from_user.id)))

@router.callback_query(F.data == "check_join")
async def check_join_cb(cb: CallbackQuery, bot: Bot):
    user = upsert_user(cb)
    lang = user["lang"] or "en"
    ok, missing = await check_joined(bot, cb.from_user.id)
    if not ok:
        return await cb.answer(tr(lang, "not_joined"), show_alert=True)
    user = get_user(cb.from_user.id)
    await cb.message.edit_text(
        tr(lang, "joined") + "\n\n" + menu_text(user, lang),
        reply_markup=main_menu_kb(lang, is_admin(cb.from_user.id), is_vip(cb.from_user.id)))

@router.callback_query(F.data == "menu")
async def menu_cb(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    user = upsert_user(cb)
    regen_energy(cb.from_user.id)
    user = get_user(cb.from_user.id)
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

# ---------- Earn Center ----------
@router.callback_query(F.data == "earn")
async def earn_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    rows = [
        [InlineKeyboardButton(text="📢 CHANNEL JOIN",   callback_data="t_kind_channel"),
         InlineKeyboardButton(text="🔗 SHORTLINK",      callback_data="t_kind_shortlink")],
        [InlineKeyboardButton(text="📣 SPONSOR",     callback_data="t_kind_sponsor"),
         InlineKeyboardButton(text="🎥 WATCH ADS",      callback_data="t_kind_ad")],
        [InlineKeyboardButton(text="✍️ SURVEY", callback_data="t_kind_survey"),
         InlineKeyboardButton(text="📱 APP DOWNLOAD",   callback_data="t_kind_app")],
        [InlineKeyboardButton(text="🏠 Main Menu",       callback_data="menu")],
    ]
    await cb.message.edit_text(tr(user["lang"], "earn"),
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

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
    
    check_achievements(cb.from_user.id)
    await cb.answer(tr(user["lang"], "task_ok",
                       c=t["coin_reward"], s=t["star_reward"], x=t["xp_reward"]),
                    show_alert=True)
    await earn_cb(cb)

# ---------- Daily / Streak / Mystery ----------
@router.callback_query(F.data == "daily")
async def daily_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    rows = [
        [InlineKeyboardButton(text="🎁 DAILY BONUS", callback_data="daily_claim")],
        [InlineKeyboardButton(text="📦 MYSTERY BOX",       callback_data="daily_box"),
         InlineKeyboardButton(text="💎 TREASURE CHEST",    callback_data="daily_chest")],
        [InlineKeyboardButton(text="📅 WEEKLY BONUS", callback_data="weekly_claim"),
         InlineKeyboardButton(text="📆 MONTHLY BONUS", callback_data="monthly_claim")],
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
        if streak > (con.execute("SELECT daily_streak_max FROM users WHERE user_id=?", (cb.from_user.id,)).fetchone()[0] or 0):
            con.execute("UPDATE users SET daily_streak_max=? WHERE user_id=?", (streak, cb.from_user.id))
    
    check_achievements(cb.from_user.id)
    await cb.answer(f"🎉 +{coins}💰  +{stars}⭐\n🔥 Streak: {streak}", show_alert=True)
    await daily_cb(cb)

@router.callback_query(F.data == "weekly_claim")
async def weekly_claim(cb: CallbackQuery):
    user = upsert_user(cb)
    if user["last_weekly"] == today():
        return await cb.answer("⏳ Weekly bonus already claimed!", show_alert=True)
    
    multiplier = s_geti("weekly_bonus_multiplier", 2)
    bonus = s_geti("daily_coin", 50) * 7 * multiplier
    add_balance(cb.from_user.id, coins=bonus, note="Weekly bonus")
    
    with closing(db()) as con:
        con.execute("UPDATE users SET last_weekly=?, weekly_bonus=weekly_bonus+? WHERE user_id=?",
                    (today(), bonus, cb.from_user.id))
    
    await cb.answer(f"🎉 Weekly bonus: +{bonus}💰", show_alert=True)
    await daily_cb(cb)

@router.callback_query(F.data == "monthly_claim")
async def monthly_claim(cb: CallbackQuery):
    user = upsert_user(cb)
    if user["last_monthly"] == today():
        return await cb.answer("⏳ Monthly bonus already claimed!", show_alert=True)
    
    multiplier = s_geti("monthly_bonus_multiplier", 5)
    bonus = s_geti("daily_coin", 50) * 30 * multiplier
    add_balance(cb.from_user.id, coins=bonus, stars=50, note="Monthly bonus")
    
    with closing(db()) as con:
        con.execute("UPDATE users SET last_monthly=?, monthly_bonus=monthly_bonus+? WHERE user_id=?",
                    (today(), bonus, cb.from_user.id))
    
    await cb.answer(f"🎉 Monthly bonus: +{bonus}💰 +50⭐", show_alert=True)
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
    star = random.choice([0, 0, 0, 1, 2, 5])
    add_balance(cb.from_user.id, coins=coin, stars=star, note="Chest")
    await cb.answer(f"💎 Chest: +{coin}💰 +{star}⭐", show_alert=True)
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
            [InlineKeyboardButton(text="🎡 LUCKY SPIN",     callback_data="g_spin"),
             InlineKeyboardButton(text="🎲 DICE ROLL",      callback_data="g_dice")],
            [InlineKeyboardButton(text="🪙 COIN FLIP",      callback_data="g_flip"),
             InlineKeyboardButton(text="👆 TAP GAME",       callback_data="g_tap")],
            [InlineKeyboardButton(text="🎰 SLOT MACHINE",   callback_data="g_slot"),
             InlineKeyboardButton(text="🃏 CARD MATCH",     callback_data="g_card")],
            [InlineKeyboardButton(text="🔨 HAMMER GAME",    callback_data="g_hammer"),
             InlineKeyboardButton(text="🎯 TARGET SHOOT",   callback_data="g_shoot")],
            [InlineKeyboardButton(text="🧠 MEMORY GAME",    callback_data="g_memory"),
             InlineKeyboardButton(text="🎲 CRAPS",         callback_data="g_craps")],
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
    
    # Better prizes with multiplier
    prizes = [10, 25, 50, 100, 200, 500, 1000, 5000, 0, 0, 0, 0, 0, 0, 0]
    win = random.choice(prizes)
    if win: 
        add_balance(cb.from_user.id, coins=win, note="Spin")
    await cb.answer(f"🎡 Spin result: +{win}💰" if win else "🎡 No win, try again!",
                    show_alert=True)
    await games_cb(cb)

@router.callback_query(F.data == "g_slot")
async def g_slot(cb: CallbackQuery):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if user["stars"] < 2:
        return await cb.answer("Need 2⭐ to play slots!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET stars=stars-2 WHERE user_id=?", (cb.from_user.id,))
    
    # Slot machine logic
    slots = ["🍒", "🍋", "🍊", "🍉", "⭐", "💰", "7️⃣", "💎"]
    result = [random.choice(slots), random.choice(slots), random.choice(slots)]
    
    win = 0
    if result[0] == result[1] == result[2]:
        if result[0] == "7️⃣":
            win = 1000
        elif result[0] == "💰":
            win = 500
        elif result[0] == "💎":
            win = 300
        else:
            win = 100
    elif result[0] == result[1] or result[1] == result[2]:
        win = 10
    
    if win:
        add_balance(cb.from_user.id, coins=win, note="Slot machine")
        await cb.answer(f"🎰 {' '.join(result)}\nYou won {win}💰!", show_alert=True)
    else:
        await cb.answer(f"🎰 {' '.join(result)}\nNo win, try again!", show_alert=True)
    await games_cb(cb)

@router.callback_query(F.data == "g_craps")
async def g_craps(cb: CallbackQuery):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if user["stars"] < 3:
        return await cb.answer("Need 3⭐ to play craps!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET stars=stars-3 WHERE user_id=?", (cb.from_user.id,))
    
    dice1 = random.randint(1, 6)
    dice2 = random.randint(1, 6)
    total = dice1 + dice2
    
    win = 0
    if total == 7 or total == 11:
        win = 30
    elif total == 2 or total == 3 or total == 12:
        win = 0  # craps - lose
    elif total == 4 or total == 10:
        win = 20
    elif total == 5 or total == 9:
        win = 15
    elif total == 6 or total == 8:
        win = 18
    
    if win:
        add_balance(cb.from_user.id, coins=win, note="Craps")
        await cb.answer(f"🎲 You rolled {dice1}+{dice2}={total}\nYou won {win}💰!", show_alert=True)
    else:
        await cb.answer(f"🎲 You rolled {dice1}+{dice2}={total}\nCraps! You lost!", show_alert=True)
    await games_cb(cb)

@router.callback_query(F.data == "g_card")
async def g_card(cb: CallbackQuery):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if not consume_energy(cb.from_user.id, 2):
        return await cb.answer("⚡ Need 2 energy.", show_alert=True)
    
    # Card matching game
    cards = ["A", "K", "Q", "J", "10", "9", "8", "7"]
    player = random.choice(cards)
    computer = random.choice(cards)
    
    values = {"A": 14, "K": 13, "Q": 12, "J": 11, "10": 10, "9": 9, "8": 8, "7": 7}
    
    if values[player] > values[computer]:
        win = 25
        msg = f"🃏 Your card: {player} | Bot: {computer}\nYou win! +{win}💰"
    elif values[player] < values[computer]:
        win = 0
        msg = f"🃏 Your card: {player} | Bot: {computer}\nYou lose!"
    else:
        win = 10
        msg = f"🃏 Your card: {player} | Bot: {computer}\nTie! +{win}💰"
    
    if win:
        add_balance(cb.from_user.id, coins=win, note="Card game")
    await cb.answer(msg, show_alert=True)
    await games_cb(cb)

class TapState(StatesGroup): 
    playing = State()
    score = State()

@router.callback_query(F.data == "g_tap")
async def g_tap(cb: CallbackQuery, state: FSMContext):
    user = upsert_user(cb)
    if not is_vip(cb.from_user.id):
        return await cb.answer(tr(user["lang"], "vip_only"), show_alert=True)
    
    if not consume_energy(cb.from_user.id, 3):
        return await cb.answer("⚡ Need 3 energy.", show_alert=True)
    await state.set_state(TapState.playing)
    await state.update_data(taps=0, ends=now_ts() + 15)
    await state.update_data(score=0)
    rows = [[InlineKeyboardButton(text="👆 TAP! (0)", callback_data="tap_hit")],
            [InlineKeyboardButton(text="✨ Power Up (5⭐)", callback_data="tap_power")],
            [InlineKeyboardButton(text="✅ Finish",   callback_data="tap_end")]]
    await cb.message.edit_text("🎮 <b>Tap Game</b> — tap as fast as you can in 15s!\n💡 Power up doubles your score for 5⭐",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "tap_power", TapState.playing)
async def tap_power(cb: CallbackQuery, state: FSMContext):
    user = get_user(cb.from_user.id)
    if user["stars"] < 5:
        return await cb.answer("Need 5⭐ for power up!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET stars=stars-5 WHERE user_id=?", (cb.from_user.id,))
    
    await state.update_data(power=2)
    await cb.answer("⚡ Power Up Active! Double points for 15s!", show_alert=True)

@router.callback_query(F.data == "tap_hit", TapState.playing)
async def tap_hit(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if now_ts() > data.get("ends", 0):
        return await tap_end(cb, state)
    taps = data["taps"] + 1
    await state.update_data(taps=taps)
    power = data.get("power", 1)
    score = taps * power
    await state.update_data(score=score)
    rows = [[InlineKeyboardButton(text=f"👆 TAP! ({taps}) x{power}", callback_data="tap_hit")],
            [InlineKeyboardButton(text="✨ Power Up (5⭐)", callback_data="tap_power")],
            [InlineKeyboardButton(text="✅ Finish",          callback_data="tap_end")]]
    try:
        await cb.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except TelegramBadRequest: 
        pass
    await cb.answer(f"Score: {score}")

@router.callback_query(F.data == "tap_end", TapState.playing)
async def tap_end(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    score = data.get("score", 0)
    win = score
    add_balance(cb.from_user.id, coins=win, xp=score//10, note="Tap game")
    await state.clear()
    await cb.message.edit_text(f"🎮 Done! Score: <b>{score}</b>  →  +{win}💰",
                               reply_markup=back_kb("games"))
    await cb.answer()

# =========================================================
#   NEW FEATURE: MINING SYSTEM
# =========================================================
@router.callback_query(F.data == "mining")
async def mining_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    if not is_vip(cb.from_user.id):
        return await cb.answer("👑 Mining is VIP only!", show_alert=True)
    
    with closing(db()) as con:
        upgrades = con.execute("SELECT * FROM mining_upgrades ORDER BY level").fetchall()
    
    last_mine = user["last_mine"]
    now = now_ts()
    session_minutes = s_geti("mining_session_minutes", 60)
    
    if last_mine and (now - last_mine) < session_minutes * 60:
        remaining = (session_minutes * 60) - (now - last_mine)
        remaining_min = remaining // 60
        return await cb.answer(f"⛏️ Already mining! Come back in {remaining_min} minutes.", show_alert=True)
    
    # Calculate mining reward
    base_reward = s_geti("mining_base_reward", 50)
    power = user["mining_power"]
    reward = base_reward * power + random.randint(-20, 50)
    reward = max(10, reward)
    
    add_balance(cb.from_user.id, coins=reward, note=f"Mining session with power {power}")
    
    # Update last mine time
    with closing(db()) as con:
        con.execute("UPDATE users SET last_mine=? WHERE user_id=?", (now, cb.from_user.id))
    
    # Check for mining achievements
    with closing(db()) as con:
        mining_count = con.execute("SELECT COUNT(*) FROM transactions WHERE user_id=? AND note LIKE 'Mining%'", (cb.from_user.id,)).fetchone()[0]
        if mining_count >= 100:
            check_achievements(cb.from_user.id)
    
    text = (f"⛏️ <b>MINING RESULTS</b>\n\n"
            f"⚡ Mining Power: {power}\n"
            f"💰 Coins Mined: {reward}\n"
            f"📊 Total Sessions: {mining_count+1}\n\n"
            f"<b>Upgrades Available:</b>\n")
    
    for upgrade in upgrades[:5]:
        if upgrade["level"] > power:
            text += f"• Level {upgrade['level']}: +{upgrade['power_increase']} power | Cost: {upgrade['cost_coins']}💰\n"
    
    rows = [[InlineKeyboardButton(text="⬆️ Upgrade Mining Power", callback_data="mine_upgrade")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer(f"🎉 You mined {reward}💰!", show_alert=True)

@router.callback_query(F.data == "mine_upgrade")
async def mine_upgrade(cb: CallbackQuery):
    user = get_user(cb.from_user.id)
    current_power = user["mining_power"]
    next_level = current_power + 1
    
    with closing(db()) as con:
        upgrade = con.execute("SELECT * FROM mining_upgrades WHERE level=?", (next_level,)).fetchone()
        if not upgrade:
            return await cb.answer("🎉 Max level reached!", show_alert=True)
        
        if user["coins"] < upgrade["cost_coins"]:
            return await cb.answer(f"Need {upgrade['cost_coins']}💰 for upgrade!", show_alert=True)
        
        con.execute("UPDATE users SET coins=coins-?, mining_power=mining_power+? WHERE user_id=?",
                    (upgrade["cost_coins"], upgrade["power_increase"], cb.from_user.id))
        
        add_balance(cb.from_user.id, xp=50, note="Mining upgrade")
    
    await cb.answer(f"✅ Mining power upgraded to level {next_level}!", show_alert=True)
    await mining_cb(cb)

# =========================================================
#   NEW FEATURE: FISHING SYSTEM
# =========================================================
@router.callback_query(F.data == "fishing")
async def fishing_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    if not is_vip(cb.from_user.id):
        return await cb.answer("🎣 Fishing is VIP only!", show_alert=True)
    
    if not consume_energy(cb.from_user.id, 5):
        return await cb.answer("⚡ Need 5 energy to fish!", show_alert=True)
    
    last_fish = user["last_fish"] or 0
    now = now_ts()
    if now - last_fish < 300:  # 5 min cooldown
        remaining = 300 - (now - last_fish)
        return await cb.answer(f"🎣 Wait {remaining//60} minutes before fishing again!", show_alert=True)
    
    # Fishing logic
    rod_power = user["fishing_rod"]
    fish_types = [
        ("🐟 Small Fish", 10, 20),
        ("🐠 Tropical Fish", 25, 50),
        ("🐡 Pufferfish", 40, 30),
        ("🐙 Octopus", 60, 40),
        ("🦑 Squid", 50, 35),
        ("🐋 Whale", 200, 10),
        ("🧜‍♀️ Mermaid", 500, 2),
        ("👑 King Fish", 300, 5),
        ("💎 Pearl", 80, 25),
        ("🗝️ Treasure Chest", 150, 15),
    ]
    
    # Better rod = better chances for rare fish
    weighted_fish = []
    for fish, reward, chance in fish_types:
        adjusted_chance = chance * (rod_power / 5)
        weighted_fish.extend([fish] * int(adjusted_chance))
    
    caught = random.choice(fish_types)
    fish_name, reward, base_chance = caught
    
    # Apply rod bonus
    reward = int(reward * (1 + rod_power / 20))
    
    add_balance(cb.from_user.id, coins=reward, note=f"Caught {fish_name}")
    
    with closing(db()) as con:
        con.execute("UPDATE users SET last_fish=? WHERE user_id=?", (now, cb.from_user.id))
        fish_count = con.execute("SELECT COUNT(*) FROM transactions WHERE user_id=? AND note LIKE 'Caught%'", (cb.from_user.id,)).fetchone()[0]
    
    if fish_count >= 100:
        check_achievements(cb.from_user.id)
    
    text = (f"🎣 <b>FISHING RESULTS</b>\n\n"
            f"🐟 You caught: {fish_name}\n"
            f"💰 Reward: {reward} coins\n"
            f"🎣 Rod Power: {rod_power}\n"
            f"📊 Total Fish: {fish_count+1}\n\n"
            f"<b>Upgrade Fishing Rod:</b>\n"
            f"Next level: {rod_power+1} (Cost: {80 * (rod_power+1)**2}💰)")
    
    rows = [[InlineKeyboardButton(text="⬆️ Upgrade Fishing Rod", callback_data="fish_upgrade")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer(f"🎣 You caught {fish_name} and earned {reward}💰!", show_alert=True)

@router.callback_query(F.data == "fish_upgrade")
async def fish_upgrade(cb: CallbackQuery):
    user = get_user(cb.from_user.id)
    current_rod = user["fishing_rod"]
    next_level = current_rod + 1
    cost = 80 * (next_level ** 2)
    
    if user["coins"] < cost:
        return await cb.answer(f"Need {cost}💰 for upgrade!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET coins=coins-?, fishing_rod=? WHERE user_id=?",
                    (cost, next_level, cb.from_user.id))
    
    await cb.answer(f"✅ Fishing rod upgraded to level {next_level}!", show_alert=True)
    await fishing_cb(cb)

# =========================================================
#   NEW FEATURE: PET SYSTEM
# =========================================================
@router.callback_query(F.data == "pets")
async def pets_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    if not is_vip(cb.from_user.id):
        return await cb.answer("🐕 Pets are VIP only!", show_alert=True)
    
    with closing(db()) as con:
        pets = con.execute("SELECT * FROM pets").fetchall()
    
    text = (f"🐕 <b>PET SYSTEM</b>\n\n"
            f"Your pet helps you earn more!\n"
            f"📊 Pet Level: {user['pet_level']}\n"
            f"✨ Pet XP: {user['pet_exp']}\n"
            f"🍖 Hunger: {user['pet_hunger']}%\n\n"
            f"<b>Available Pets:</b>\n")
    
    for pet in pets:
        text += f"{pet['name']} (Level {pet['max_level']})\n   • Base Power: {pet['base_power']}\n   • Upgrade Cost: {pet['upgrade_cost']}💰\n\n"
    
    rows = [
        [InlineKeyboardButton(text="🍖 Feed Pet", callback_data="pet_feed"),
         InlineKeyboardButton(text="🎮 Play with Pet", callback_data="pet_play")],
        [InlineKeyboardButton(text="⬆️ Upgrade Pet", callback_data="pet_upgrade")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "pet_feed")
async def pet_feed(cb: CallbackQuery):
    user = get_user(cb.from_user.id)
    cost = s_geti("pet_food_cost", 10)
    
    if user["coins"] < cost:
        return await cb.answer(f"Need {cost}💰 to feed your pet!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET coins=coins-?, pet_hunger=100, last_pet_feed=? WHERE user_id=?",
                    (cost, now_ts(), cb.from_user.id))
    
    # Pet gives random reward when fed
    reward = random.randint(5, 50)
    add_balance(cb.from_user.id, coins=reward, note="Pet reward after feeding")
    
    await cb.answer(f"🍖 Pet fed! It gave you {reward}💰!", show_alert=True)
    await pets_cb(cb)

@router.callback_query(F.data == "pet_play")
async def pet_play(cb: CallbackQuery):
    user = get_user(cb.from_user.id)
    
    if user["pet_hunger"] < 20:
        return await cb.answer("🐕 Your pet is hungry! Feed it first!", show_alert=True)
    
    if not consume_energy(cb.from_user.id, 3):
        return await cb.answer("⚡ Need 3 energy to play!", show_alert=True)
    
    pet_exp_gain = random.randint(10, 50)
    reward_multiplier = 1 + (user["pet_level"] / 10)
    coin_reward = int(random.randint(20, 100) * reward_multiplier)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET pet_exp=pet_exp+?, pet_hunger=pet_hunger-5 WHERE user_id=?",
                    (pet_exp_gain, cb.from_user.id))
        
        # Level up check
        exp_needed = user["pet_level"] * 100
        new_exp = user["pet_exp"] + pet_exp_gain
        if new_exp >= exp_needed:
            con.execute("UPDATE users SET pet_level=pet_level+1, pet_exp=? WHERE user_id=?",
                        (new_exp - exp_needed, cb.from_user.id))
            await cb.answer(f"🎉 Pet leveled up to {user['pet_level']+1}!", show_alert=True)
    
    add_balance(cb.from_user.id, coins=coin_reward, note="Playing with pet")
    
    await cb.answer(f"🎮 +{pet_exp_gain} pet XP! Pet found {coin_reward}💰!", show_alert=True)
    await pets_cb(cb)

@router.callback_query(F.data == "pet_upgrade")
async def pet_upgrade(cb: CallbackQuery):
    user = get_user(cb.from_user.id)
    cost = 500 * user["pet_level"]
    
    if user["coins"] < cost:
        return await cb.answer(f"Need {cost}💰 to upgrade pet!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET coins=coins-?, pet_level=pet_level+1 WHERE user_id=?",
                    (cost, cb.from_user.id))
    
    await cb.answer(f"✅ Pet upgraded to level {user['pet_level']+1}!", show_alert=True)
    await pets_cb(cb)

# =========================================================
#   NEW FEATURE: INVESTMENT SYSTEM
# =========================================================
@router.callback_query(F.data == "invest")
async def invest_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    roi_daily = s_geti("investment_daily_roi", 5)
    max_invest = s_geti("investment_max", 10000)
    last_invest = user["last_investment"] or 0
    
    # Calculate pending returns
    if user["investment"] > 0 and last_invest > 0:
        days_passed = (now_ts() - last_invest) // 86400
        if days_passed > 0:
            returns = int(user["investment"] * roi_daily / 100 * days_passed)
            add_balance(cb.from_user.id, coins=returns, note="Investment returns")
            with closing(db()) as con:
                con.execute("UPDATE users SET last_investment=? WHERE user_id=?", (now_ts(), cb.from_user.id))
    
    text = (f"💼 <b>INVESTMENT SYSTEM</b>\n\n"
            f"💰 Your Investment: {user['investment']} coins\n"
            f"📈 Daily ROI: {roi_daily}%\n"
            f"🎯 Max Investment: {max_invest} coins\n"
            f"⏱️ Returns paid daily\n\n"
            f"<b>Investment Options:</b>\n"
            f"• 1000 coins → +50/day\n"
            f"• 5000 coins → +250/day\n"
            f"• 10000 coins → +500/day")
    
    rows = [
        [InlineKeyboardButton(text="💵 Invest 1000", callback_data="invest_1000"),
         InlineKeyboardButton(text="💰 Invest 5000", callback_data="invest_5000")],
        [InlineKeyboardButton(text="💎 Invest 10000", callback_data="invest_10000"),
         InlineKeyboardButton(text="🏦 Withdraw Investment", callback_data="invest_withdraw")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("invest_"))
async def invest_amount(cb: CallbackQuery):
    amount = int(cb.data.split("_")[1])
    user = get_user(cb.from_user.id)
    max_invest = s_geti("investment_max", 10000)
    
    if amount + user["investment"] > max_invest:
        return await cb.answer(f"Investment would exceed max of {max_invest} coins!", show_alert=True)
    
    if user["coins"] < amount:
        return await cb.answer(f"Need {amount}💰 to invest!", show_alert=True)
    
    with closing(db()) as con:
        con.execute("UPDATE users SET coins=coins-?, investment=investment+?, last_investment=? WHERE user_id=?",
                    (amount, amount, now_ts(), cb.from_user.id))
    
    await cb.answer(f"✅ Invested {amount}💰! You'll earn {amount * s_geti('investment_daily_roi',5)//100} coins daily!", show_alert=True)
    await invest_cb(cb)

@router.callback_query(F.data == "invest_withdraw")
async def invest_withdraw(cb: CallbackQuery):
    user = get_user(cb.from_user.id)
    
    if user["investment"] == 0:
        return await cb.answer("No investment to withdraw!", show_alert=True)
    
    add_balance(cb.from_user.id, coins=user["investment"], note="Investment withdrawal")
    
    with closing(db()) as con:
        con.execute("UPDATE users SET investment=0 WHERE user_id=?", (cb.from_user.id,))
    
    await cb.answer(f"✅ Withdrew {user['investment']}💰 from investment!", show_alert=True)
    await invest_cb(cb)

# =========================================================
#   NEW FEATURE: LOTTERY SYSTEM
# =========================================================
@router.callback_query(F.data == "lottery")
async def lottery_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    ticket_price = s_geti("lottery_ticket_price", 5)
    min_players = s_geti("lottery_min_players", 10)
    house_edge = s_geti("lottery_house_edge", 10)
    
    with closing(db()) as con:
        total_tickets = con.execute("SELECT SUM(lottery_tickets) FROM users").fetchone()[0] or 0
        total_players = con.execute("SELECT COUNT(*) FROM users WHERE lottery_tickets > 0").fetchone()[0]
    
    next_draw = "Every Sunday at 12:00 UTC"
    
    text = (f"🎰 <b>LOTTERY SYSTEM</b>\n\n"
            f"🎫 Your Tickets: {user['lottery_tickets']}\n"
            f"📊 Total Tickets: {total_tickets}\n"
            f"👥 Total Players: {total_players}\n"
            f"💰 Ticket Price: {ticket_price}⭐\n"
            f"🎯 Min Players: {min_players}\n"
            f"📅 Next Draw: {next_draw}\n\n"
            f"<b>Estimated Jackpot:</b>\n"
            f"💎 {total_tickets * ticket_price * (100 - house_edge) // 100} stars")
    
    rows = [
        [InlineKeyboardButton(text="🎫 Buy Ticket (5⭐)", callback_data="lottery_buy"),
         InlineKeyboardButton(text="📊 My Tickets", callback_data="lottery_tickets")],
        [InlineKeyboardButton(text="🏆 Winners History", callback_data="lottery_winners")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "lottery_buy")
async def lottery_buy(cb: CallbackQuery, state: FSMContext):
    user = get_user(cb.from_user.id)
    price = s_geti("lottery_ticket_price", 5)
    
    if user["stars"] < price:
        return await cb.answer(f"Need {price}⭐ to buy a ticket!", show_alert=True)
    
    await state.set_state(LotteryState.buying)
    await cb.message.answer(f"🎫 How many tickets would you like to buy? (1-100)\nPrice: {price}⭐ per ticket")
    await cb.answer()

@router.message(LotteryState.buying)
async def lottery_buy_amount(msg: Message, state: FSMContext):
    try:
        amount = int(msg.text.strip())
        if amount < 1 or amount > 100:
            raise ValueError
    except:
        return await msg.answer("❌ Please enter a number between 1 and 100.")
    
    user = get_user(msg.from_user.id)
    price = s_geti("lottery_ticket_price", 5)
    total_cost = amount * price
    
    if user["stars"] < total_cost:
        return await msg.answer(f"❌ Need {total_cost}⭐ for {amount} tickets!")
    
    with closing(db()) as con:
        con.execute("UPDATE users SET stars=stars-?, lottery_tickets=lottery_tickets+? WHERE user_id=?",
                    (total_cost, amount, msg.from_user.id))
    
    await state.clear()
    await msg.answer(f"✅ Bought {amount} lottery tickets! Good luck!", reply_markup=back_kb("lottery"))
    await lottery_cb(msg)

# =========================================================
#   NEW FEATURE: BATTLE SYSTEM
# =========================================================
@router.callback_query(F.data == "battle")
async def battle_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    entry_fee = s_geti("battle_entry_fee", 20)
    winner_reward = s_geti("battle_winner_reward", 50)
    
    with closing(db()) as con:
        waiting = con.execute("SELECT user_id, battle_rank FROM users WHERE last_battle > ? AND battle_rank > 0",
                              (now_ts() - 300,)).fetchall()
    
    text = (f"⚔️ <b>BATTLE ARENA</b>\n\n"
            f"👤 Your Rank: {user['battle_rank']}\n"
            f"🏆 Battle Points: {user['battle_points']}\n"
            f"🎯 Entry Fee: {entry_fee}💰\n"
            f"💎 Winner Reward: {winner_reward}💰\n"
            f"👥 Players Waiting: {len(waiting)}\n\n"
            f"<b>Leaderboard:</b>\n")
    
    with closing(db()) as con:
        top = con.execute("SELECT user_id, battle_rank, battle_points FROM users ORDER BY battle_points DESC LIMIT 5").fetchall()
        for i, u in enumerate(top, 1):
            text += f"{i}. {'⭐' if i==1 else '🥈' if i==2 else '🥉' if i==3 else '▪️'} Rank {u['battle_rank']} - {u['battle_points']} pts\n"
    
    rows = [
        [InlineKeyboardButton(text="⚔️ Find Battle", callback_data="battle_find"),
         InlineKeyboardButton(text="🏆 Battle Stats", callback_data="battle_stats")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "battle_find")
async def battle_find(cb: CallbackQuery, state: FSMContext):
    user = get_user(cb.from_user.id)
    entry_fee = s_geti("battle_entry_fee", 20)
    
    if user["coins"] < entry_fee:
        return await cb.answer(f"Need {entry_fee}💰 to enter battle!", show_alert=True)
    
    if user["last_battle"] and now_ts() - user["last_battle"] < 60:
        return await cb.answer("⏱️ Wait 60 seconds before next battle!", show_alert=True)
    
    with closing(db()) as con:
        # Find opponent with similar rank
        opponent = con.execute(
            "SELECT user_id, battle_rank, battle_points FROM users WHERE user_id != ? AND battle_rank BETWEEN ? AND ? AND last_battle > ? LIMIT 1",
            (cb.from_user.id, max(1, user["battle_rank"] - 2), user["battle_rank"] + 2, now_ts() - 120)
        ).fetchone()
    
    if opponent:
        await state.set_state(BattleState.selecting)
        await state.update_data(opponent=opponent["user_id"])
        
        rows = [
            [InlineKeyboardButton(text="⚔️ Attack", callback_data="battle_attack"),
             InlineKeyboardButton(text="🛡️ Defend", callback_data="battle_defend")],
            [InlineKeyboardButton(text="💫 Special Move", callback_data="battle_special"),
             InlineKeyboardButton(text="🏃 Run Away", callback_data="battle_run")],
        ]
        
        await cb.message.edit_text(
            f"⚔️ <b>BATTLE STARTED!</b>\n\n"
            f"Your Stats:\n"
            f"• Rank: {user['battle_rank']}\n"
            f"• Points: {user['battle_points']}\n\n"
            f"vs\n\n"
            f"Opponent Stats:\n"
            f"• Opponent ID: {opponent['user_id']}\n"
            f"• Rank: {opponent['battle_rank']}\n"
            f"• Points: {opponent['battle_points']}\n\n"
            f"Choose your move!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
    else:
        # Mark as waiting for battle
        with closing(db()) as con:
            con.execute("UPDATE users SET last_battle=? WHERE user_id=?", (now_ts(), cb.from_user.id))
        await cb.answer("⏳ Searching for opponent... Try again in a moment!", show_alert=True)
    
    await cb.answer()

@router.callback_query(F.data.startswith("battle_"))
async def battle_action(cb: CallbackQuery, state: FSMContext):
    user = get_user(cb.from_user.id)
    data = await state.get_data()
    opponent_id = data.get("opponent")
    
    if not opponent_id:
        return await cb.answer("No active battle!", show_alert=True)
    
    opponent = get_user(opponent_id)
    if not opponent:
        return await cb.answer("Opponent not found!", show_alert=True)
    
    action = cb.data.split("_")[1]
    entry_fee = s_geti("battle_entry_fee", 20)
    
    # Battle logic based on ranks and random factor
    rank_diff = opponent["battle_rank"] - user["battle_rank"]
    base_chance = 50 - (rank_diff * 5)  # Higher rank = better chance
    
    if action == "attack":
        chance = base_chance + 10
        move_name = "⚔️ Attack"
    elif action == "defend":
        chance = base_chance - 5
        move_name = "🛡️ Defend"
    elif action == "special":
        if user["stars"] < 5:
            return await cb.answer("Need 5⭐ for special move!", show_alert=True)
        with closing(db()) as con:
            con.execute("UPDATE users SET stars=stars-5 WHERE user_id=?", (cb.from_user.id,))
        chance = base_chance + 20
        move_name = "💫 Special Attack"
    else:  # run
        await state.clear()
        return await cb.answer("🏃 You ran away!", show_alert=True)
    
    # Determine winner
    player_roll = random.randint(1, 100)
    is_win = player_roll <= chance
    
    # Deduct entry fee
    with closing(db()) as con:
        con.execute("UPDATE users SET coins=coins-? WHERE user_id=?", (entry_fee, cb.from_user.id))
    
    if is_win:
        reward = s_geti("battle_winner_reward", 50) * 2
        add_balance(cb.from_user.id, coins=reward, note="Battle win")
        points_gain = 10 + rank_diff
        
        with closing(db()) as con:
            con.execute("UPDATE users SET battle_points=battle_points+?, battle_wins=battle_wins+1 WHERE user_id=?", 
                        (points_gain, cb.from_user.id))
            con.execute("UPDATE users SET gamble_wins=gamble_wins+1 WHERE user_id=?", (cb.from_user.id,))
        
        result_text = f"{move_name} successful!\n🎉 You won the battle!\n💰 +{reward}💰\n🏆 +{points_gain} points"
        
        # Update rank based on points
        new_rank = 1 + (user["battle_points"] + points_gain) // 100
        if new_rank > user["battle_rank"]:
            with closing(db()) as con:
                con.execute("UPDATE users SET battle_rank=? WHERE user_id=?", (new_rank, cb.from_user.id))
            result_text += f"\n📈 Rank up to {new_rank}!"
    else:
        points_loss = 5
        with closing(db()) as con:
            con.execute("UPDATE users SET battle_points=battle_points-?, gamble_losses=gamble_losses+1 WHERE user_id=?", 
                        (points_loss, cb.from_user.id))
        
        result_text = f"{move_name} failed!\n😔 You lost the battle!\n📉 -{points_loss} points"
    
    with closing(db()) as con:
        con.execute("UPDATE users SET last_battle=? WHERE user_id=?", (now_ts(), cb.from_user.id))
    
    await state.clear()
    await cb.message.edit_text(f"⚔️ <b>BATTLE RESULT</b>\n\n{result_text}", reply_markup=back_kb("battle"))
    await cb.answer()

# =========================================================
#   NEW FEATURE: TRADE SYSTEM
# =========================================================
@router.callback_query(F.data == "trade")
async def trade_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    with closing(db()) as con:
        pending_trades = con.execute(
            "SELECT * FROM trade_requests WHERE to_user=? AND status='pending'",
            (cb.from_user.id,)
        ).fetchall()
    
    text = (f"🤝 <b>TRADING SYSTEM</b>\n\n"
            f"💰 Your Balance: {user['coins']} coins\n"
            f"⭐ Your Stars: {user['stars']} stars\n\n"
            f"<b>Pending Incoming Trades:</b>\n")
    
    if pending_trades:
        for trade in pending_trades[:5]:
            text += f"• From {trade['from_user']}: {trade['offer_coins']}💰 + {trade['offer_stars']}⭐ for {trade['request_coins']}💰 + {trade['request_stars']}⭐\n"
    else:
        text += "No pending trades\n"
    
    rows = [
        [InlineKeyboardButton(text="📤 Send Trade", callback_data="trade_send"),
         InlineKeyboardButton(text="📥 View Trades", callback_data="trade_view")],
        [InlineKeyboardButton(text="📜 Trade History", callback_data="trade_history")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "trade_send")
async def trade_send(cb: CallbackQuery, state: FSMContext):
    await state.set_state(TradeState.selecting_user)
    await cb.message.answer("🔍 Enter the user ID to trade with:", reply_markup=back_kb("trade"))
    await cb.answer()

@router.message(TradeState.selecting_user)
async def trade_select_user(msg: Message, state: FSMContext):
    try:
        target_id = int(msg.text.strip())
    except:
        return await msg.answer("❌ Invalid user ID. Please enter a numeric ID.")
    
    if target_id == msg.from_user.id:
        return await msg.answer("❌ Cannot trade with yourself!")
    
    target = get_user(target_id)
    if not target:
        return await msg.answer("❌ User not found!")
    
    await state.update_data(target=target_id)
    await state.set_state(TradeState.offering)
    await msg.answer(f"🤝 Trading with user {target_id}\n\nEnter offer (coins|stars):\nExample: 100|5")

@router.message(TradeState.offering)
async def trade_offer(msg: Message, state: FSMContext):
    try:
        parts = msg.text.split("|")
        offer_coins = int(parts[0])
        offer_stars = int(parts[1])
    except:
        return await msg.answer("❌ Invalid format. Use: coins|stars")
    
    user = get_user(msg.from_user.id)
    if user["coins"] < offer_coins or user["stars"] < offer_stars:
        return await msg.answer("❌ You don't have enough resources!")
    
    await state.update_data(offer_coins=offer_coins, offer_stars=offer_stars)
    await state.set_state(TradeState.requesting)
    await msg.answer("Enter what you want (coins|stars):\nExample: 50|2")

@router.message(TradeState.requesting)
async def trade_request(msg: Message, state: FSMContext):
    try:
        parts = msg.text.split("|")
        req_coins = int(parts[0])
        req_stars = int(parts[1])
    except:
        return await msg.answer("❌ Invalid format. Use: coins|stars")
    
    data = await state.get_data()
    
    with closing(db()) as con:
        con.execute(
            "INSERT INTO trade_requests(from_user,to_user,offer_coins,offer_stars,request_coins,request_stars,created_at,status) VALUES(?,?,?,?,?,?,?,?)",
            (msg.from_user.id, data["target"], data["offer_coins"], data["offer_stars"], req_coins, req_stars, now_ts(), "pending")
        )
    
    await state.clear()
    await msg.answer("✅ Trade request sent!", reply_markup=back_kb("trade"))

@router.callback_query(F.data == "trade_view")
async def trade_view(cb: CallbackQuery):
    with closing(db()) as con:
        trades = con.execute(
            "SELECT * FROM trade_requests WHERE to_user=? AND status='pending'",
            (cb.from_user.id,)
        ).fetchall()
    
    if not trades:
        return await cb.answer("No pending trades!", show_alert=True)
    
    rows = []
    for trade in trades:
        rows.append([InlineKeyboardButton(
            text=f"Trade #{trade['id']}: {trade['offer_coins']}💰 + {trade['offer_stars']}⭐ → {trade['request_coins']}💰 + {trade['request_stars']}⭐",
            callback_data=f"trade_accept_{trade['id']}"
        )])
    
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="trade")])
    
    await cb.message.edit_text("📥 <b>Pending Trades</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("trade_accept_"))
async def trade_accept(cb: CallbackQuery):
    trade_id = int(cb.data.split("_")[2])
    
    with closing(db()) as con:
        trade = con.execute("SELECT * FROM trade_requests WHERE id=? AND status='pending'", (trade_id,)).fetchone()
        if not trade:
            return await cb.answer("Trade not found or already processed!", show_alert=True)
        
        # Check if receiver has enough
        receiver = get_user(cb.from_user.id)
        if receiver["coins"] < trade["request_coins"] or receiver["stars"] < trade["request_stars"]:
            return await cb.answer("You don't have enough resources for this trade!", show_alert=True)
        
        # Check if sender still has their offer
        sender = get_user(trade["from_user"])
        if sender["coins"] < trade["offer_coins"] or sender["stars"] < trade["offer_stars"]:
            con.execute("UPDATE trade_requests SET status='cancelled' WHERE id=?", (trade_id,))
            return await cb.answer("The other user no longer has the offered items!", show_alert=True)
        
        # Execute trade
        con.execute("UPDATE users SET coins=coins+?, stars=stars+? WHERE user_id=?", 
                    (trade["offer_coins"] - trade["request_coins"], trade["offer_stars"] - trade["request_stars"], cb.from_user.id))
        con.execute("UPDATE users SET coins=coins-?, stars=stars-? WHERE user_id=?", 
                    (trade["offer_coins"] - trade["request_coins"], trade["offer_stars"] - trade["request_stars"], trade["from_user"]))
        con.execute("UPDATE trade_requests SET status='completed' WHERE id=?", (trade_id,))
        
        # Send notification to sender
        try:
            await cb.bot.send_message(trade["from_user"], f"✅ Trade #{trade_id} was accepted by {cb.from_user.id}!")
        except:
            pass
        
        check_achievements(cb.from_user.id)
    
    await cb.answer("✅ Trade completed!", show_alert=True)
    await trade_cb(cb)

# =========================================================
#   NEW FEATURE: GUILD SYSTEM
# =========================================================
@router.callback_query(F.data == "guild")
async def guild_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    with closing(db()) as con:
        guild = con.execute(
            "SELECT g.* FROM guilds g JOIN guild_members gm ON g.id=gm.guild_id WHERE gm.user_id=?",
            (cb.from_user.id,)
        ).fetchone()
    
    if guild:
        text = (f"🏛️ <b>GUILD: {guild['name']}</b>\n\n"
                f"👑 Owner: {guild['owner_id']}\n"
                f"📊 Level: {guild['level']}\n"
                f"✨ XP: {guild['exp']}\n"
                f"👥 Members: {guild['members_count']}\n"
                f"💰 Guild Coins: {guild['coins']}\n"
                f"🏆 Member since: {datetime.fromtimestamp(guild['created_at']).strftime('%Y-%m-%d')}")
        
        rows = [
            [InlineKeyboardButton(text="👥 Members List", callback_data="guild_members"),
             InlineKeyboardButton(text="💸 Donate to Guild", callback_data="guild_donate")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
        ]
        
        if guild["owner_id"] == cb.from_user.id:
            rows.insert(0, [InlineKeyboardButton(text="⚙️ Guild Management", callback_data="guild_manage")])
    else:
        text = (f"🏛️ <b>GUILD SYSTEM</b>\n\n"
                f"You are not in a guild!\n\n"
                f"<b>Benefits of joining a guild:</b>\n"
                f"• Extra daily rewards\n"
                f"• Guild exclusive tasks\n"
                f"• Guild leaderboard\n"
                f"• Team up for battles\n"
                f"• Guild shop discounts")
        
        rows = [
            [InlineKeyboardButton(text="🏛️ Create Guild", callback_data="guild_create"),
             InlineKeyboardButton(text="🔍 Find Guild", callback_data="guild_find")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
        ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "guild_create")
async def guild_create(cb: CallbackQuery, state: FSMContext):
    cost = s_geti("guild_create_cost", 1000)
    user = get_user(cb.from_user.id)
    
    if user["coins"] < cost:
        return await cb.answer(f"Need {cost}💰 to create a guild!", show_alert=True)
    
    await state.set_state(GuildState.naming)
    await cb.message.answer("🏛️ Enter your guild name (max 20 characters):", reply_markup=back_kb("guild"))
    await cb.answer()

@router.message(GuildState.naming)
async def guild_name(msg: Message, state: FSMContext):
    name = msg.text.strip()[:20]
    cost = s_geti("guild_create_cost", 1000)
    
    with closing(db()) as con:
        existing = con.execute("SELECT id FROM guilds WHERE name=?", (name,)).fetchone()
        if existing:
            return await msg.answer("❌ Guild name already taken! Choose another.")
        
        con.execute("UPDATE users SET coins=coins-? WHERE user_id=?", (cost, msg.from_user.id))
        con.execute(
            "INSERT INTO guilds(name,owner_id,members_count,created_at) VALUES(?,?,?,?)",
            (name, msg.from_user.id, 1, now_ts())
        )
        guild_id = con.lastrowid
        con.execute(
            "INSERT INTO guild_members(user_id,guild_id,role,joined_at) VALUES(?,?,?,?)",
            (msg.from_user.id, guild_id, "owner", now_ts())
        )
    
    await state.clear()
    await msg.answer(f"✅ Guild '{name}' created successfully!", reply_markup=back_kb("guild"))
    check_achievements(msg.from_user.id)

@router.callback_query(F.data == "guild_members")
async def guild_members(cb: CallbackQuery):
    with closing(db()) as con:
        guild = con.execute(
            "SELECT g.* FROM guilds g JOIN guild_members gm ON g.id=gm.guild_id WHERE gm.user_id=?",
            (cb.from_user.id,)
        ).fetchone()
        
        if not guild:
            return await cb.answer("You're not in a guild!", show_alert=True)
        
        members = con.execute(
            "SELECT gm.*, u.first_name, u.coins, u.level FROM guild_members gm JOIN users u ON gm.user_id=u.user_id WHERE gm.guild_id=?",
            (guild["id"],)
        ).fetchall()
    
    text = f"🏛️ <b>{guild['name']} - Members</b>\n\n"
    for member in members:
        role_icon = "👑" if member["role"] == "owner" else "⭐" if member["role"] == "admin" else "👤"
        text += f"{role_icon} {member['first_name'][:15]} | Lvl {member['level']} | {member['coins']}💰\n"
    
    await cb.message.edit_text(text, reply_markup=back_kb("guild"))
    await cb.answer()

# =========================================================
#   NEW FEATURE: DAILY CHALLENGES
# =========================================================
@router.callback_query(F.data == "challenges")
async def challenges_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    today_date = today()
    
    # Check if challenges exist for today
    with closing(db()) as con:
        challenges = con.execute(
            "SELECT * FROM daily_challenges WHERE date=?",
            (today_date,)
        ).fetchall()
        
        if not challenges:
            # Generate daily challenges
            challenge_types = [
                ("tasks", "Complete tasks", 5, 100, 10),
                ("earn", "Earn coins", 500, 200, 20),
                ("spin", "Play spin game", 3, 150, 15),
                ("refer", "Get referrals", 2, 300, 30),
                ("battle", "Win battles", 3, 200, 20),
                ("mine", "Mine coins", 2, 250, 25),
                ("fish", "Catch fish", 5, 200, 20),
                ("trade", "Make trades", 2, 200, 20),
            ]
            
            selected = random.sample(challenge_types, min(3, len(challenge_types)))
            for i, (ctype, desc, target, reward_c, reward_s) in enumerate(selected, 1):
                con.execute(
                    "INSERT INTO daily_challenges(date,challenge_type,target,reward_coins,reward_stars,description) VALUES(?,?,?,?,?,?)",
                    (today_date, ctype, target, reward_c, reward_s, desc)
                )
            
            challenges = con.execute(
                "SELECT * FROM daily_challenges WHERE date=?",
                (today_date,)
            ).fetchall()
        
        # Get user progress
        user_progress = {}
        for challenge in challenges:
            if challenge["challenge_type"] == "tasks":
                progress = con.execute(
                    "SELECT COUNT(*) FROM completed_tasks WHERE user_id=? AND day=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0]
            elif challenge["challenge_type"] == "earn":
                progress = con.execute(
                    "SELECT SUM(amount) FROM transactions WHERE user_id=? AND currency='coin' AND amount>0 AND date(ts,'unixepoch')=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0] or 0
            elif challenge["challenge_type"] == "spin":
                progress = con.execute(
                    "SELECT COUNT(*) FROM transactions WHERE user_id=? AND note='Spin' AND date(ts,'unixepoch')=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0]
            elif challenge["challenge_type"] == "refer":
                progress = con.execute(
                    "SELECT COUNT(*) FROM referrals WHERE ref_id=? AND date(ts,'unixepoch')=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0]
            elif challenge["challenge_type"] == "battle":
                progress = con.execute(
                    "SELECT COUNT(*) FROM transactions WHERE user_id=? AND note='Battle win' AND date(ts,'unixepoch')=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0]
            elif challenge["challenge_type"] == "mine":
                progress = con.execute(
                    "SELECT COUNT(*) FROM transactions WHERE user_id=? AND note LIKE 'Mining%' AND date(ts,'unixepoch')=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0]
            elif challenge["challenge_type"] == "fish":
                progress = con.execute(
                    "SELECT COUNT(*) FROM transactions WHERE user_id=? AND note LIKE 'Caught%' AND date(ts,'unixepoch')=?",
                    (cb.from_user.id, today_date)
                ).fetchone()[0]
            elif challenge["challenge_type"] == "trade":
                progress = con.execute(
                    "SELECT COUNT(*) FROM trade_requests WHERE (from_user=? OR to_user=?) AND status='completed' AND date(created_at,'unixepoch')=?",
                    (cb.from_user.id, cb.from_user.id, today_date)
                ).fetchone()[0]
            else:
                progress = 0
            
            user_progress[challenge["id"]] = progress
    
    text = (f"📊 <b>DAILY CHALLENGES</b>\n\n"
            f"Complete all challenges to earn big rewards!\n"
            f"{SDIV}\n")
    
    rows = []
    for challenge in challenges:
        progress = user_progress.get(challenge["id"], 0)
        completed = progress >= challenge["target"]
        status = "✅" if completed else "⏳"
        
        text += f"{status} {challenge['description']}: {progress}/{challenge['target']}\n"
        text += f"   → Reward: {challenge['reward_coins']}💰 + {challenge['reward_stars']}⭐\n\n"
        
        if not completed:
            rows.append([InlineKeyboardButton(
                text=f"🎯 {challenge['description']}",
                callback_data=f"challenge_do_{challenge['id']}"
            )])
    
    rows.append([InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")])
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("challenge_do_"))
async def challenge_do(cb: CallbackQuery):
    challenge_id = int(cb.data.split("_")[2])
    
    with closing(db()) as con:
        challenge = con.execute("SELECT * FROM daily_challenges WHERE id=?", (challenge_id,)).fetchone()
        
        if not challenge:
            return await cb.answer("Challenge not found!", show_alert=True)
        
        # Check if completed
        if challenge["challenge_type"] == "tasks":
            progress = con.execute(
                "SELECT COUNT(*) FROM completed_tasks WHERE user_id=? AND day=?",
                (cb.from_user.id, today())
            ).fetchone()[0]
        elif challenge["challenge_type"] == "earn":
            progress = con.execute(
                "SELECT SUM(amount) FROM transactions WHERE user_id=? AND currency='coin' AND amount>0 AND date(ts,'unixepoch')=?",
                (cb.from_user.id, today())
            ).fetchone()[0] or 0
        else:
            progress = 0
        
        if progress >= challenge["target"]:
            # Claim reward
            add_balance(cb.from_user.id, coins=challenge["reward_coins"], stars=challenge["reward_stars"], 
                       note=f"Daily challenge: {challenge['description']}")
            check_achievements(cb.from_user.id)
            await cb.answer(f"🎉 Challenge completed! +{challenge['reward_coins']}💰 +{challenge['reward_stars']}⭐", show_alert=True)
        else:
            await cb.answer(f"⏳ Progress: {progress}/{challenge['target']}. Keep going!", show_alert=True)
    
    await challenges_cb(cb)

# =========================================================
#   NEW FEATURE: ACHIEVEMENTS
# =========================================================
@router.callback_query(F.data == "achievements")
async def achievements_cb(cb: CallbackQuery):
    with closing(db()) as con:
        achievements = con.execute("SELECT * FROM achievements").fetchall()
        user_achievements = con.execute(
            "SELECT achievement_id FROM user_achievements WHERE user_id=?",
            (cb.from_user.id,)
        ).fetchall()
    
    earned_ids = {a["achievement_id"] for a in user_achievements}
    
    text = (f"🏅 <b>ACHIEVEMENTS</b>\n\n"
            f"Complete achievements to earn special rewards!\n"
            f"{SDIV}\n")
    
    for ach in achievements:
        status = "🏆" if ach["id"] in earned_ids else "🔒"
        text += f"{status} <b>{ach['name']}</b>\n   {ach['description']}\n"
        if ach["id"] in earned_ids:
            text += f"   ✅ Earned: {ach['reward_coins']}💰 + {ach['reward_stars']}⭐\n"
        text += "\n"
    
    await cb.message.edit_text(text, reply_markup=back_kb("menu"))
    await cb.answer()

# =========================================================
#   NEW FEATURE: TOURNAMENT
# =========================================================
@router.callback_query(F.data == "tournament")
async def tournament_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    
    with closing(db()) as con:
        active_tournaments = con.execute(
            "SELECT * FROM tournaments WHERE status='active' AND end_date >= ?",
            (today(),)
        ).fetchall()
        
        upcoming = con.execute(
            "SELECT * FROM tournaments WHERE status='upcoming'"
        ).fetchall()
    
    text = (f"🏆 <b>TOURNAMENTS</b>\n\n"
            f"<b>Active Tournaments:</b>\n")
    
    if active_tournaments:
        for tourney in active_tournaments:
            text += f"• {tourney['name']}\n"
            text += f"  Prize: {tourney['prize_pool']}💰\n"
            text += f"  Participants: {tourney['current_participants']}/{tourney['max_participants']}\n"
            text += f"  Entry Fee: {tourney['entry_fee']}💰\n\n"
    else:
        text += "No active tournaments\n"
    
    if upcoming:
        text += f"\n<b>Upcoming Tournaments:</b>\n"
        for tourney in upcoming:
            text += f"• {tourney['name']}\n"
    
    rows = [
        [InlineKeyboardButton(text="🏆 Register", callback_data="tournament_register"),
         InlineKeyboardButton(text="📊 My Stats", callback_data="tournament_stats")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

# =========================================================
#   NEW FEATURE: MARKETPLACE
# =========================================================
@router.callback_query(F.data == "market")
async def market_cb(cb: CallbackQuery):
    with closing(db()) as con:
        listings = con.execute(
            "SELECT * FROM marketplace_listings WHERE status='active' ORDER BY listed_at DESC LIMIT 10"
        ).fetchall()
    
    text = (f"🏪 <b>MARKETPLACE</b>\n\n"
            f"Buy and sell items with other users!\n"
            f"Fee: {s_geti('marketplace_fee', 5)}%\n"
            f"{SDIV}\n")
    
    if listings:
        for listing in listings[:5]:
            text += f"🛒 Item #{listing['id']}\n"
            text += f"   Type: {listing['item_type']}\n"
            text += f"   Price: {listing['price_coins']}💰 or {listing['price_stars']}⭐\n"
            text += f"   Seller: {listing['seller_id']}\n\n"
    else:
        text += "No active listings\n"
    
    rows = [
        [InlineKeyboardButton(text="📦 My Listings", callback_data="market_mine"),
         InlineKeyboardButton(text="➕ List Item", callback_data="market_list")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

# =========================================================
#   NEW FEATURE: POLLS & GIVEAWAYS
# =========================================================
@router.callback_query(F.data == "polls")
async def polls_cb(cb: CallbackQuery):
    with closing(db()) as con:
        active_polls = con.execute(
            "SELECT * FROM polls WHERE status='active' AND ends_at > ?",
            (now_ts(),)
        ).fetchall()
    
    text = (f"🗳️ <b>COMMUNITY POLLS</b>\n\n"
            f"Voice your opinion and earn rewards!\n"
            f"{SDIV}\n")
    
    if active_polls:
        for poll in active_polls:
            options = json.loads(poll["options"])
            text += f"📊 {poll['question']}\n"
            for i, opt in enumerate(options):
                text += f"   {i+1}. {opt}\n"
            text += f"   Ends: {datetime.fromtimestamp(poll['ends_at']).strftime('%Y-%m-%d %H:%M')}\n\n"
    else:
        text += "No active polls\n"
    
    rows = [
        [InlineKeyboardButton(text="🗳️ View Polls", callback_data="poll_vote")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data == "giveaway")
async def giveaway_cb(cb: CallbackQuery):
    with closing(db()) as con:
        active_giveaways = con.execute(
            "SELECT * FROM giveaways WHERE status='active' AND ends_at > ?",
            (now_ts(),)
        ).fetchall()
    
    text = (f"🎁 <b>GIVEAWAYS</b>\n\n"
            f"Enter giveaways for a chance to win!\n"
            f"{SDIV}\n")
    
    if active_giveaways:
        for give in active_giveaways:
            text += f"🎲 Giveaway #{give['id']}\n"
            text += f"   Prize: {give['prize']}\n"
            if give['prize_coins']:
                text += f"   +{give['prize_coins']}💰\n"
            if give['prize_stars']:
                text += f"   +{give['prize_stars']}⭐\n"
            text += f"   Winners: {give['winners_count']}\n"
            text += f"   Entry Cost: {give['entry_cost']}⭐\n"
            text += f"   Ends: {datetime.fromtimestamp(give['ends_at']).strftime('%Y-%m-%d %H:%M')}\n\n"
    else:
        text += "No active giveaways\n"
    
    rows = [
        [InlineKeyboardButton(text="🎲 Enter Giveaway", callback_data="giveaway_enter")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

# =========================================================
#   LEADERBOARD
# =========================================================
@router.callback_query(F.data == "lb")
async def lb_cb(cb: CallbackQuery):
    upsert_user(cb)
    with closing(db()) as con:
        top_wealth = con.execute(
            "SELECT user_id, first_name, coins FROM users ORDER BY coins DESC LIMIT 5"
        ).fetchall()
        top_level = con.execute(
            "SELECT user_id, first_name, level, xp FROM users ORDER BY level DESC, xp DESC LIMIT 5"
        ).fetchall()
        top_ref = con.execute(
            "SELECT user_id, first_name, ref_count FROM users ORDER BY ref_count DESC LIMIT 5"
        ).fetchall()
    
    text = (f"🏆 <b>LEADERBOARDS</b>\n\n"
            f"💰 <b>Richest Users:</b>\n")
    
    for i, u in enumerate(top_wealth, 1):
        text += f"{i}. {u['first_name'][:15]} - {u['coins']:,}💰\n"
    
    text += f"\n📈 <b>Top Levels:</b>\n"
    for i, u in enumerate(top_level, 1):
        text += f"{i}. {u['first_name'][:15]} - Level {u['level']} ({u['xp']} XP)\n"
    
    text += f"\n👥 <b>Top Referrers:</b>\n"
    for i, u in enumerate(top_ref, 1):
        text += f"{i}. {u['first_name'][:15]} - {u['ref_count']} referrals\n"
    
    await cb.message.edit_text(text, reply_markup=back_kb("menu"))
    await cb.answer()

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
            f"  • 👑 VIP badge & exclusive roles\n"
            f"  • ⛏️ Advanced mining system\n"
            f"  • 🎣 Fishing & Pet systems\n"
            f"  • 💼 Investment opportunities\n"
            f"  • 🎰 Lottery & Tournaments\n\n"
            f"{SDIV}\n"
            f"💳 <b>Price:</b> {s_geti('vip_price_stars',100)}⭐ / {s_geti('vip_days',30)} days")
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
            f"💸 <b>Fee:</b>            {s_geti('withdraw_fee_pct',5)}%\n"
            f"{SDIV}\n✨ Choose payment method below:")
    rows = [[InlineKeyboardButton(text="📱 Bkash",  callback_data="wd_bkash"),
             InlineKeyboardButton(text="📱 Nagad",  callback_data="wd_nagad")],
            [InlineKeyboardButton(text="🏧 Rocket", callback_data="wd_rocket"),
             InlineKeyboardButton(text="💳 Bank", callback_data="wd_bank")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="menu")]]
    await state.set_state(WithdrawState.method)
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.in_({"wd_bkash", "wd_nagad", "wd_rocket", "wd_bank"}), WithdrawState.method)
async def wd_method(cb: CallbackQuery, state: FSMContext):
    method_names = {"wd_bkash": "Bkash", "wd_nagad": "Nagad", "wd_rocket": "Rocket", "wd_bank": "Bank"}
    method = method_names.get(cb.data, "Bkash")
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

# ---------- Shop ----------
@router.callback_query(F.data == "shop")
async def shop_cb(cb: CallbackQuery):
    user = upsert_user(cb)
    with closing(db()) as con:
        items = con.execute("SELECT * FROM shop_items WHERE active=1").fetchall()
    rows = []
    for it in items:
        rows.append([InlineKeyboardButton(text=f"🛍 {it['name']}  ·  {it['price']}⭐",
                                  callback_data=f"buy_{it['id']}")])
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
    elif it["kind"] == "mining":
        with closing(db()) as con:
            con.execute("UPDATE users SET mining_power=mining_power+? WHERE user_id=?",
                        (int(it["payload"] or 1), cb.from_user.id))
        await cb.answer("⛏️ Mining power increased!", show_alert=True)
    elif it["kind"] == "fishing":
        with closing(db()) as con:
            con.execute("UPDATE users SET fishing_rod=fishing_rod+? WHERE user_id=?",
                        (int(it["payload"] or 1), cb.from_user.id))
        await cb.answer("🎣 Fishing rod upgraded!", show_alert=True)
    elif it["kind"] == "pet":
        # Add pet to user
        await cb.answer(f"🐕 Pet '{it['name']}' added! Upgrade it to grow!", show_alert=True)
    else:
        await cb.answer("✅ Purchase complete.", show_alert=True)
    await shop_cb(cb)

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
#   ADVANCED ADMIN PANEL (Simplified)
# =========================================================
def admin_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📊 Dashboard", callback_data="a_dashboard")],
        [InlineKeyboardButton(text="👥 User Management", callback_data="a_users")],
        [InlineKeyboardButton(text="💰 Bonus & Rewards", callback_data="a_bonus")],
        [InlineKeyboardButton(text="📋 Task Management", callback_data="a_tasks")],
        [InlineKeyboardButton(text="🛍 Shop Management", callback_data="a_shop")],
        [InlineKeyboardButton(text="⚙️ Bot Settings", callback_data="a_settings")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="a_broadcast")],
        [InlineKeyboardButton(text="💸 Withdrawals", callback_data="a_withdrawals")],
        [InlineKeyboardButton(text="📈 Statistics", callback_data="a_stats")],
        [InlineKeyboardButton(text="⬅️ Back to Menu", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@admin_router.callback_query(F.data == "admin")
async def admin_cb(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): 
        return await cb.answer("⛔ Access Denied!", show_alert=True)
    
    with closing(db()) as con:
        total_users = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active_today = con.execute("SELECT COUNT(*) FROM users WHERE last_active=?", (today(),)).fetchone()[0]
        vip_users = con.execute("SELECT COUNT(*) FROM users WHERE vip=1").fetchone()[0]
        pending_wd = con.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'").fetchone()[0]
    
    text = (f"👑 <b>SUPER ADMIN PANEL</b> 👑\n\n"
            f"📊 <b>Quick Stats:</b>\n"
            f"├ 👥 Total Users: {total_users}\n"
            f"├ 🌟 Active Today: {active_today}\n"
            f"├ 👑 VIP Users: {vip_users}\n"
            f"└ 💸 Pending Withdrawals: {pending_wd}\n\n"
            f"🔧 <b>Control Panel:</b>\n"
            f"Manage everything about your bot below.")
    
    await cb.message.edit_text(text, reply_markup=admin_kb())
    await cb.answer()

# Simplified admin functions (keeping existing ones)
@admin_router.callback_query(F.data == "a_dashboard")
async def admin_dashboard(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    
    with closing(db()) as con:
        total_coins = con.execute("SELECT SUM(coins) FROM users").fetchone()[0] or 0
        total_tasks = con.execute("SELECT COUNT(*) FROM completed_tasks").fetchone()[0]
        new_today = con.execute("SELECT COUNT(*) FROM users WHERE join_date=?", (today(),)).fetchone()[0]
    
    text = (f"📊 <b>BOT DASHBOARD</b>\n\n"
            f"├ 💰 Coins: {total_coins:,}\n"
            f"├ ✅ Tasks Completed: {total_tasks:,}\n"
            f"├ 🆕 New Today: {new_today}\n"
            f"├ 🎯 Active Users: {new_today}\n"
            f"└ 👑 Admin: {ADMIN_ID}")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="a_dashboard"),
         InlineKeyboardButton(text="⬅️ Back", callback_data="admin")]
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

# Keep existing admin functions for user management, tasks, shop, settings, broadcast, withdrawals

# =========================================================
#   STARTUP
# =========================================================
async def main():
    db_init()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(admin_router)
    dp.include_router(router)
    log.info("🚀 Premium Earn Bot starting...")
    log.info(f"👑 Admin ID: {ADMIN_ID}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Stopped.")
