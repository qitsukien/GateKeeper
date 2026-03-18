import asyncio
import csv
import json
import os
import re
import sqlite3
import threading
import time
import unicodedata
import zipfile
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv


load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
CONFIG_PATH = Path("config.json")
DB_PATH = Path("registrations.db")
EXPORTS_DIR = Path("exports")
EXPORT_ARCHIVES_DIR = EXPORTS_DIR / "archive"
BADWORDS_PATH = Path("badwords.json")
BACKUPS_DIR = Path("backups")

DEFAULT_CONFIG = {
    "guild_id": 0,
    "registration_channel_id": 0,
    "log_channel_id": 0,
    "welcome_channel_id": 0,
    "rules_channel_id": 0,
    "unregistered_role_id": 0,
    "member_role_id": 0,
    "rename_cooldown_hours": 24,
    "registration_message_id": 0,
    "rules_message_id": 0,
    "registration_message_text": "",
    "rules_message_text": "",
    "backup_interval_hours": 24,
    "backup_max_files": 5,
    "last_auto_backup_at": None,
    "registration_attempt_cooldown_seconds": 15,
    "exports_archive_max_files": 5,
}

DEFAULT_BADWORDS = [
    "хуй", "хуйня", "хуе", "хуё", "хуев", "хуёв", "хуесос", "хуила", "хуило",
    "пизда", "пиздец", "пизду", "пизды", "пизд", "пидор", "пидорас",
    "пидорасина", "ебать", "ебан", "ёбан", "ебуч", "ёбуч", "бля", "бляд",
    "блять", "блядь", "сука", "шлюха", "шалава", "мразь", "гандон", "гондон",
    "мудак", "долбоеб", "долбоёб", "уеб", "уёб", "нахуй", "наху", "похуй",
    "fuck", "fucking", "fucker", "motherfucker", "bitch", "bitches", "slut",
    "whore", "asshole", "dick", "cock", "pussy", "cunt", "bastard", "shit",
    "bullshit", "retard", "niga", "nigger", "nigga"
]


BADWORDS_CACHE: list[str] = []
DEFAULT_BADWORDS_CACHE: Optional[list[str]] = None
REGISTRATION_ATTEMPTS: dict[tuple[int, int], datetime] = {}
REGISTRATION_PROCESSING_USERS: set[tuple[int, int]] = set()
REGISTRATION_STATE_LOCK = threading.Lock()


def ensure_runtime_files() -> None:
    EXPORTS_DIR.mkdir(exist_ok=True)
    EXPORT_ARCHIVES_DIR.mkdir(parents=True, exist_ok=True)
    BACKUPS_DIR.mkdir(exist_ok=True)

    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(
            json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    if not BADWORDS_PATH.exists():
        BADWORDS_PATH.write_text(
            json.dumps({"words": []}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )


def build_default_config(current: Optional[dict] = None) -> dict:
    data = dict(DEFAULT_CONFIG)
    if isinstance(current, dict):
        data.update(current)
    return data


def ensure_int_config(value: object, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback



def load_config() -> dict:
    ensure_runtime_files()

    try:
        raw_text = CONFIG_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        cfg = build_default_config()
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return cfg

    if not raw_text:
        cfg = build_default_config()
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return cfg

    try:
        raw_cfg = json.loads(raw_text)
    except json.JSONDecodeError:
        backup_path = CONFIG_PATH.with_suffix(".broken.json")
        try:
            CONFIG_PATH.replace(backup_path)
        except OSError:
            pass
        cfg = build_default_config()
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return cfg

    if not isinstance(raw_cfg, dict):
        cfg = build_default_config()
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return cfg

    cfg = build_default_config(raw_cfg)
    cfg["rename_cooldown_hours"] = max(0, ensure_int_config(cfg.get("rename_cooldown_hours"), 24))
    cfg["registration_message_id"] = max(0, ensure_int_config(cfg.get("registration_message_id"), 0))
    cfg["rules_message_id"] = max(0, ensure_int_config(cfg.get("rules_message_id"), 0))
    cfg["registration_message_text"] = str(cfg.get("registration_message_text") or "").strip()
    cfg["rules_message_text"] = str(cfg.get("rules_message_text") or "").strip()
    cfg["backup_interval_hours"] = max(1, ensure_int_config(cfg.get("backup_interval_hours"), 24))
    cfg["backup_max_files"] = max(1, ensure_int_config(cfg.get("backup_max_files"), 5))
    cfg["registration_attempt_cooldown_seconds"] = max(0, ensure_int_config(cfg.get("registration_attempt_cooldown_seconds"), 15))
    cfg["exports_archive_max_files"] = max(1, ensure_int_config(cfg.get("exports_archive_max_files"), 5))

    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return cfg


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


config = load_config()


def load_last_auto_backup_at() -> Optional[datetime]:
    raw = config.get("last_auto_backup_at")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None


def save_last_auto_backup_at(dt: Optional[datetime]) -> None:
    config["last_auto_backup_at"] = dt.isoformat() if dt else None
    save_config(config)


GUILD_ID = int(config.get("guild_id", 0) or 0)

intents = discord.Intents.default()
intents.members = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)
BOT_START_TIME = time.time()
LAST_AUTO_BACKUP_AT: Optional[datetime] = load_last_auto_backup_at()


class LogType(Enum):
    JOIN = "join"
    REGISTER = "register"
    RESTORE = "restore"
    RENAME = "rename"
    ADMIN = "admin"
    ERROR = "error"
    BACKUP = "backup"


class AdminAction(Enum):
    DELETE_DB_USER = "delete_db_user"
    RESET_DB_USER = "reset_db_user"
    RESTORE_FROM_DB = "restore_from_db"


# =========================
# Антимат только для имени
# =========================

BASE_CHAR_MAP = {
    "@": "a",
    "4": "a",
    "3": "e",
    "1": "i",
    "!": "i",
    "|": "i",
    "0": "o",
    "$": "s",
    "5": "s",
    "7": "t",
    "+": "t",
    "8": "b",
    "9": "g",
    "6": "b",
}

LATIN_TO_CYRILLIC_SIMILAR = {
    "a": "а",
    "b": "б",
    "c": "с",
    "d": "д",
    "e": "е",
    "f": "ф",
    "g": "г",
    "h": "н",
    "i": "и",
    "j": "й",
    "k": "к",
    "l": "л",
    "m": "м",
    "n": "п",
    "o": "о",
    "p": "р",
    "q": "к",
    "r": "г",
    "s": "с",
    "t": "т",
    "u": "и",
    "v": "в",
    "x": "х",
    "y": "у",
    "z": "з",
}

CYRILLIC_TO_LATIN_SIMILAR = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "j",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "h",
    "о": "o",
    "п": "n",
    "р": "p",
    "с": "c",
    "т": "t",
    "у": "y",
    "ф": "f",
    "х": "x",
}


def strip_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_base_symbols(text: str) -> str:
    return "".join(BASE_CHAR_MAP.get(ch, ch) for ch in text)


def normalize_to_cyrillic(text: str) -> str:
    return "".join(LATIN_TO_CYRILLIC_SIMILAR.get(ch.lower(), ch.lower()) for ch in text)


def normalize_to_latin(text: str) -> str:
    result = []
    for ch in text.lower():
        mapped = CYRILLIC_TO_LATIN_SIMILAR.get(ch, ch)
        result.append(mapped)
    return "".join(result)


def collapse_separators(text: str) -> str:
    return re.sub(r"[^a-zа-яё0-9]", "", text, flags=re.IGNORECASE)


def normalize_repeated_letters(text: str) -> str:
    return re.sub(r"(.)\1{2,}", r"\1\1", text)


def build_text_variants(text: str) -> dict[str, list[str]]:
    text = text.lower()
    text = strip_diacritics(text)
    text = normalize_base_symbols(text)

    raw = text
    raw_collapsed = collapse_separators(raw)

    ru_1 = normalize_to_cyrillic(raw)
    ru_2 = collapse_separators(ru_1)
    ru_3 = normalize_repeated_letters(ru_2)

    en_1 = normalize_to_latin(raw)
    en_2 = collapse_separators(en_1)
    en_3 = normalize_repeated_letters(en_2)

    def unique(items: list[str]) -> list[str]:
        result = []
        for item in items:
            if item not in result:
                result.append(item)
        return result

    return {
        "ru": unique([ru_1, ru_2, ru_3]),
        "en": unique([en_1, en_2, en_3]),
        "raw": unique([raw, raw_collapsed]),
    }


def normalize_custom_badword(word: str) -> str:
    word = strip_diacritics(word.lower().strip())
    word = normalize_base_symbols(word)
    word = collapse_separators(word)
    return word


def get_default_badwords() -> list[str]:
    global DEFAULT_BADWORDS_CACHE

    if DEFAULT_BADWORDS_CACHE is not None:
        return DEFAULT_BADWORDS_CACHE.copy()

    cleaned = []
    seen = set()

    for word in DEFAULT_BADWORDS:
        normalized = normalize_custom_badword(word)
        if normalized and len(normalized) >= 3 and normalized not in seen:
            cleaned.append(normalized)
            seen.add(normalized)

    DEFAULT_BADWORDS_CACHE = sorted(cleaned)
    return DEFAULT_BADWORDS_CACHE.copy()


def load_badwords(force_reload: bool = False) -> dict:
    global BADWORDS_CACHE

    ensure_runtime_files()

    if BADWORDS_CACHE and not force_reload:
        return {"words": BADWORDS_CACHE.copy()}

    try:
        with open(BADWORDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            data = {"words": []}

        words = data.get("words", [])
        if not isinstance(words, list):
            words = []

        cleaned_words = []
        seen = set()

        for word in words:
            if not isinstance(word, str):
                continue
            normalized = normalize_custom_badword(word)
            if normalized and len(normalized) >= 3 and normalized not in seen:
                cleaned_words.append(normalized)
                seen.add(normalized)

        BADWORDS_CACHE = sorted(cleaned_words)
        save_badwords({"words": BADWORDS_CACHE})
        return {"words": BADWORDS_CACHE.copy()}

    except (json.JSONDecodeError, OSError):
        BADWORDS_CACHE = []
        save_badwords({"words": []})
        return {"words": []}


def save_badwords(data: dict) -> None:
    global BADWORDS_CACHE

    ensure_runtime_files()
    words = data.get("words", []) if isinstance(data, dict) else []
    BADWORDS_CACHE = list(words) if isinstance(words, list) else []

    with open(BADWORDS_PATH, "w", encoding="utf-8") as f:
        json.dump({"words": BADWORDS_CACHE}, f, ensure_ascii=False, indent=2)


def get_custom_badwords() -> list[str]:
    data = load_badwords()
    return data.get("words", []).copy()


def add_custom_badword(word: str) -> tuple[bool, str]:
    normalized = normalize_custom_badword(word)
    if not normalized:
        return False, "После нормализации слово оказалось пустым."
    if len(normalized) < 3:
        return False, "Слово должно содержать минимум 3 символа после нормализации."
    if normalized in get_default_badwords():
        return False, "Это слово уже есть во встроенном списке."

    data = load_badwords()
    words = set(data.get("words", []))
    if normalized in words:
        return False, "Это слово уже есть в пользовательском списке."

    words.add(normalized)
    data["words"] = sorted(words)
    save_badwords(data)
    return True, normalized


def remove_custom_badword(word: str) -> tuple[bool, str]:
    normalized = normalize_custom_badword(word)
    data = load_badwords()
    words = set(data.get("words", []))

    if normalized not in words:
        if normalized in get_default_badwords():
            return False, "Это слово находится во встроенном списке и не удаляется через эту команду."
        return False, "Такого слова нет в пользовательском списке."

    words.remove(normalized)
    data["words"] = sorted(words)
    save_badwords(data)
    return True, normalized


def clear_custom_badwords() -> None:
    save_badwords({"words": []})


def export_badwords_to_json() -> Path:
    EXPORTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = EXPORTS_DIR / f"badwords_export_{timestamp}.json"

    data = {
        "default_words": get_default_badwords(),
        "custom_words": get_custom_badwords(),
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    archive_old_exports()
    return file_path


def contains_badword_from_list(text: str, words: list[str]) -> Optional[str]:
    variants = build_text_variants(text)
    all_variants = variants["raw"] + variants["ru"] + variants["en"]

    for badword in words:
        if not badword or len(badword) < 3:
            continue

        badword_ru = normalize_to_cyrillic(badword)
        badword_en = normalize_to_latin(badword)

        badword_variants = {
            collapse_separators(badword),
            collapse_separators(badword_ru),
            collapse_separators(badword_en),
        }

        for variant in all_variants:
            collapsed_variant = collapse_separators(variant)
            for bw in badword_variants:
                if bw and bw in collapsed_variant:
                    return badword

    return None


def contains_profanity(text: str) -> tuple[bool, Optional[str]]:
    default_match = contains_badword_from_list(text, get_default_badwords())
    if default_match:
        return True, f"DEFAULT:{default_match}"

    custom_match = contains_badword_from_list(text, get_custom_badwords())
    if custom_match:
        return True, f"CUSTOM:{custom_match}"

    return False, None


# =========================
# База данных
# =========================

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS registrations (
            user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            discord_name TEXT NOT NULL,
            real_name TEXT NOT NULL,
            nickname TEXT NOT NULL,
            registered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, guild_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rename_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            old_real_name TEXT,
            new_real_name TEXT NOT NULL,
            changed_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()

    ensure_runtime_files()
    load_badwords(force_reload=True)


def get_registration(user_id: int, guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM registrations
        WHERE user_id = ? AND guild_id = ?
    """, (user_id, guild_id))
    row = cur.fetchone()
    conn.close()
    return row


def get_all_registrations(guild_id: int) -> list[sqlite3.Row]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM registrations
        WHERE guild_id = ?
        ORDER BY updated_at DESC
    """, (guild_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_last_registrations(guild_id: int, limit: int = 10) -> list[sqlite3.Row]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM registrations
        WHERE guild_id = ?
        ORDER BY updated_at DESC
        LIMIT ?
    """, (guild_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def count_registrations(guild_id: int) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) AS count
        FROM registrations
        WHERE guild_id = ?
    """, (guild_id,))
    row = cur.fetchone()
    conn.close()
    return int(row["count"]) if row else 0


def upsert_registration(
    user_id: int,
    guild_id: int,
    discord_name: str,
    real_name: str,
    nickname: str
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    existing = get_registration(user_id, guild_id)

    conn = get_db_connection()
    cur = conn.cursor()

    if existing:
        cur.execute("""
            UPDATE registrations
            SET discord_name = ?, real_name = ?, nickname = ?, updated_at = ?
            WHERE user_id = ? AND guild_id = ?
        """, (discord_name, real_name, nickname, now, user_id, guild_id))
    else:
        cur.execute("""
            INSERT INTO registrations (
                user_id, guild_id, discord_name, real_name, nickname,
                registered_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, guild_id, discord_name, real_name, nickname, now, now))

    conn.commit()
    conn.close()


def add_rename_history(
    user_id: int,
    guild_id: int,
    old_real_name: Optional[str],
    new_real_name: str
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO rename_history (
            user_id, guild_id, old_real_name, new_real_name, changed_at
        )
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, guild_id, old_real_name, new_real_name, now))
    conn.commit()
    conn.close()


def get_last_rename_time(user_id: int, guild_id: int) -> Optional[datetime]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT changed_at
        FROM rename_history
        WHERE user_id = ? AND guild_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (user_id, guild_id))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return datetime.fromisoformat(row["changed_at"])


def get_name_history(user_id: int, guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM rename_history
        WHERE user_id = ? AND guild_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (user_id, guild_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def delete_registration(user_id: int, guild_id: int) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM registrations
        WHERE user_id = ? AND guild_id = ?
    """, (user_id, guild_id))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def delete_rename_history(user_id: int, guild_id: int) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM rename_history
        WHERE user_id = ? AND guild_id = ?
    """, (user_id, guild_id))
    deleted_count = cur.rowcount
    conn.commit()
    conn.close()
    return deleted_count


def export_registrations_to_csv(guild_id: int) -> Optional[Path]:
    rows = get_all_registrations(guild_id)
    if not rows:
        return None

    EXPORTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = EXPORTS_DIR / f"registrations_{guild_id}_{timestamp}.csv"

    with open(file_path, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow([
            "user_id",
            "guild_id",
            "discord_name",
            "real_name",
            "nickname",
            "registered_at",
            "updated_at",
        ])

        for row in rows:
            writer.writerow([
                row["user_id"],
                row["guild_id"],
                row["discord_name"],
                row["real_name"],
                row["nickname"],
                row["registered_at"],
                row["updated_at"],
            ])

    archive_old_exports()
    return file_path


def get_exports_archive_max_files() -> int:
    try:
        return max(1, int(config.get("exports_archive_max_files", 5)))
    except (TypeError, ValueError):
        return 5


def list_export_files() -> list[Path]:
    ensure_runtime_files()
    return sorted(
        [p for p in EXPORTS_DIR.glob("*") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )


def list_export_archives() -> list[Path]:
    ensure_runtime_files()
    return sorted(
        EXPORT_ARCHIVES_DIR.glob("export_archive_*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )


def prune_old_export_archives() -> None:
    archives = list_export_archives()
    max_files = get_exports_archive_max_files()
    for old_file in archives[max_files:]:
        try:
            old_file.unlink()
        except OSError:
            pass


def archive_old_exports() -> list[Path]:
    ensure_runtime_files()
    export_files = [p for p in list_export_files() if p.parent == EXPORTS_DIR]
    keep_live = 5
    archived: list[Path] = []

    for old_file in export_files[keep_live:]:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        zip_path = EXPORT_ARCHIVES_DIR / f"export_archive_{timestamp}_{old_file.stem}.zip"
        try:
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(old_file, arcname=old_file.name)
            old_file.unlink()
            archived.append(zip_path)
        except OSError:
            try:
                if zip_path.exists():
                    zip_path.unlink()
            except OSError:
                pass

    prune_old_export_archives()
    return archived


# =========================
# Бэкапы
# =========================

def ensure_backup_dir() -> None:
    BACKUPS_DIR.mkdir(exist_ok=True)


def get_backup_interval_hours() -> int:
    try:
        return max(1, int(config.get("backup_interval_hours", 24)))
    except (TypeError, ValueError):
        return 24


def get_backup_max_files() -> int:
    try:
        return max(1, int(config.get("backup_max_files", 5)))
    except (TypeError, ValueError):
        return 5


def list_backup_archives() -> list[Path]:
    ensure_backup_dir()
    return sorted(
        BACKUPS_DIR.glob("backup_*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )


def prune_old_backups() -> None:
    archives = list_backup_archives()
    max_files = get_backup_max_files()

    for old_file in archives[max_files:]:
        try:
            old_file.unlink()
        except OSError:
            pass


def create_backup_bundle() -> list[Path]:
    ensure_runtime_files()
    ensure_backup_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_path = BACKUPS_DIR / f"backup_{timestamp}.zip"

    source_files = [DB_PATH, CONFIG_PATH, BADWORDS_PATH]
    existing_sources = [path for path in source_files if path.exists()]

    if not existing_sources:
        return []

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for source in existing_sources:
            zf.write(source, arcname=source.name)

    prune_old_backups()
    return [zip_path]


# =========================
# Вспомогательные функции
# =========================

def is_valid_name(name: str) -> bool:
    name = re.sub(r"\s+", " ", name.strip())
    if not (2 <= len(name) <= 20):
        return False
    return bool(re.fullmatch(r"[А-Яа-яЁё -]+", name))


def normalize_real_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip())


def capitalize_name_part(part: str) -> str:
    if not part:
        return part
    return part[0].upper() + part[1:].lower()


def format_real_name(name: str) -> str:
    name = normalize_real_name(name)
    words = []
    for word in name.split(" "):
        hyphen_parts = [capitalize_name_part(part) for part in word.split("-")]
        words.append("-".join(hyphen_parts))
    return " ".join(words)


def get_name_letters(name: str) -> list[str]:
    return [ch.lower() for ch in name if ch.isalpha()]


def has_too_many_repeated_letters(name: str) -> bool:
    return re.search(r"(.)\1{2,}", name.lower()) is not None


def has_required_vowels(name: str) -> bool:
    vowels = "аеёиоуыэюя"
    lowered = name.lower()
    return any(vowel in lowered for vowel in vowels)


def looks_like_keyboard_smash(name: str) -> bool:
    cleaned = normalize_real_name(name)
    letters = get_name_letters(cleaned)

    if not letters:
        return True

    unique_letters = set(letters)
    if len(letters) >= 4 and len(unique_letters) <= 2:
        return True

    if len(letters) >= 5 and has_too_many_repeated_letters(cleaned):
        return True

    if len(letters) >= 4 and not has_required_vowels(cleaned):
        return True

    consonant_groups = re.findall(r"[бвгджзйклмнпрстфхцчшщ]{4,}", cleaned.lower())
    if consonant_groups:
        return True

    return False


def looks_like_garbage(name: str) -> bool:
    name = normalize_real_name(name).lower()
    letters = [c for c in name if c.isalpha()]

    if len(letters) < 2:
        return True

    text = "".join(letters)

    vowels = "аеёиоуыэюя"
    consonants = "бвгджзйклмнпрстфхцчшщ"

    vowel_count = sum(1 for c in text if c in vowels)
    consonant_count = sum(1 for c in text if c in consonants)

    # слишком мало разных букв
    if len(set(text)) <= 2:
        return True

    # слишком низкое разнообразие символов
    if len(set(text)) / len(text) < 0.45:
        return True

    # слишком много одинаковых подряд
    if re.search(r"(.)\1{2,}", text):
        return True

    # нет гласных
    if vowel_count == 0:
        return True

    # слишком мало гласных
    if vowel_count / len(text) < 0.25:
        return True

    # слишком длинная цепочка согласных
    if re.search(rf"[{consonants}]{{4,}}", text):
        return True

    # слишком много согласных относительно гласных
    if consonant_count > vowel_count * 2:
        return True

    # типичные keyboard smash паттерны
    keyboard_smashes = (
        "qwerty", "asdf", "zxcv", "йцук", "фыва", "ячсм",
        "цук", "куц", "рпра", "прар", "трвг", "вгд", "ghbdtn"
    )
    if any(seq in text for seq in keyboard_smashes):
        return True

    return False

def analyze_name(name: str) -> dict:
    original = normalize_real_name(name)
    suggested = format_real_name(original)
    valid = is_valid_name(suggested)
    found_profanity, matched_rule = contains_profanity(suggested)
    garbage_name = looks_like_garbage(suggested)

    reason = None
    if found_profanity:
        reason = "Имя содержит запрещённые слова или их маскировку."
    elif garbage_name:
        reason = "Имя выглядит как случайный набор букв. Введите настоящее имя."
    elif not valid:
        if len(suggested) < 2:
            reason = "Имя слишком короткое. Минимум 2 символа."
        elif len(suggested) > 20:
            reason = "Имя слишком длинное. Максимум 20 символов."
        else:
            reason = "Разрешены только русские буквы, пробелы и дефис."

    return {
        "original": original,
        "suggested": suggested,
        "valid": valid,
        "found_profanity": found_profanity,
        "matched_rule": matched_rule,
        "garbage_name": garbage_name,
        "ok": valid and not found_profanity and not garbage_name,
        "reason": reason,
        "changed_case": original != suggested,
    }

def extract_base_name(member: discord.Member) -> str:
    source = member.display_name

    match = re.match(r"^(.*?)\s*\([^()]+\)\s*$", source)
    base = match.group(1).strip() if match else source.strip()

    return base or member.display_name

def build_nickname(base_name: str, real_name: str) -> str:
    suffix = f" ({real_name})"
    full = f"{base_name}{suffix}"

    if len(full) <= 32:
        return full

    max_base_len = max(1, 32 - len(suffix))
    trimmed_base = base_name[:max_base_len].rstrip()
    if not trimmed_base:
        trimmed_base = base_name[:max_base_len]
    return f"{trimmed_base}{suffix}"


def get_unregistered_role(guild: discord.Guild) -> Optional[discord.Role]:
    return guild.get_role(int(config["unregistered_role_id"]))


def get_member_role(guild: discord.Guild) -> Optional[discord.Role]:
    return guild.get_role(int(config["member_role_id"]))


def get_unregistered_role_name(guild: discord.Guild) -> str:
    role = get_unregistered_role(guild)
    return role.name if role else "Не выбрана"


def get_member_role_name(guild: discord.Guild) -> str:
    role = get_member_role(guild)
    return role.name if role else "Не выбрана"


def get_registration_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    channel = guild.get_channel(int(config["registration_channel_id"]))
    return channel if isinstance(channel, discord.TextChannel) else None


def get_log_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    channel = guild.get_channel(int(config["log_channel_id"]))
    return channel if isinstance(channel, discord.TextChannel) else None


def get_welcome_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    channel = guild.get_channel(int(config["welcome_channel_id"]))
    return channel if isinstance(channel, discord.TextChannel) else None


def get_rules_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    channel = guild.get_channel(int(config.get("rules_channel_id", 0)))
    return channel if isinstance(channel, discord.TextChannel) else None


def get_unregistered_role_mention(guild: discord.Guild) -> str:
    role = get_unregistered_role(guild)
    return role.mention if role else "@Не выбрана"


def get_member_role_mention(guild: discord.Guild) -> str:
    role = get_member_role(guild)
    return role.mention if role else "@Не выбрана"


def get_registration_channel_mention(guild: discord.Guild) -> str:
    channel = get_registration_channel(guild)
    return channel.mention if channel else "#не-настроен"


def get_rules_channel_mention(guild: discord.Guild) -> str:
    channel = get_rules_channel(guild)
    return channel.mention if channel else "#не-настроен"


def get_welcome_channel_mention(guild: discord.Guild) -> str:
    channel = get_welcome_channel(guild)
    return channel.mention if channel else "#не-настроен"


def get_log_channel_mention(guild: discord.Guild) -> str:
    channel = get_log_channel(guild)
    return channel.mention if channel else "#не-настроен"


def render_message_template(guild: discord.Guild, text: str) -> str:
    return (
        text.replace("{unregistered_role}", get_unregistered_role_mention(guild))
        .replace("{member_role}", get_member_role_mention(guild))
        .replace("{registration_channel}", get_registration_channel_mention(guild))
        .replace("{rules_channel}", get_rules_channel_mention(guild))
        .replace("{welcome_channel}", get_welcome_channel_mention(guild))
        .replace("{log_channel}", get_log_channel_mention(guild))
        .replace("{guild_name}", guild.name)
    )


def get_registration_message_template() -> str:
    custom_text = str(config.get("registration_message_text") or "").strip()
    return custom_text if custom_text else make_registration_message_text()


def get_registration_message_text(guild: discord.Guild) -> str:
    return render_message_template(guild, get_registration_message_template())


def make_rules_message_text() -> str:
    return """# 📜 Правила сервера

**Перед началом общения обязательно ознакомьтесь с правилами.**

━━━━━━━━━━━━━━━━

**Основное**

• уважайте других участников сервера
• запрещены оскорбления, токсичность и провокации
• запрещён спам, флуд и реклама без разрешения
• не публикуйте вредоносные ссылки и подозрительные файлы
• соблюдайте правила Discord и требования сервера

━━━━━━━━━━━━━━━━

✅ **После принятия правил и регистрации**

Вы получите доступ к основным каналам сервера
и роль {member_role} после успешной проверки.

━━━━━━━━━━━━━━━━

⚠️ Администрация может удалить сообщения
или ограничить доступ при нарушении правил.
"""


def get_rules_message_template() -> str:
    custom_text = str(config.get("rules_message_text") or "").strip()
    return custom_text if custom_text else make_rules_message_text()


def get_rules_message_text(guild: discord.Guild) -> str:
    return render_message_template(guild, get_rules_message_template())


def format_dt(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str)
    return dt.strftime("%d.%m.%Y %H:%M UTC")


def get_registration_attempt_cooldown_seconds() -> int:
    try:
        return max(0, int(config.get("registration_attempt_cooldown_seconds", 15)))
    except (TypeError, ValueError):
        return 15


def check_registration_antispam(user_id: int, guild_id: int) -> tuple[bool, Optional[str]]:
    cooldown_seconds = get_registration_attempt_cooldown_seconds()
    if cooldown_seconds <= 0:
        return True, None

    key = (guild_id, user_id)
    now = datetime.now(timezone.utc)
    last_attempt = REGISTRATION_ATTEMPTS.get(key)
    if last_attempt is not None:
        elapsed = (now - last_attempt).total_seconds()
        if elapsed < cooldown_seconds:
            remaining = max(1, int(cooldown_seconds - elapsed))
            return False, f"Слишком часто. Подождите **{remaining} сек.** и попробуйте снова."

    REGISTRATION_ATTEMPTS[key] = now
    return True, None


def begin_registration_processing(user_id: int, guild_id: int) -> bool:
    key = (guild_id, user_id)
    with REGISTRATION_STATE_LOCK:
        if key in REGISTRATION_PROCESSING_USERS:
            return False
        REGISTRATION_PROCESSING_USERS.add(key)
        return True


def end_registration_processing(user_id: int, guild_id: int) -> None:
    key = (guild_id, user_id)
    with REGISTRATION_STATE_LOCK:
        REGISTRATION_PROCESSING_USERS.discard(key)


def log_internal_exception(prefix: str, error: Exception) -> None:
    print(f"{prefix}: {type(error).__name__}: {error}")


def is_admin_interaction(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    if not isinstance(interaction.user, discord.Member):
        return False
    return interaction.user.guild_permissions.administrator


def make_success_embed(title: str, description: str) -> discord.Embed:
    embed = discord.Embed(
        title=f"✅ {title}",
        description=description,
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text="Успешно")
    return embed


def make_error_embed(title: str, description: str) -> discord.Embed:
    embed = discord.Embed(
        title=f"⚠️ {title}",
        description=description,
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text="Ошибка")
    return embed


def make_warning_embed(title: str, description: str) -> discord.Embed:
    embed = discord.Embed(
        title=f"⚡ {title}",
        description=description,
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text="Требуется внимание")
    return embed


def make_info_embed(title: str, description: str) -> discord.Embed:
    embed = discord.Embed(
        title=f"📘 {title}",
        description=description,
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text="Информация")
    return embed


async def ensure_message_pinned(message: Optional[discord.Message]) -> bool:
    if message is None:
        return False
    return True


def make_registration_message_text() -> str:
    return """# 🔐 Verification Center

**Добро пожаловать на сервер!**

Перед тем как начать общение,
необходимо пройти короткую проверку.

━━━━━━━━━━━━━━━━

📌 **Как пройти проверку**

1️⃣ Нажмите кнопку **«Указать имя»**
2️⃣ Введите своё имя в появившемся окне
3️⃣ Бот автоматически обновит ваш ник

━━━━━━━━━━━━━━━━

✅ **После успешной проверки**

• снимется роль {unregistered_role}
• выдастся роль {member_role}
• откроется доступ ко всем каналам

━━━━━━━━━━━━━━━━

⚠ **Требования к имени**

• только русские буквы
• допускаются пробелы и дефис
• без мата и оскорблений

 ‎
"""
def make_welcome_embed(member: discord.Member, nickname: str) -> discord.Embed:
    embed = discord.Embed(
        title="🎉 Новый участник",
        description=(
            f"Поприветствуем {member.mention}!\n\n"
            f"Пользователь успешно завершил регистрацию\n"
            f"и теперь известен как **{nickname}**."
        ),
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )

    embed.set_thumbnail(url=member.display_avatar.url)

    embed.add_field(
        name="👋 Добро пожаловать",
        value="Надеемся, вам понравится на сервере.",
        inline=False
    )

    embed.add_field(
        name="📣 Что дальше",
        value=(
            "• загляните в важные каналы\n"
            "• познакомьтесь с участниками\n"
            "• приятного общения"
        ),
        inline=False
    )

    embed.set_footer(text=f"User ID: {member.id}")
    return embed


def make_log_embed(
    log_type: LogType,
    title: str,
    description: str,
    member: Optional[discord.Member] = None,
    moderator: Optional[discord.abc.User] = None
) -> discord.Embed:
    styles = {
        LogType.JOIN: {"color": discord.Color.orange(), "emoji": "📥"},
        LogType.REGISTER: {"color": discord.Color.green(), "emoji": "✅"},
        LogType.RESTORE: {"color": discord.Color.blue(), "emoji": "♻️"},
        LogType.RENAME: {"color": discord.Color.gold(), "emoji": "✏️"},
        LogType.ADMIN: {"color": discord.Color.dark_teal(), "emoji": "🛠️"},
        LogType.ERROR: {"color": discord.Color.red(), "emoji": "⚠️"},
        LogType.BACKUP: {"color": discord.Color.dark_blue(), "emoji": "💾"},
    }

    style = styles[log_type]

    embed = discord.Embed(
        title=f"{style['emoji']} {title}",
        description=description,
        color=style["color"],
        timestamp=datetime.now(timezone.utc)
    )

    if member:
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(
            name="Пользователь",
            value=f"{member.mention}\n`{member.id}`",
            inline=True
        )
        embed.add_field(
            name="Аккаунт",
            value=f"`{member.name}`",
            inline=True
        )

    if moderator:
        embed.add_field(
            name="Администратор",
            value=f"{moderator.mention}\n`{moderator.id}`",
            inline=False
        )

    embed.set_footer(text="Registration System Logs")
    return embed


def build_user_db_embed_from_member(member: discord.Member, registration: sqlite3.Row) -> discord.Embed:
    embed = discord.Embed(
        title="🗂️ Запись пользователя в базе",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="Пользователь", value=f"{member.mention}\n`{member.id}`", inline=False)
    embed.add_field(name="Discord username", value=registration["discord_name"], inline=True)
    embed.add_field(name="Имя", value=registration["real_name"], inline=True)
    embed.add_field(name="Ник", value=registration["nickname"], inline=False)
    embed.add_field(name="Первая регистрация", value=format_dt(registration["registered_at"]), inline=False)
    embed.add_field(name="Последнее обновление", value=format_dt(registration["updated_at"]), inline=False)
    embed.set_footer(text="Database Viewer")
    return embed


def build_user_db_embed_offline(user_id: int, registration: sqlite3.Row) -> discord.Embed:
    embed = discord.Embed(
        title="🗂️ Запись пользователя в базе",
        description="Пользователь сейчас не найден на сервере, но запись в базе существует.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="User ID", value=f"`{user_id}`", inline=False)
    embed.add_field(name="Discord username", value=registration["discord_name"], inline=True)
    embed.add_field(name="Имя", value=registration["real_name"], inline=True)
    embed.add_field(name="Ник", value=registration["nickname"], inline=False)
    embed.add_field(name="Первая регистрация", value=format_dt(registration["registered_at"]), inline=False)
    embed.add_field(name="Последнее обновление", value=format_dt(registration["updated_at"]), inline=False)
    embed.set_footer(text="Database Viewer")
    return embed


def build_name_history_embed(
    title_text: str,
    description_text: str,
    avatar_url: Optional[str],
    current_real_name: Optional[str],
    current_nickname: Optional[str],
    rows: list[sqlite3.Row]
) -> discord.Embed:
    embed = discord.Embed(
        title=title_text,
        description=description_text,
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc)
    )

    if avatar_url:
        embed.set_thumbnail(url=avatar_url)

    if current_real_name:
        embed.add_field(
            name="Текущее имя в базе",
            value=f"**{current_real_name}**",
            inline=False
        )

    if current_nickname:
        embed.add_field(
            name="Текущий ник",
            value=f"`{current_nickname}`",
            inline=False
        )

    if rows:
        lines = []
        for idx, row in enumerate(rows[:10], start=1):
            old_name = row["old_real_name"] if row["old_real_name"] else "—"
            new_name = row["new_real_name"]
            changed_at = format_dt(row["changed_at"])
            lines.append(
                f"**{idx}.** `{changed_at}`\n"
                f"Старое: **{old_name}**\n"
                f"Новое: **{new_name}**"
            )
        embed.add_field(
            name=f"Последние изменения ({len(rows)})",
            value="\n\n".join(lines),
            inline=False
        )
    else:
        embed.add_field(
            name="История изменений",
            value="Смен имени ещё не было.",
            inline=False
        )

    return embed


async def send_log(
    guild: discord.Guild,
    log_type: LogType,
    title: str,
    description: str,
    member: Optional[discord.Member] = None,
    moderator: Optional[discord.abc.User] = None
) -> None:
    channel = get_log_channel(guild)
    if not channel:
        return

    embed = make_log_embed(
        log_type=log_type,
        title=title,
        description=description,
        member=member,
        moderator=moderator
    )

    try:
        await channel.send(embed=embed)
    except discord.Forbidden:
        print("Нет прав на отправку логов.")
    except discord.HTTPException as e:
        print(f"Ошибка отправки логов: {e}")


def build_system_check_embed(guild: discord.Guild) -> discord.Embed:
    me = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    perms = me.guild_permissions if me else None

    registration_channel = get_registration_channel(guild)
    log_channel = get_log_channel(guild)
    welcome_channel = get_welcome_channel(guild)
    rules_channel = get_rules_channel(guild)
    unregistered_role = get_unregistered_role(guild)
    member_role = get_member_role(guild)

    checks = [
        ("Manage Roles", perms.manage_roles if perms else False),
        ("Manage Nicknames", perms.manage_nicknames if perms else False),
        ("Manage Channels", perms.manage_channels if perms else False),
        ("View Audit Log", perms.view_audit_log if perms else False),
        ("Embed Links", perms.embed_links if perms else False),
        ("Attach Files", perms.attach_files if perms else False),
        ("View Channels", perms.view_channel if perms else False),
        ("Send Messages", perms.send_messages if perms else False),
    ]

    lines = [f"{'✅' if ok else '❌'} {name}" for name, ok in checks]

    embed = discord.Embed(
        title="🩺 Диагностика сервера",
        description="\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )

    embed.add_field(
        name="Каналы",
        value=(
            f"Регистрация: {registration_channel.mention if registration_channel else '❌ не найден'}\n"
            f"Логи: {log_channel.mention if log_channel else '❌ не найден'}\n"
            f"Welcome: {welcome_channel.mention if welcome_channel else '❌ не найден'}\n"
            f"Правила: {rules_channel.mention if rules_channel else '❌ не найден'}"
        ),
        inline=False
    )

    embed.add_field(
        name="Роли",
        value=(
            f"{get_unregistered_role_name(guild)}: {unregistered_role.mention if unregistered_role else '❌ не найдена'}\n"
            f"{get_member_role_name(guild)}: {member_role.mention if member_role else '❌ не найдена'}"
        ),
        inline=False
    )

    embed.add_field(
        name="Автобэкап",
        value=(
            f"Интервал: **{get_backup_interval_hours()} ч.**\n"
            f"Хранить архивов: **{get_backup_max_files()}**\n"
            f"Последний автозапуск: **{LAST_AUTO_BACKUP_AT.strftime('%d.%m.%Y %H:%M UTC') if LAST_AUTO_BACKUP_AT else 'ещё не выполнялся'}**\n"
            f"Антиспам регистрации: **{get_registration_attempt_cooldown_seconds()} сек.**\n"
            f"Архивов экспортов: **{get_exports_archive_max_files()}**"
        ),
        inline=False
    )

    if me:
        member_role_position_ok = True
        if member_role is not None:
            member_role_position_ok = me.top_role > member_role

        embed.add_field(
            name="Позиция бота",
            value=(
                f"Верхняя роль бота: {me.top_role.mention if me.top_role else '—'}\n"
                f"ID бота: `{me.id}`\n"
                f"Выше роли участника: {'✅' if member_role_position_ok else '❌'}"
            ),
            inline=False
        )

    embed.set_footer(text="Раздел: Диагностика")
    return embed


# =========================
# Постоянное сообщение регистрации
# =========================

async def ensure_registration_message(guild: discord.Guild) -> Optional[discord.Message]:
    channel = get_registration_channel(guild)
    if channel is None:
        return None

    message_id = int(config.get("registration_message_id", 0))

    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            await ensure_message_pinned(message)
            return message
        except discord.NotFound:
            pass
        except discord.Forbidden:
            print("Нет прав для получения сообщения регистрации.")
            return None
        except discord.HTTPException as e:
            print(f"Ошибка при получении сообщения регистрации: {e}")
            return None

    message_text = get_registration_message_text(guild)

    try:
        message = await channel.send(message_text, view=RegistrationView(), allowed_mentions=discord.AllowedMentions.none())
    except discord.Forbidden:
        print("Нет прав на отправку постоянного сообщения регистрации.")
        return None
    except discord.HTTPException as e:
        print(f"Ошибка при отправке постоянного сообщения регистрации: {e}")
        return None

    config["registration_message_id"] = message.id
    save_config(config)
    await ensure_message_pinned(message)
    return message


async def refresh_registration_message(guild: discord.Guild) -> bool:
    channel = get_registration_channel(guild)
    if channel is None:
        return False

    message_id = int(config.get("registration_message_id", 0))
    message_text = get_registration_message_text(guild)

    if not message_id:
        return (await ensure_registration_message(guild)) is not None

    try:
        message = await channel.fetch_message(message_id)
        await message.edit(content=message_text, embed=None, view=RegistrationView(), allowed_mentions=discord.AllowedMentions.none())
        await ensure_message_pinned(message)
        return True
    except discord.NotFound:
        config["registration_message_id"] = 0
        save_config(config)
        return (await ensure_registration_message(guild)) is not None
    except discord.Forbidden:
        print("Нет прав на обновление сообщения регистрации.")
        return False
    except discord.HTTPException as e:
        print(f"Ошибка при обновлении сообщения регистрации: {e}")
        return False


async def ensure_rules_message(guild: discord.Guild) -> Optional[discord.Message]:
    channel = get_rules_channel(guild)
    if channel is None:
        return None

    message_id = int(config.get("rules_message_id", 0))

    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            await ensure_message_pinned(message)
            return message
        except discord.NotFound:
            pass
        except discord.Forbidden:
            print("Нет прав для получения сообщения правил.")
            return None
        except discord.HTTPException as e:
            print(f"Ошибка при получении сообщения правил: {e}")
            return None

    message_text = get_rules_message_text(guild)

    try:
        message = await channel.send(message_text, allowed_mentions=discord.AllowedMentions.none())
    except discord.Forbidden:
        print("Нет прав на отправку сообщения правил.")
        return None
    except discord.HTTPException as e:
        print(f"Ошибка при отправке сообщения правил: {e}")
        return None

    config["rules_message_id"] = message.id
    save_config(config)
    await ensure_message_pinned(message)
    return message


async def refresh_rules_message(guild: discord.Guild) -> bool:
    channel = get_rules_channel(guild)
    if channel is None:
        return False

    message_id = int(config.get("rules_message_id", 0))
    message_text = get_rules_message_text(guild)

    if not message_id:
        return (await ensure_rules_message(guild)) is not None

    try:
        message = await channel.fetch_message(message_id)
        await message.edit(content=message_text, embed=None, allowed_mentions=discord.AllowedMentions.none())
        await ensure_message_pinned(message)
        return True
    except discord.NotFound:
        config["rules_message_id"] = 0
        save_config(config)
        return (await ensure_rules_message(guild)) is not None
    except discord.Forbidden:
        print("Нет прав на обновление сообщения правил.")
        return False
    except discord.HTTPException as e:
        print(f"Ошибка при обновлении сообщения правил: {e}")
        return False


# =========================
# Основная логика регистрации
# =========================

async def apply_registration(
    member: discord.Member,
    real_name: str,
    rename_mode: bool = False
) -> tuple[bool, str, Optional[str]]:
    guild = member.guild
    analysis = analyze_name(real_name)
    real_name = analysis["suggested"]

    if analysis["found_profanity"]:
        await send_log(
            guild,
            LogType.ERROR,
            "Попытка использовать запрещённое имя",
            (
                f"Пользователь попытался указать имя: **{analysis['original']}**\n"
                f"После нормализации: **{real_name}**\n"
                f"Совпадение: `{analysis['matched_rule']}`"
            ),
            member=member
        )
        return False, (
            "Имя содержит запрещённые слова или их маскировку.\n"
            "Используйте нормальное имя без мата и оскорблений."
        ), None

    if not analysis["ok"] and not analysis["found_profanity"]:
        return False, (
            analysis["reason"]
            or "Имя не прошло проверку."
        ), analysis["suggested"] if analysis["changed_case"] else None

    unregistered_role = get_unregistered_role(guild)
    member_role = get_member_role(guild)

    if not unregistered_role:
        return False, "Роль незарегистрированных не найдена. Установите её через `/set_unregistered_role`.", None
    if not member_role:
        return False, "Роль участников не найдена. Установите её через `/set_member_role`.", None

    already_registered = unregistered_role not in member.roles
    registration = get_registration(member.id, guild.id)

    if already_registered and not rename_mode:
        return False, "Вы уже зарегистрированы. Используйте кнопку **«Изменить имя»**.", None

    if rename_mode and not already_registered:
        return False, "Сначала завершите обычную регистрацию.", None

    if rename_mode:
        cooldown_hours = int(config.get("rename_cooldown_hours", 24))
        last_rename = get_last_rename_time(member.id, guild.id)

        if last_rename:
            next_allowed = last_rename + timedelta(hours=cooldown_hours)
            now = datetime.now(timezone.utc)

            if now < next_allowed:
                remaining = next_allowed - now
                total_seconds = int(remaining.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                return False, (
                    f"Имя можно менять не чаще одного раза в {cooldown_hours} ч.\n"
                    f"Попробуйте снова через **{hours} ч. {minutes} мин.**"
                ), None

    old_nick = member.nick
    had_unregistered_role = unregistered_role in member.roles
    had_member_role = member_role in member.roles

    base_name = extract_base_name(member)
    new_nick = build_nickname(base_name, real_name)

    try:
        await member.edit(nick=new_nick, reason="Регистрация / смена имени")
    except discord.Forbidden:
        return False, (
            "Я не смог изменить ник.\n"
            "Проверьте права `Manage Nicknames` и что роль бота выше роли пользователя."
        ), None
    except discord.HTTPException:
        return False, "Discord вернул ошибку при изменении ника.", None

    try:
        if not already_registered:
            if had_unregistered_role:
                await member.remove_roles(unregistered_role, reason="Регистрация завершена")

            if not had_member_role:
                await member.add_roles(member_role, reason="Регистрация завершена")
    except (discord.Forbidden, discord.HTTPException):
        try:
            await member.edit(nick=old_nick, reason="Откат ника после ошибки ролей")
        except (discord.Forbidden, discord.HTTPException):
            pass

        return False, (
            "Не удалось завершить регистрацию полностью.\n"
            "Ник был возвращён обратно. Проверьте `Manage Roles` и позицию роли бота."
        ), None

    old_real_name = registration["real_name"] if registration else None

    upsert_registration(
        user_id=member.id,
        guild_id=guild.id,
        discord_name=member.name,
        real_name=real_name,
        nickname=new_nick
    )

    format_note = (
        f"\nАвтоформат: **{analysis['original']}** → **{real_name}**"
        if analysis["changed_case"] else ""
    )

    if rename_mode:
        add_rename_history(
            user_id=member.id,
            guild_id=guild.id,
            old_real_name=old_real_name,
            new_real_name=real_name
        )

        await send_log(
            guild,
            LogType.RENAME,
            "Имя изменено",
            f"Пользователь изменил имя на **{real_name}**.\nНовый ник: **{new_nick}**{format_note}",
            member=member
        )
    else:
        await send_log(
            guild,
            LogType.REGISTER,
            "Пользователь зарегистрирован",
            f"Регистрация завершена.\nИмя: **{real_name}**\nНик: **{new_nick}**{format_note}",
            member=member
        )

        welcome_channel = get_welcome_channel(guild)
        if welcome_channel:
            embed = make_welcome_embed(member, new_nick)
            try:
                await welcome_channel.send(embed=embed)
            except (discord.Forbidden, discord.HTTPException):
                pass

    suggestion = analysis["suggested"] if analysis["changed_case"] else None
    return True, new_nick, suggestion


async def restore_member_from_db(
    member: discord.Member,
    reason_text: str = "Автовосстановление зарегистрированного пользователя",
    add_log: bool = True
) -> tuple[bool, str]:
    registration = get_registration(member.id, member.guild.id)
    if not registration:
        return False, "Пользователь не найден в базе."

    real_name = registration["real_name"]
    base_name = extract_base_name(member)
    new_nick = build_nickname(base_name, real_name)

    unregistered_role = get_unregistered_role(member.guild)
    member_role = get_member_role(member.guild)

    old_nick = member.nick
    had_unregistered_role = bool(unregistered_role and unregistered_role in member.roles)
    had_member_role = bool(member_role and member_role in member.roles)

    try:
        await member.edit(nick=new_nick, reason=reason_text)
    except discord.Forbidden:
        return False, "Не удалось восстановить ник: нет прав."
    except discord.HTTPException:
        return False, "Не удалось восстановить ник из-за ошибки Discord."

    try:
        if unregistered_role and had_unregistered_role:
            await member.remove_roles(
                unregistered_role,
                reason="Пользователь уже зарегистрирован в базе"
            )

        if member_role and not had_member_role:
            await member.add_roles(
                member_role,
                reason="Восстановление роли зарегистрированного пользователя"
            )
    except (discord.Forbidden, discord.HTTPException):
        try:
            await member.edit(nick=old_nick, reason="Откат восстановления после ошибки ролей")
        except (discord.Forbidden, discord.HTTPException):
            pass

        return False, "Не удалось полностью восстановить пользователя. Ник был возвращён обратно."

    upsert_registration(
        user_id=member.id,
        guild_id=member.guild.id,
        discord_name=member.name,
        real_name=real_name,
        nickname=new_nick
    )

    if add_log:
        await send_log(
            member.guild,
            LogType.RESTORE,
            "Автовосстановление пользователя",
            f"Пользователь уже был в базе.\nИмя восстановлено: **{real_name}**\nНик: **{new_nick}**",
            member=member
        )

    return True, new_nick


# =========================
# Админ-действия
# =========================

async def execute_admin_action(
    guild: discord.Guild,
    action: AdminAction,
    member: discord.Member,
    moderator: discord.abc.User
) -> tuple[bool, str]:
    if action == AdminAction.DELETE_DB_USER:
        registration = get_registration(member.id, guild.id)
        if not registration:
            return False, "У этого пользователя нет записи в базе."

        deleted_registration = delete_registration(member.id, guild.id)
        deleted_history_count = delete_rename_history(member.id, guild.id)

        if not deleted_registration:
            return False, "Не удалось удалить запись из базы."

        await send_log(
            guild,
            LogType.ADMIN,
            "Запись удалена из базы",
            f"Запись пользователя удалена.\nУдалено записей истории имён: **{deleted_history_count}**",
            member=member,
            moderator=moderator
        )

        return True, (
            f"Пользователь **{member}** удалён из базы.\n"
            f"История смен имени: **{deleted_history_count}** записей."
        )

    if action == AdminAction.RESET_DB_USER:
        deleted_registration = delete_registration(member.id, guild.id)
        deleted_history_count = delete_rename_history(member.id, guild.id)

        unregistered_role = get_unregistered_role(guild)
        member_role = get_member_role(guild)

        try:
            await member.edit(nick=None, reason="Администратор сбросил регистрацию")

            if member_role and member_role in member.roles:
                await member.remove_roles(member_role, reason="Сброс регистрации")

            if unregistered_role and unregistered_role not in member.roles:
                await member.add_roles(unregistered_role, reason="Сброс регистрации")
        except discord.Forbidden:
            return False, "Не удалось обновить ник или роли пользователя."
        except discord.HTTPException:
            return False, "Discord вернул ошибку при сбросе пользователя."

        await send_log(
            guild,
            LogType.ADMIN,
            "Регистрация пользователя сброшена",
            f"Пользователь переведён обратно в незарегистрированные.\n"
            f"Запись в базе удалена: **{'да' if deleted_registration else 'нет'}**\n"
            f"История смен имени удалена: **{deleted_history_count}**",
            member=member,
            moderator=moderator
        )

        return True, (
            f"Регистрация пользователя {member.mention} сброшена.\n"
            "Он должен будет пройти регистрацию заново."
        )

    if action == AdminAction.RESTORE_FROM_DB:
        registration = get_registration(member.id, guild.id)
        if not registration:
            return False, "У этого пользователя нет записи в базе."

        ok, result = await restore_member_from_db(
            member=member,
            reason_text="Администратор восстановил пользователя из базы",
            add_log=False
        )

        if not ok:
            return False, result

        await send_log(
            guild,
            LogType.ADMIN,
            "Пользователь восстановлен из базы",
            f"Ник и роли пользователя восстановлены.\nНовый ник: **{result}**",
            member=member,
            moderator=moderator
        )

        return True, f"Пользователь {member.mention} восстановлен из базы.\nТекущий ник: **{result}**"

    return False, "Неизвестное действие."


# =========================
# UI регистрации
# =========================

class NameModal(discord.ui.Modal):
    def __init__(self, rename_mode: bool = False):
        super().__init__(title="Изменить имя" if rename_mode else "Регистрация")
        self.rename_mode = rename_mode

        self.name_input = discord.ui.TextInput(
            label="Введите ваше имя",
            placeholder="Например: Денис",
            min_length=2,
            max_length=20,
            required=True
        )
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=make_error_embed("Неверное место", "Эту кнопку нужно использовать на сервере."),
                ephemeral=True
            )
            return

        member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message(
                embed=make_error_embed("Пользователь не найден", "Не удалось найти вас на сервере."),
                ephemeral=True
            )
            return

        real_name = self.name_input.value.strip()

        allowed, spam_message = check_registration_antispam(member.id, interaction.guild.id)
        if not allowed:
            await interaction.response.send_message(
                embed=make_warning_embed("Антиспам", spam_message),
                ephemeral=True
            )
            return

        if not begin_registration_processing(member.id, interaction.guild.id):
            await interaction.response.send_message(
                embed=make_warning_embed("Уже обрабатывается", "Ваш предыдущий запрос ещё обрабатывается. Подождите пару секунд."),
                ephemeral=True
            )
            return

        try:
            ok, result, suggestion = await apply_registration(
                member=member,
                real_name=real_name,
                rename_mode=self.rename_mode
            )

            if ok:
                extra = ""
                if suggestion:
                    extra = f"\nАвтоформат имени: **{real_name}** → **{suggestion}**"

                await interaction.response.send_message(
                    embed=make_success_embed("Готово", f"Ваш ник теперь: **{result}**{extra}"),
                    ephemeral=True
                )
            else:
                analysis = analyze_name(real_name)
                hint = ""
                if analysis["changed_case"]:
                    hint = f"\nПодсказка: попробуйте формат **{analysis['suggested']}**"

                await interaction.response.send_message(
                    embed=make_error_embed("Ошибка", result + hint),
                    ephemeral=True
                )
        finally:
            end_registration_processing(member.id, interaction.guild.id)


class RegistrationView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Указать имя",
        style=discord.ButtonStyle.primary,
        emoji="📝",
        custom_id="registration_register"
    )
    async def register_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=make_error_embed("Неверное место", "Эта кнопка работает только на сервере."),
                ephemeral=True
            )
            return

        member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message(
                embed=make_error_embed("Пользователь не найден", "Не удалось найти вас на сервере."),
                ephemeral=True
            )
            return

        unregistered_role = get_unregistered_role(interaction.guild)
        if unregistered_role and unregistered_role not in member.roles:
            await interaction.response.send_message(
                embed=make_warning_embed("Вы уже зарегистрированы", "Используйте кнопку **«Изменить имя»**."),
                ephemeral=True
            )
            return

        await interaction.response.send_modal(NameModal(rename_mode=False))

    @discord.ui.button(
        label="Изменить имя",
        style=discord.ButtonStyle.secondary,
        emoji="✏️",
        custom_id="registration_rename"
    )
    async def rename_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=make_error_embed("Неверное место", "Эта кнопка работает только на сервере."),
                ephemeral=True
            )
            return

        member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message(
                embed=make_error_embed("Пользователь не найден", "Не удалось найти вас на сервере."),
                ephemeral=True
            )
            return

        unregistered_role = get_unregistered_role(interaction.guild)
        if unregistered_role and unregistered_role in member.roles:
            await interaction.response.send_message(
                embed=make_warning_embed("Сначала регистрация", "Сначала нажмите **«Указать имя»**."),
                ephemeral=True
            )
            return

        await interaction.response.send_modal(NameModal(rename_mode=True))


# =========================
# UI badwords
# =========================

class ConfirmClearBadwordsView(discord.ui.View):
    def __init__(self, parent_view: "BadwordsListView", author_id: int):
        super().__init__(timeout=120)
        self.parent_view = parent_view
        self.author_id = author_id
        self.action_taken = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=make_warning_embed("Нет доступа", "Это подтверждение предназначено не для вас."),
                ephemeral=True
            )
            return False

        if not is_admin_interaction(interaction):
            await interaction.response.send_message(
                embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."),
                ephemeral=True
            )
            return False

        return True

    @discord.ui.button(label="Да, очистить", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def confirm_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.action_taken:
            await interaction.response.send_message(embed=make_warning_embed("Уже обработано", "Это действие уже было выполнено."), ephemeral=True)
            return

        self.action_taken = True
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

        clear_custom_badwords()
        self.parent_view.reload_data()

        if interaction.guild is not None:
            await send_log(
                interaction.guild,
                LogType.ADMIN,
                "Очищен пользовательский список запрещённых слов",
                "Пользовательский список запрещённых слов полностью очищен.",
                moderator=interaction.user
            )

        await interaction.response.edit_message(embed=self.parent_view.build_embed(), view=self.parent_view)
        self.stop()

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.action_taken:
            await interaction.response.send_message(embed=make_warning_embed("Уже обработано", "Это действие уже было выполнено."), ephemeral=True)
            return

        self.action_taken = True
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        await interaction.response.edit_message(embed=self.parent_view.build_embed(), view=self.parent_view)
        self.stop()


class CustomBadwordsSelect(discord.ui.Select):
    def __init__(self, parent_view: "BadwordsListView"):
        self.parent_view = parent_view
        page_words = parent_view.get_page_slice()

        options = [
            discord.SelectOption(
                label=word[:100],
                value=word,
                description=f"Удалить слово: {word[:50]}"
            )
            for word in page_words[:25]
        ]

        super().__init__(
            placeholder="Выберите пользовательское слово для удаления...",
            min_values=1,
            max_values=1,
            options=options,
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        word = self.values[0]
        ok, result = remove_custom_badword(word)

        if not ok:
            await interaction.response.send_message(
                embed=make_error_embed("Ошибка удаления", result),
                ephemeral=True
            )
            return

        self.parent_view.reload_data()

        if interaction.guild is not None:
            await send_log(
                interaction.guild,
                LogType.ADMIN,
                "Удалено запрещённое слово",
                f"Из пользовательского списка удалено слово: **{result}**",
                moderator=interaction.user
            )

        await interaction.response.edit_message(embed=self.parent_view.build_embed(), view=self.parent_view)


class BadwordsListView(discord.ui.View):
    def __init__(self, author_id: int, per_page: int = 15):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.per_page = per_page
        self.page = 0
        self.reload_data()

    def reload_data(self) -> None:
        self.default_words = get_default_badwords()
        self.custom_words = get_custom_badwords()
        self.max_page = max(0, (len(self.custom_words) - 1) // self.per_page)
        if self.page > self.max_page:
            self.page = self.max_page
        self.refresh_components()

    def get_page_slice(self) -> list[str]:
        start = self.page * self.per_page
        end = start + self.per_page
        return self.custom_words[start:end]

    def build_embed(self) -> discord.Embed:
        custom_page_words = self.get_page_slice()

        default_preview = self.default_words[:15]
        default_text = "\n".join(
            f"**{idx}.** `{word}`" for idx, word in enumerate(default_preview, start=1)
        )
        if not default_text:
            default_text = "Нет встроенных слов."
        elif len(self.default_words) > 15:
            default_text += f"\n\n`... и ещё {len(self.default_words) - 15}`"

        custom_text = "\n".join(
            f"**{idx}.** `{word}`"
            for idx, word in enumerate(custom_page_words, start=self.page * self.per_page + 1)
        )
        if not custom_text:
            custom_text = "Пользовательский список пуст."

        embed = discord.Embed(
            title="📛 Список запрещённых слов",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(
            name=f"Встроенный список ({len(self.default_words)})",
            value=default_text,
            inline=False
        )
        embed.add_field(
            name=f"Пользовательский список ({len(self.custom_words)})",
            value=custom_text,
            inline=False
        )

        embed.set_footer(text=f"Страница пользовательского списка {self.page + 1}/{self.max_page + 1}")
        return embed

    def update_button_states(self) -> None:
        self.prev_button.disabled = self.page <= 0 or len(self.custom_words) == 0
        self.next_button.disabled = self.page >= self.max_page or len(self.custom_words) == 0
        self.delete_all_button.disabled = len(self.custom_words) == 0

    def refresh_components(self) -> None:
        self.clear_items()

        if self.get_page_slice():
            self.add_item(CustomBadwordsSelect(self))

        self.add_item(self.prev_button)
        self.add_item(self.next_button)
        self.add_item(self.delete_all_button)
        self.update_button_states()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=make_warning_embed("Нет доступа", "Эта панель предназначена не для вас."),
                ephemeral=True
            )
            return False

        if not is_admin_interaction(interaction):
            await interaction.response.send_message(
                embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."),
                ephemeral=True
            )
            return False

        return True

    @discord.ui.button(label="◀ Назад", style=discord.ButtonStyle.secondary, row=1)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page -= 1
        self.refresh_components()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Вперёд ▶", style=discord.ButtonStyle.secondary, row=1)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page += 1
        self.refresh_components()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Очистить пользовательский список", style=discord.ButtonStyle.danger, row=1)
    async def delete_all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        confirm_view = ConfirmClearBadwordsView(self, self.author_id)
        await interaction.response.edit_message(
            embed=make_warning_embed(
                "Подтверждение очистки",
                "Вы точно хотите полностью очистить пользовательский список запрещённых слов?"
            ),
            view=confirm_view
        )


# =========================
# UI админки
# =========================

def build_admin_dashboard_embed(guild: discord.Guild) -> discord.Embed:
    total_members = guild.member_count or len(guild.members)
    registered = count_registrations(guild.id)
    unregistered_role = get_unregistered_role(guild)
    unregistered_count = len(unregistered_role.members) if unregistered_role else 0

    backup_files = list_backup_archives()

    registration_channel = get_registration_channel(guild)
    rules_channel = get_rules_channel(guild)
    log_channel = get_log_channel(guild)
    welcome_channel = get_welcome_channel(guild)
    member_role = get_member_role(guild)

    registration_channel_id = int(config.get("registration_channel_id", 0) or 0)
    rules_channel_id = int(config.get("rules_channel_id", 0) or 0)
    log_channel_id = int(config.get("log_channel_id", 0) or 0)
    welcome_channel_id = int(config.get("welcome_channel_id", 0) or 0)
    unregistered_role_id = int(config.get("unregistered_role_id", 0) or 0)
    member_role_id = int(config.get("member_role_id", 0) or 0)

    registration_message_ok = bool(int(config.get("registration_message_id", 0) or 0))
    rules_message_ok = bool(int(config.get("rules_message_id", 0) or 0))

    rows = get_last_registrations(guild.id, limit=1)
    if rows:
        try:
            last_registration_text = format_dt(rows[0]["updated_at"])
        except Exception:
            last_registration_text = "есть запись"
    else:
        last_registration_text = "нет данных"

    last_backup_text = (
        LAST_AUTO_BACKUP_AT.strftime("%d.%m.%Y %H:%M UTC")
        if LAST_AUTO_BACKUP_AT else "ещё не выполнялся"
    )

    uptime_seconds = int(time.time() - BOT_START_TIME)
    uptime_days = uptime_seconds // 86400
    uptime_hours = (uptime_seconds % 86400) // 3600
    uptime_minutes = (uptime_seconds % 3600) // 60
    if uptime_days > 0:
        uptime_text = f"{uptime_days}д {uptime_hours}ч {uptime_minutes}м"
    elif uptime_hours > 0:
        uptime_text = f"{uptime_hours}ч {uptime_minutes}м"
    else:
        uptime_text = f"{uptime_minutes}м"

    def channel_status(channel: Optional[discord.TextChannel]) -> str:
        return channel.mention if channel else "❌"

    def role_status(role: Optional[discord.Role]) -> str:
        return role.mention if role else "❌"

    def message_status(ok: bool) -> str:
        return "✅" if ok else "❌"

    issues = []

    if registration_channel_id and registration_channel is None:
        issues.append("⚠ Канал регистрации удалён или недоступен")
    elif not registration_channel_id:
        issues.append("⚠ Канал регистрации не настроен")

    if rules_channel_id and rules_channel is None:
        issues.append("⚠ Канал правил удалён или недоступен")
    elif not rules_channel_id:
        issues.append("⚠ Канал правил не настроен")

    if log_channel_id and log_channel is None:
        issues.append("⚠ Лог-канал удалён или недоступен")
    elif not log_channel_id:
        issues.append("⚠ Лог-канал не настроен")

    if welcome_channel_id and welcome_channel is None:
        issues.append("⚠ Welcome-канал удалён или недоступен")

    if unregistered_role_id and unregistered_role is None:
        issues.append("⚠ Роль новичка удалена или недоступна")
    elif not unregistered_role_id:
        issues.append("⚠ Роль новичка не настроена")

    if member_role_id and member_role is None:
        issues.append("⚠ Роль участника удалена или недоступна")
    elif not member_role_id:
        issues.append("⚠ Роль участника не настроена")

    if registration_channel and not registration_message_ok:
        issues.append("⚠ Сообщение регистрации не создано")

    if rules_channel and not rules_message_ok:
        issues.append("⚠ Сообщение правил не создано")
    system_line_1 = f"Verification {message_status(registration_message_ok)} | Rules {message_status(rules_message_ok)}"
    system_line_2 = f"Badwords: {len(get_custom_badwords())} | Бэкапы: {len(backup_files)}"
    system_line_3 = f"Время работы: {uptime_text}"
    system_line_4 = f"Автобэкап: {last_backup_text}"

    if issues:
        issues_value = "\n".join(issues[:4])
        if len(issues) > 4:
            issues_value += f"\n… и ещё **{len(issues) - 4}**"
    else:
        issues_value = "✅ Нет"

    embed = discord.Embed(
        title="🛠 Панель управления",
        color=discord.Color.dark_teal(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="👥 Пользователи",
        value=(
            f"{total_members} участников • {registered} в базе • {unregistered_count} ожидают\n"
            f"Последняя регистрация: {last_registration_text}"
        ),
        inline=False
    )
    embed.add_field(
        name="🗄 Система",
        value=(system_line_1 + "\n" + system_line_2 + "\n" + system_line_3 + "\n" + system_line_4),
        inline=False
    )
    embed.add_field(
        name="⚙ Настройки",
        value=(
            f"{channel_status(registration_channel)} | {channel_status(rules_channel)} | {channel_status(welcome_channel)} | {channel_status(log_channel)}\n"
            f"{role_status(unregistered_role)} | {role_status(member_role)}"
        ),
        inline=False
    )
    embed.add_field(
        name="⚠ Ошибки",
        value=issues_value,
        inline=False
    )
    embed.set_footer(text="Панель доступна только вам • автообновление каждые 15 сек.")
    return embed

def build_stats_embed(guild: discord.Guild) -> discord.Embed:
    total_members = guild.member_count or len(guild.members)
    registered = count_registrations(guild.id)
    unregistered_role = get_unregistered_role(guild)
    unregistered_count = len(unregistered_role.members) if unregistered_role else 0

    registrations = get_all_registrations(guild.id)
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = now - timedelta(days=7)

    today_count = 0
    week_count = 0
    last_registration_text = "нет данных"
    if registrations:
        for row in registrations:
            try:
                registered_at = datetime.fromisoformat(row["registered_at"])
            except Exception:
                continue
            if registered_at.tzinfo is None:
                registered_at = registered_at.replace(tzinfo=timezone.utc)
            if registered_at >= today_start:
                today_count += 1
            if registered_at >= week_start:
                week_count += 1
        try:
            last_registration_text = format_dt(registrations[0]["updated_at"])
        except Exception:
            last_registration_text = "есть запись"

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM rename_history WHERE guild_id = ?", (guild.id,))
    count_row = cur.fetchone()
    rename_history_count = int(count_row["count"]) if count_row else 0
    cur.execute("SELECT changed_at FROM rename_history WHERE guild_id = ? ORDER BY id DESC LIMIT 1", (guild.id,))
    rename_row = cur.fetchone()
    conn.close()

    last_rename_text = "нет данных"
    if rename_row:
        try:
            last_rename_text = format_dt(rename_row["changed_at"])
        except Exception:
            last_rename_text = "есть запись"

    last_backup_text = (
        LAST_AUTO_BACKUP_AT.strftime("%d.%m.%Y %H:%M UTC")
        if LAST_AUTO_BACKUP_AT else "ещё не выполнялся"
    )

    uptime_seconds = int(time.time() - BOT_START_TIME)
    uptime_days = uptime_seconds // 86400
    uptime_hours = (uptime_seconds % 86400) // 3600
    uptime_minutes = (uptime_seconds % 3600) // 60
    if uptime_days > 0:
        uptime_text = f"{uptime_days} д. {uptime_hours} ч. {uptime_minutes} мин."
    elif uptime_hours > 0:
        uptime_text = f"{uptime_hours} ч. {uptime_minutes} мин."
    else:
        uptime_text = f"{uptime_minutes} мин."

    embed = discord.Embed(
        title="📊 Статистика сервера",
        description="Краткая сводка по участникам, регистрации, базе данных и работе бота.",
        color=discord.Color.blurple(),
        timestamp=now
    )
    embed.add_field(
        name="👥 Участники",
        value=(
            f"Всего на сервере: **{total_members}**\n"
            f"Зарегистрировано: **{registered}**\n"
            f"Ожидают регистрацию: **{unregistered_count}**"
        ),
        inline=False
    )
    embed.add_field(
        name="📝 Регистрация",
        value=(
            f"За сегодня: **{today_count}**\n"
            f"За 7 дней: **{week_count}**\n"
            f"Последняя регистрация: **{last_registration_text}**"
        ),
        inline=False
    )
    embed.add_field(
        name="🗄 База данных",
        value=(
            f"Всего записей: **{registered}**\n"
            f"История имён: **{rename_history_count}**\n"
            f"Последнее изменение имени: **{last_rename_text}**"
        ),
        inline=False
    )
    embed.add_field(
        name="📛 Badwords",
        value=(
            f"Слов в пользовательском списке: **{len(get_custom_badwords())}**\n"
            f"Кэш загружен: **✅**"
        ),
        inline=False
    )
    embed.add_field(
        name="💾 Бэкапы",
        value=(
            f"Всего архивов: **{len(list_backup_archives())}**\n"
            f"Последний бэкап: **{last_backup_text}**\n"
            f"Интервал: **{get_backup_interval_hours()} ч.**"
        ),
        inline=False
    )
    embed.add_field(
        name="🤖 Бот",
        value=(
            f"Аптайм: **{uptime_text}**\n"
            f"Ошибок за сессию: **0**"
        ),
        inline=False
    )
    embed.set_footer(text="Раздел: Статистика")
    return embed


def build_settings_embed(guild: discord.Guild) -> discord.Embed:
    registration_channel = get_registration_channel(guild)
    log_channel = get_log_channel(guild)
    welcome_channel = get_welcome_channel(guild)
    rules_channel = get_rules_channel(guild)
    unregistered_role = get_unregistered_role(guild)
    member_role = get_member_role(guild)

    embed = discord.Embed(
        title="⚙ Настройки сервера",
        description="Здесь находятся каналы, роли, тексты и системные числовые параметры сервера.",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Каналы",
        value=(
            f"📺 Регистрация: {registration_channel.mention if registration_channel else 'не выбрано'}\n"
            f"📘 Правила: {rules_channel.mention if rules_channel else 'не выбрано'}\n"
            f"👋 Приветствие: {welcome_channel.mention if welcome_channel else 'не выбрано'}\n"
            f"📜 Логи: {log_channel.mention if log_channel else 'не выбрано'}"
        ),
        inline=False
    )
    embed.add_field(
        name="Роли",
        value=(
            f"🎭 Гости: {unregistered_role.mention if unregistered_role else 'не выбрано'}\n"
            f"🎭 Участники: {member_role.mention if member_role else 'не выбрано'}"
        ),
        inline=False
    )
    embed.add_field(
        name="Числовые настройки",
        value=(
            f"⏱ Кулдаун смены имени: **{int(config.get('rename_cooldown_hours', 24))} ч.**\n"
            f"🚫 Антиспам регистрации: **{get_registration_attempt_cooldown_seconds()} сек.**\n"
            f"💾 Интервал автобэкапа: **{get_backup_interval_hours()} ч.**\n"
            f"📦 Макс. бэкапов: **{get_backup_max_files()}**\n"
            f"🗃 Архивов экспортов: **{get_exports_archive_max_files()}**"
        ),
        inline=False
    )
    embed.set_footer(text="Раздел: Настройки")
    return embed


def build_backup_manager_embed() -> discord.Embed:
    backups = list_backup_archives()
    exports = list_export_archives()
    latest = backups[0].name if backups else 'нет'

    embed = discord.Embed(
        title="💾 Бэкапы",
        description="Управление резервными копиями и архивами экспортов.",
        color=discord.Color.dark_blue(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Текущее состояние",
        value=(
            f"Последний бэкап: **{latest}**\n"
            f"Всего архивов бэкапа: **{len(backups)}**\n"
            f"Архивов экспортов: **{len(exports)}**"
        ),
        inline=False
    )
    embed.add_field(
        name="Настройки",
        value=(
            f"Интервал автобэкапа: **{get_backup_interval_hours()} ч.**\n"
            f"Хранить бэкапов: **{get_backup_max_files()}**\n"
            f"Хранить архивов экспортов: **{get_exports_archive_max_files()}**"
        ),
        inline=False
    )
    embed.set_footer(text="Раздел: Бэкапы")
    return embed


def build_badwords_manager_embed() -> discord.Embed:
    default_words = get_default_badwords()
    custom_words = get_custom_badwords()
    preview = custom_words[:10]
    preview_text = "\n".join(f"• `{word}`" for word in preview) if preview else "Пользовательский список пуст."
    if len(custom_words) > 10:
        preview_text += f"\n\n`... и ещё {len(custom_words) - 10}`"

    embed = discord.Embed(
        title="📛 Badwords Manager",
        description="Управление встроенным и пользовательским списком запрещённых слов.",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Счётчики",
        value=(
            f"Встроенных слов: **{len(default_words)}**\n"
            f"Пользовательских слов: **{len(custom_words)}**"
        ),
        inline=False
    )
    embed.add_field(name="Предпросмотр пользовательского списка", value=preview_text, inline=False)
    embed.set_footer(text="Раздел: Badwords")
    return embed


def build_quick_actions_embed(guild: discord.Guild) -> discord.Embed:
    embed = discord.Embed(
        title="⚡ Quick Actions",
        description="Быстрые действия для админа без ручных команд.",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Доступно",
        value=(
            "• Обновить Verification Center\n"
            "• Создать бэкап\n"
            "• Экспортировать БД\n"
            "• Очистить старые архивы\n"
            "• Запустить диагностику\n"
            "• Обновить сообщение правил"
        ),
        inline=False
    )
    embed.set_footer(text=f"Сервер: {guild.name}")
    return embed


def get_bot_member(guild: discord.Guild) -> Optional[discord.Member]:
    member = guild.me
    if member is None and bot.user is not None:
        member = guild.get_member(bot.user.id)
    return member


async def set_log_channel_locked(guild: discord.Guild, locked: bool) -> None:
    channel = get_log_channel(guild)
    if channel is None:
        raise ValueError("Лог-канал не настроен")

    bot_member = get_bot_member(guild)
    me = bot_member or guild.me
    if me is None:
        raise ValueError("Не удалось определить участника-бота на сервере")

    permissions = channel.permissions_for(me)
    if not permissions.manage_channels:
        raise discord.Forbidden(None, "Bot requires Manage Channels in the log channel")

    if locked:
        await channel.set_permissions(guild.default_role, view_channel=False, send_messages=False)
        await channel.set_permissions(
            me,
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            embed_links=True,
            attach_files=True,
        )
    else:
        await channel.set_permissions(guild.default_role, overwrite=None)
        await channel.set_permissions(me, overwrite=None)



def build_roles_embed(guild: discord.Guild) -> discord.Embed:
    unregistered_role = get_unregistered_role(guild)
    member_role = get_member_role(guild)
    me = guild.me

    can_manage_roles = bool(guild.me.guild_permissions.manage_roles) if guild.me else False
    bot_top_role = me.top_role if me else None
    member_role_ok = bool(bot_top_role and member_role and bot_top_role > member_role)
    unregistered_role_ok = bool(bot_top_role and unregistered_role and bot_top_role > unregistered_role)

    embed = discord.Embed(
        title="🎭 Роли",
        description="Настройка ролей регистрации и быстрая проверка, сможет ли бот ими управлять.",
        color=discord.Color.purple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Текущие роли",
        value=(
            f"Новички: {unregistered_role.mention if unregistered_role else 'не выбрано'}\n"
            f"Участники: {member_role.mention if member_role else 'не выбрано'}"
        ),
        inline=False
    )
    embed.add_field(
        name="Проверка",
        value=(
            f"Manage Roles: {'✅' if can_manage_roles else '❌'}\n"
            f"Бот выше роли новичка: {'✅' if unregistered_role_ok else '❌'}\n"
            f"Бот выше роли участника: {'✅' if member_role_ok else '❌'}"
        ),
        inline=False
    )
    embed.set_footer(text="Раздел: Роли")
    return embed


def build_messages_embed(guild: discord.Guild) -> discord.Embed:
    registration_channel = get_registration_channel(guild)
    rules_channel = get_rules_channel(guild)
    registration_message_id = int(config.get("registration_message_id", 0) or 0)
    rules_message_id = int(config.get("rules_message_id", 0) or 0)
    registration_template = get_registration_message_template()
    rules_template = get_rules_message_template()

    embed = discord.Embed(
        title="📝 Сообщения",
        description="Управление текстами, предпросмотром и обновлением служебных сообщений сервера.",
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Текущее состояние",
        value=(
            f"Канал регистрации: {registration_channel.mention if registration_channel else 'не выбрано'}\n"
            f"Сообщение регистрации: {'✅' if registration_message_id else '❌'}\n"
            f"Канал правил: {rules_channel.mention if rules_channel else 'не выбрано'}\n"
            f"Сообщение правил: {'✅' if rules_message_id else '❌'}"
        ),
        inline=False
    )
    embed.add_field(
        name="Шаблоны",
        value=(
            f"Регистрация: **{'пользовательский' if str(config.get('registration_message_text') or '').strip() else 'стандартный'}**\n"
            f"Правила: **{'пользовательский' if str(config.get('rules_message_text') or '').strip() else 'стандартный'}**\n"
            f"Длина текста регистрации: **{len(registration_template)}**\n"
            f"Длина текста правил: **{len(rules_template)}**"
        ),
        inline=False
    )
    embed.set_footer(text="Раздел: Сообщения")
    return embed


def build_database_embed(guild: discord.Guild) -> discord.Embed:
    rows = get_last_registrations(guild.id, limit=5)
    preview = "\n".join(
        f"• **{row['real_name']}** — `{row['user_id']}`"
        for row in rows
    ) if rows else "Пока нет зарегистрированных пользователей."

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM rename_history WHERE guild_id = ?", (guild.id,))
    count_row = cur.fetchone()
    rename_history_count = int(count_row["count"]) if count_row else 0
    conn.close()

    embed = discord.Embed(
        title="🗄 База данных",
        description="Поиск, экспорт и быстрый обзор записей регистрации.",
        color=discord.Color.dark_gold(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Статистика",
        value=(
            f"Записей регистрации: **{count_registrations(guild.id)}**\n"
            f"История имён: **{rename_history_count}**\n"
            f"Архивов экспортов: **{len(list_export_archives())}**"
        ),
        inline=False
    )
    embed.add_field(name="Последние регистрации", value=preview, inline=False)
    embed.set_footer(text="Раздел: База данных")
    return embed


async def build_logs_embed(guild: discord.Guild) -> discord.Embed:
    channel = get_log_channel(guild)
    embed = discord.Embed(
        title="📋 Логи",
        description="Управление лог-каналом и просмотр последних записей.",
        color=discord.Color.dark_grey(),
        timestamp=datetime.now(timezone.utc)
    )

    if channel is None:
        embed.add_field(name="Канал", value="❌ Лог-канал не настроен.", inline=False)
        embed.add_field(name="Последние записи", value="Сначала выберите лог-канал в настройках.", inline=False)
        embed.set_footer(text="Раздел: Логи")
        return embed

    everyone_overwrite = channel.overwrites_for(guild.default_role)
    view_channel = everyone_overwrite.view_channel
    if view_channel is False:
        visibility = "🔒 Скрыт от @everyone"
    elif view_channel is True:
        visibility = "🔓 Видим для @everyone"
    else:
        visibility = "⚪ Наследует права"

    send_messages = everyone_overwrite.send_messages
    if send_messages is False:
        write_status = "✍️ Запись запрещена"
    elif send_messages is True:
        write_status = "✍️ Запись разрешена"
    else:
        write_status = "✍️ Запись наследует права"

    bot_member = get_bot_member(guild)
    bot_status = "🤖 Права бота не определены"
    if bot_member is not None:
        perms = channel.permissions_for(bot_member)
        if perms.view_channel and perms.send_messages and perms.read_message_history:
            bot_status = "🤖 Бот имеет доступ к чтению и записи"
        elif perms.view_channel:
            bot_status = "🤖 Бот видит канал, но права неполные"
        else:
            bot_status = "🤖 Бот не видит канал"

    embed.add_field(
        name="Канал",
        value=f"{channel.mention}\n{visibility}\n{write_status}\n{bot_status}",
        inline=False
    )

    try:
        messages = [message async for message in channel.history(limit=25)]
    except (discord.Forbidden, discord.HTTPException):
        embed.add_field(name="Последние записи", value="❌ Не удалось прочитать лог-канал.", inline=False)
        embed.set_footer(text="Раздел: Логи")
        return embed

    lines = []
    for message in messages[:10]:
        title = message.embeds[0].title if message.embeds else (message.content[:60] or 'Сообщение')
        lines.append(f"• `{message.created_at.strftime('%d.%m %H:%M')}` — {title}")

    embed.add_field(
        name="Последние записи",
        value="\n".join(lines) if lines else "Логи пока пусты.",
        inline=False
    )
    embed.set_footer(text="Раздел: Логи")
    return embed


async def build_db_search_result_embed(guild: discord.Guild, query: str) -> tuple[Optional[discord.Embed], Optional[int], str]:
    query = query.strip()
    if not query:
        return None, None, "Введите ID или имя для поиска."

    registration = None
    user_id = None

    if query.isdigit():
        user_id = int(query)
        registration = get_registration(user_id, guild.id)
    else:
        for row in get_all_registrations(guild.id):
            if query.lower() in row['real_name'].lower() or query.lower() in row['discord_name'].lower() or query.lower() in row['nickname'].lower():
                registration = row
                user_id = int(row['user_id'])
                break

    if registration is None or user_id is None:
        return None, None, "Совпадений в базе не найдено."

    member = guild.get_member(user_id)
    if member is not None:
        return build_user_db_embed_from_member(member, registration), user_id, ""
    return build_user_db_embed_offline(user_id, registration), user_id, ""


async def admin_force_rename_member(member: discord.Member, real_name: str, moderator: discord.abc.User) -> tuple[bool, str]:
    analysis = analyze_name(real_name)
    if not analysis['ok']:
        return False, analysis['reason'] or 'Имя не прошло проверку.'

    registration = get_registration(member.id, member.guild.id)
    if registration is None:
        return False, "У пользователя нет записи в базе."

    formatted_name = analysis['suggested']
    base_name = extract_base_name(member)
    new_nick = build_nickname(base_name, formatted_name)
    old_real_name = registration['real_name']

    try:
        await member.edit(nick=new_nick, reason=f"Администратор {moderator} изменил имя")
    except discord.Forbidden:
        return False, "Не удалось изменить ник: нет прав."
    except discord.HTTPException:
        return False, "Discord вернул ошибку при изменении ника."

    upsert_registration(
        user_id=member.id,
        guild_id=member.guild.id,
        discord_name=member.name,
        real_name=formatted_name,
        nickname=new_nick
    )
    add_rename_history(member.id, member.guild.id, old_real_name=old_real_name, new_real_name=formatted_name)

    await send_log(
        member.guild,
        LogType.ADMIN,
        "Администратор изменил имя",
        f"Новое имя: **{formatted_name}**\nНовый ник: **{new_nick}**",
        member=member,
        moderator=moderator
    )
    return True, new_nick


class ConfirmAdminActionView(discord.ui.View):
    def __init__(self, requester_id: int, target_member_id: int, action: AdminAction):
        super().__init__(timeout=120)
        self.requester_id = requester_id
        self.target_member_id = target_member_id
        self.action = action
        self.action_taken = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message(
                embed=make_warning_embed("Нет доступа", "Это подтверждение предназначено не для вас."),
                ephemeral=True
            )
            return False

        if not is_admin_interaction(interaction):
            await interaction.response.send_message(
                embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."),
                ephemeral=True
            )
            return False

        return True

    async def disable_all_buttons(self, message: discord.Message) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        try:
            await message.edit(view=self)
        except (discord.Forbidden, discord.HTTPException):
            pass

    @discord.ui.button(label="Подтвердить", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.action_taken:
            await interaction.response.send_message(
                embed=make_warning_embed("Уже обработано", "Это действие уже было выполнено."),
                ephemeral=True
            )
            return

        self.action_taken = True

        if interaction.guild is None:
            await interaction.response.send_message(
                embed=make_error_embed("Неверное место", "Эта кнопка работает только на сервере."),
                ephemeral=True
            )
            return

        member = interaction.guild.get_member(self.target_member_id)
        if member is None:
            await interaction.response.send_message(
                embed=make_error_embed("Пользователь не найден", "Не удалось найти пользователя на сервере."),
                ephemeral=True
            )
            return

        ok, result = await execute_admin_action(
            guild=interaction.guild,
            action=self.action,
            member=member,
            moderator=interaction.user
        )

        if interaction.message is not None:
            await self.disable_all_buttons(interaction.message)

        if ok:
            await interaction.response.send_message(embed=make_success_embed("Готово", result), ephemeral=True)
        else:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", result), ephemeral=True)

        self.stop()

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.action_taken:
            await interaction.response.send_message(
                embed=make_warning_embed("Уже обработано", "Это действие уже было выполнено."),
                ephemeral=True
            )
            return

        self.action_taken = True

        if interaction.message is not None:
            await self.disable_all_buttons(interaction.message)

        await interaction.response.send_message(
            embed=make_warning_embed("Отменено", "Действие отменено."),
            ephemeral=True
        )
        self.stop()


class AdminRenameModal(discord.ui.Modal, title="Изменить имя пользователю"):
    def __init__(self, member_id: int):
        super().__init__()
        self.member_id = member_id
        self.name_input = discord.ui.TextInput(
            label="Новое имя",
            placeholder="Например: Денис",
            min_length=2,
            max_length=20,
            required=True,
        )
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        member = interaction.guild.get_member(self.member_id)
        if member is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Пользователь не найден на сервере."), ephemeral=True)
            return

        ok, result = await admin_force_rename_member(member, self.name_input.value, interaction.user)
        if ok:
            await interaction.response.send_message(
                embed=make_success_embed("Имя обновлено", f"Новый ник пользователя: **{result}**"),
                ephemeral=True
            )
        else:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", result), ephemeral=True)


class DBSearchModal(discord.ui.Modal, title="Поиск пользователя в базе"):
    def __init__(self, author_id: int):
        super().__init__()
        self.author_id = author_id
        self.query_input = discord.ui.TextInput(
            label="ID или имя",
            placeholder="Например: 123456789 или Денис",
            min_length=1,
            max_length=100,
            required=True,
        )
        self.add_item(self.query_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        embed, user_id, error_text = await build_db_search_result_embed(interaction.guild, self.query_input.value)
        if embed is None or user_id is None:
            await interaction.response.send_message(embed=make_warning_embed("Поиск", error_text), ephemeral=True)
            return

        view = AdminUserActionView(author_id=self.author_id, member_id=user_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)




class ActionSearchModal(discord.ui.Modal):
    def __init__(self, author_id: int, action: str):
        titles = {
            "info": "Найти пользователя",
            "history": "История имён",
            "restore": "Восстановить из БД",
            "reset": "Сбросить регистрацию",
            "delete": "Удалить из базы",
            "rename": "Изменить имя",
        }
        super().__init__(title=titles.get(action, "Пользователь"))
        self.author_id = author_id
        self.action = action
        self.query_input = discord.ui.TextInput(
            label="ID или имя",
            placeholder="Например: 123456789 или Денис",
            min_length=1,
            max_length=100,
            required=True,
        )
        self.add_item(self.query_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        embed, user_id, error_text = await build_db_search_result_embed(interaction.guild, self.query_input.value)
        if embed is None or user_id is None:
            await interaction.response.send_message(embed=make_warning_embed("Поиск", error_text), ephemeral=True)
            return

        if self.action == "info":
            view = AdminUserActionView(author_id=self.author_id, member_id=user_id)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            return

        member = interaction.guild.get_member(user_id)
        registration = get_registration(user_id, interaction.guild.id)

        if self.action == "history":
            rows = get_name_history(user_id, interaction.guild.id, limit=20)
            if member:
                hist_embed = build_name_history_embed(
                    title_text="📜 История имён",
                    description_text=f"История для {member.mention}",
                    avatar_url=member.display_avatar.url,
                    current_real_name=registration["real_name"] if registration else None,
                    current_nickname=registration["nickname"] if registration else None,
                    rows=rows
                )
            else:
                hist_embed = build_name_history_embed(
                    title_text="📜 История имён",
                    description_text=f"История для пользователя с ID `{user_id}`",
                    avatar_url=None,
                    current_real_name=registration["real_name"] if registration else None,
                    current_nickname=registration["nickname"] if registration else None,
                    rows=rows
                )
            await interaction.response.send_message(embed=hist_embed, ephemeral=True)
            return

        if member is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Пользователь не найден на сервере."), ephemeral=True)
            return

        if self.action == "rename":
            await interaction.response.send_modal(AdminRenameModal(user_id))
            return

        action_map = {
            "restore": AdminAction.RESTORE_FROM_DB,
            "reset": AdminAction.RESET_DB_USER,
            "delete": AdminAction.DELETE_DB_USER,
        }
        title_map = {
            "restore": ("Подтверждение восстановления", f"Вы точно хотите восстановить ник и роли пользователя {member.mention} из базы?"),
            "reset": ("Подтверждение сброса", f"Вы точно хотите сбросить регистрацию пользователя {member.mention}?"),
            "delete": ("Подтверждение удаления", f"Вы точно хотите удалить **{member}** из базы?"),
        }
        view = ConfirmAdminActionView(
            requester_id=interaction.user.id,
            target_member_id=user_id,
            action=action_map[self.action]
        )
        title, desc = title_map[self.action]
        await interaction.response.send_message(embed=make_warning_embed(title, desc), view=view, ephemeral=True)


class NumericSettingModal(discord.ui.Modal):
    def __init__(self, title_text: str, field_name: str, min_value: int, max_value: int, current_value: int, config_key: str, success_label: str):
        super().__init__(title=title_text)
        self.field_name = field_name
        self.min_value = min_value
        self.max_value = max_value
        self.config_key = config_key
        self.success_label = success_label
        self.value_input = discord.ui.TextInput(
            label=field_name,
            placeholder=f"От {min_value} до {max_value}",
            default=str(current_value),
            min_length=1,
            max_length=10,
            required=True,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            value = int(self.value_input.value.strip())
        except ValueError:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужно ввести целое число."), ephemeral=True)
            return

        if not (self.min_value <= value <= self.max_value):
            await interaction.response.send_message(
                embed=make_error_embed("Ошибка", f"Допустимый диапазон: от {self.min_value} до {self.max_value}."),
                ephemeral=True
            )
            return

        config[self.config_key] = value
        save_config(config)
        if interaction.guild is not None and self.config_key in {"rename_cooldown_hours", "registration_attempt_cooldown_seconds"}:
            await refresh_registration_message(interaction.guild)
            await refresh_rules_message(interaction.guild)

        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"{self.success_label}: **{value}**"),
            ephemeral=True
        )


class MessageEditorModal(discord.ui.Modal):
    def __init__(self, title_text: str, field_label: str, config_key: str, success_label: str, default_text: str):
        super().__init__(title=title_text)
        self.config_key = config_key
        self.success_label = success_label
        self.text_input = discord.ui.TextInput(
            label=field_label,
            style=discord.TextStyle.paragraph,
            default=default_text[:4000],
            min_length=1,
            max_length=4000,
            required=True,
        )
        self.add_item(self.text_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        value = self.text_input.value.strip()
        config[self.config_key] = value
        save_config(config)

        if interaction.guild is not None:
            if self.config_key == "registration_message_text":
                await ensure_registration_message(interaction.guild)
                await refresh_registration_message(interaction.guild)
            elif self.config_key == "rules_message_text":
                await ensure_rules_message(interaction.guild)
                await refresh_rules_message(interaction.guild)

        await interaction.response.send_message(
            embed=make_success_embed("Сообщение обновлено", f"{self.success_label} успешно сохранено."),
            ephemeral=True
        )


class SetRulesChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="Выберите канал правил", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["rules_channel_id"] = channel.id
        config["rules_message_id"] = 0
        save_config(config)
        if interaction.guild is not None:
            await ensure_rules_message(interaction.guild)
            await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"Канал правил изменён на {channel.mention}."),
            ephemeral=True
        )


class SetRegistrationChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="Выберите канал регистрации", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["registration_channel_id"] = channel.id
        config["registration_message_id"] = 0
        save_config(config)
        if interaction.guild is not None:
            await ensure_registration_message(interaction.guild)
            await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"Канал регистрации изменён на {channel.mention}."),
            ephemeral=True
        )


class SetLogChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="Выберите лог-канал", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=3)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["log_channel_id"] = channel.id
        save_config(config)
        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"Лог-канал изменён на {channel.mention}."),
            ephemeral=True
        )


class SetWelcomeChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="Выберите welcome-канал", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=2)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["welcome_channel_id"] = channel.id
        save_config(config)
        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"Welcome-канал изменён на {channel.mention}."),
            ephemeral=True
        )


class SetUnregisteredRoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="Выберите роль гостей", min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        role = self.values[0]
        config["unregistered_role_id"] = role.id
        save_config(config)
        if interaction.guild is not None:
            await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"Роль незарегистрированных изменена на **{role.name}**."),
            ephemeral=True
        )


class SetMemberRoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="Выберите роль участников", min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        role = self.values[0]
        config["member_role_id"] = role.id
        save_config(config)
        if interaction.guild is not None:
            await refresh_registration_message(interaction.guild)
            await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(
            embed=make_success_embed("Настройки обновлены", f"Роль участников изменена на **{role.name}**."),
            ephemeral=True
        )


class BaseAdminSubview(discord.ui.View):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(embed=make_warning_embed("Нет доступа", "Эта панель предназначена не для вас."), ephemeral=True)
            return False
        if not is_admin_interaction(interaction):
            await interaction.response.send_message(embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."), ephemeral=True)
            return False
        return True

    @discord.ui.button(label="◀ Назад", style=discord.ButtonStyle.secondary, row=4)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = AdminPanelView(author_id=self.author_id, guild_id=self.guild_id)
        await interaction.response.edit_message(
            embed=build_admin_dashboard_embed(interaction.guild),
            view=view
        )
        await view.bind_message(interaction.message)


class ChannelSetupView(BaseAdminSubview):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(author_id, guild_id)
        self.add_item(SetRegistrationChannelSelect())
        self.add_item(SetRulesChannelSelect())
        self.add_item(SetWelcomeChannelSelect())
        self.add_item(SetLogChannelSelect())

    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        registration_channel = get_registration_channel(guild)
        rules_channel = get_rules_channel(guild)
        welcome_channel = get_welcome_channel(guild)
        log_channel = get_log_channel(guild)
        embed = discord.Embed(
            title="📺 Каналы",
            description=(
                "Настройка служебных каналов сервера.\n\n"
                "Здесь выбираются основные каналы, которые использует бот для работы."
            ),
            color=discord.Color.light_grey(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(
            name="Текущая конфигурация",
            value=(
                f"• Регистрация — {registration_channel.mention if registration_channel else 'не выбрано'}\n"
                f"• Правила — {rules_channel.mention if rules_channel else 'не выбрано'}\n"
                f"• Welcome — {welcome_channel.mention if welcome_channel else 'не выбрано'}\n"
                f"• Логи — {log_channel.mention if log_channel else 'не выбрано'}"
            ),
            inline=False
        )
        embed.add_field(
            name="Управление доступом",
            value="Для registration / rules / logs можно открыть отдельное меню блокировки.",
            inline=False
        )
        embed.set_footer(text="Раздел: Каналы")
        return embed

    @discord.ui.button(label="🔒 Управление", style=discord.ButtonStyle.secondary, row=4)
    async def lock_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = ChannelLockView(author_id=self.author_id, guild_id=self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)


class ChannelLockView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        registration_channel = get_registration_channel(guild)
        rules_channel = get_rules_channel(guild)
        log_channel = get_log_channel(guild)
        embed = discord.Embed(
            title="🔒 Управление каналами",
            description="Здесь можно закрыть или открыть служебные каналы для `@everyone`.",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(
            name="Текущие каналы",
            value=(
                f"Регистрация: {registration_channel.mention if registration_channel else 'не выбрано'}\n"
                f"Правила: {rules_channel.mention if rules_channel else 'не выбрано'}\n"
                f"Логи: {log_channel.mention if log_channel else 'не выбрано'}"
            ),
            inline=False
        )
        embed.add_field(
            name="Что меняется",
            value="Кнопки ниже меняют право `send_messages` для `@everyone`.",
            inline=False
        )
        embed.set_footer(text="Раздел: Каналы → Управление")
        return embed

    async def _set_channel_send_permission(self, interaction: discord.Interaction, channel_key: str, allowed: bool, success_text: str):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        channel_id = int(config.get(channel_key, 0) or 0)
        if not channel_id:
            await interaction.response.send_message(embed=make_warning_embed("Канал не настроен", "Сначала выберите нужный канал в разделе каналов."), ephemeral=True)
            return
        channel = interaction.guild.get_channel(channel_id)
        if channel is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось найти выбранный канал."), ephemeral=True)
            return
        try:
            await channel.set_permissions(interaction.guild.default_role, send_messages=allowed)
        except discord.Forbidden:
            await interaction.response.send_message(embed=make_error_embed("Нет прав", "Бот не может изменить права этого канала."), ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось изменить права канала."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Готово", success_text), ephemeral=True)

    @discord.ui.button(label="🔒 Lock регистрация", style=discord.ButtonStyle.secondary, row=0)
    async def lock_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "registration_channel_id", False, "Канал регистрации закрыт для отправки сообщений.")

    @discord.ui.button(label="🔓 Unlock регистрация", style=discord.ButtonStyle.success, row=0)
    async def unlock_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "registration_channel_id", True, "Канал регистрации снова открыт для отправки сообщений.")

    @discord.ui.button(label="🔒 Lock правила", style=discord.ButtonStyle.secondary, row=1)
    async def lock_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "rules_channel_id", False, "Канал правил закрыт для отправки сообщений.")

    @discord.ui.button(label="🔓 Unlock правила", style=discord.ButtonStyle.success, row=1)
    async def unlock_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "rules_channel_id", True, "Канал правил снова открыт для отправки сообщений.")

    @discord.ui.button(label="🔒 Lock логи", style=discord.ButtonStyle.secondary, row=2)
    async def lock_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "log_channel_id", False, "Лог-канал закрыт для отправки сообщений.")

    @discord.ui.button(label="🔓 Unlock логи", style=discord.ButtonStyle.success, row=2)
    async def unlock_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "log_channel_id", True, "Лог-канал снова открыт для отправки сообщений.")

    @discord.ui.button(label="◀ К каналам", style=discord.ButtonStyle.secondary, row=4)
    async def back_to_channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = ChannelSetupView(author_id=self.author_id, guild_id=self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)


class RoleSetupView(BaseAdminSubview):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(author_id, guild_id)
        self.add_item(SetUnregisteredRoleSelect())
        self.add_item(SetMemberRoleSelect())

    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_roles_embed(guild)

    @discord.ui.button(label="📏 Проверить позиции", style=discord.ButtonStyle.secondary, row=2)
    async def check_positions_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.edit_message(embed=self.build_embed(interaction.guild), view=RoleSetupView(self.author_id, self.guild_id))

    @discord.ui.button(label="🔄 Синхронизировать роли", style=discord.ButtonStyle.primary, row=2)
    async def sync_roles_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        member_role = get_member_role(interaction.guild)
        unregistered_role = get_unregistered_role(interaction.guild)
        if member_role is None and unregistered_role is None:
            await interaction.response.send_message(
                embed=make_warning_embed("Роли не настроены", "Сначала выберите роли в этом разделе."),
                ephemeral=True
            )
            return

        updated = 0
        skipped = 0
        for row in get_all_registrations(interaction.guild.id):
            member = interaction.guild.get_member(int(row['user_id']))
            if member is None:
                skipped += 1
                continue
            changed = False
            try:
                if member_role and member_role not in member.roles:
                    await member.add_roles(member_role, reason="Синхронизация ролей через админ-панель")
                    changed = True
                if unregistered_role and unregistered_role in member.roles:
                    await member.remove_roles(unregistered_role, reason="Синхронизация ролей через админ-панель")
                    changed = True
            except discord.Forbidden:
                skipped += 1
                continue
            except discord.HTTPException:
                skipped += 1
                continue
            if changed:
                updated += 1

        await interaction.response.send_message(
            embed=make_success_embed("Синхронизация завершена", f"Обновлено участников: **{updated}**\nПропущено: **{skipped}**"),
            ephemeral=True
        )

    @discord.ui.button(label="🧪 Тест ролей", style=discord.ButtonStyle.secondary, row=2)
    async def test_roles_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        me = interaction.guild.me
        if me is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Бот не найден на сервере."), ephemeral=True)
            return
        member_role = get_member_role(interaction.guild)
        unregistered_role = get_unregistered_role(interaction.guild)
        lines = [f"Manage Roles: {'✅' if me.guild_permissions.manage_roles else '❌'}"]
        if unregistered_role:
            lines.append(f"Роль новичка ниже бота: {'✅' if me.top_role > unregistered_role else '❌'}")
        if member_role:
            lines.append(f"Роль участника ниже бота: {'✅' if me.top_role > member_role else '❌'}")
        await interaction.response.send_message(
            embed=discord.Embed(title="🧪 Тест ролей", description="\n".join(lines), color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc)),
            ephemeral=True
        )


class TextSetupView(BaseAdminSubview):
    def __init__(self, author_id: int, guild_id: int, guild: discord.Guild):
        super().__init__(author_id, guild_id)
        self.guild = guild

    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_messages_embed(guild)

    @discord.ui.button(label="🔐 Текст регистрации", style=discord.ButtonStyle.secondary, row=0)
    async def edit_registration_text_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            MessageEditorModal(
                "Редактировать сообщение регистрации",
                "Текст сообщения",
                "registration_message_text",
                "Сообщение регистрации",
                get_registration_message_template(),
            )
        )

    @discord.ui.button(label="📜 Текст правил", style=discord.ButtonStyle.secondary, row=0)
    async def edit_rules_text_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            MessageEditorModal(
                "Редактировать сообщение правил",
                "Текст сообщения",
                "rules_message_text",
                "Сообщение правил",
                get_rules_message_template(),
            )
        )

    @discord.ui.button(label="👁 Предпросмотр регистрации", style=discord.ButtonStyle.primary, row=1)
    async def preview_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        text = get_registration_message_text(interaction.guild)
        embed = discord.Embed(title="👁 Предпросмотр сообщения регистрации", description=text[:4000], color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="👁 Предпросмотр правил", style=discord.ButtonStyle.primary, row=1)
    async def preview_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        text = get_rules_message_text(interaction.guild)
        embed = discord.Embed(title="👁 Предпросмотр сообщения правил", description=text[:4000], color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🔄 Обновить регистрацию", style=discord.ButtonStyle.success, row=2)
    async def refresh_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        msg = await ensure_registration_message(interaction.guild)
        if msg is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось создать или найти сообщение регистрации."), ephemeral=True)
            return
        await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение регистрации обновлено."), ephemeral=True)

    @discord.ui.button(label="🔄 Обновить правила", style=discord.ButtonStyle.success, row=2)
    async def refresh_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        msg = await ensure_rules_message(interaction.guild)
        if msg is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось создать или найти сообщение правил."), ephemeral=True)
            return
        await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение правил обновлено."), ephemeral=True)

    @discord.ui.button(label="♻ Переотправить регистрацию", style=discord.ButtonStyle.secondary, row=3)
    async def recreate_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        config["registration_message_id"] = 0
        save_config(config)
        msg = await ensure_registration_message(interaction.guild)
        if msg is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось переотправить сообщение регистрации."), ephemeral=True)
            return
        await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение регистрации переотправлено."), ephemeral=True)

    @discord.ui.button(label="♻ Переотправить правила", style=discord.ButtonStyle.secondary, row=3)
    async def recreate_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        config["rules_message_id"] = 0
        save_config(config)
        msg = await ensure_rules_message(interaction.guild)
        if msg is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось переотправить сообщение правил."), ephemeral=True)
            return
        await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение правил переотправлено."), ephemeral=True)


class SingleUseSelectView(discord.ui.View):
    def __init__(self, author_id: int, item: discord.ui.Item, title: str, description: str):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.add_item(item)
        self.title = title
        self.description = description

    def build_embed(self) -> discord.Embed:
        return discord.Embed(title=self.title, description=self.description, color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(embed=make_warning_embed("Нет доступа", "Эта панель предназначена не для вас."), ephemeral=True)
            return False
        return True


class AdminUserActionView(discord.ui.View):
    def __init__(self, author_id: int, member_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.member_id = member_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=make_warning_embed("Нет доступа", "Эта панель предназначена не для вас."),
                ephemeral=True
            )
            return False

        if not is_admin_interaction(interaction):
            await interaction.response.send_message(
                embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."),
                ephemeral=True
            )
            return False

        return True

    def _get_member(self, guild: discord.Guild) -> Optional[discord.Member]:
        return guild.get_member(self.member_id)

    @discord.ui.button(label="📜 История имён", style=discord.ButtonStyle.secondary)
    async def history_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        member = self._get_member(interaction.guild)
        registration = get_registration(self.member_id, interaction.guild.id)
        rows = get_name_history(self.member_id, interaction.guild.id, limit=20)

        if member:
            embed = build_name_history_embed(
                title_text="📜 История имён",
                description_text=f"История для {member.mention}",
                avatar_url=member.display_avatar.url,
                current_real_name=registration["real_name"] if registration else None,
                current_nickname=registration["nickname"] if registration else None,
                rows=rows
            )
        else:
            embed = build_name_history_embed(
                title_text="📜 История имён",
                description_text=f"История для пользователя с ID `{self.member_id}`",
                avatar_url=None,
                current_real_name=registration["real_name"] if registration else None,
                current_nickname=registration["nickname"] if registration else None,
                rows=rows
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🔁 Восстановить из БД", style=discord.ButtonStyle.success)
    async def restore_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        member = self._get_member(interaction.guild)
        if member is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Пользователь не найден на сервере."), ephemeral=True)
            return

        view = ConfirmAdminActionView(
            requester_id=interaction.user.id,
            target_member_id=member.id,
            action=AdminAction.RESTORE_FROM_DB
        )
        await interaction.response.send_message(
            embed=make_warning_embed(
                "Подтверждение восстановления",
                f"Вы точно хотите восстановить ник и роли пользователя {member.mention} из базы?"
            ),
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="♻ Сбросить регистрацию", style=discord.ButtonStyle.danger)
    async def reset_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        member = self._get_member(interaction.guild)
        if member is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Пользователь не найден на сервере."), ephemeral=True)
            return

        view = ConfirmAdminActionView(
            requester_id=interaction.user.id,
            target_member_id=member.id,
            action=AdminAction.RESET_DB_USER
        )
        await interaction.response.send_message(
            embed=make_warning_embed("Подтверждение сброса", f"Вы точно хотите сбросить регистрацию пользователя {member.mention}?"),
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="🗑 Удалить из базы", style=discord.ButtonStyle.danger)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        member = self._get_member(interaction.guild)
        if member is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Пользователь не найден на сервере."), ephemeral=True)
            return

        view = ConfirmAdminActionView(
            requester_id=interaction.user.id,
            target_member_id=member.id,
            action=AdminAction.DELETE_DB_USER
        )
        await interaction.response.send_message(
            embed=make_warning_embed("Подтверждение удаления", f"Вы точно хотите удалить **{member}** из базы?"),
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="✏ Изменить имя", style=discord.ButtonStyle.primary)
    async def rename_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AdminRenameModal(self.member_id))


class AdminUserSelect(discord.ui.Select):
    def __init__(self, author_id: int, guild_id: int):
        self.author_id = author_id
        self.guild_id = guild_id
        rows = get_last_registrations(guild_id, limit=25)

        options = []
        for row in rows:
            label = row["real_name"][:100]
            description = f"{row['discord_name']} • {row['user_id']}"
            options.append(
                discord.SelectOption(
                    label=label,
                    value=str(row["user_id"]),
                    description=description[:100]
                )
            )

        if not options:
            options = [
                discord.SelectOption(
                    label="Нет записей",
                    value="0",
                    description="В базе пока нет зарегистрированных пользователей."
                )
            ]

        super().__init__(
            placeholder="Выберите пользователя из последних регистраций...",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
            disabled=(len(rows) == 0)
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=make_error_embed("Ошибка", "Эта панель работает только на сервере."),
                ephemeral=True
            )
            return

        if self.values[0] == "0":
            await interaction.response.send_message(
                embed=make_warning_embed("База пуста", "В базе нет пользователей для выбора."),
                ephemeral=True
            )
            return

        user_id = int(self.values[0])
        member = interaction.guild.get_member(user_id)
        registration = get_registration(user_id, interaction.guild.id)

        if registration is None:
            await interaction.response.send_message(
                embed=make_error_embed("Ошибка", "Не удалось найти запись пользователя в базе."),
                ephemeral=True
            )
            return

        if member is not None:
            embed = build_user_db_embed_from_member(member, registration)
        else:
            embed = build_user_db_embed_offline(user_id, registration)

        view = AdminUserActionView(author_id=interaction.user.id, member_id=user_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class UsersDashboardView(BaseAdminSubview):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(author_id, guild_id)
        self.add_item(AdminUserSelect(author_id, guild_id))

    @staticmethod
    def build_embed(guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(
            title="👥 Управление пользователями",
            description="Здесь собраны все действия, связанные с участниками сервера, регистрацией и именами.",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(
            name="Доступные действия",
            value=(
                "📋 Список ожидающих\n"
                "🔎 Найти пользователя\n"
                "📜 История имён\n"
                "♻ Сбросить регистрацию\n"
                "🗑 Удалить из базы\n"
                "🔁 Восстановить\n"
                "✏ Изменить имя\n"
                "⏱ Кулдаун смены имени\n"
                "🚫 Антиспам регистрации"
            ),
            inline=False
        )
        rows = get_last_registrations(guild.id, limit=5)
        if rows:
            embed.add_field(
                name="Последние регистрации",
                value="\n".join(f"• **{row['real_name']}** — `<@{row['user_id']}>`" for row in rows),
                inline=False
            )
        else:
            embed.add_field(name="Последние регистрации", value="Пока нет данных.", inline=False)
        return embed

    @discord.ui.button(label="📋 Список ожидающих", style=discord.ButtonStyle.secondary, row=1)
    async def pending_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return

        unregistered_role = get_unregistered_role(interaction.guild)
        if unregistered_role is None:
            await interaction.response.send_message(
                embed=make_warning_embed("Роль не настроена", "Сначала выберите роль незарегистрированных в разделе «🎭 Роли»."),
                ephemeral=True
            )
            return

        pending_members = [member for member in unregistered_role.members if not member.bot]
        if not pending_members:
            await interaction.response.send_message(
                embed=make_success_embed("Ожидающих нет", "Сейчас нет пользователей, ожидающих регистрацию."),
                ephemeral=True
            )
            return

        lines = []
        for idx, member in enumerate(pending_members[:20], start=1):
            joined_text = format_dt(member.joined_at) if member.joined_at else "Неизвестно"
            lines.append(
                f"**{idx}.** {member.mention}\n"
                f"ID: `{member.id}`\n"
                f"Зашёл: `{joined_text}`"
            )

        embed = discord.Embed(
            title="📋 Список ожидающих",
            description="\n\n".join(lines),
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        if len(pending_members) > 20:
            embed.set_footer(text=f"Показано 20 из {len(pending_members)} пользователей")
        else:
            embed.set_footer(text=f"Пользователей в ожидании: {len(pending_members)}")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🔎 Найти пользователя", style=discord.ButtonStyle.primary, row=1)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="info"))

    @discord.ui.button(label="📜 История имён", style=discord.ButtonStyle.secondary, row=1)
    async def history_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="history"))

    @discord.ui.button(label="♻ Сбросить регистрацию", style=discord.ButtonStyle.danger, row=2)
    async def reset_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="reset"))

    @discord.ui.button(label="🗑 Удалить из базы", style=discord.ButtonStyle.danger, row=2)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="delete"))

    @discord.ui.button(label="🔁 Восстановить", style=discord.ButtonStyle.success, row=2)
    async def restore_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="restore"))

    @discord.ui.button(label="✏ Изменить имя", style=discord.ButtonStyle.primary, row=3)
    async def rename_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="rename"))

    @discord.ui.button(label="⏱ Кулдаун смены имени", style=discord.ButtonStyle.secondary, row=3)
    async def rename_cooldown_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Кулдаун смены имени", "Часы", 0, 720, int(config.get("rename_cooldown_hours", 24)), "rename_cooldown_hours", "Кулдаун смены имени"))

    @discord.ui.button(label="🚫 Антиспам регистрации", style=discord.ButtonStyle.secondary, row=3)
    async def antispam_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Антиспам регистрации", "Секунды", 0, 3600, get_registration_attempt_cooldown_seconds(), "registration_attempt_cooldown_seconds", "Антиспам регистрации"))


class StatisticsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.primary, row=0)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.edit_message(embed=build_stats_embed(interaction.guild), view=StatisticsDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="📦 Экспорт CSV", style=discord.ButtonStyle.success, row=0)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        file_path = export_registrations_to_csv(interaction.guild.id)
        if file_path is None:
            await interaction.response.send_message(embed=make_warning_embed("Нет данных", "В базе нет записей для экспорта."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Экспорт готов", "CSV-файл с регистрациями прикреплён ниже."), file=discord.File(file_path), ephemeral=True)


class SettingsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="📺 Каналы", style=discord.ButtonStyle.primary, row=0)
    async def channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = ChannelSetupView(self.author_id, self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)

    @discord.ui.button(label="🎭 Роли", style=discord.ButtonStyle.secondary, row=0)
    async def roles_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = RoleSetupView(self.author_id, self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)

    @discord.ui.button(label="📝 Текст", style=discord.ButtonStyle.secondary, row=0)
    async def text_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = TextSetupView(self.author_id, self.guild_id, interaction.guild)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)

    @discord.ui.button(label="⏱ Кулдаун смены имени", style=discord.ButtonStyle.secondary, row=1)
    async def rename_cooldown_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Кулдаун смены имени", "Часы", 0, 720, int(config.get("rename_cooldown_hours", 24)), "rename_cooldown_hours", "Кулдаун смены имени"))

    @discord.ui.button(label="🚫 Антиспам регистрации", style=discord.ButtonStyle.secondary, row=1)
    async def antispam_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Антиспам регистрации", "Секунды", 0, 3600, get_registration_attempt_cooldown_seconds(), "registration_attempt_cooldown_seconds", "Антиспам регистрации"))

    @discord.ui.button(label="💾 Интервал бэкапа", style=discord.ButtonStyle.success, row=2)
    async def backup_interval_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Интервал автобэкапа", "Часы", 1, 168, get_backup_interval_hours(), "backup_interval_hours", "Интервал автобэкапа"))

    @discord.ui.button(label="📦 Лимит бэкапов", style=discord.ButtonStyle.success, row=2)
    async def backup_limit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Лимит бэкапов", "Количество", 1, 100, get_backup_max_files(), "backup_max_files", "Максимум архивов бэкапа"))

    @discord.ui.button(label="🗃 Архивы экспортов", style=discord.ButtonStyle.success, row=2)
    async def export_archive_limit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Лимит архивов экспортов", "Количество", 1, 100, get_exports_archive_max_files(), "exports_archive_max_files", "Максимум архивов экспортов"))


class DatabaseDashboardView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_database_embed(guild)

    @discord.ui.button(label="🔍 Найти запись", style=discord.ButtonStyle.primary, row=0)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DBSearchModal(author_id=self.author_id))

    @discord.ui.button(label="📤 Экспорт CSV", style=discord.ButtonStyle.success, row=0)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        file_path = export_registrations_to_csv(interaction.guild.id)
        if file_path is None:
            await interaction.response.send_message(embed=make_warning_embed("Нет данных", "В базе нет записей для экспорта."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Экспорт готов", "CSV-файл с регистрациями прикреплён ниже."), file=discord.File(file_path), ephemeral=True)

    @discord.ui.button(label="📊 Статистика", style=discord.ButtonStyle.secondary, row=1)
    async def stats_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.send_message(embed=build_stats_embed(interaction.guild), ephemeral=True)

    @discord.ui.button(label="🕒 Последние регистрации", style=discord.ButtonStyle.secondary, row=1)
    async def recent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        rows = get_last_registrations(interaction.guild.id, limit=10)
        description = "\n".join(
            f"**{idx}.** {row['real_name']} — `{row['user_id']}`"
            for idx, row in enumerate(rows, start=1)
        ) if rows else "Пока нет данных."
        embed = discord.Embed(title="🕒 Последние регистрации", description=description, color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🔄 Обновить", style=discord.ButtonStyle.secondary, row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.edit_message(embed=self.build_embed(interaction.guild), view=DatabaseDashboardView(self.author_id, self.guild_id))


class BadwordsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="➕ Добавить слово", style=discord.ButtonStyle.primary, row=0)
    async def add_word_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        class AddBadwordModal(discord.ui.Modal, title="Добавить запрещённое слово"):
            def __init__(self):
                super().__init__()
                self.word_input = discord.ui.TextInput(label="Слово", min_length=1, max_length=100, required=True)
                self.add_item(self.word_input)
            async def on_submit(self, interaction: discord.Interaction) -> None:
                ok, result = add_custom_badword(self.word_input.value)
                if ok:
                    await interaction.response.send_message(embed=make_success_embed("Слово добавлено", f"Добавлено: `{result}`"), ephemeral=True)
                else:
                    await interaction.response.send_message(embed=make_error_embed("Ошибка", result), ephemeral=True)
        await interaction.response.send_modal(AddBadwordModal())

    @discord.ui.button(label="➖ Удалить слово", style=discord.ButtonStyle.secondary, row=0)
    async def remove_word_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BadwordsListView(author_id=self.author_id, per_page=15)
        await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

    @discord.ui.button(label="📄 Показать список", style=discord.ButtonStyle.secondary, row=1)
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BadwordsListView(author_id=self.author_id, per_page=15)
        await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

    @discord.ui.button(label="🗑 Очистить список", style=discord.ButtonStyle.danger, row=1)
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BadwordsListView(author_id=self.author_id, per_page=15)
        confirm_view = ConfirmClearBadwordsView(view, self.author_id)
        await interaction.response.send_message(embed=make_warning_embed("Подтверждение очистки", "Полностью очистить пользовательский список badwords?"), view=confirm_view, ephemeral=True)

    @discord.ui.button(label="📤 Экспорт JSON", style=discord.ButtonStyle.success, row=2)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        file_path = export_badwords_to_json()
        await interaction.response.send_message(embed=make_success_embed("Экспорт готов", "JSON-файл прикреплён ниже."), file=discord.File(file_path), ephemeral=True)


class BackupDashboardView(BaseAdminSubview):
    @discord.ui.button(label="📦 Создать бэкап", style=discord.ButtonStyle.primary, row=0)
    async def create_backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            files = create_backup_bundle()
        except Exception as e:
            log_internal_exception("Ручной бэкап", e)
            await interaction.response.send_message(embed=make_error_embed("Ошибка бэкапа", "Не удалось создать бэкап."), ephemeral=True)
            return
        if not files:
            await interaction.response.send_message(embed=make_warning_embed("Нет файлов", "Не найдено файлов для резервного копирования."), ephemeral=True)
            return
        names = "\n".join(f"• `{file.name}`" for file in files)
        await interaction.response.send_message(embed=make_success_embed("Бэкап создан", names), ephemeral=True)

    @discord.ui.button(label="📂 Скачать последний", style=discord.ButtonStyle.success, row=0)
    async def download_latest_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        backups = list_backup_archives()
        if not backups:
            await interaction.response.send_message(embed=make_warning_embed("Нет бэкапов", "Архивы бэкапа ещё не созданы."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Последний бэкап", backups[0].name), file=discord.File(backups[0]), ephemeral=True)

    @discord.ui.button(label="📜 Список архивов", style=discord.ButtonStyle.secondary, row=1)
    async def list_archives_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        backups = list_backup_archives()
        exports = list_export_archives()
        text = "\n".join(f"• `{file.name}`" for file in backups[:10]) if backups else "Нет архивов бэкапа."
        text2 = "\n".join(f"• `{file.name}`" for file in exports[:10]) if exports else "Нет архивов экспортов."
        embed = discord.Embed(title="📜 Архивы", color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Бэкапы", value=text, inline=False)
        embed.add_field(name="Архивы экспортов", value=text2, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🗑 Очистить старые", style=discord.ButtonStyle.danger, row=1)
    async def prune_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        prune_old_backups()
        prune_old_export_archives()
        await interaction.response.send_message(embed=make_success_embed("Готово", "Старые архивы очищены по текущим лимитам."), ephemeral=True)

    @discord.ui.button(label="⏱ Интервал бэкапов", style=discord.ButtonStyle.secondary, row=2)
    async def backup_interval_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Интервал автобэкапа", "Часы", 1, 168, get_backup_interval_hours(), "backup_interval_hours", "Интервал автобэкапа"))

    @discord.ui.button(label="📦 Лимит бэкапов", style=discord.ButtonStyle.secondary, row=2)
    async def backup_limit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Лимит бэкапов", "Количество", 1, 100, get_backup_max_files(), "backup_max_files", "Максимум архивов бэкапа"))

    @discord.ui.button(label="🗃 Лимит архивов", style=discord.ButtonStyle.secondary, row=2)
    async def export_archive_limit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Лимит архивов экспортов", "Количество", 1, 100, get_exports_archive_max_files(), "exports_archive_max_files", "Максимум архивов экспортов"))


class DiagnosticsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="🔄 Обновить", style=discord.ButtonStyle.primary, row=0)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=build_system_check_embed(interaction.guild),
            view=DiagnosticsDashboardView(self.author_id, self.guild_id)
        )

    @discord.ui.button(label="🧪 Проверить права", style=discord.ButtonStyle.secondary, row=0)
    async def check_permissions_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.send_message(
            embed=build_system_check_embed(interaction.guild),
            ephemeral=True
        )


class LogsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="🔒 Lock логов", style=discord.ButtonStyle.secondary, row=0)
    async def lock_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        channel = get_log_channel(interaction.guild)
        if channel is None:
            await interaction.response.send_message(embed=make_warning_embed("Канал не настроен", "Сначала выберите лог-канал в настройках."), ephemeral=True)
            return
        bot_member = get_bot_member(interaction.guild)
        if bot_member is None or not channel.permissions_for(bot_member).manage_channels:
            await interaction.response.send_message(
                embed=make_warning_embed(
                    "Нужно право Manage Channels",
                    (
                        "Бот не может сам залочить лог-канал, потому что у него нет права **Manage Channels / Управление каналами**.\n\n"
                        "Что можно сделать:\n"
                        "• выдать это право боту в лог-канале\n"
                        "• или вручную запретить **@everyone** просмотр и отправку сообщений, а боту оставить доступ"
                    )
                ),
                ephemeral=True
            )
            return
        try:
            await set_log_channel_locked(interaction.guild, True)
        except discord.Forbidden:
            await interaction.response.send_message(
                embed=make_warning_embed(
                    "Недостаточно прав",
                    "Discord не дал изменить права лог-канала. Проверь, что у бота есть **Manage Channels** и его роль стоит выше ограничений канала."
                ),
                ephemeral=True
            )
            return
        except discord.HTTPException:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось изменить права лог-канала."), ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=await build_logs_embed(interaction.guild),
            view=LogsDashboardView(self.author_id, self.guild_id)
        )
        await send_log(interaction.guild, LogType.ADMIN, "Lock логов", f"Лог-канал {channel.mention} скрыт от @everyone.", moderator=interaction.user)
        await interaction.followup.send(embed=make_success_embed("Готово", "Лог-канал скрыт от обычных пользователей, бот сохранил доступ."), ephemeral=True)

    @discord.ui.button(label="🔓 Unlock логов", style=discord.ButtonStyle.success, row=0)
    async def unlock_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        channel = get_log_channel(interaction.guild)
        if channel is None:
            await interaction.response.send_message(embed=make_warning_embed("Канал не настроен", "Сначала выберите лог-канал в настройках."), ephemeral=True)
            return
        bot_member = get_bot_member(interaction.guild)
        if bot_member is None or not channel.permissions_for(bot_member).manage_channels:
            await interaction.response.send_message(
                embed=make_warning_embed(
                    "Нужно право Manage Channels",
                    (
                        "Бот не может сам снять lock с лог-канала, потому что у него нет права **Manage Channels / Управление каналами**.\n\n"
                        "Что можно сделать:\n"
                        "• выдать это право боту в лог-канале\n"
                        "• или вручную вернуть права **@everyone** в настройках канала"
                    )
                ),
                ephemeral=True
            )
            return
        try:
            await set_log_channel_locked(interaction.guild, False)
        except discord.Forbidden:
            await interaction.response.send_message(
                embed=make_warning_embed(
                    "Недостаточно прав",
                    "Discord не дал изменить права лог-канала. Проверь, что у бота есть **Manage Channels** и его роль стоит выше ограничений канала."
                ),
                ephemeral=True
            )
            return
        except discord.HTTPException:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось изменить права лог-канала."), ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=await build_logs_embed(interaction.guild),
            view=LogsDashboardView(self.author_id, self.guild_id)
        )
        await send_log(interaction.guild, LogType.ADMIN, "Unlock логов", f"Для лог-канала {channel.mention} восстановлены наследуемые права @everyone.", moderator=interaction.user)
        await interaction.followup.send(embed=make_success_embed("Готово", "Для лог-канала восстановлены обычные права доступа."), ephemeral=True)

    @discord.ui.button(label="🧹 Очистить логи", style=discord.ButtonStyle.danger, row=1)
    async def clear_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        channel = get_log_channel(interaction.guild)
        if channel is None:
            await interaction.response.send_message(embed=make_warning_embed("Канал не настроен", "Сначала выберите лог-канал в настройках."), ephemeral=True)
            return
        try:
            deleted = await channel.purge(limit=100)
        except discord.Forbidden:
            await interaction.response.send_message(embed=make_error_embed("Нет прав", "Бот не может очистить лог-канал."), ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось очистить лог-канал."), ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=await build_logs_embed(interaction.guild),
            view=LogsDashboardView(self.author_id, self.guild_id)
        )
        await interaction.followup.send(embed=make_success_embed("Готово", f"Удалено сообщений: **{len(deleted)}**"), ephemeral=True)


class QuickActionsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="🛡 Обновить Verification", style=discord.ButtonStyle.success, row=0)
    async def refresh_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        msg = await ensure_registration_message(interaction.guild)
        if msg is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось создать или найти сообщение регистрации."), ephemeral=True)
            return
        await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Verification Center обновлён."), ephemeral=True)

    @discord.ui.button(label="📘 Обновить правила", style=discord.ButtonStyle.secondary, row=0)
    async def refresh_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        msg = await ensure_rules_message(interaction.guild)
        if msg is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось создать или найти сообщение правил."), ephemeral=True)
            return
        await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение правил обновлено."), ephemeral=True)

    @discord.ui.button(label="💾 Создать бэкап", style=discord.ButtonStyle.primary, row=0)
    async def backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            files = create_backup_bundle()
        except Exception as e:
            log_internal_exception("Ручной бэкап", e)
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось создать бэкап."), ephemeral=True)
            return
        if not files:
            await interaction.response.send_message(embed=make_warning_embed("Нет файлов", "Не найдено файлов для резервного копирования."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Бэкап создан", "\n".join(f"• `{file.name}`" for file in files)), ephemeral=True)

    @discord.ui.button(label="📤 Экспорт БД", style=discord.ButtonStyle.secondary, row=1)
    async def export_db_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        file_path = export_registrations_to_csv(interaction.guild.id)
        if file_path is None:
            await interaction.response.send_message(embed=make_warning_embed("Нет данных", "В базе нет записей для экспорта."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Экспорт готов", "CSV-файл прикреплён ниже."), file=discord.File(file_path), ephemeral=True)

    @discord.ui.button(label="🧹 Очистить архивы", style=discord.ButtonStyle.danger, row=1)
    async def cleanup_archives_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        prune_old_backups()
        prune_old_export_archives()
        await interaction.response.send_message(embed=make_success_embed("Готово", "Старые архивы очищены."), ephemeral=True)

    @discord.ui.button(label="🧪 Диагностика", style=discord.ButtonStyle.secondary, row=2)
    async def diagnostics_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop_live_updates()
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.send_message(embed=build_system_check_embed(interaction.guild), ephemeral=True)


class AdminPanelView(discord.ui.View):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.guild_id = guild_id
        self.message: Optional[discord.Message] = None
        self._live_update_task: Optional[asyncio.Task] = None

    async def bind_message(self, message: discord.Message):
        self.message = message
        self.start_live_updates()

    async def _show_subview(self, interaction: discord.Interaction, embed: discord.Embed, view: discord.ui.View):
        self.stop_live_updates()
        await interaction.response.edit_message(embed=embed, view=view)

    def start_live_updates(self):
        self.stop_live_updates()
        self._live_update_task = asyncio.create_task(self._live_update_loop())

    def stop_live_updates(self):
        if self._live_update_task and not self._live_update_task.done():
            self._live_update_task.cancel()
        self._live_update_task = None

    async def _live_update_loop(self):
        try:
            while not self.is_finished():
                await asyncio.sleep(15)
                if self.message is None:
                    continue
                guild = bot.get_guild(self.guild_id)
                if guild is None:
                    continue
                try:
                    await self.message.edit(embed=build_admin_dashboard_embed(guild), view=self)
                except (discord.NotFound, discord.Forbidden):
                    break
                except discord.HTTPException:
                    continue
        except asyncio.CancelledError:
            pass

    async def on_timeout(self):
        self.stop_live_updates()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=make_warning_embed("Нет доступа", "Эта админ-панель предназначена не для вас."),
                ephemeral=True
            )
            return False

        if not is_admin_interaction(interaction):
            await interaction.response.send_message(
                embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."),
                ephemeral=True
            )
            return False

        return True

    @discord.ui.button(label="👥 Пользователи", style=discord.ButtonStyle.primary, row=0)
    async def users_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await self._show_subview(interaction, UsersDashboardView.build_embed(interaction.guild), UsersDashboardView(self.author_id, self.guild_id))


    @discord.ui.button(label="📺 Каналы", style=discord.ButtonStyle.secondary, row=0)
    async def channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = ChannelSetupView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="🎭 Роли", style=discord.ButtonStyle.secondary, row=1)
    async def roles_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = RoleSetupView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="📝 Сообщения", style=discord.ButtonStyle.secondary, row=0)
    async def messages_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = TextSetupView(self.author_id, self.guild_id, interaction.guild)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="🗄 База данных", style=discord.ButtonStyle.secondary, row=1)
    async def db_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        view = DatabaseDashboardView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="📛 Badwords", style=discord.ButtonStyle.secondary, row=1)
    async def badwords_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_subview(interaction, build_badwords_manager_embed(), BadwordsDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="💾 Бэкапы", style=discord.ButtonStyle.success, row=2)
    async def backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_subview(interaction, build_backup_manager_embed(), BackupDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="🩺 Диагностика", style=discord.ButtonStyle.success, row=2)
    async def diagnostics_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await self._show_subview(interaction, build_system_check_embed(interaction.guild), DiagnosticsDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="🔄 Обновить", style=discord.ButtonStyle.primary, row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        await interaction.response.edit_message(embed=build_admin_dashboard_embed(interaction.guild), view=self)

# =========================
# Автобэкап
# =========================

@tasks.loop(minutes=10)
async def auto_backup_loop():
    global LAST_AUTO_BACKUP_AT

    interval_hours = get_backup_interval_hours()
    now = datetime.now(timezone.utc)

    if LAST_AUTO_BACKUP_AT is not None:
        if now - LAST_AUTO_BACKUP_AT < timedelta(hours=interval_hours):
            return

    try:
        files = create_backup_bundle()
    except Exception as e:
        log_internal_exception("Ошибка автобэкапа", e)
        return

    if not files:
        return

    LAST_AUTO_BACKUP_AT = now
    save_last_auto_backup_at(now)

    guild = bot.get_guild(GUILD_ID)
    if guild:
        names = "\n".join(f"• `{file.name}`" for file in files)
        await send_log(
            guild,
            LogType.BACKUP,
            "Автобэкап выполнен",
            f"Создано архивов: **{len(files)}**\n{names}"
        )


@auto_backup_loop.before_loop
async def before_auto_backup_loop():
    await bot.wait_until_ready()


# =========================
# Events
# =========================

@bot.event
async def on_ready():
    bot.add_view(RegistrationView())

    try:
        synced = await bot.tree.sync()
        print(f"Синхронизировано global slash-команд: {len(synced)}")
    except Exception as e:
        log_internal_exception("Ошибка синхронизации slash-команд", e)

    if GUILD_ID:
        real_guild = bot.get_guild(GUILD_ID)
        if real_guild is not None:
            await ensure_registration_message(real_guild)
            await refresh_registration_message(real_guild)
            await ensure_rules_message(real_guild)
            await refresh_rules_message(real_guild)

    if not auto_backup_loop.is_running():
        auto_backup_loop.start()

    print(f"Бот запущен как {bot.user} (ID: {bot.user.id})")


@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    if payload.guild_id is None:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    registration_message_id = int(config.get("registration_message_id", 0) or 0)
    rules_message_id = int(config.get("rules_message_id", 0) or 0)

    if payload.message_id == registration_message_id:
        config["registration_message_id"] = 0
        save_config(config)
        await ensure_registration_message(guild)
        return

    if payload.message_id == rules_message_id:
        config["rules_message_id"] = 0
        save_config(config)
        await ensure_rules_message(guild)
        return


@bot.event
async def on_raw_bulk_message_delete(payload: discord.RawBulkMessageDeleteEvent):
    if payload.guild_id is None:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    registration_message_id = int(config.get("registration_message_id", 0) or 0)
    rules_message_id = int(config.get("rules_message_id", 0) or 0)

    if registration_message_id and registration_message_id in payload.message_ids:
        config["registration_message_id"] = 0
        save_config(config)
        await ensure_registration_message(guild)

    if rules_message_id and rules_message_id in payload.message_ids:
        config["rules_message_id"] = 0
        save_config(config)
        await ensure_rules_message(guild)


@bot.event
async def on_message(message: discord.Message):
    if message.guild is None:
        await bot.process_commands(message)
        return

    registration_channel = get_registration_channel(message.guild)
    registration_message_id = int(config.get("registration_message_id", 0) or 0)

    if registration_channel and message.channel.id == registration_channel.id:
        if message.author.bot:
            await bot.process_commands(message)
            return

        if message.id == registration_message_id:
            await bot.process_commands(message)
            return

        try:
            await message.delete()
        except discord.Forbidden:
            pass
        except discord.HTTPException:
            pass

        try:
            await message.channel.send(
                f"{message.author.mention}, в этом канале нельзя писать сообщения. Используйте кнопки Verification выше.",
                delete_after=8
            )
        except (discord.Forbidden, discord.HTTPException):
            pass

    await bot.process_commands(message)


@bot.event
async def on_member_join(member: discord.Member):
    if not GUILD_ID or member.guild.id != GUILD_ID:
        return

    registration = get_registration(member.id, member.guild.id)

    if registration is not None:
        ok, result = await restore_member_from_db(member)

        if ok:
            return
        else:
            await send_log(
                member.guild,
                LogType.ERROR,
                "Ошибка автовосстановления",
                result,
                member=member
            )

    unregistered_role = get_unregistered_role(member.guild)

    if unregistered_role:
        try:
            await member.add_roles(
                unregistered_role,
                reason="Новый пользователь ожидает регистрацию"
            )
        except discord.Forbidden:
            print("Нет прав на выдачу роли незарегистрированного.")
        except discord.HTTPException as e:
            print(f"Ошибка Discord при выдаче роли: {e}")

    await send_log(
        member.guild,
        LogType.JOIN,
        "Новый вход",
        "Пользователь зашёл на сервер и ожидает регистрацию.",
        member=member
    )


# =========================
# Slash-команды
# =========================

@bot.tree.command(
    name="setup_registration",
    description="Создать или обновить постоянное сообщение регистрации"
)
@app_commands.checks.has_permissions(administrator=True)
async def setup_registration_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    msg = await ensure_registration_message(interaction.guild)
    if msg is None:
        await interaction.response.send_message(
            embed=make_error_embed("Ошибка", "Не удалось создать или найти сообщение регистрации."),
            ephemeral=True
        )
        return

    await refresh_registration_message(interaction.guild)
    await interaction.response.send_message(
        embed=make_success_embed("Готово", "Постоянное сообщение регистрации создано или обновлено."),
        ephemeral=True
    )


@bot.tree.command(
    name="admin_panel",
    description="Открыть красивую админ-панель"
)
@app_commands.checks.has_permissions(administrator=True)
async def admin_panel_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    view = AdminPanelView(author_id=interaction.user.id, guild_id=interaction.guild.id)
    await interaction.response.send_message(
        embed=build_admin_dashboard_embed(interaction.guild),
        view=view,
        ephemeral=True
    )
    try:
        message = await interaction.original_response()
        await view.bind_message(message)
    except discord.HTTPException:
        pass


@bot.tree.command(
    name="system_check",
    description="Проверить права бота, роли и каналы"
)
@app_commands.checks.has_permissions(administrator=True)
async def system_check_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    await interaction.response.send_message(embed=build_system_check_embed(interaction.guild), ephemeral=True)


@bot.tree.command(
    name="check_name",
    description="Проверить, пропустит ли бот имя"
)
@app_commands.checks.has_permissions(administrator=True)
async def check_name_slash(interaction: discord.Interaction, name: str):
    analysis = analyze_name(name)

    color = discord.Color.green() if analysis["ok"] else discord.Color.red()
    status = "✅ Имя пройдёт проверку" if analysis["ok"] else "⚠️ Имя не пройдёт проверку"

    embed = discord.Embed(
        title="🔎 Проверка имени",
        description=status,
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Введено", value=f"`{analysis['original']}`", inline=False)
    embed.add_field(name="Автоформат", value=f"`{analysis['suggested']}`", inline=False)
    embed.add_field(name="Формат", value="✅ корректный" if analysis["valid"] else "❌ некорректный", inline=True)
    embed.add_field(name="Антимат", value="❌ найдено совпадение" if analysis["found_profanity"] else "✅ не найдено", inline=True)

    if analysis["matched_rule"]:
        embed.add_field(name="Совпадение", value=f"`{analysis['matched_rule']}`", inline=False)

    if analysis["reason"]:
        embed.add_field(name="Причина", value=analysis["reason"], inline=False)

    if analysis["changed_case"]:
        embed.add_field(name="Подсказка", value=f"Лучше использовать: **{analysis['suggested']}**", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="name_history",
    description="Показать историю смен имени пользователя"
)
@app_commands.checks.has_permissions(administrator=True)
async def name_history_slash(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    rows = get_name_history(member.id, interaction.guild.id, limit=20)
    registration = get_registration(member.id, interaction.guild.id)

    if not rows and not registration:
        await interaction.response.send_message(
            embed=make_warning_embed("Нет данных", "У пользователя нет истории имён и записи о регистрации."),
            ephemeral=True
        )
        return

    embed = build_name_history_embed(
        title_text="🕘 История имён",
        description_text=f"История для {member.mention}",
        avatar_url=member.display_avatar.url,
        current_real_name=registration["real_name"] if registration else None,
        current_nickname=registration["nickname"] if registration else None,
        rows=rows
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="set_registration_channel",
    description="Установить канал регистрации"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_registration_channel_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    config["registration_channel_id"] = channel.id
    config["registration_message_id"] = 0
    save_config(config)

    await interaction.response.send_message(
        embed=make_success_embed(
            "Настройки обновлены",
            f"Канал регистрации изменён на {channel.mention}.\n"
            "ID сообщения регистрации сброшен. Выполните `/setup_registration`."
        ),
        ephemeral=True
    )


@bot.tree.command(
    name="set_log_channel",
    description="Установить лог-канал"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_log_channel_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    config["log_channel_id"] = channel.id
    save_config(config)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Лог-канал изменён на {channel.mention}."),
        ephemeral=True
    )


@bot.tree.command(
    name="set_welcome_channel",
    description="Установить welcome-канал"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_welcome_channel_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    config["welcome_channel_id"] = channel.id
    save_config(config)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Welcome-канал изменён на {channel.mention}."),
        ephemeral=True
    )


@bot.tree.command(
    name="set_unregistered_role",
    description="Установить роль незарегистрированных"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_unregistered_role_slash(interaction: discord.Interaction, role: discord.Role):
    config["unregistered_role_id"] = role.id
    save_config(config)

    if interaction.guild is not None:
        await refresh_registration_message(interaction.guild)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Роль незарегистрированных изменена на **{role.name}**."),
        ephemeral=True
    )


@bot.tree.command(
    name="set_member_role",
    description="Установить роль участников"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_member_role_slash(interaction: discord.Interaction, role: discord.Role):
    config["member_role_id"] = role.id
    save_config(config)

    if interaction.guild is not None:
        await refresh_registration_message(interaction.guild)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Роль участников изменена на **{role.name}**."),
        ephemeral=True
    )


@bot.tree.command(
    name="set_rename_cooldown",
    description="Установить задержку между сменами имени в часах"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_rename_cooldown_slash(interaction: discord.Interaction, hours: app_commands.Range[int, 0]):
    config["rename_cooldown_hours"] = hours
    save_config(config)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Задержка между сменами имени: **{hours} ч.**"),
        ephemeral=True
    )


@bot.tree.command(
    name="set_backup_interval",
    description="Установить интервал автобэкапа в часах"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_backup_interval_slash(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 168]):
    config["backup_interval_hours"] = hours
    save_config(config)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Интервал автобэкапа: **{hours} ч.**"),
        ephemeral=True
    )


@bot.tree.command(
    name="set_backup_max_files",
    description="Установить число хранимых файлов автобэкапа"
)
@app_commands.checks.has_permissions(administrator=True)
async def set_backup_max_files_slash(interaction: discord.Interaction, count: app_commands.Range[int, 1, 100]):
    config["backup_max_files"] = count
    save_config(config)

    await interaction.response.send_message(
        embed=make_success_embed("Настройки обновлены", f"Максимум архивов бэкапа: **{count}**"),
        ephemeral=True
    )


@bot.tree.command(
    name="backup_now",
    description="Создать резервную копию прямо сейчас"
)
@app_commands.checks.has_permissions(administrator=True)
async def backup_now_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    try:
        files = create_backup_bundle()
    except Exception as e:
        await interaction.response.send_message(
            embed=make_error_embed("Ошибка бэкапа", "Не удалось создать бэкап. Подробности сохранены во внутренних логах."),
            ephemeral=True
        )
        return

    if not files:
        await interaction.response.send_message(
            embed=make_warning_embed("Нет файлов", "Не найдено файлов для резервного копирования."),
            ephemeral=True
        )
        return

    names = "\n".join(f"• `{file.name}`" for file in files)

    await send_log(
        interaction.guild,
        LogType.BACKUP,
        "Создан бэкап вручную",
        f"Создано архивов: **{len(files)}**\n{names}",
        moderator=interaction.user
    )

    await interaction.response.send_message(
        embed=make_success_embed("Бэкап создан", f"Создано архивов: **{len(files)}**\n{names}"),
        ephemeral=True
    )


@bot.tree.command(
    name="whois",
    description="Показать данные регистрации пользователя"
)
@app_commands.checks.has_permissions(administrator=True)
async def whois_slash(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    registration = get_registration(member.id, interaction.guild.id)
    if not registration:
        await interaction.response.send_message(
            embed=make_warning_embed("Нет данных", "Этот пользователь ещё не зарегистрирован в базе."),
            ephemeral=True
        )
        return

    view = AdminUserActionView(author_id=interaction.user.id, member_id=member.id)
    await interaction.response.send_message(embed=build_user_db_embed_from_member(member, registration), view=view, ephemeral=True)


@bot.tree.command(
    name="registrations_count",
    description="Показать число зарегистрированных пользователей"
)
@app_commands.checks.has_permissions(administrator=True)
async def registrations_count_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    count = count_registrations(interaction.guild.id)
    await interaction.response.send_message(
        embed=make_info_embed("Статистика", f"Всего зарегистрированных пользователей: **{count}**"),
        ephemeral=True
    )


@bot.tree.command(
    name="db_user",
    description="Посмотреть запись пользователя в базе"
)
@app_commands.checks.has_permissions(administrator=True)
async def db_user_slash(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    registration = get_registration(member.id, interaction.guild.id)
    if not registration:
        await interaction.response.send_message(
            embed=make_warning_embed("Запись не найдена", "У этого пользователя нет записи в базе."),
            ephemeral=True
        )
        return

    view = AdminUserActionView(author_id=interaction.user.id, member_id=member.id)
    await interaction.response.send_message(embed=build_user_db_embed_from_member(member, registration), view=view, ephemeral=True)


@bot.tree.command(
    name="db_recent",
    description="Показать последние записи в базе"
)
@app_commands.checks.has_permissions(administrator=True)
async def db_recent_slash(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 20] = 10):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    rows = get_last_registrations(interaction.guild.id, limit=limit)
    if not rows:
        await interaction.response.send_message(
            embed=make_warning_embed("База пуста", "В базе пока нет зарегистрированных пользователей."),
            ephemeral=True
        )
        return

    lines = []
    for idx, row in enumerate(rows, start=1):
        lines.append(
            f"**{idx}.** <@{row['user_id']}>\n"
            f"Имя: **{row['real_name']}**\n"
            f"Ник: `{row['nickname']}`\n"
            f"Обновлено: `{format_dt(row['updated_at'])}`"
        )

    embed = discord.Embed(
        title="📚 Последние регистрации",
        description="\n\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text=f"Показано записей: {len(rows)}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="db_export",
    description="Выгрузить базу регистраций в CSV"
)
@app_commands.checks.has_permissions(administrator=True)
async def db_export_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    file_path = export_registrations_to_csv(interaction.guild.id)
    if file_path is None:
        await interaction.response.send_message(
            embed=make_warning_embed("Нет данных", "В базе нет записей для экспорта."),
            ephemeral=True
        )
        return

    await send_log(
        interaction.guild,
        LogType.ADMIN,
        "Экспорт базы",
        f"База регистраций экспортирована в CSV.\nФайл: **{file_path.name}**",
        moderator=interaction.user
    )

    await interaction.response.send_message(
        embed=make_success_embed("Экспорт готов", "CSV-файл с регистрациями прикреплён ниже."),
        file=discord.File(file_path),
        ephemeral=True
    )


@bot.tree.command(
    name="db_delete_user",
    description="Удалить пользователя из базы регистраций"
)
@app_commands.checks.has_permissions(administrator=True)
async def db_delete_user_slash(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    registration = get_registration(member.id, interaction.guild.id)
    if not registration:
        await interaction.response.send_message(
            embed=make_warning_embed("Нечего удалять", "У этого пользователя нет записи в базе."),
            ephemeral=True
        )
        return

    view = ConfirmAdminActionView(
        requester_id=interaction.user.id,
        target_member_id=member.id,
        action=AdminAction.DELETE_DB_USER
    )

    await interaction.response.send_message(
        embed=make_warning_embed(
            "Подтверждение удаления",
            f"Вы точно хотите удалить **{member}** из базы?\n"
            "Это удалит запись регистрации и историю смен имени."
        ),
        view=view,
        ephemeral=True
    )


@bot.tree.command(
    name="db_reset_user",
    description="Сбросить регистрацию пользователя и вернуть его в незарегистрированные"
)
@app_commands.checks.has_permissions(administrator=True)
async def db_reset_user_slash(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    view = ConfirmAdminActionView(
        requester_id=interaction.user.id,
        target_member_id=member.id,
        action=AdminAction.RESET_DB_USER
    )

    await interaction.response.send_message(
        embed=make_warning_embed(
            "Подтверждение сброса",
            f"Вы точно хотите сбросить регистрацию пользователя {member.mention}?\n"
            "Будет удалена запись из базы, история имён, ник сбросится, а пользователь вернётся в незарегистрированные."
        ),
        view=view,
        ephemeral=True
    )


@bot.tree.command(
    name="db_restore_user",
    description="Восстановить ник и роли пользователя из базы"
)
@app_commands.checks.has_permissions(administrator=True)
async def db_restore_user_slash(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message(
            embed=make_error_embed("Неверное место", "Эта команда работает только на сервере."),
            ephemeral=True
        )
        return

    registration = get_registration(member.id, interaction.guild.id)
    if not registration:
        await interaction.response.send_message(
            embed=make_warning_embed("Нет записи", "У этого пользователя нет записи в базе."),
            ephemeral=True
        )
        return

    view = ConfirmAdminActionView(
        requester_id=interaction.user.id,
        target_member_id=member.id,
        action=AdminAction.RESTORE_FROM_DB
    )

    await interaction.response.send_message(
        embed=make_warning_embed(
            "Подтверждение восстановления",
            f"Вы точно хотите восстановить ник и роли пользователя {member.mention} из базы?"
        ),
        view=view,
        ephemeral=True
    )


@bot.tree.command(
    name="badwords_add",
    description="Добавить слово в пользовательский список запрещённых слов"
)
@app_commands.checks.has_permissions(administrator=True)
async def badwords_add_slash(interaction: discord.Interaction, word: str):
    ok, result = add_custom_badword(word)

    if not ok:
        await interaction.response.send_message(
            embed=make_warning_embed("Не добавлено", result),
            ephemeral=True
        )
        return

    if interaction.guild is not None:
        await send_log(
            interaction.guild,
            LogType.ADMIN,
            "Добавлено запрещённое слово",
            f"В пользовательский список добавлено слово: **{result}**",
            moderator=interaction.user
        )

    await interaction.response.send_message(
        embed=make_success_embed("Слово добавлено", f"Добавлено слово: **{result}**"),
        ephemeral=True
    )


@bot.tree.command(
    name="badwords_remove",
    description="Удалить слово из пользовательского списка запрещённых слов"
)
@app_commands.checks.has_permissions(administrator=True)
async def badwords_remove_slash(interaction: discord.Interaction, word: str):
    ok, result = remove_custom_badword(word)

    if not ok:
        await interaction.response.send_message(
            embed=make_warning_embed("Не удалено", result),
            ephemeral=True
        )
        return

    if interaction.guild is not None:
        await send_log(
            interaction.guild,
            LogType.ADMIN,
            "Удалено запрещённое слово",
            f"Из пользовательского списка удалено слово: **{result}**",
            moderator=interaction.user
        )

    await interaction.response.send_message(
        embed=make_success_embed("Слово удалено", f"Удалено слово: **{result}**"),
        ephemeral=True
    )


@bot.tree.command(
    name="badwords_list",
    description="Показать встроенный и пользовательский список запрещённых слов"
)
@app_commands.checks.has_permissions(administrator=True)
async def badwords_list_slash(interaction: discord.Interaction):
    view = BadwordsListView(author_id=interaction.user.id, per_page=15)
    await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)


@bot.tree.command(
    name="badwords_export",
    description="Экспортировать списки запрещённых слов в JSON"
)
@app_commands.checks.has_permissions(administrator=True)
async def badwords_export_slash(interaction: discord.Interaction):
    file_path = export_badwords_to_json()

    if interaction.guild is not None:
        await send_log(
            interaction.guild,
            LogType.ADMIN,
            "Экспорт списка запрещённых слов",
            f"Списки запрещённых слов экспортированы.\nФайл: **{file_path.name}**",
            moderator=interaction.user
        )

    await interaction.response.send_message(
        embed=make_success_embed("Экспорт готов", "JSON-файл со списками слов прикреплён ниже."),
        file=discord.File(file_path),
        ephemeral=True
    )


@bot.tree.command(
    name="badwords_import",
    description="Импортировать пользовательский список запрещённых слов из JSON-файла"
)
@app_commands.checks.has_permissions(administrator=True)
async def badwords_import_slash(interaction: discord.Interaction, file: discord.Attachment):
    if not file.filename.lower().endswith(".json"):
        await interaction.response.send_message(
            embed=make_error_embed("Неверный файл", "Нужен JSON-файл."),
            ephemeral=True
        )
        return

    try:
        file_bytes = await file.read()
        data = json.loads(file_bytes.decode("utf-8-sig"))
    except Exception:
        await interaction.response.send_message(
            embed=make_error_embed("Ошибка чтения", "Не удалось прочитать JSON-файл."),
            ephemeral=True
        )
        return

    if not isinstance(data, dict):
        await interaction.response.send_message(
            embed=make_error_embed("Неверный формат", "JSON должен быть объектом."),
            ephemeral=True
        )
        return

    words = data.get("custom_words", data.get("words"))
    if not isinstance(words, list):
        await interaction.response.send_message(
            embed=make_error_embed(
                "Неверный формат",
                "JSON должен содержать поле `custom_words` или `words` со списком слов."
            ),
            ephemeral=True
        )
        return

    cleaned_words = []
    seen = set()
    default_words = set(get_default_badwords())

    for word in words:
        if not isinstance(word, str):
            continue
        normalized = normalize_custom_badword(word)
        if (
            normalized
            and len(normalized) >= 3
            and normalized not in seen
            and normalized not in default_words
        ):
            cleaned_words.append(normalized)
            seen.add(normalized)

    save_badwords({"words": sorted(cleaned_words)})

    if interaction.guild is not None:
        await send_log(
            interaction.guild,
            LogType.ADMIN,
            "Импорт пользовательского списка запрещённых слов",
            f"Импортировано слов: **{len(cleaned_words)}**",
            moderator=interaction.user
        )

    await interaction.response.send_message(
        embed=make_success_embed("Импорт завершён", f"Импортировано пользовательских слов: **{len(cleaned_words)}**"),
        ephemeral=True
    )


# =========================
# Ошибки
# =========================

@setup_registration_slash.error
@admin_panel_slash.error
@system_check_slash.error
@check_name_slash.error
@name_history_slash.error
@set_registration_channel_slash.error
@set_log_channel_slash.error
@set_welcome_channel_slash.error
@set_unregistered_role_slash.error
@set_member_role_slash.error
@set_rename_cooldown_slash.error
@set_backup_interval_slash.error
@set_backup_max_files_slash.error
@backup_now_slash.error
@whois_slash.error
@registrations_count_slash.error
@db_user_slash.error
@db_recent_slash.error
@db_export_slash.error
@db_delete_user_slash.error
@db_reset_user_slash.error
@db_restore_user_slash.error
@badwords_add_slash.error
@badwords_remove_slash.error
@badwords_list_slash.error
@badwords_export_slash.error
@badwords_import_slash.error
async def slash_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        message_embed = make_error_embed(
            "Нет доступа",
            "У вас нет прав администратора для этой команды."
        )
    else:
        log_internal_exception("Slash command error", error)
        message_embed = make_error_embed(
            "Ошибка",
            "Произошла внутренняя ошибка. Подробности скрыты от пользователей и выведены в консоль бота."
        )

    if interaction.response.is_done():
        await interaction.followup.send(embed=message_embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=message_embed, ephemeral=True)


# =========================
# FINAL UI OVERRIDES
# =========================

def clear_all_guild_database(guild_id: int) -> tuple[int, int]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM registrations WHERE guild_id = ?", (guild_id,))
    reg_count = int(cur.fetchone()["count"])
    cur.execute("SELECT COUNT(*) AS count FROM rename_history WHERE guild_id = ?", (guild_id,))
    hist_count = int(cur.fetchone()["count"])
    cur.execute("DELETE FROM registrations WHERE guild_id = ?", (guild_id,))
    cur.execute("DELETE FROM rename_history WHERE guild_id = ?", (guild_id,))
    conn.commit()
    conn.close()
    return reg_count, hist_count


class ConfirmClearDatabaseView(discord.ui.View):
    def __init__(self, requester_id: int, guild_id: int):
        super().__init__(timeout=120)
        self.requester_id = requester_id
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message(embed=make_warning_embed("Нет доступа", "Это подтверждение предназначено не для вас."), ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Да, очистить", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        regs, hist = clear_all_guild_database(self.guild_id)
        await interaction.response.edit_message(embed=make_success_embed("База очищена", f"Удалено регистраций: **{regs}**\nУдалено записей истории: **{hist}**"), view=None)

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(embed=make_warning_embed("Отменено", "Очистка базы отменена."), view=None)


class SetRegistrationChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="📌 Регистрация", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["registration_channel_id"] = channel.id
        config["registration_message_id"] = 0
        save_config(config)
        if interaction.guild is not None:
            await ensure_registration_message(interaction.guild)
            await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Настройки обновлены", f"Канал регистрации изменён на {channel.mention}."), ephemeral=True)


class SetRulesChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="📜 Правила", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["rules_channel_id"] = channel.id
        config["rules_message_id"] = 0
        save_config(config)
        if interaction.guild is not None:
            await ensure_rules_message(interaction.guild)
            await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Настройки обновлены", f"Канал правил изменён на {channel.mention}."), ephemeral=True)


class SetWelcomeChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="👋 Welcome", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=2)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["welcome_channel_id"] = channel.id
        save_config(config)
        await interaction.response.send_message(embed=make_success_embed("Настройки обновлены", f"Welcome-канал изменён на {channel.mention}."), ephemeral=True)


class SetLogChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(placeholder="📊 Логи", channel_types=[discord.ChannelType.text], min_values=1, max_values=1, row=3)

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        if getattr(channel, "type", None) != discord.ChannelType.text:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Нужен текстовый канал."), ephemeral=True)
            return
        config["log_channel_id"] = channel.id
        save_config(config)
        await interaction.response.send_message(embed=make_success_embed("Настройки обновлены", f"Лог-канал изменён на {channel.mention}."), ephemeral=True)


class ChannelSetupView(BaseAdminSubview):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(author_id, guild_id)
        self.add_item(SetRegistrationChannelSelect())
        self.add_item(SetRulesChannelSelect())
        self.add_item(SetWelcomeChannelSelect())
        self.add_item(SetLogChannelSelect())

    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(title="📺 Каналы", description="Настройка служебных каналов сервера.", color=discord.Color.light_grey(), timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Текущая конфигурация", value=(
            f"• Регистрация — {get_registration_channel_mention(guild)}\n"
            f"• Правила — {get_rules_channel_mention(guild)}\n"
            f"• Welcome — {get_welcome_channel_mention(guild)}\n"
            f"• Логи — {get_log_channel_mention(guild)}"
        ), inline=False)
        embed.set_footer(text="Раздел: Каналы")
        return embed

    @discord.ui.button(label="🔒 Lock меню", style=discord.ButtonStyle.secondary, row=4)
    async def lock_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ChannelLockView(self.author_id, self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)


class ChannelLockView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(title="🔒 Lock меню", description="Управление блокировкой служебных каналов.", color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Текущие каналы", value=(
            f"Регистрация: {get_registration_channel_mention(guild)}\n"
            f"Правила: {get_rules_channel_mention(guild)}\n"
            f"Логи: {get_log_channel_mention(guild)}"
        ), inline=False)
        return embed

    async def _set_channel_send_permission(self, interaction: discord.Interaction, channel_key: str, allowed: bool, success_text: str):
        if interaction.guild is None:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Только на сервере."), ephemeral=True)
            return
        channel_id = int(config.get(channel_key, 0) or 0)
        channel = interaction.guild.get_channel(channel_id) if channel_id else None
        if channel is None:
            await interaction.response.send_message(embed=make_warning_embed("Канал не настроен", "Сначала выберите канал в разделе каналов."), ephemeral=True)
            return
        try:
            await channel.set_permissions(interaction.guild.default_role, send_messages=allowed)
        except discord.Forbidden:
            await interaction.response.send_message(embed=make_error_embed("Нет прав", "Бот не может изменить права этого канала."), ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message(embed=make_error_embed("Ошибка", "Не удалось изменить права канала."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Готово", success_text), ephemeral=True)

    @discord.ui.button(label="🔒 Lock регистрация", style=discord.ButtonStyle.secondary, row=0)
    async def lock_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "registration_channel_id", False, "Канал регистрации закрыт.")

    @discord.ui.button(label="🔓 Unlock регистрация", style=discord.ButtonStyle.success, row=0)
    async def unlock_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "registration_channel_id", True, "Канал регистрации открыт.")

    @discord.ui.button(label="🔒 Lock правила", style=discord.ButtonStyle.secondary, row=1)
    async def lock_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "rules_channel_id", False, "Канал правил закрыт.")

    @discord.ui.button(label="🔓 Unlock правила", style=discord.ButtonStyle.success, row=1)
    async def unlock_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "rules_channel_id", True, "Канал правил открыт.")

    @discord.ui.button(label="🔒 Lock логи", style=discord.ButtonStyle.secondary, row=2)
    async def lock_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "log_channel_id", False, "Лог-канал закрыт.")

    @discord.ui.button(label="🔓 Unlock логи", style=discord.ButtonStyle.success, row=2)
    async def unlock_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_channel_send_permission(interaction, "log_channel_id", True, "Лог-канал открыт.")

    @discord.ui.button(label="◀ Назад", style=discord.ButtonStyle.secondary, row=4)
    async def back_button_local(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ChannelSetupView(self.author_id, self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)


def build_messages_dashboard_embed(guild: discord.Guild) -> discord.Embed:
    embed = discord.Embed(title="📝 Сообщения", description="Массовые и точечные действия для служебных сообщений.", color=discord.Color.teal(), timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Каналы", value=f"Регистрация: {get_registration_channel_mention(guild)}\nПравила: {get_rules_channel_mention(guild)}", inline=False)
    embed.add_field(name="Статус", value=f"Сообщение регистрации: {'✅' if int(config.get('registration_message_id',0) or 0) else '❌'}\nСообщение правил: {'✅' if int(config.get('rules_message_id',0) or 0) else '❌'}", inline=False)
    return embed


def build_single_message_embed(guild: discord.Guild, mode: str) -> discord.Embed:
    if mode == 'registration':
        title = '📝 Регистрация'
        desc = f"Канал: {get_registration_channel_mention(guild)}"
    else:
        title = '📜 Правила'
        desc = f"Канал: {get_rules_channel_mention(guild)}"
    return discord.Embed(title=title, description=desc, color=discord.Color.teal(), timestamp=datetime.now(timezone.utc))


class MessagesDashboardView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_messages_dashboard_embed(guild)

    @discord.ui.button(label="📝 Регистрация", style=discord.ButtonStyle.secondary, row=0)
    async def registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RegistrationMessageView(self.author_id, self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)

    @discord.ui.button(label="📜 Правила", style=discord.ButtonStyle.secondary, row=0)
    async def rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RulesMessageView(self.author_id, self.guild_id)
        await interaction.response.edit_message(embed=view.build_embed(interaction.guild), view=view)

    @discord.ui.button(label="🔄 Обновить всё", style=discord.ButtonStyle.success, row=1)
    async def refresh_all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ensure_registration_message(interaction.guild)
        await ensure_rules_message(interaction.guild)
        ok1 = await refresh_registration_message(interaction.guild)
        ok2 = await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", f"Регистрация: {'✅' if ok1 else '❌'}\nПравила: {'✅' if ok2 else '❌'}"), ephemeral=True)

    @discord.ui.button(label="♻ Переотправить всё", style=discord.ButtonStyle.secondary, row=1)
    async def recreate_all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config['registration_message_id'] = 0
        config['rules_message_id'] = 0
        save_config(config)
        msg1 = await ensure_registration_message(interaction.guild)
        msg2 = await ensure_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", f"Регистрация: {'✅' if msg1 else '❌'}\nПравила: {'✅' if msg2 else '❌'}"), ephemeral=True)

    @discord.ui.button(label="👁 Предпросмотр", style=discord.ButtonStyle.primary, row=2)
    async def preview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        text = get_registration_message_text(interaction.guild)
        rules = get_rules_message_text(interaction.guild)
        embed = discord.Embed(title="👁 Предпросмотр", color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Регистрация", value=text[:1024], inline=False)
        embed.add_field(name="Правила", value=rules[:1024], inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class RegistrationMessageView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_single_message_embed(guild, 'registration')

    @discord.ui.button(label="✏ Изменить текст", style=discord.ButtonStyle.secondary, row=0)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(MessageEditorModal("Редактировать сообщение регистрации", "Текст сообщения", "registration_message_text", "Сообщение регистрации", get_registration_message_template()))

    @discord.ui.button(label="🔄 Обновить", style=discord.ButtonStyle.success, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ensure_registration_message(interaction.guild)
        ok = await refresh_registration_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение регистрации обновлено." if ok else "Не удалось обновить сообщение регистрации."), ephemeral=True)

    @discord.ui.button(label="♻ Переотправить", style=discord.ButtonStyle.secondary, row=1)
    async def recreate_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config['registration_message_id'] = 0
        save_config(config)
        msg = await ensure_registration_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение регистрации переотправлено." if msg else "Не удалось переотправить сообщение регистрации."), ephemeral=True)

    @discord.ui.button(label="👁 Предпросмотр", style=discord.ButtonStyle.primary, row=2)
    async def preview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(embed=discord.Embed(title="👁 Предпросмотр", description=get_registration_message_text(interaction.guild)[:4000], color=discord.Color.blurple()), ephemeral=True)


class RulesMessageView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_single_message_embed(guild, 'rules')

    @discord.ui.button(label="✏ Изменить текст", style=discord.ButtonStyle.secondary, row=0)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(MessageEditorModal("Редактировать сообщение правил", "Текст сообщения", "rules_message_text", "Сообщение правил", get_rules_message_template()))

    @discord.ui.button(label="🔄 Обновить", style=discord.ButtonStyle.success, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ensure_rules_message(interaction.guild)
        ok = await refresh_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение правил обновлено." if ok else "Не удалось обновить сообщение правил."), ephemeral=True)

    @discord.ui.button(label="♻ Переотправить", style=discord.ButtonStyle.secondary, row=1)
    async def recreate_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config['rules_message_id'] = 0
        save_config(config)
        msg = await ensure_rules_message(interaction.guild)
        await interaction.response.send_message(embed=make_success_embed("Готово", "Сообщение правил переотправлено." if msg else "Не удалось переотправить сообщение правил."), ephemeral=True)

    @discord.ui.button(label="👁 Предпросмотр", style=discord.ButtonStyle.primary, row=2)
    async def preview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(embed=discord.Embed(title="👁 Предпросмотр", description=get_rules_message_text(interaction.guild)[:4000], color=discord.Color.blurple()), ephemeral=True)


class UsersDashboardView(BaseAdminSubview):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(author_id, guild_id)
        self.add_item(AdminUserSelect(author_id, guild_id))

    @staticmethod
    def build_embed(guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(title="👥 Пользователи", description="Управление участниками сервера.", color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        rows = get_last_registrations(guild.id, limit=5)
        preview = "\n".join(f"• **{row['real_name']}** — `{row['user_id']}`" for row in rows) if rows else "Пока нет данных."
        embed.add_field(name="Последние регистрации", value=preview, inline=False)
        return embed

    @discord.ui.button(label="🔍 Найти", style=discord.ButtonStyle.primary, row=1)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="info"))

    @discord.ui.button(label="📜 История", style=discord.ButtonStyle.secondary, row=1)
    async def history_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="history"))

    @discord.ui.button(label="♻ Восстановить", style=discord.ButtonStyle.success, row=2)
    async def restore_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="restore"))

    @discord.ui.button(label="🗑 Удалить", style=discord.ButtonStyle.danger, row=2)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ActionSearchModal(author_id=self.author_id, action="delete"))

    @discord.ui.button(label="⏱ Кулдаун", style=discord.ButtonStyle.secondary, row=3)
    async def rename_cooldown_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Кулдаун смены имени", "Часы", 0, 720, int(config.get("rename_cooldown_hours", 24)), "rename_cooldown_hours", "Кулдаун смены имени"))

    @discord.ui.button(label="🚫 Антиспам", style=discord.ButtonStyle.secondary, row=3)
    async def antispam_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Антиспам регистрации", "Секунды", 0, 3600, get_registration_attempt_cooldown_seconds(), "registration_attempt_cooldown_seconds", "Антиспам регистрации"))


class DatabaseDashboardView(BaseAdminSubview):
    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return build_database_embed(guild)

    @discord.ui.button(label="📄 Экспорт CSV", style=discord.ButtonStyle.success, row=0)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        file_path = export_registrations_to_csv(interaction.guild.id)
        if file_path is None:
            await interaction.response.send_message(embed=make_warning_embed("Нет данных", "В базе нет записей для экспорта."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Экспорт готов", "CSV-файл с регистрациями прикреплён ниже."), file=discord.File(file_path), ephemeral=True)

    @discord.ui.button(label="📊 Статистика", style=discord.ButtonStyle.secondary, row=0)
    async def stats_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(embed=build_stats_embed(interaction.guild), ephemeral=True)

    @discord.ui.button(label="🗑 Очистить БД", style=discord.ButtonStyle.danger, row=1)
    async def clear_db_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ConfirmClearDatabaseView(interaction.user.id, interaction.guild.id)
        await interaction.response.send_message(embed=make_warning_embed("Подтверждение", "Очистить все регистрации и историю имён на этом сервере?"), view=view, ephemeral=True)


class BadwordsDashboardView(BaseAdminSubview):
    @discord.ui.button(label="➕ Добавить", style=discord.ButtonStyle.primary, row=0)
    async def add_word_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        class AddBadwordModal(discord.ui.Modal, title="Добавить запрещённое слово"):
            def __init__(self):
                super().__init__()
                self.word_input = discord.ui.TextInput(label="Слово", min_length=1, max_length=100, required=True)
                self.add_item(self.word_input)
            async def on_submit(self, interaction: discord.Interaction) -> None:
                ok, result = add_custom_badword(self.word_input.value)
                if ok:
                    await interaction.response.send_message(embed=make_success_embed("Слово добавлено", f"Добавлено: `{result}`"), ephemeral=True)
                else:
                    await interaction.response.send_message(embed=make_error_embed("Ошибка", result), ephemeral=True)
        await interaction.response.send_modal(AddBadwordModal())

    @discord.ui.button(label="➖ Удалить", style=discord.ButtonStyle.secondary, row=0)
    async def remove_word_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BadwordsListView(author_id=self.author_id, per_page=15)
        await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

    @discord.ui.button(label="📜 Список", style=discord.ButtonStyle.secondary, row=1)
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BadwordsListView(author_id=self.author_id, per_page=15)
        await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

    @discord.ui.button(label="🧹 Очистить", style=discord.ButtonStyle.danger, row=1)
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BadwordsListView(author_id=self.author_id, per_page=15)
        confirm_view = ConfirmClearBadwordsView(view, self.author_id)
        await interaction.response.send_message(embed=make_warning_embed("Подтверждение очистки", "Полностью очистить пользовательский список badwords?"), view=confirm_view, ephemeral=True)

    @discord.ui.button(label="📤 Экспорт", style=discord.ButtonStyle.success, row=2)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        file_path = export_badwords_to_json()
        await interaction.response.send_message(embed=make_success_embed("Экспорт готов", "JSON-файл прикреплён ниже."), file=discord.File(file_path), ephemeral=True)

    @discord.ui.button(label="📥 Импорт", style=discord.ButtonStyle.secondary, row=2)
    async def import_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(embed=make_info_embed("Импорт", "Для импорта используйте команду `/badwords_import` и приложите JSON-файл."), ephemeral=True)


class BackupDashboardView(BaseAdminSubview):
    @discord.ui.button(label="💾 Создать", style=discord.ButtonStyle.primary, row=0)
    async def create_backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            files = create_backup_bundle()
        except Exception as e:
            log_internal_exception("Ручной бэкап", e)
            await interaction.response.send_message(embed=make_error_embed("Ошибка бэкапа", "Не удалось создать бэкап."), ephemeral=True)
            return
        if not files:
            await interaction.response.send_message(embed=make_warning_embed("Нет файлов", "Не найдено файлов для резервного копирования."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Бэкап создан", files[0].name), ephemeral=True)

    @discord.ui.button(label="📂 Скачать", style=discord.ButtonStyle.success, row=0)
    async def download_latest_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        backups = list_backup_archives()
        if not backups:
            await interaction.response.send_message(embed=make_warning_embed("Нет бэкапов", "Архивы бэкапа ещё не созданы."), ephemeral=True)
            return
        await interaction.response.send_message(embed=make_success_embed("Последний бэкап", backups[0].name), file=discord.File(backups[0]), ephemeral=True)

    @discord.ui.button(label="📜 Архивы", style=discord.ButtonStyle.secondary, row=1)
    async def list_archives_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        backups = list_backup_archives()
        exports = list_export_archives()
        text = "\n".join(f"• `{file.name}`" for file in backups[:10]) if backups else "Нет архивов бэкапа."
        text2 = "\n".join(f"• `{file.name}`" for file in exports[:10]) if exports else "Нет архивов экспортов."
        embed = discord.Embed(title="📜 Архивы", color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Бэкапы", value=text, inline=False)
        embed.add_field(name="Архивы экспортов", value=text2, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🧹 Очистить", style=discord.ButtonStyle.danger, row=1)
    async def prune_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        prune_old_backups()
        prune_old_export_archives()
        await interaction.response.send_message(embed=make_success_embed("Готово", "Старые архивы очищены по текущим лимитам."), ephemeral=True)

    @discord.ui.button(label="⏱ Интервал", style=discord.ButtonStyle.secondary, row=2)
    async def backup_interval_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Интервал автобэкапа", "Часы", 1, 168, get_backup_interval_hours(), "backup_interval_hours", "Интервал автобэкапа"))

    @discord.ui.button(label="📦 Лимит", style=discord.ButtonStyle.secondary, row=2)
    async def backup_limit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Лимит бэкапов", "Количество", 1, 100, get_backup_max_files(), "backup_max_files", "Максимум архивов бэкапа"))

    @discord.ui.button(label="🗃 Архивы", style=discord.ButtonStyle.secondary, row=2)
    async def export_archive_limit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NumericSettingModal("Лимит архивов экспортов", "Количество", 1, 100, get_exports_archive_max_files(), "exports_archive_max_files", "Максимум архивов экспортов"))


class AdminPanelView(discord.ui.View):
    def __init__(self, author_id: int, guild_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.guild_id = guild_id
        self.message: Optional[discord.Message] = None
        self._live_update_task: Optional[asyncio.Task] = None

    async def bind_message(self, message: discord.Message):
        self.message = message
        self.start_live_updates()

    async def _show_subview(self, interaction: discord.Interaction, embed: discord.Embed, view: discord.ui.View):
        self.stop_live_updates()
        await interaction.response.edit_message(embed=embed, view=view)

    def start_live_updates(self):
        self.stop_live_updates()
        self._live_update_task = asyncio.create_task(self._live_update_loop())

    def stop_live_updates(self):
        if self._live_update_task and not self._live_update_task.done():
            self._live_update_task.cancel()
        self._live_update_task = None

    async def _live_update_loop(self):
        try:
            while not self.is_finished():
                await asyncio.sleep(15)
                if self.message is None:
                    continue
                guild = bot.get_guild(self.guild_id)
                if guild is None:
                    continue
                try:
                    await self.message.edit(embed=build_admin_dashboard_embed(guild), view=self)
                except (discord.NotFound, discord.Forbidden):
                    break
                except discord.HTTPException:
                    continue
        except asyncio.CancelledError:
            pass

    async def on_timeout(self):
        self.stop_live_updates()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(embed=make_warning_embed("Нет доступа", "Эта админ-панель предназначена не для вас."), ephemeral=True)
            return False
        if not is_admin_interaction(interaction):
            await interaction.response.send_message(embed=make_error_embed("Нет доступа", "У вас больше нет прав администратора."), ephemeral=True)
            return False
        return True

    @discord.ui.button(label="👥 Пользователи", style=discord.ButtonStyle.primary, row=0)
    async def users_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_subview(interaction, UsersDashboardView.build_embed(interaction.guild), UsersDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="📺 Каналы", style=discord.ButtonStyle.secondary, row=0)
    async def channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ChannelSetupView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="📝 Сообщения", style=discord.ButtonStyle.secondary, row=0)
    async def messages_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = MessagesDashboardView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="🎭 Роли", style=discord.ButtonStyle.secondary, row=1)
    async def roles_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RoleSetupView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="🗄 База данных", style=discord.ButtonStyle.secondary, row=1)
    async def db_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = DatabaseDashboardView(self.author_id, self.guild_id)
        await self._show_subview(interaction, view.build_embed(interaction.guild), view)

    @discord.ui.button(label="📛 Badwords", style=discord.ButtonStyle.secondary, row=1)
    async def badwords_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_subview(interaction, build_badwords_manager_embed(), BadwordsDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="💾 Бэкапы", style=discord.ButtonStyle.success, row=2)
    async def backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_subview(interaction, build_backup_manager_embed(), BackupDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="🩺 Диагностика", style=discord.ButtonStyle.success, row=2)
    async def diagnostics_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_subview(interaction, build_system_check_embed(interaction.guild), DiagnosticsDashboardView(self.author_id, self.guild_id))

    @discord.ui.button(label="🔄 Обновить", style=discord.ButtonStyle.primary, row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(embed=build_admin_dashboard_embed(interaction.guild), view=self)


if not TOKEN:
    raise ValueError("Не найден DISCORD_TOKEN в .env")


if __name__ == "__main__":
    init_db()
    bot.run(TOKEN)
