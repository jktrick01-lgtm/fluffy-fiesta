#!/usr/bin/env python3
"""
DK Scraping Bot — Master Final
- Full base64 / atob JS redirect support (cashwingo etc.)
- Full 302 / meta / JS chain following
- Admin approval system
- High-burst unique number extraction
- No artificial limits
Python 3.11+
"""

import os
import re
import time
import base64
import random
import sqlite3
import tempfile
import threading
import urllib.parse
from pathlib import Path
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, Tuple

import requests
import urllib3
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# Config
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8702508093:AAFrqo7guvR0fmXYqHrR8ROqqrjoSN_u6m8")
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "8753914631").split(",") if x.strip().isdigit()]

THREADS = int(os.environ.get("THREADS", "30"))
BURST_PER_CYCLE = int(os.environ.get("BURST_PER_CYCLE", "18"))
MAX_HOPS = 30
TIMEOUT = 15
PORT = int(os.environ.get("PORT", "8080"))

WORK_DIR = Path(tempfile.gettempdir()) / "wa_extractor_bot"
WORK_DIR.mkdir(parents=True, exist_ok=True)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
MAINTENANCE_MODE = False
user_states: dict = {}
active_jobs: dict = {}

# =========================================================
# Health Check
# =========================================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - DK Scraping Bot Live")
    def log_message(self, format, *args):
        return

def start_health_server():
    try:
        HTTPServer(("0.0.0.0", PORT), HealthCheckHandler).serve_forever()
    except Exception as e:
        print(f"Health warning: {e}")

# =========================================================
# Database
# =========================================================
DB_FILE = str(WORK_DIR / "bot_database.db")
_db_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with _db_lock:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                status TEXT DEFAULT 'pending',
                total_extractions INTEGER DEFAULT 0,
                total_numbers_found INTEGER DEFAULT 0,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_at TIMESTAMP,
                approved_by INTEGER
            );
            CREATE TABLE IF NOT EXISTS extraction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                url TEXT,
                cycles INTEGER,
                unique_numbers INTEGER,
                duplicate_count INTEGER,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_history_user ON extraction_history(user_id);
            CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
        """)
        for col, typ in [("status", "TEXT DEFAULT 'pending'"), ("approved_at", "TIMESTAMP"), ("approved_by", "INTEGER")]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typ}")
            except Exception:
                pass
        conn.commit()
        conn.close()

def register_user(user_id, username=None, first_name=None):
    with _db_lock:
        conn = get_conn()
        try:
            row = conn.execute("SELECT status FROM users WHERE user_id=?", (user_id,)).fetchone()
            if row:
                conn.execute(
                    "UPDATE users SET last_active=CURRENT_TIMESTAMP, username=COALESCE(?,username), first_name=COALESCE(?,first_name) WHERE user_id=?",
                    (username, first_name, user_id)
                )
                conn.commit()
                return row["status"]
            conn.execute(
                "INSERT INTO users (user_id,username,first_name,status) VALUES (?,?,?,'pending')",
                (user_id, username, first_name)
            )
            conn.commit()
            return "pending"
        finally:
            conn.close()

def get_user_status(user_id):
    conn = get_conn()
    try:
        row = conn.execute("SELECT status FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["status"] if row else "pending"
    finally:
        conn.close()

def set_user_status(user_id, status, admin_id=None):
    with _db_lock:
        conn = get_conn()
        try:
            if status == "approved":
                conn.execute(
                    "UPDATE users SET status=?, approved_at=CURRENT_TIMESTAMP, approved_by=?, last_active=CURRENT_TIMESTAMP WHERE user_id=?",
                    (status, admin_id, user_id)
                )
            else:
                conn.execute(
                    "UPDATE users SET status=?, last_active=CURRENT_TIMESTAMP WHERE user_id=?",
                    (status, user_id)
                )
            conn.commit()
            return conn.total_changes > 0
        finally:
            conn.close()

def update_user_stats(user_id, unique_count):
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "UPDATE users SET total_extractions=total_extractions+1, total_numbers_found=total_numbers_found+?, last_active=CURRENT_TIMESTAMP WHERE user_id=?",
                (unique_count, user_id)
            )
            conn.commit()
        finally:
            conn.close()

def save_history(user_id, url, cycles, unique_numbers, duplicate_count):
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO extraction_history (user_id,url,cycles,unique_numbers,duplicate_count,completed_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
                (user_id, url, cycles, unique_numbers, duplicate_count)
            )
            conn.commit()
        finally:
            conn.close()

def get_user_stats(user_id):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def get_user_history(user_id, limit=10):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM extraction_history WHERE user_id=? ORDER BY started_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def get_admin_stats():
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT COUNT(*) AS total_users,
                   SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) AS approved,
                   SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) AS rejected,
                   SUM(total_extractions) AS total_ex,
                   SUM(total_numbers_found) AS total_nums
            FROM users
        """).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()

def get_all_user_ids(status=None):
    conn = get_conn()
    try:
        if status:
            rows = conn.execute("SELECT user_id FROM users WHERE status=?", (status,)).fetchall()
        else:
            rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]
    finally:
        conn.close()

def get_pending_users(limit=20):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT user_id,username,first_name,joined_at FROM users WHERE status='pending' ORDER BY joined_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

# =========================================================
# AES-128 (legacy testcookie support)
# =========================================================
SBOX = [
    0x63,0x7C,0x77,0x7B,0xF2,0x6B,0x6F,0xC5,0x30,0x01,0x67,0x2B,0xFE,0xD7,0xAB,0x76,
    0xCA,0x82,0xC9,0x7D,0xFA,0x59,0x47,0xF0,0xAD,0xD4,0xA2,0xAF,0x9C,0xA4,0x72,0xC0,
    0xB7,0xFD,0x93,0x26,0x36,0x3F,0xF7,0xCC,0x34,0xA5,0xE5,0xF1,0x71,0xD8,0x31,0x15,
    0x04,0xC7,0x23,0xC3,0x18,0x96,0x05,0x9A,0x07,0x12,0x80,0xE2,0xEB,0x27,0xB2,0x75,
    0x09,0x83,0x2C,0x1A,0x1B,0x6E,0x5A,0xA0,0x52,0x3B,0xD6,0xB3,0x29,0xE3,0x2F,0x84,
    0x53,0xD1,0x00,0xED,0x20,0xFC,0xB1,0x5B,0x6A,0xCB,0xBE,0x39,0x4A,0x4C,0x58,0xCF,
    0xD0,0xEF,0xAA,0xFB,0x43,0x4D,0x33,0x85,0x45,0xF9,0x02,0x7F,0x50,0x3C,0x9F,0xA8,
    0x51,0xA3,0x40,0x8F,0x92,0x9D,0x38,0xF5,0xBC,0xB6,0xDA,0x21,0x10,0xFF,0xF3,0xD2,
    0xCD,0x0C,0x13,0xEC,0x5F,0x97,0x44,0x17,0xC4,0xA7,0x7E,0x3D,0x64,0x5D,0x19,0x73,
    0x60,0x81,0x4F,0xDC,0x22,0x2A,0x90,0x88,0x46,0xEE,0xB8,0x14,0xDE,0x5E,0x0B,0xDB,
    0xE0,0x32,0x3A,0x0A,0x49,0x06,0x24,0x5C,0xC2,0xD3,0xAC,0x62,0x91,0x95,0xE4,0x79,
    0xE7,0xC8,0x37,0x6D,0x8D,0xD5,0x4E,0xA9,0x6C,0x56,0xF4,0xEA,0x65,0x7A,0xAE,0x08,
    0xBA,0x78,0x25,0x2E,0x1C,0xA6,0xB4,0xC6,0xE8,0xDD,0x74,0x1F,0x4B,0xBD,0x8B,0x8A,
    0x70,0x3E,0xB5,0x66,0x48,0x03,0xF6,0x0E,0x61,0x35,0x57,0xB9,0x86,0xC1,0x1D,0x9E,
    0xE1,0xF8,0x98,0x11,0x69,0xD9,0x8E,0x94,0x9B,0x1E,0x87,0xE9,0xCE,0x55,0x28,0xDF,
    0x8C,0xA1,0x89,0x0D,0xBF,0xE6,0x42,0x68,0x41,0x99,0x2D,0x0F,0xB0,0x54,0xBB,0x16
]
INV_SBOX = [0] * 256
for i, v in enumerate(SBOX):
    INV_SBOX[v] = i
RCON = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1B, 0x36]

def _xtime(a):
    return ((a << 1) ^ 0x1B) & 0xFF if a & 0x80 else (a << 1)

def _mul(a, b):
    r = 0
    for _ in range(8):
        if b & 1:
            r ^= a
        b >>= 1
        a = _xtime(a)
    return r

def _expand_key(key):
    w = [list(key[i*4:i*4+4]) for i in range(4)]
    for i in range(4, 44):
        t = list(w[i-1])
        if i % 4 == 0:
            t = t[1:] + t[:1]
            t = [SBOX[x] for x in t]
            t[0] ^= RCON[i//4 - 1]
        w.append([w[i-4][j] ^ t[j] for j in range(4)])
    return [bytes(b for word in w[i*4:i*4+4] for b in word) for i in range(11)]

def _add_round_key(state, rk):
    return bytearray(a ^ b for a, b in zip(state, rk))

def _inv_shift_rows(state):
    new = bytearray(16)
    for c in range(4):
        for r in range(4):
            new[4*c + r] = state[4*((c - r) % 4) + r]
    return new

def _inv_mix_columns(state):
    new = bytearray(16)
    for c in range(4):
        a0, a1, a2, a3 = state[4*c:4*c+4]
        new[4*c+0] = _mul(a0,14) ^ _mul(a1,11) ^ _mul(a2,13) ^ _mul(a3,9)
        new[4*c+1] = _mul(a0,9)  ^ _mul(a1,14) ^ _mul(a2,11) ^ _mul(a3,13)
        new[4*c+2] = _mul(a0,13) ^ _mul(a1,9)  ^ _mul(a2,14) ^ _mul(a3,11)
        new[4*c+3] = _mul(a0,11) ^ _mul(a1,13) ^ _mul(a2,9)  ^ _mul(a3,14)
    return new

def _decrypt_block(block, rks):
    state = _add_round_key(bytearray(block), rks[10])
    for r in range(9, 0, -1):
        state = _inv_shift_rows(state)
        state = bytearray(INV_SBOX[b] for b in state)
        state = _add_round_key(state, rks[r])
        state = _inv_mix_columns(state)
    state = _inv_shift_rows(state)
    state = bytearray(INV_SBOX[b] for b in state)
    state = _add_round_key(state, rks[0])
    return bytes(state)

def aes_cbc_decrypt(ct, key, iv):
    rks = _expand_key(key)
    out = bytearray()
    prev = iv
    for i in range(0, len(ct), 16):
        block = ct[i:i+16]
        dec = _decrypt_block(block, rks)
        out.extend(a ^ b for a, b in zip(dec, prev))
        prev = block
    return bytes(out)

def aes_ecb_decrypt(ct, key):
    rks = _expand_key(key)
    out = bytearray()
    for i in range(0, len(ct), 16):
        out.extend(_decrypt_block(ct[i:i+16], rks))
    return bytes(out)

# =========================================================
# Headers / Session
# =========================================================
ANDROID_MODELS = [
    "I2410 Build/BP2A.250605.031.A3_V000L1", "CPH2665 Build/BP2A.250605.015",
    "SM-S928B Build/AP3A.240905.015", "23021RAAEG Build/UKQ1.231003.002",
    "V2312 Build/AP3A.240905.015", "CPH2591 Build/BP2A.250605.015",
    "SM-S921B Build/AP3A.240905.015", "RMX3840 Build/UKQ1.230924.001",
    "M2101K6P Build/TKQ1.221013.002", "SM-A556E Build/UP1A.231005.007",
    "OnePlus 12 Build/UKQ1.230924.001", "vivo V29e Build/TP1A.220624.014",
    "Pixel 8a Build/AP2A.240805.005", "22101316I Build/TP1A.220624.014"
]
INDIAN_IP_PREFIXES = [
    "49.36.", "49.37.", "49.44.", "49.45.", "106.51.", "106.192.", "106.193.",
    "117.96.", "117.97.", "117.98.", "117.99.", "122.160.", "122.161.", "122.162.",
    "182.64.", "182.65.", "182.66.", "182.67.", "223.176.", "223.177.", "223.178.",
    "43.241.", "43.242.", "43.243.", "59.144.", "59.145.", "59.146.",
    "27.56.", "27.57.", "27.58.", "27.59.", "106.79.", "106.80.", "106.81.",
    "152.58.", "152.59.", "157.32.", "157.33.", "157.34.", "103.21.", "103.22.",
    "115.96.", "115.97.", "117.200.", "117.201.", "14.139.", "103.48.", "106.215."
]
CACHE_BUST_PARAMS = ["_r", "igshid", "fbclid", "utm_content", "v", "t", "s", "ref", "cb", "ts", "r", "nocache"]

def normalize_input_url(raw):
    if not raw:
        return None
    m = re.search(r"(https?://[^\s<>\"']+)", raw.strip(), re.I)
    if not m:
        return None
    url = m.group(1)
    se = url.find("://")
    if se != -1:
        url = url[:se].lower() + url[se:]
    return url.rstrip(".,);]")

def random_indian_ip():
    return random.choice(INDIAN_IP_PREFIXES) + str(random.randint(1, 254)) + "." + str(random.randint(1, 254))

def build_headers(include_xff=True):
    av = random.choice([14, 15, 16])
    model = random.choice(ANDROID_MODELS)
    cm = random.randint(148, 154)
    cs = random.randint(6900, 8037)
    cp = random.randint(100, 210)
    ua = f"Mozilla/5.0 (Linux; Android {av}; {model}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{cm}.0.{cs}.{cp} Mobile Safari/537.36"
    h = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Upgrade-Insecure-Requests": "1",
        "X-Requested-With": random.choice(["mark.via.gp", "via.bolte", "com.android.browser"]),
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "sec-ch-ua": f'"Chromium";v="{cm}", "Android WebView";v="{cm}", "Not A(Brand";v="99"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "Connection": "keep-alive",
    }
    if include_xff:
        ip = random_indian_ip()
        h["X-Forwarded-For"] = ip
        h["X-Real-IP"] = ip
        h["Client-IP"] = ip
        h["Forwarded"] = f"for={ip}"
    return h

class CffiSessionWrapper:
    def __init__(self):
        try:
            self.s = cffi_requests.Session(impersonate=random.choice(["chrome120", "chrome119", "chrome116", "chrome110"]), verify=False)
        except Exception:
            self.s = cffi_requests.Session(impersonate="chrome120", verify=False)
        self.cookies = self.s.cookies

    def get(self, url, headers=None, timeout=10, allow_redirects=False):
        return self.s.get(url, headers=headers, timeout=timeout, allow_redirects=allow_redirects)

def make_session():
    if HAS_CFFI:
        try:
            return CffiSessionWrapper()
        except Exception:
            pass
    s = requests.Session()
    s.verify = False
    ad = requests.adapters.HTTPAdapter(pool_connections=THREADS, pool_maxsize=THREADS * 2, max_retries=1)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s

def smart_get(session, url, headers, allow_redirects=False):
    r = session.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=allow_redirects)
    if r.status_code == 403:
        alt = build_headers(include_xff=False)
        r2 = session.get(url, headers=alt, timeout=TIMEOUT, allow_redirects=allow_redirects)
        if r2.status_code != 403:
            return r2, alt
    return r, headers

def resolve_url(base, loc):
    if not loc:
        return None
    loc = loc.strip().replace("&amp;", "&")
    if loc.startswith(("whatsapp://", "intent://")):
        return loc
    if loc.startswith("/"):
        p = urllib.parse.urlparse(base)
        return p.scheme + "://" + p.netloc + loc
    if not loc.startswith("http"):
        p = urllib.parse.urlparse(base)
        return p.scheme + "://" + p.netloc + "/" + loc.lstrip("/")
    return loc

def bust_url(url):
    r = f"{random.randint(100000,9999999)}_{random.randint(100,999)}_{int(time.time()*1000)%100000}"
    return f"{url}{'&' if '?' in url else '?'}{random.choice(CACHE_BUST_PARAMS)}={r}"

# =========================================================
# Number Extraction
# =========================================================
_PHONE_PATTERNS = [
    re.compile(r"wa\.me/(?:message/[A-Z0-9]+.*?)?(\+?\d{10,15})", re.I),
    re.compile(r"(?:api\.|web\.)?whatsapp\.com/send/?\?[^\"'\s<>]*?phone=(\+?\d{10,15})", re.I),
    re.compile(r"whatsapp://send/?\?[^\"'\s<>]*?phone=(\+?\d{10,15})", re.I),
    re.compile(r"intent://send/?\?[^\"'\s<>]*?phone=(\+?\d{10,15})", re.I),
    re.compile(r"[\?&]phone=(\+?\d{10,15})(?:[&\s\"'<>]|$)", re.I),
    re.compile(r"data-phone=[\"']?(\+?\d{10,15})", re.I),
    re.compile(r"data-wa=[\"']?(\+?\d{10,15})", re.I),
    re.compile(r"data-number=[\"']?(\+?\d{10,15})", re.I),
    re.compile(r"tel:(\+?\d{10,15})", re.I),
    re.compile(r"href=[\"']https?://wa\.me/(\+?\d{10,15})", re.I),
    re.compile(r"https?://wa\.me/(\+?\d{10,15})", re.I),
    re.compile(r"phone%3D(\d{10,15})", re.I),
    re.compile(r"phone%3D%2B(\d{10,15})", re.I),
    re.compile(r"wa\.me%2F(\d{10,15})", re.I),
]

def validate_and_clean_number(raw):
    if not raw:
        return None
    clean = re.sub(r"\D", "", str(raw))
    if not clean or len(clean) < 10 or len(clean) > 15:
        return None
    if len(clean) == 11 and clean.startswith("0") and clean[1] in "6789":
        clean = "91" + clean[1:]
    elif len(clean) == 10 and clean[0] in "6789":
        clean = "91" + clean
    if clean.startswith("91") and len(clean) == 12 and clean[2] in "6789":
        return clean
    if 10 <= len(clean) <= 15:
        return clean
    return None

def extract_numbers_from_text(text):
    if not text:
        return set()
    found = set()
    decoded = urllib.parse.unquote(text)
    for sample in (text, decoded):
        for pat in _PHONE_PATTERNS:
            for m in pat.findall(sample):
                if isinstance(m, tuple):
                    m = m[0]
                v = validate_and_clean_number(m)
                if v:
                    found.add(v)
    return found

def is_whatsapp_destination(url):
    if not url:
        return False
    u = url.lower()
    return "wa.me/" in u or "whatsapp.com/send" in u or u.startswith("whatsapp://") or u.startswith("intent://send")

def is_telegram_destination(url):
    if not url:
        return False
    u = url.lower()
    return "t.me/" in u or "telegram.me/" in u or "telegram.org" in u

def try_decode_b64(s):
    try:
        s2 = s.strip() + "=" * ((-len(s.strip())) % 4)
        return base64.b64decode(s2).decode("utf-8", "ignore")
    except Exception:
        return None

def find_all_b64_redirects(html):
    out = []
    for m in re.finditer(r"atob\s*\(\s*['\"]([A-Za-z0-9+/=]{20,})['\"]\s*\)", html, re.I):
        dec = try_decode_b64(m.group(1))
        if dec and ("wa.me" in dec or "whatsapp" in dec or "http" in dec):
            out.append(dec.strip())
    for m in re.finditer(r"location\.(?:replace|href|assign)\s*\(\s*atob\s*\(\s*['\"]([A-Za-z0-9+/=]{20,})['\"]", html, re.I):
        dec = try_decode_b64(m.group(1))
        if dec:
            out.append(dec.strip())
    for m in re.finditer(r"['\"]([A-Za-z0-9+/]{40,}={0,2})['\"]", html):
        dec = try_decode_b64(m.group(1))
        if dec and ("wa.me" in dec or "whatsapp.com/send" in dec or dec.startswith("http")):
            out.append(dec.strip())
    for m in re.finditer(r"window\.location\s*=\s*atob\s*\(\s*['\"]([A-Za-z0-9+/=]{20,})['\"]", html, re.I):
        dec = try_decode_b64(m.group(1))
        if dec:
            out.append(dec.strip())
    seen = set()
    res = []
    for x in out:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res

def find_js_string_redirects(html):
    out = []
    for m in re.finditer(r"location\.(?:replace|href|assign)\s*\(?\s*['\"]([^'\"]+)['\"]", html, re.I):
        v = m.group(1).strip()
        if v.startswith(("http", "/", "whatsapp://", "intent://")):
            out.append(v)
    for m in re.finditer(r"(?:window\.|document\.)?location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]", html, re.I):
        v = m.group(1).strip()
        if v.startswith(("http", "/", "whatsapp://", "intent://")):
            out.append(v)
    return out

def find_meta_refresh(html):
    out = []
    for m in re.finditer(r'<meta[^>]+http-equiv\s*=\s*["\']?refresh["\']?[^>]+content\s*=\s*["\'][^"\']*url=([^"\'>\s]+)', html, re.I):
        out.append(m.group(1).strip())
    return out

def solve_testcookie(html):
    m = re.search(r"slowAES\.decrypt\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\w+)\s*,\s*(\w+)\s*\)", html)
    if m:
        c_var, mode, a_var, b_var = m.group(1), int(m.group(2)), m.group(3), m.group(4)
        def grab(v):
            m2 = re.search(v + r"\s*=\s*toNumbers\(\s*['\"]([0-9a-fA-F]+)['\"]\s*\)", html)
            if m2:
                h = m2.group(1)
                return bytes(int(h[i:i+2], 16) for i in range(0, len(h), 2))
            m3 = re.search(v + r"\s*=\s*\[([^\]]+)\]", html)
            if m3:
                nums = [int(x) for x in re.findall(r"\d+", m3.group(1))]
                if nums:
                    return bytes(nums)
            return None
        key, iv, ct = grab(a_var), grab(b_var), grab(c_var)
    else:
        matches = re.findall(r'toNumbers\(\s*["\']([a-fA-F0-9]+)["\']\s*\)', html)
        if len(matches) < 3:
            return None
        key = bytes.fromhex(matches[0])
        iv = bytes.fromhex(matches[1])
        ct = bytes.fromhex(matches[2])
        mode = 2
    if not key or not iv or not ct:
        return None
    m_name = re.search(r"document\.cookie\s*=\s*['\"]([^=;]+)=", html)
    cookie_name = m_name.group(1).strip() if m_name else "__test"
    try:
        pt = aes_cbc_decrypt(ct, key, iv) if mode == 2 else aes_ecb_decrypt(ct, key)
    except Exception:
        return None
    if pt:
        pad = pt[-1]
        if 1 <= pad <= 16 and pt.endswith(bytes([pad]) * pad):
            pt = pt[:-pad]
    return pt.hex(), cookie_name

def bypass_testcookie(session, url, headers, resp):
    if "slowAES" not in resp.text and "__test" not in resp.text:
        return None
    result = solve_testcookie(resp.text)
    if not result:
        return None
    cookie_val, cookie_name = result
    try:
        session.cookies.set(cookie_name, cookie_val, domain=urllib.parse.urlparse(url).hostname, path="/")
    except Exception:
        pass
    retry_url = url
    m_loc = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", resp.text)
    if m_loc:
        retry_url = resolve_url(url, m_loc.group(1)) or url
    return session.get(retry_url, headers=headers, timeout=TIMEOUT, allow_redirects=False)

# =========================================================
# Core Scrape Engine
# =========================================================
def single_attempt_scrape(seed_url, use_cache_buster=True):
    session = make_session()
    found = set()
    url = bust_url(seed_url) if use_cache_buster else seed_url
    headers = build_headers(True)
    visited = set()
    any_ok = False
    telegram_hits = 0

    for _ in range(MAX_HOPS):
        if not url or url in visited:
            break
        visited.add(url)

        if is_whatsapp_destination(url):
            found |= extract_numbers_from_text(url)
            return found, True

        if is_telegram_destination(url):
            telegram_hits += 1
            if telegram_hits >= 4:
                break
            url = bust_url(seed_url)
            headers = build_headers(True)
            continue

        try:
            r, headers = smart_get(session, url, headers, allow_redirects=False)
            if r.status_code != 403:
                any_ok = True
        except Exception:
            break

        body = r.text or ""
        found |= extract_numbers_from_text(body)
        found |= extract_numbers_from_text(r.headers.get("Location") or "")

        if r.status_code == 200 and ("slowAES" in body or "__test" in body):
            try:
                solved = bypass_testcookie(session, url, headers, r)
                if solved is not None:
                    r = solved
                    body = r.text or ""
                    found |= extract_numbers_from_text(body)
                    found |= extract_numbers_from_text(r.headers.get("Location") or "")
            except Exception:
                pass

        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("Location")
            if not loc:
                break
            loc = resolve_url(url, loc)
            found |= extract_numbers_from_text(loc or "")
            if is_whatsapp_destination(loc):
                return found, True
            if is_telegram_destination(loc):
                telegram_hits += 1
                if telegram_hits >= 4:
                    break
                url = bust_url(seed_url)
                headers = build_headers(True)
                continue
            url = loc
            continue

        if r.status_code == 200 and body:
            b64s = find_all_b64_redirects(body)
            followed = False
            for tgt in b64s:
                resolved = resolve_url(url, tgt)
                if not resolved:
                    continue
                found |= extract_numbers_from_text(resolved)
                if is_whatsapp_destination(resolved):
                    return found, True
                if not is_telegram_destination(resolved):
                    url = resolved
                    followed = True
                    break
            if followed:
                continue

            redirects = find_js_string_redirects(body) + find_meta_refresh(body)
            next_hop = None
            for tgt in redirects:
                resolved = resolve_url(url, tgt)
                if not resolved:
                    continue
                found |= extract_numbers_from_text(resolved)
                if is_whatsapp_destination(resolved):
                    return found, True
                if is_telegram_destination(resolved):
                    telegram_hits += 1
                    continue
                if not next_hop:
                    next_hop = resolved
            if next_hop:
                url = next_hop
                continue
            break

        break

    return found, any_ok

def worker_scrape(seed_url, worker_idx=0):
    if worker_idx > 0:
        time.sleep(random.uniform(0.02, 0.14) * (worker_idx % 10))
    cycle_numbers = set()
    raw_hits = 0
    for _ in range(BURST_PER_CYCLE):
        nums, ok = single_attempt_scrape(seed_url, True)
        if nums:
            raw_hits += len(nums)
            cycle_numbers.update(nums)
        else:
            for _ in range(2):
                retry, _ = single_attempt_scrape(seed_url, True)
                if retry:
                    raw_hits += len(retry)
                    cycle_numbers.update(retry)
                    break
                time.sleep(random.uniform(0.05, 0.16))
        time.sleep(random.uniform(0.03, 0.10))
    return cycle_numbers, raw_hits

def analyze_url(url):
    session = make_session()
    headers = build_headers(True)
    info = {"protection": "Direct / Standard", "chain": [], "reachable": False, "note": "", "initial_numbers": set()}
    cur = url
    visited = set()
    for _ in range(MAX_HOPS):
        if not cur or cur in visited:
            break
        visited.add(cur)
        info["initial_numbers"] |= extract_numbers_from_text(cur)
        if is_whatsapp_destination(cur):
            info["reachable"] = True
            info["chain"].append("→ wa.me / api.whatsapp.com ✅")
            break
        if is_telegram_destination(cur):
            info["chain"].append("→ t.me (soft)")
            info["note"] = "rotating"
            break
        try:
            r, headers = smart_get(session, cur, headers, False)
        except Exception as e:
            info["chain"].append("ERR " + str(e)[:50])
            break
        info["reachable"] = True
        body = r.text or ""
        info["initial_numbers"] |= extract_numbers_from_text(body)
        info["initial_numbers"] |= extract_numbers_from_text(r.headers.get("Location") or "")

        if r.status_code == 200 and ("slowAES" in body or "__test" in body):
            info["protection"] = "testcookie AES"
            r2 = bypass_testcookie(session, cur, headers, r)
            if r2 is not None:
                info["chain"].append("challenge bypassed → " + str(r2.status_code))
                info["initial_numbers"] |= extract_numbers_from_text(r2.text or "")
                if r2.status_code in (301, 302, 303, 307, 308):
                    cur = resolve_url(cur, r2.headers.get("Location"))
                    continue
            break

        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("Location") or ""
            info["chain"].append(f"{r.status_code} → {loc[:80]}")
            cur = resolve_url(cur, loc)
            info["initial_numbers"] |= extract_numbers_from_text(cur or "")
            if cur and is_whatsapp_destination(cur):
                info["chain"].append("→ wa.me ✅")
                break
            if cur and is_telegram_destination(cur):
                info["chain"].append("→ t.me")
                info["note"] = "rotating"
                break
            continue

        if r.status_code == 200 and body:
            b64s = find_all_b64_redirects(body)
            if b64s:
                info["protection"] = "base64 / atob JS redirect"
                for t in b64s:
                    info["chain"].append("atob → " + t[:80])
                    info["initial_numbers"] |= extract_numbers_from_text(t)
                cur = resolve_url(cur, b64s[0])
                if cur and is_whatsapp_destination(cur):
                    info["chain"].append("→ wa.me ✅")
                    break
                continue
            metas = find_meta_refresh(body)
            if metas:
                info["protection"] = "meta refresh"
                info["chain"].append("meta → " + metas[0][:75])
                cur = resolve_url(cur, metas[0])
                continue
            jss = find_js_string_redirects(body)
            if jss:
                info["protection"] = "JS redirect"
                info["chain"].append("js → " + jss[0][:75])
                cur = resolve_url(cur, jss[0])
                continue
        info["chain"].append(f"{r.status_code} → {cur[:65]}")
        break

    if not info["initial_numbers"]:
        nums, ok = single_attempt_scrape(url, True)
        if ok:
            info["reachable"] = True
        info["initial_numbers"] |= nums
    return info

# =========================================================
# Extraction Worker
# =========================================================
def extraction_worker(chat_id, user_id, url, total_cycles, message_id, initial_numbers=None):
    active_jobs[user_id] = True
    t0 = time.time()
    found_numbers = set(initial_numbers or set())
    total_seen = len(found_numbers)
    completed = 0
    total_ok = 0
    errors = 0
    last_ui = 0.0
    worker_count = min(THREADS, max(1, total_cycles))

    with ThreadPoolExecutor(max_workers=worker_count) as ex:
        futures = [ex.submit(worker_scrape, url, i) for i in range(total_cycles)]
        for fut in as_completed(futures):
            if not active_jobs.get(user_id, True):
                for f in futures:
                    f.cancel()
                break
            completed += 1
            try:
                nums, hits = fut.result()
                if nums or hits > 0:
                    total_ok += 1
                else:
                    errors += 1
                total_seen += hits
                found_numbers.update(nums)
            except Exception:
                errors += 1
            now = time.time()
            if (now - last_ui > 1.8) or completed == total_cycles:
                last_ui = now
                try:
                    done = int((completed / total_cycles) * 10)
                    bar = "█" * done + "░" * (10 - done)
                    pct = int((completed / total_cycles) * 100)
                    bot.edit_message_text(
                        chat_id=chat_id, message_id=message_id,
                        text=(
                            f"⚡ *Extraction Running*\n━━━━━━━━━━━━━━━━━━━━━\n"
                            f"🔗 `{url[:38]}{'...' if len(url) > 38 else ''}`\n"
                            f"📊 `[{bar}] {pct}%`\n"
                            f"🔄 `{completed}/{total_cycles}` (`{completed * BURST_PER_CYCLE}` hits)\n"
                            f"🧵 `{worker_count}` | 📱 Unique: `{len(found_numbers)}`\n"
                            f"🔁 Dupes: `{max(total_seen - len(found_numbers), 0)}` | ⏱️ `{round(now - t0, 1)}s`\n"
                            f"━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass

    active_jobs.pop(user_id, None)
    elapsed = round(time.time() - t0, 1)
    unique = len(found_numbers)
    dupes = max(total_seen - unique, 0)
    update_user_stats(user_id, unique)
    save_history(user_id, url, completed, unique, dupes)

    bot.send_message(
        chat_id,
        f"✅ *Done!*\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Cycles: `{completed}` (`{completed * BURST_PER_CYCLE}` req)\n"
        f"✅ OK: `{total_ok}` | ❌ Empty: `{errors}`\n"
        f"🎯 Unique: `{unique}` | 🔁 Dupes: `{dupes}`\n"
        f"⏱️ `{elapsed}s`\n━━━━━━━━━━━━━━━━━━━━━",
        parse_mode="Markdown",
        reply_markup=main_keyboard()
    )

    if unique > 0:
        sorted_nums = sorted(found_numbers)
        numbers_text = "\n".join(f"+{n}" for n in sorted_nums)
        CHUNK = 3500
        header = f"📱 *Numbers*\n━━━━━━━━━━━━━━━━━━━━━\n🎯 Unique: `{unique}`\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        lines = numbers_text.split("\n")
        chunks = []
        cur = ""
        for line in lines:
            if len(cur) + len(line) + 1 > CHUNK:
                chunks.append(cur.strip())
                cur = line + "\n"
            else:
                cur += line + "\n"
        if cur.strip():
            chunks.append(cur.strip())
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📋 Copy", switch_inline_query=numbers_text[:250]))
        for i, ch in enumerate(chunks):
            h = header if i == 0 else f"📱 *Part {i+1}/{len(chunks)}*\n\n"
            try:
                bot.send_message(chat_id, h + f"`{ch}`", parse_mode="Markdown", reply_markup=markup if i == 0 else None)
            except Exception:
                bot.send_message(chat_id, ch)
        fp = WORK_DIR / f"wa_{user_id}_{int(time.time())}.txt"
        try:
            with open(fp, "w", encoding="utf-8") as f:
                f.write(f"DK Bot\nSource: {url}\nTime: {datetime.now()}\nUnique: {unique}\nDupes: {dupes}\n{'='*40}\n\n")
                for n in sorted_nums:
                    f.write(f"+{n}\n")
            with open(fp, "rb") as doc:
                bot.send_document(chat_id, doc, caption=f"📁 Unique: `{unique}` | Dupes: `{dupes}`", parse_mode="Markdown")
        except Exception as e:
            bot.send_message(chat_id, f"File error: `{str(e)[:60]}`")
        finally:
            if fp.exists():
                try:
                    fp.unlink()
                except Exception:
                    pass
    else:
        bot.send_message(chat_id, "⚠️ *No numbers found.* Link not redirecting to any valid WhatsApp number.", reply_markup=main_keyboard())

# =========================================================
# Keyboards
# =========================================================
def main_keyboard():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(
        types.KeyboardButton("🔗 Send New Link"),
        types.KeyboardButton("📋 My History"),
        types.KeyboardButton("📊 My Stats"),
        types.KeyboardButton("❓ Help"),
        types.KeyboardButton("📞 Support")
    )
    return m

def extraction_keyboard():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(
        types.KeyboardButton("🧪 Test (1x)"),
        types.KeyboardButton("🚀 20 Times"),
        types.KeyboardButton("⚡ 50 Times"),
        types.KeyboardButton("🔥 100 Times"),
        types.KeyboardButton("💎 200 Times (Turbo)"),
        types.KeyboardButton("❌ Cancel")
    )
    return m

def admin_keyboard():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    lab = "🔴 Maintenance: ON" if MAINTENANCE_MODE else "🟢 Maintenance: OFF"
    m.add(
        types.KeyboardButton("📈 Bot Stats"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("👥 Pending Users"),
        types.KeyboardButton("👤 Recent Users"),
        types.KeyboardButton(lab),
        types.KeyboardButton("🔙 Main Menu")
    )
    return m

def approval_keyboard(uid):
    m = types.InlineKeyboardMarkup()
    m.row(
        types.InlineKeyboardButton("✅ Approve", callback_data=f"approve:{uid}"),
        types.InlineKeyboardButton("❌ Reject", callback_data=f"reject:{uid}")
    )
    return m

def notify_admins_new_user(uid, username, first_name):
    name = first_name or "Unknown"
    uname = f"@{username}" if username else "no_username"
    text = (
        f"🆕 *New User*\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {name}\n🆔 `{uid}`\n🔗 {uname}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\nApprove / Reject:"
    )
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, text, reply_markup=approval_keyboard(uid))
        except Exception:
            pass

# =========================================================
# Handlers
# =========================================================
_COUNT_MAP = {
    "🧪 Test (1x)": 1,
    "🚀 20 Times": 20,
    "⚡ 50 Times": 50,
    "🔥 100 Times": 100,
    "💎 200 Times (Turbo)": 200
}

@bot.message_handler(commands=["start"])
def cmd_start(message):
    user = message.from_user
    status = register_user(user.id, user.username, user.first_name)
    if user.id in ADMIN_IDS:
        bot.send_message(message.chat.id, "👋 *Admin*\nUse /admin for panel.", reply_markup=main_keyboard())
        return
    if status == "approved":
        bot.send_message(
            message.chat.id,
            "👋 *DK Scraping Bot*\n━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ Works on cashwingo, cloaknews, any short link\n"
            "• Full base64 / atob support\n"
            "• High burst rotation\n\n"
            "📌 Send any link → choose cycles → get unique numbers",
            reply_markup=main_keyboard()
        )
    elif status == "rejected":
        bot.send_message(message.chat.id, "🚫 *Access Denied*")
    else:
        bot.send_message(message.chat.id, "⏳ *Pending Approval*\nAdmin has been notified.")
        notify_admins_new_user(user.id, user.username, user.first_name)

@bot.message_handler(commands=["admin"])
def cmd_admin(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ Denied")
        return
    bot.send_message(message.chat.id, "🔐 *Admin Panel*", reply_markup=admin_keyboard())

@bot.callback_query_handler(func=lambda c: c.data.startswith(("approve:", "reject:")))
def cb_approval(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "No")
        return
    action, uid_str = call.data.split(":", 1)
    try:
        tid = int(uid_str)
    except Exception:
        bot.answer_callback_query(call.id, "Bad ID")
        return
    if action == "approve":
        if set_user_status(tid, "approved", call.from_user.id):
            bot.answer_callback_query(call.id, "Approved")
            try:
                bot.edit_message_text(
                    call.message.text + f"\n\n✅ Approved by `{call.from_user.id}`",
                    call.message.chat.id, call.message.message_id
                )
            except Exception:
                pass
            try:
                bot.send_message(tid, "✅ *Approved!* Send /start", reply_markup=main_keyboard())
            except Exception:
                pass
        else:
            bot.answer_callback_query(call.id, "Not found")
    else:
        if set_user_status(tid, "rejected", call.from_user.id):
            bot.answer_callback_query(call.id, "Rejected")
            try:
                bot.edit_message_text(
                    call.message.text + f"\n\n❌ Rejected by `{call.from_user.id}`",
                    call.message.chat.id, call.message.message_id
                )
            except Exception:
                pass
            try:
                bot.send_message(tid, "🚫 Rejected.")
            except Exception:
                pass
        else:
            bot.answer_callback_query(call.id, "Not found")

@bot.message_handler(func=lambda m: True)
def handle(message):
    global MAINTENANCE_MODE
    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()
    status = register_user(user.id, user.username, user.first_name)

    if user.id not in ADMIN_IDS:
        if status == "pending":
            bot.send_message(chat_id, "⏳ Still pending.")
            return
        if status == "rejected":
            bot.send_message(chat_id, "🚫 Denied.")
            return
        if status != "approved":
            bot.send_message(chat_id, "⏳ Not approved.")
            return

    if MAINTENANCE_MODE and user.id not in ADMIN_IDS:
        bot.send_message(chat_id, "🔧 Maintenance")
        return

    state = user_states.get(user.id, {})

    if text in ("❌ Cancel", "🔙 Main Menu", "/cancel"):
        if active_jobs.get(user.id):
            active_jobs[user.id] = False
        user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 Menu", reply_markup=main_keyboard())
        return

    if user.id in ADMIN_IDS and state.get("awaiting_broadcast"):
        user_states[user.id] = {}
        users = get_all_user_ids("approved")
        sent = failed = 0
        msg = bot.send_message(chat_id, f"Broadcasting to {len(users)}...")
        for uid in users:
            try:
                bot.send_message(uid, text)
                sent += 1
                time.sleep(0.04)
            except Exception:
                failed += 1
        try:
            bot.edit_message_text(f"✅ Sent: `{sent}` | Failed: `{failed}`", chat_id, msg.message_id, parse_mode="Markdown")
        except Exception:
            pass
        bot.send_message(chat_id, "Admin:", reply_markup=admin_keyboard())
        return

    if user.id in ADMIN_IDS:
        if text == "📈 Bot Stats":
            s = get_admin_stats()
            bot.send_message(
                chat_id,
                f"📈 *Stats*\nTotal: `{s.get('total_users') or 0}`\n"
                f"✅ `{s.get('approved') or 0}` | ⏳ `{s.get('pending') or 0}` | 🚫 `{s.get('rejected') or 0}`\n"
                f"🔄 `{s.get('total_ex') or 0}` | 📱 `{s.get('total_nums') or 0}`",
                reply_markup=admin_keyboard()
            )
            return
        if text == "📢 Broadcast":
            user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(chat_id, "Type broadcast message:")
            return
        if text == "👥 Pending Users":
            pending = get_pending_users(15)
            if not pending:
                bot.send_message(chat_id, "No pending.", reply_markup=admin_keyboard())
                return
            for p in pending:
                name = (p.get("first_name") or "N/A").replace("*", "").replace("_", "")
                uname = (p.get("username") or "none").replace("_", "\\_")
                bot.send_message(chat_id, f"⏳ {name} (`{p['user_id']}`)\n@{uname}", reply_markup=approval_keyboard(p["user_id"]))
            bot.send_message(chat_id, "Admin:", reply_markup=admin_keyboard())
            return
        if text == "👤 Recent Users":
            conn = get_conn()
            try:
                rows = conn.execute(
                    "SELECT user_id,first_name,username,status,total_numbers_found FROM users ORDER BY joined_at DESC LIMIT 12"
                ).fetchall()
            finally:
                conn.close()
            if not rows:
                bot.send_message(chat_id, "None", reply_markup=admin_keyboard())
                return
            msg = "👤 *Recent*\n"
            for i, r in enumerate(rows, 1):
                name = (r["first_name"] or "N/A").replace("*", "").replace("_", "")
                uname = (r["username"] or "none").replace("_", "\\_")
                msg += f"#{i} {name} (`{r['user_id']}`)\n📱 {r['total_numbers_found']} | {r['status']} | @{uname}\n\n"
            bot.send_message(chat_id, msg, reply_markup=admin_keyboard())
            return
        if text in ("🟢 Maintenance: OFF", "🔴 Maintenance: ON"):
            MAINTENANCE_MODE = not MAINTENANCE_MODE
            bot.send_message(chat_id, f"Maintenance: {'🔴 ON' if MAINTENANCE_MODE else '🟢 OFF'}", reply_markup=admin_keyboard())
            return

    if text == "🔗 Send New Link":
        if active_jobs.get(user.id):
            bot.send_message(chat_id, "Already running. Cancel first.")
            return
        user_states[user.id] = {"awaiting_url": True}
        bot.send_message(chat_id, "🔗 Paste link:", reply_markup=types.ReplyKeyboardRemove())
        return
    if text == "📊 My Stats":
        s = get_user_stats(user.id)
        if not s:
            bot.send_message(chat_id, "No stats", reply_markup=main_keyboard())
            return
        bot.send_message(
            chat_id,
            f"📊 *Stats*\nID: `{user.id}`\n🔄 `{s.get('total_extractions', 0)}`\n📱 `{s.get('total_numbers_found', 0)}`\n📅 `{s.get('joined_at', 'N/A')}`",
            reply_markup=main_keyboard()
        )
        return
    if text == "📋 My History":
        hist = get_user_history(user.id, 10)
        if not hist:
            bot.send_message(chat_id, "No history", reply_markup=main_keyboard())
            return
        msg = "📋 *History*\n"
        for i, h in enumerate(hist, 1):
            ud = (h["url"][:32] + "...") if len(h["url"]) > 32 else h["url"]
            msg += f"#{i} `{str(h.get('completed_at', 'N/A'))[:16]}`\n🔗 `{ud}`\n🔄 {h['cycles']} | 📱 {h['unique_numbers']} | 🔁 {h['duplicate_count']}\n\n"
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return
    if text == "❓ Help":
        bot.send_message(
            chat_id,
            "❓ Send any link → choose cycles → get unique WhatsApp numbers.\nWorks on cashwingo, cloaknews, base64, any redirect.",
            reply_markup=main_keyboard()
        )
        return
    if text == "📞 Support":
        bot.send_message(chat_id, "📞 Contact Admin", reply_markup=main_keyboard())
        return

    if "url" in state:
        count = _COUNT_MAP.get(text)
        if count is None and re.match(r"^\d+$", text):
            val = int(text)
            if 1 <= val <= 5000:
                count = val
            else:
                bot.send_message(chat_id, "1 to 5000 only")
                return
        if count is not None:
            target = state["url"]
            init = state.get("initial_numbers", set())
            user_states[user.id] = {}
            start = bot.send_message(
                chat_id,
                f"🚀 Starting...\n🔗 `{target[:40]}`\n🔄 `{count}` cycles (`{count * BURST_PER_CYCLE}` hits)",
                reply_markup=main_keyboard()
            )
            threading.Thread(
                target=extraction_worker,
                args=(chat_id, user.id, target, count, start.message_id, init),
                daemon=True
            ).start()
            return

    norm = normalize_input_url(text)
    if norm or state.get("awaiting_url"):
        if not norm:
            bot.send_message(chat_id, "Invalid URL")
            return
        msg = bot.send_message(chat_id, f"🔍 Analyzing...\n`{norm[:80]}`")
        try:
            info = analyze_url(norm)
        except Exception as e:
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
            bot.send_message(chat_id, f"Error: `{str(e)[:80]}`", reply_markup=main_keyboard())
            return
        chain = "\n".join(f"  • `{c}`" for c in info["chain"][:8]) or "  • Direct"
        init = info["initial_numbers"]
        note = f"\n🔄 Mode: `{info['note']}`" if info.get("note") else ""
        preview = f"\n🎯 First: `+{next(iter(init))}`" if init else ""
        user_states[user.id] = {"url": norm, "initial_numbers": init}
        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        bot.send_message(
            chat_id,
            f"✅ *Analysis*\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 Reachable: `{'Yes' if info['reachable'] else 'No'}`\n"
            f"🛡️ `{info['protection']}`{note}\n"
            f"📱 Instant: `{len(init)}`{preview}\n"
            f"🔗 Chain:\n{chain}\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Cycles? Button or type 1–5000:",
            reply_markup=extraction_keyboard()
        )
        return

    bot.send_message(chat_id, "Choose menu or send link", reply_markup=main_keyboard())

# =========================================================
# Start
# =========================================================
if __name__ == "__main__":
    init_db()
    threading.Thread(target=start_health_server, daemon=True).start()
    print(f"Bot starting (curl_cffi={HAS_CFFI})")
    try:
        bot.remove_webhook()
        time.sleep(1)
    except Exception as e:
        print(f"Webhook: {e}")
    print("Polling...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"Poll error: {e}")
            time.sleep(4)
