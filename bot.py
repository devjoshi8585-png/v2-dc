import os, sys, io, json, random, hashlib, logging, re, asyncio, base64, datetime
import xml.etree.ElementTree as ET
import difflib
from collections import deque
from urllib.parse import quote_plus

import aiohttp
import discord
from discord.ext import commands, tasks

try:
    from PIL import Image
except Exception:
    Image = None

# ── Keep-alive server ─────────────────────────────────────────────────────────

async def _keep_alive_server():
    async def _handle(reader, writer):
        try:
            await reader.read(2048)
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 13\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                b"Bot is alive!"
            )
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    port = int(os.environ.get("PORT", 10000))
    server = await asyncio.start_server(_handle, "0.0.0.0", port)
    logger.info(f"Keep-alive server listening on port {port}")
    async with server:
        await server.serve_forever()

# ── Environment variables ──────────────────────────────────────────────────────

NSFW_MODE = True

TOKEN              = os.getenv("TOKEN", "")
WAIFUIM_API_KEY    = os.getenv("WAIFUIM_API_KEY", "")
DANBOORU_USER      = os.getenv("DANBOORU_USER", "")
DANBOORU_API_KEY   = os.getenv("DANBOORU_API_KEY", "")
GELBOORU_API_KEY   = os.getenv("GELBOORU_API_KEY", "")
GELBOORU_USER      = os.getenv("GELBOORU_USER", "")
E621_USER          = os.getenv("E621_USER", "")
E621_API_KEY       = os.getenv("E621_API_KEY", "")
WAIFU_IT_API_KEY   = os.getenv("WAIFU_IT_API_KEY", "")
BOT_PERSONA        = os.getenv("BOT_PERSONA_NAME", "Yuki")

DEBUG_FETCH            = str(os.getenv("DEBUG_FETCH", "")).strip().lower() in ("1","true","yes","on")
TRUE_RANDOM            = str(os.getenv("TRUE_RANDOM", "")).strip().lower() in ("1","true","yes")
REQUEST_TIMEOUT        = int(os.getenv("REQUEST_TIMEOUT", "20"))        # ↑ 14→20 (Railway has better network)
DISCORD_MAX_UPLOAD     = int(os.getenv("DISCORD_MAX_UPLOAD", str(8 * 1024 * 1024)))
HEAD_SIZE_LIMIT        = DISCORD_MAX_UPLOAD
DATA_FILE              = os.getenv("DATA_FILE", "data_nsfw.json")
AUTOSAVE_INTERVAL      = int(os.getenv("AUTOSAVE_INTERVAL", "60"))      # ↑ 120→60 (Railway is always on)
FETCH_ATTEMPTS         = int(os.getenv("FETCH_ATTEMPTS", "40"))
MAX_USED_GIFS_PER_USER = int(os.getenv("MAX_USED_GIFS_PER_USER", "5000")) # ↑ 1000→5000 (more RAM)

VC_CHANNEL_ID = int(os.getenv("VC_CHANNEL_ID", "0"))
_VC_IDS_RAW   = os.getenv("VC_IDS", "")
VC_IDS        = [int(x.strip()) for x in _VC_IDS_RAW.split(",") if x.strip().isdigit()] if _VC_IDS_RAW.strip() else []

COMMAND_CHANNEL_ID = int(os.getenv("COMMAND_CHANNEL_ID", "0"))

# ── Hardcoded log channel ──────────────────────────────────────────────────────
LOG_CHANNEL_ID = 1476889446728204333

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG if DEBUG_FETCH else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("waifu-bot")

if not VC_IDS:
    logger.warning("[VC] VC_IDS env var not set — voice channel features disabled.")
if not VC_CHANNEL_ID:
    logger.warning("[VC] VC_CHANNEL_ID env var not set — text channel messages disabled.")
if not COMMAND_CHANNEL_ID:
    logger.warning("[CMD] COMMAND_CHANNEL_ID env var not set — neko command filter disabled.")

# ── Uptime tracking ────────────────────────────────────────────────────────────
_bot_start_time: datetime.datetime = None
_total_images_sent: int = 0          # running counter across the session

# ── Global deduplication rolling window ───────────────────────────────────────
# Prevents ANY image from being sent twice globally (rolling 3000-hash window)
_global_sent_hashes: deque = deque(maxlen=3000)

# ── Data persistence ───────────────────────────────────────────────────────────

if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        json.dump({
            "sent_history": {}, "vc_state": {},
            "greeted_users": [], "visit_count": {},
            "last_daily_open": "", "user_tiers": {},
            "streaks": {}, "last_visit_dates": {},
        }, f, indent=2)

with open(DATA_FILE, "r") as f:
    _raw = json.load(f)

data = {
    "sent_history": {
        uid: deque(h, maxlen=MAX_USED_GIFS_PER_USER)
        for uid, h in _raw.get("sent_history", {}).items()
    },
    "vc_state":         _raw.get("vc_state", {}),
    "greeted_users":    _raw.get("greeted_users", []),
    "visit_count":      _raw.get("visit_count", {}),
    "last_daily_open":  _raw.get("last_daily_open", ""),
    "user_tiers":       _raw.get("user_tiers", {}),
    "streaks":          _raw.get("streaks", {}),
    "last_visit_dates": _raw.get("last_visit_dates", {}),
}

def _write_json(payload):
    with open(DATA_FILE, "w") as f:
        json.dump(payload, f, indent=2)

async def save_data():
    try:
        serializable = {
            "sent_history":    {uid: list(h) for uid, h in data["sent_history"].items()},
            "vc_state":        data["vc_state"],
            "greeted_users":   data["greeted_users"],
            "visit_count":     data["visit_count"],
            "last_daily_open": data["last_daily_open"],
            "user_tiers":      data["user_tiers"],
            "streaks":         data["streaks"],
            "last_visit_dates":data["last_visit_dates"],
        }
        await asyncio.to_thread(_write_json, serializable)
    except Exception as e:
        logger.warning(f"Save failed: {e}")

# ── Image utilities ────────────────────────────────────────────────────────────

async def _download_bytes(session, url, size_limit=HEAD_SIZE_LIMIT, timeout=REQUEST_TIMEOUT):
    try:
        to = aiohttp.ClientTimeout(total=timeout)
        hdrs = {"Referer": "https://www.pixiv.net/", "User-Agent": "WaifuBot/2.0"}
        async with session.get(url, timeout=to, allow_redirects=True, headers=hdrs) as resp:
            if resp.status != 200:
                return None, None
            ctype = resp.content_type or ""
            total, chunks = 0, []
            async for chunk in resp.content.iter_chunked(4096):   # ↑ 1024→4096 (faster on Railway)
                chunks.append(chunk)
                total += len(chunk)
                if total > size_limit:
                    return None, ctype
            return b"".join(chunks), ctype
    except Exception:
        return None, None

def _compress_image_sync(image_bytes, target_size):
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.format == "GIF":
            return image_bytes
        output = io.BytesIO()
        quality = 95
        while quality > 10:
            output.seek(0)
            output.truncate()
            img.save(output, format=img.format or "JPEG", quality=quality, optimize=True)
            if output.tell() <= target_size:
                return output.getvalue()
            quality -= 10
        return output.getvalue()
    except Exception:
        return image_bytes

async def compress_image(image_bytes, target_size=DISCORD_MAX_UPLOAD):
    if not Image:
        return image_bytes
    return await asyncio.to_thread(_compress_image_sync, image_bytes, target_size)

# ── API provider tag lists ─────────────────────────────────────────────────────

SPICY_TAGS = [
    "ahegao", "creampie", "cum_inside", "gangbang", "double_penetration",
    "deepthroat", "paizuri", "titfuck", "throatfuck", "facesitting",
    "doggy_style", "missionary", "squirting", "bondage", "bdsm",
    "tentacles", "orgasm", "riding", "thighjob", "cumshot", "blowjob",
    "anal", "pussy", "hardcore", "futanari", "public", "group",
    "nude", "naked", "sex", "handjob", "footjob", "femdom",
    "harem", "milf", "big_breasts", "large_breasts", "busty",
    "ass", "buttjob", "spanking", "hypnosis", "mind_break",
    "pov", "solo_female", "multiple_boys", "cum_on_face",
    "spread_legs", "cum_on_body", "breast_grab", "nipples",
]

GIF_TAGS = [
    "hentai", "sex", "blowjob", "anal", "creampie", "cumshot", "ahegao",
    "paizuri", "gangbang", "deepthroat", "tentacles", "futanari", "orgasm",
    "squirt", "bondage", "milf", "oppai", "pussy", "hardcore", "animated",
    "nude", "naked", "big_breasts", "femdom", "pov", "ass", "busty",
    "nipples", "spread_legs", "riding",
]

# ── API provider functions ─────────────────────────────────────────────────────

async def _gelbooru_compat(session, base_url, api_key=None, user_id=None, extra_tags=None):
    """Gelbooru-compatible booru endpoint — fully unrestricted."""
    try:
        tags = ["rating:explicit"]
        if extra_tags:
            tags.extend(extra_tags)
        params = {
            "page": "dapi", "s": "post", "q": "index",
            "json": "1", "tags": " ".join(tags), "limit": 100,
        }
        if api_key and user_id:
            params["api_key"] = api_key
            params["user_id"] = user_id
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(base_url, params=params, headers=hdrs, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            posts = payload if isinstance(payload, list) else payload.get("post", [])
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or gif_url.lower().endswith((".webm", ".mp4", ".swf")):
                return None, None, None
            return gif_url, base_url, post
    except Exception:
        return None, None, None

async def fetch_rule34(session, positive=None):
    """Rule34 — fully unrestricted, no key needed."""
    try:
        params = {
            "page": "dapi", "s": "post", "q": "index",
            "json": "1", "tags": "rating:explicit", "limit": 200,
        }
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://api.rule34.xxx/index.php", params=params, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            posts = await resp.json(content_type=None)
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or gif_url.lower().endswith((".webm", ".mp4", ".swf")):
                return None, None, None
            return gif_url, "rule34", post
    except Exception:
        return None, None, None

async def fetch_gelbooru(session, positive=None):
    """Gelbooru — fully unrestricted. Optional: set GELBOORU_API_KEY + GELBOORU_USER."""
    url, _, post = await _gelbooru_compat(
        session, "https://gelbooru.com/index.php",
        GELBOORU_API_KEY or None, GELBOORU_USER or None
    )
    return url, "gelbooru", post

async def fetch_nekosapi(session, positive=None):
    """NekosAPI v4 — free, no key, fully unrestricted explicit."""
    try:
        params = {"rating": "explicit", "limit": 20}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://api.nekosapi.com/v4/images/random", params=params, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            images = payload.get("items", [])
            if not images: return None, None, None
            img = random.choice(images)
            return img.get("url"), "nekosapi", img
    except Exception:
        return None, None, None

async def fetch_konachan(session, positive=None):
    """Konachan — free, no key, fully unrestricted explicit."""
    try:
        params = {"tags": "rating:explicit", "limit": 100}
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://konachan.com/post.json", params=params, headers=hdrs, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            posts = await resp.json(content_type=None)
            if not posts: return None, None, None
            image_posts = [
                p for p in posts
                if p.get("file_url") and not p.get("file_url", "").lower().endswith((".webm", ".mp4"))
            ]
            if not image_posts: return None, None, None
            post = random.choice(image_posts)
            return post.get("file_url"), "konachan", post
    except Exception:
        return None, None, None

async def fetch_nekobot(session, positive=None):
    """Nekobot — free, no key, all NSFW categories."""
    try:
        category = random.choice([
            "hentai", "hentai_anal", "hass", "hboobs", "hthigh",
            "paizuri", "tentacle", "pgif", "pussy", "hkuni",
            "hanal", "hfeet", "hbdsm", "hfutanari", "hmilf",
            "hnude", "hpov", "hcreampie", "hgangbang",
        ])
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(f"https://nekobot.xyz/api/image?type={category}", timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            if not payload.get("success"): return None, None, None
            return payload.get("message"), f"nekobot_{category}", payload
    except Exception:
        return None, None, None

async def fetch_danbooru(session, positive=None):
    """Danbooru — optional key (DANBOORU_USER + DANBOORU_API_KEY), fully unrestricted."""
    try:
        params = {"tags": "rating:explicit", "limit": 100, "random": "true"}
        auth = aiohttp.BasicAuth(DANBOORU_USER, DANBOORU_API_KEY) if (DANBOORU_USER and DANBOORU_API_KEY) else None
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://danbooru.donmai.us/posts.json", params=params, auth=auth, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            posts = await resp.json(content_type=None)
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url") or post.get("large_file_url")
            if not gif_url or gif_url.lower().endswith((".webm", ".mp4", ".swf")):
                return None, None, None
            return gif_url, "danbooru", post
    except Exception:
        return None, None, None

async def fetch_nekos_life(session, positive=None):
    """Nekos.life — free, no key, all known NSFW categories."""
    try:
        category = random.choice([
            "blowjob", "cum", "hentai", "classical", "ero", "spank",
            "lewd", "feet", "solo", "yuri", "trap", "futanari",
            "hololewd", "lewdk", "nekolewd", "pwankg", "feetg",
            "bj", "holoero", "pussy", "tits", "anal", "bdsm",
            "creampie", "gangbang",
        ])
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(f"https://nekos.life/api/v2/img/{category}", timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            return payload.get("url"), f"nekos_life_{category}", payload
    except Exception:
        return None, None, None

async def fetch_tbib(session, positive=None):
    """TBIB — free, no key, gelbooru-compatible."""
    url, _, post = await _gelbooru_compat(session, "https://tbib.org/index.php")
    return url, "tbib", post

async def fetch_xbooru(session, positive=None):
    """Xbooru — free, no key, gelbooru-compatible."""
    url, _, post = await _gelbooru_compat(session, "https://xbooru.com/index.php")
    return url, "xbooru", post

async def fetch_realbooru(session, positive=None):
    """Realbooru — free, no key, gelbooru-compatible."""
    url, _, post = await _gelbooru_compat(session, "https://realbooru.com/index.php")
    return url, "realbooru", post

async def fetch_waifu_im(session, positive=None):
    """Waifu.im — optional key (WAIFUIM_API_KEY), fully unrestricted NSFW."""
    try:
        params = {"is_nsfw": "true", "limit": 30}
        headers = {}
        if WAIFUIM_API_KEY:
            headers["Authorization"] = f"Bearer {WAIFUIM_API_KEY}"
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://api.waifu.im/search", params=params, headers=headers or None, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            images = payload.get("images", [])
            if not images: return None, None, None
            img = random.choice(images)
            return img.get("url"), "waifu_im", img
    except Exception:
        return None, None, None

async def fetch_paheal(session, positive=None):
    """Paheal — free, no key, fully unrestricted explicit."""
    try:
        params = {"tags": "rating:explicit", "limit": 100}
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(
            "https://rule34.paheal.net/api/danbooru/find_posts/index.xml",
            params=params, headers=hdrs, timeout=to
        ) as resp:
            if resp.status != 200: return None, None, None
            text = await resp.text()
            root = ET.fromstring(text)
            posts = root.findall(".//post")
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or gif_url.lower().endswith((".webm", ".mp4", ".swf", ".flv")):
                return None, None, None
            return gif_url, "paheal", {"file_url": gif_url}
    except Exception:
        return None, None, None

async def fetch_waifu_it(session, positive=None):
    """Waifu.it — REQUIRES WAIFU_IT_API_KEY env var, all NSFW categories."""
    if not WAIFU_IT_API_KEY: return None, None, None
    try:
        category = random.choice([
            "creampie", "thighjob", "ero", "paizuri", "oppai", "anal",
            "blowjob", "hentai", "ass", "bdsm", "cum", "feet",
            "femdom", "futanari", "gangbang", "group", "handjob",
            "hardcore", "lewd", "milf", "naked", "nude", "pussy",
            "riding", "sex", "solo", "tentacle", "uniform",
            "yaoi", "yuri",
        ])
        hdrs = {"Authorization": WAIFU_IT_API_KEY}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(f"https://waifu.it/api/v4/{category}", headers=hdrs, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            return payload.get("url"), f"waifu_it_{category}", payload
    except Exception:
        return None, None, None

async def fetch_nekos_moe(session, positive=None):
    """Nekos.moe — free, no key."""
    try:
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(
            "https://nekos.moe/api/v1/random/image?nsfw=true&count=1",
            headers=hdrs, timeout=to
        ) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            images = payload.get("images", [])
            if not images: return None, None, None
            img = random.choice(images)
            img_id = img.get("id")
            if not img_id: return None, None, None
            return f"https://nekos.moe/image/{img_id}.jpg", "nekos_moe", img
    except Exception:
        return None, None, None

async def fetch_waifu_pics(session, positive=None):
    """Waifu.pics — free, no key, all NSFW categories."""
    try:
        category = random.choice(["waifu", "neko", "trap", "blowjob"])
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(f"https://api.waifu.pics/nsfw/{category}", timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            return payload.get("url") or payload.get("image"), f"waifu_pics_{category}", payload
    except Exception:
        return None, None, None

async def fetch_e621(session, positive=None):
    """e621 — optional key (E621_USER + E621_API_KEY), fully unrestricted explicit."""
    try:
        params = {"tags": "rating:explicit order:random", "limit": 100}
        hdrs = {"User-Agent": "WaifuBot/2.0 (by discord_bot_operator on e621)"}
        auth = aiohttp.BasicAuth(E621_USER, E621_API_KEY) if E621_USER and E621_API_KEY else None
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://e621.net/posts.json", params=params, headers=hdrs, auth=auth, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            posts = payload.get("posts", [])
            if not posts: return None, None, None
            image_posts = [
                p for p in posts
                if p.get("file", {}).get("ext") in ("jpg", "jpeg", "png", "gif", "webp")
            ]
            if not image_posts: return None, None, None
            post = random.choice(image_posts)
            gif_url = post.get("file", {}).get("url")
            if not gif_url: return None, None, None
            return gif_url, "e621", post
    except Exception:
        return None, None, None

async def fetch_yandere(session, positive=None):
    """Yande.re — free, no key, fully unrestricted explicit."""
    try:
        params = {"tags": "rating:explicit order:random", "limit": 100}
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://yande.re/post.json", params=params, headers=hdrs, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            posts = await resp.json(content_type=None)
            if not posts: return None, None, None
            image_posts = [
                p for p in posts
                if p.get("file_url") and not p.get("file_url", "").lower().endswith((".webm", ".mp4"))
            ]
            if not image_posts: return None, None, None
            post = random.choice(image_posts)
            return post.get("file_url"), "yandere", post
    except Exception:
        return None, None, None

async def fetch_hypnohub(session, positive=None):
    """Hypnohub — free, no key, gelbooru-compatible."""
    url, _, post = await _gelbooru_compat(session, "https://hypnohub.net/index.php")
    return url, "hypnohub", post

async def fetch_hmtai(session, positive=None):
    """hmtai.hatsunemiku.club — free, no key, large NSFW category list."""
    try:
        category = random.choice([
            "blowjob", "cum", "hentai", "anal", "ero", "paizuri",
            "tentacles", "pussy", "ahegao", "ass", "pgif", "boobs",
            "creampie", "nsfwNeko", "solo", "bdsm", "femdom",
            "gangbang", "yuri", "uniform", "hass", "hboobs",
        ])
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(
            f"https://hmtai.hatsunemiku.club/nsfw/{category}", timeout=to
        ) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            url = payload.get("url")
            if not url: return None, None, None
            return url, f"hmtai_{category}", payload
    except Exception:
        return None, None, None

async def fetch_lolicon(session, positive=None):
    """Lolicon API — free, no key needed, POST endpoint, returns hentai images."""
    try:
        body = {"r18": 1, "num": 5, "size": ["original", "regular"]}
        to   = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.post(
            "https://api.lolicon.app/setu/v2", json=body, timeout=to
        ) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            items = payload.get("data", [])
            if not items: return None, None, None
            item  = random.choice(items)
            urls  = item.get("urls", {})
            url   = urls.get("original") or urls.get("regular") or urls.get("small")
            if not url: return None, None, None
            return url, "lolicon", item
    except Exception:
        return None, None, None

async def fetch_lolibooru(session, positive=None):
    """Lolibooru — free, no key, gelbooru-compatible booru."""
    url, _, post = await _gelbooru_compat(session, "https://lolibooru.moe/index.php")
    return url, "lolibooru", post

async def fetch_allthefallen(session, positive=None):
    """All The Fallen booru — free, no key, gelbooru-compatible."""
    url, _, post = await _gelbooru_compat(session, "https://booru.allthefallen.moe/index.php")
    return url, "allthefallen", post

async def fetch_safebooru_r(session, positive=None):
    """Safebooru.org (rule34 variant) — free, no key, gelbooru-compatible."""
    url, _, post = await _gelbooru_compat(session, "https://safebooru.org/index.php")
    return url, "safebooru_r", post

async def fetch_rule34us(session, positive=None):
    """Rule34.us — free, no key, gelbooru-compatible, separate pool from rule34.xxx."""
    url, _, post = await _gelbooru_compat(session, "https://rule34.us/index.php")
    return url, "rule34us", post

# ── NEW providers added for Railway (more RAM/CPU to handle larger pools) ──────

async def fetch_behoimi(session, positive=None):
    """Behoimi — free, no key, Moebooru-compatible."""
    try:
        params = {"tags": "rating:explicit", "limit": 100}
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://behoimi.org/post.json", params=params, headers=hdrs, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            posts = await resp.json(content_type=None)
            if not posts: return None, None, None
            image_posts = [
                p for p in posts
                if p.get("file_url") and not p.get("file_url", "").lower().endswith((".webm", ".mp4", ".swf"))
            ]
            if not image_posts: return None, None, None
            post = random.choice(image_posts)
            return post.get("file_url"), "behoimi", post
    except Exception:
        return None, None, None

async def fetch_rule34_paheal_popular(session, positive=None):
    """Paheal popular posts — different pool from random paheal."""
    try:
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(
            "https://rule34.paheal.net/api/danbooru/find_posts/index.xml?tags=score%3Agt%3A10+rating%3Aexplicit&limit=100",
            headers=hdrs, timeout=to
        ) as resp:
            if resp.status != 200: return None, None, None
            text = await resp.text()
            root = ET.fromstring(text)
            posts = root.findall(".//post")
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or gif_url.lower().endswith((".webm", ".mp4", ".swf", ".flv")):
                return None, None, None
            return gif_url, "paheal_popular", {"file_url": gif_url}
    except Exception:
        return None, None, None

async def fetch_gelbooru_popular(session, positive=None):
    """Gelbooru sorted by score — higher quality pool."""
    try:
        tags = "rating:explicit sort:score:desc"
        params = {
            "page": "dapi", "s": "post", "q": "index",
            "json": "1", "tags": tags, "limit": 100,
        }
        if GELBOORU_API_KEY and GELBOORU_USER:
            params["api_key"] = GELBOORU_API_KEY
            params["user_id"] = GELBOORU_USER
        hdrs = {"User-Agent": "WaifuBot/2.0"}
        to = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get("https://gelbooru.com/index.php", params=params, headers=hdrs, timeout=to) as resp:
            if resp.status != 200: return None, None, None
            payload = await resp.json(content_type=None)
            posts = payload if isinstance(payload, list) else payload.get("post", [])
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or gif_url.lower().endswith((".webm", ".mp4", ".swf")):
                return None, None, None
            return gif_url, "gelbooru_popular", post
    except Exception:
        return None, None, None

# ── Provider list & health tracking ───────────────────────────────────────────

_BASE_PROVIDERS = [
    ("rule34",             fetch_rule34,              45),
    ("gelbooru",           fetch_gelbooru,             18),
    ("gelbooru_popular",   fetch_gelbooru_popular,     14),  # NEW
    ("nekosapi",           fetch_nekosapi,             15),
    ("konachan",           fetch_konachan,             12),
    ("hmtai",              fetch_hmtai,                12),
    ("lolicon",            fetch_lolicon,              12),
    ("nekobot",            fetch_nekobot,              10),
    ("danbooru",           fetch_danbooru,              8),
    ("nekos_life",         fetch_nekos_life,            8),
    ("tbib",               fetch_tbib,                  7),
    ("xbooru",             fetch_xbooru,                7),
    ("realbooru",          fetch_realbooru,             6),
    ("lolibooru",          fetch_lolibooru,             6),
    ("allthefallen",       fetch_allthefallen,          5),
    ("waifu_im",           fetch_waifu_im,              5),
    ("paheal",             fetch_paheal,                5),
    ("paheal_popular",     fetch_rule34_paheal_popular, 4),  # NEW
    ("rule34us",           fetch_rule34us,              5),
    ("waifu_it",           fetch_waifu_it,              4),
    ("nekos_moe",          fetch_nekos_moe,             3),
    ("waifu_pics",         fetch_waifu_pics,            2),
    ("safebooru_r",        fetch_safebooru_r,           2),
    ("behoimi",            fetch_behoimi,               4),  # NEW
]

_NSFW_EXTRA_PROVIDERS = [
    ("e621",     fetch_e621,     18),
    ("yandere",  fetch_yandere,  14),
    ("hypnohub", fetch_hypnohub,  6),
]

PROVIDERS = _BASE_PROVIDERS + _NSFW_EXTRA_PROVIDERS

_provider_failures  = {}
_provider_last_used = {}
_PROVIDER_RATE_GAPS  = {"danbooru": 1.0, "e621": 1.0, "gelbooru": 0.5, "gelbooru_popular": 0.5}
_PROVIDER_FAIL_LIMIT = 12   # ↑ 10→12 (Railway is stable, give providers more chances)

def _hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def _choose_provider():
    if TRUE_RANDOM:
        return random.choice(PROVIDERS)
    eligible = [
        (n, f, w) for n, f, w in PROVIDERS
        if _provider_failures.get(n, 0) < _PROVIDER_FAIL_LIMIT
    ]
    if not eligible:
        _provider_failures.clear()
        eligible = PROVIDERS
    weights = [w for _, _, w in eligible]
    return random.choices(eligible, weights=weights, k=1)[0]

async def _fetch_one(session, used_hashes=None):
    if used_hashes is None:
        used_hashes = set()
    name, fetch_func, _ = _choose_provider()
    gap = _PROVIDER_RATE_GAPS.get(name, 0)
    if gap:
        last = _provider_last_used.get(name, 0)
        now  = asyncio.get_event_loop().time()
        wait = gap - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)
    _provider_last_used[name] = asyncio.get_event_loop().time()
    try:
        url, source, meta = await fetch_func(session)
        if url:
            h = _hash_url(url)
            # Check both per-user history AND global dedup window
            if h not in used_hashes and h not in _global_sent_hashes:
                _provider_failures[name] = 0
                return url, source, meta, h
        _provider_failures[name] = _provider_failures.get(name, 0) + 1
        if _provider_failures[name] == _PROVIDER_FAIL_LIMIT:
            asyncio.ensure_future(_log(
                f"⚠️ **Provider `{name}` hit fail limit ({_PROVIDER_FAIL_LIMIT})** — temporarily removed from pool."
            ))
    except Exception:
        _provider_failures[name] = _provider_failures.get(name, 0) + 1
    return None, None, None, None

async def fetch_gif(session, user_id=None):
    global _total_images_sent
    uid     = str(user_id) if user_id else "global"
    history = data["sent_history"].setdefault(uid, deque(maxlen=MAX_USED_GIFS_PER_USER))
    used    = set(history)
    rounds  = max(1, FETCH_ATTEMPTS // 3)
    for _ in range(rounds):
        # ↑ Parallel batch of 5 (was 3) — Railway has more CPU
        tasks_list = [_fetch_one(session, used) for _ in range(5)]
        results    = await asyncio.gather(*tasks_list, return_exceptions=True)
        for res in results:
            if isinstance(res, tuple) and res[0]:
                url, source, meta, url_hash = res
                history.append(url_hash)
                data["sent_history"][uid] = history
                _global_sent_hashes.append(url_hash)  # register globally
                _total_images_sent += 1
                return url, source, meta
    return None, None, None

# ── Streak tracking ────────────────────────────────────────────────────────────

def _update_streak(uid: str) -> int:
    """Update the consecutive-day visit streak for a user. Returns current streak."""
    today     = datetime.date.today().isoformat()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    last      = data["last_visit_dates"].get(uid)
    streak    = data["streaks"].get(uid, 0)

    if last == today:
        pass
    elif last == yesterday:
        streak += 1
    else:
        streak = 1

    data["streaks"][uid]          = streak
    data["last_visit_dates"][uid] = today
    return streak

def _streak_badge(streak: int) -> str:
    if streak >= 30: return "👑 30-day streak"
    if streak >= 14: return "💎 14-day streak"
    if streak >= 7:  return "⚡ 7-day streak"
    if streak >= 3:  return "🔥 3-day streak"
    return ""

# ── Holiday detection ──────────────────────────────────────────────────────────

def _get_holiday() -> str:
    today = datetime.date.today()
    mm, dd = today.month, today.day
    if mm == 10 and 25 <= dd <= 31:          return "halloween"
    if mm == 12 and 24 <= dd <= 26:          return "christmas"
    if mm == 1  and dd == 1:                 return "newyear"
    if mm == 2  and dd == 14:                return "valentine"
    if mm == 3  and 20 <= dd <= 31:          return "sakura"
    if mm == 4  and 1  <= dd <= 10:          return "sakura"
    return ""

HOLIDAY_JOINS = {
    "halloween": [
        "🎃 {display_name} rises from the dark — Halloween claimed another soul.",
        "👻 {display_name} arrived through the veil. The spirits parted for them.",
        "🕷️ {display_name} crept in on Halloween night. Dangerous timing.",
        "🦇 {display_name} appeared — the bats announced them first.",
        "🌑 {display_name} joins on the night the dead walk. Perfect.",
        "🕯️ {display_name} steps in — the candles flickered and didn't lie.",
        "🎃 {display_name} arrived. Even the monsters turned to look.",
        "🕸️ {display_name} slid through the web — Halloween approved.",
    ],
    "christmas": [
        "🎄 {display_name} arrives — even winter can't cool this entrance.",
        "❄️ {display_name} stepped in from the cold. The warmth suits them.",
        "🎁 {display_name} joined — the best gift this room has received.",
        "✨ {display_name} arrives glowing — the holidays agree with them.",
        "🌟 {display_name} stepped in under the stars. The season bows.",
        "☃️ {display_name} arrives — the snow stopped to watch.",
        "🎄 {display_name} joins. Merry, dangerous, unforgettable.",
    ],
    "newyear": [
        "🎆 {display_name} enters the new year first — bold move.",
        "🥂 {display_name} arrives — the countdown was for them all along.",
        "✨ {display_name} steps into a new year. The future just got interesting.",
        "🎇 {display_name} joins — the fireworks were unnecessary, honestly.",
        "🌅 {display_name} arrived at the turn of the year. Iconic timing.",
        "🎊 {display_name} steps in — the new year started the moment they arrived.",
    ],
    "valentine": [
        "💌 {display_name} arrives on Valentine's — the room didn't need more tension.",
        "🌹 {display_name} steps in carrying something unspoken. Today of all days.",
        "💋 {display_name} joins on the most dangerous holiday of the year.",
        "🩸 {display_name} arrived — love is complicated. So are they.",
        "🌸 {display_name} enters — February 14th finally makes sense.",
        "💘 {display_name} arrives. Hearts didn't stand a chance.",
    ],
    "sakura": [
        "🌸 {display_name} arrives with the cherry blossoms — fleeting and unforgettable.",
        "🌺 {display_name} steps in during sakura season. Petals followed.",
        "🌸 {display_name} joined — spring arrived a little early.",
        "🌿 {display_name} appears like new growth. The season approves.",
        "🌸 {display_name} enters — soft as petals, sharp underneath.",
        "🌸 {display_name} arrives with the bloom. The air changed.",
    ],
}

# ── Greeting text pools ────────────────────────────────────────────────────────

TIERS = ["shadow", "flame", "frost", "bloom", "storm"]

TIER_LABELS = {
    "shadow": "🌑 Shadow",
    "flame":  "🔥 Flame",
    "frost":  "🧊 Frost",
    "bloom":  "🌸 Bloom",
    "storm":  "⚡ Storm",
}

TIER_FIRST_GREETINGS = {
    "shadow": "🌑 **{display_name}** — the shadow faction claims its own. Welcome to the dark.",
    "flame":  "🔥 **{display_name}** — the flame court rises. Burn bright, burn long.",
    "frost":  "🧊 **{display_name}** — cold, precise, and inevitable. The frost welcomes you.",
    "bloom":  "🌸 **{display_name}** — rare beauty, quiet danger. The bloom recognizes you.",
    "storm":  "⚡ **{display_name}** — the weather just became a warning. Storm faction, rise.",
}

MILESTONE_GREETINGS = [
    "🔥 **{display_name}** is back for visit #{count}. A true regular. The room noticed.",
    "👑 **{display_name}** returns again — #{count}. The throne is yours. Sit.",
    "🖤 **{display_name}** visit #{count}. Some presences become permanent. You're one of them.",
    "⚡ **{display_name}** — #{count} times through these doors. Electric as always.",
    "🌙 **{display_name}** #{count}. The night keeps track even when you don't.",
    "🐍 **{display_name}** visit #{count}. Patient, consistent, inevitable.",
    "💎 **{display_name}** back for #{count}. Rare things keep returning here.",
    "🌌 **{display_name}** #{count}. The void missed you. It always does.",
]

THEMED_JOINS = {
    "midnight": [
        "🌑 {display_name} crept in past midnight — the darkest hours are the most honest.",
        "🕯️ {display_name} arrived while everyone sleeps. The night belongs to you both.",
        "🌙 {display_name} showed up at midnight. Some invitations aren't spoken aloud.",
        "🖤 {display_name} joins the midnight crowd — awake when the world forgets to watch.",
        "🌌 {display_name} drifted in under starless dark. Something about them fits.",
        "🔮 {display_name} steps through at midnight. The hour chooses its people carefully.",
        "🐺 {display_name} arrived in the witching hour. The dark recognised them immediately.",
    ],
    "morning": [
        "☀️ {display_name} is here early — the ambitious ones always are.",
        "🍵 {display_name} arrived with the morning light. First one in, boldest one here.",
        "🌅 {display_name} stepped in at dawn. The day just got more interesting.",
        "☕ {display_name} arrived before the world woke up. Respect.",
        "🌤️ {display_name} joins with the morning — fresh and already dangerous.",
        "🌄 {display_name} showed up at sunrise. That kind of energy sets the tone.",
    ],
    "afternoon": [
        "🌤️ {display_name} arrived — the afternoon shift just got dangerous.",
        "☕ {display_name} showed up midday. Energy high, patience short.",
        "🌞 {display_name} joins at peak hours — the sharpest one in the room.",
        "🍃 {display_name} steps in with the afternoon breeze. Casual but noticed.",
        "🌻 {display_name} arrived in the golden afternoon. Easy timing, hard to forget.",
    ],
    "evening": [
        "🌆 {display_name} arrived as the sun drops. Evening energy hits different.",
        "🍷 {display_name} joined at dusk — every good story starts now.",
        "🌇 {display_name} stepped in with golden hour. The best part of the day just started.",
        "🕯️ {display_name} arrives as the lights go warm. Perfect timing.",
        "🌃 {display_name} joined with the evening crowd. The room shifts gear.",
        "🌆 {display_name} stepped into the blue hour. Everything is softer and sharper at once.",
    ],
}

JOIN_GREETINGS = [
    "💋 {display_name} slips in like a slow caress — the room just warmed up.",
    "🔥 {display_name} arrived, tracing heat across the air; someone hold the temperature.",
    "✨ {display_name} joins — all eyes and soft smiles. Dare to stir trouble?",
    "😈 {display_name} steps through the door with a dangerous smile and a hungry look.",
    "👀 {display_name} appeared — sudden quiet, then the world leans in.",
    "🖤 {display_name} joined, breath shallow, pulse audible — tempting, isn't it?",
    "🌙 {display_name} glides in as if they own the moment — claim it or be claimed.",
    "🕯️ {display_name} arrives wrapped in dusk and whispering promises.",
    "🍷 {display_name} joined — like a warm pour, smooth and slow.",
    "🥀 {display_name} walked in with a smile that asked for trouble.",
    "🕶️ {display_name} stepped in cool, but the air around them is anything but.",
    "💎 {display_name} joined — rare, polished, and distractingly beautiful.",
    "👑 {display_name} arrived; treat them like royalty or lose the crown.",
    "🌫️ {display_name} drifted in; the air tastes sweeter already.",
    "🪞 {display_name} joined — catch their reflection if you dare.",
    "⚡ {display_name} joined and the electricity in the room changed lanes.",
    "🧠 {display_name} arrived with a mind on fire — play smart, play dangerous.",
    "💋 {display_name} slipped in with a grin; the night just leaned forward.",
    "🩸 {display_name} joined — bold, a little wicked, entirely noticed.",
    "🐍 {display_name} slithered in, sly and sure. Watch your step.",
    "🌒 {display_name} arrived quietly — but the silence hums with intent.",
    "🧿 {display_name} joined; the air says stay, the body says closer.",
    "🎭 {display_name} entered with a mischievous tilt — masks are optional.",
    "🪶 {display_name} stepped in with featherlight steps and heavy intent.",
    "🩶 {display_name} joined, calm on the outside, simmering on the inside.",
    "👁️ {display_name} arrived; one look and the night got complicated.",
    "🕸️ {display_name} stepped into the web — enjoy getting tangled.",
    "🌘 {display_name} joined — shadow-soft and dangerously inviting.",
    "🧊 {display_name} arrived cool, but their presence melts the room.",
    "⚖️ {display_name} walked in — the balance shifted toward desire.",
    "🪄 {display_name} joined and something magical tightened in the chest.",
    "🌺 {display_name} arrived like a slow bloom — intoxicating.",
    "🫦 {display_name} joined — lips curved, promise implied.",
    "🎶 {display_name} arrived on a private rhythm; follow if you want to sway.",
    "🌪️ {display_name} joined — whirlwinds look calm until they hit.",
    "🖤 {display_name} slipped in, hush and hunger wrapped together.",
    "💼 {display_name} entered composed — look closer, there's mischief under the suit.",
    "💫 {display_name} joined and the room took a breathless pause.",
    "🩸 {display_name} enters — the room tightens like it knows what's coming.",
    "🖤 {display_name} joined. Lock your thoughts, not your doors.",
    "🌑 {display_name} stepped in — eyes linger longer than they should.",
    "😈 {display_name} arrived with intent. Pretend you don't feel it.",
    "🕷️ {display_name} entered — something just wrapped around your focus.",
    "🔥 {display_name} joined. Heat climbs. Control slips.",
    "👁️ {display_name} is here — watched before watching back.",
    "🖤 {display_name} arrived. Breathe slow. This one doesn't rush.",
    "🌒 {display_name} slipped in — confidence sharp enough to cut.",
    "🩶 {display_name} joined quietly. Dangerous people don't announce themselves.",
    "😼 {display_name} joined with a look that asks permission from no one.",
    "🕯️ {display_name} arrived — slow burn, no mercy.",
    "🐍 {display_name} slid in — smooth, patient, inevitable.",
    "🧿 {display_name} joined — attention captured, consent assumed.",
    "🕶️ {display_name} arrived — unreadable, unbothered, unresisted.",
    "🎯 {display_name} joined — precise, unavoidable, magnetic.",
    "🔒 {display_name} arrived — doors close a little tighter.",
    "🗝️ {display_name} unlocked the room; keys aren't always literal.",
    "🧨 {display_name} entered — contained chaos with an inviting grin.",
    "🌌 {display_name} joined — vast, dark, and impossible to ignore.",
    "🖤 {display_name} slips in — the shadows made room for them.",
    "🌑 {display_name} arrived; the air tightened at their name.",
    "🩸 {display_name} walked in with an intent that hums.",
    "🔥 {display_name} entered — eyes sharpen, breaths slow.",
    "😈 {display_name} came — dangerous grace, measured steps.",
    "🕯️ {display_name} arrives, dusk trailing like a promise.",
    "👁️ {display_name} joined — every light found them first.",
    "🐍 {display_name} slipped through; patience wrapped around them.",
    "⚡ {display_name} stepped in and the dark learned to behave.",
    "🗝️ {display_name} unlocked eyes; rooms rearranged themselves.",
    "🖤 {display_name} arrived — presence heavy, attention willing.",
    "🌘 {display_name} joined; the hush leaned forward.",
    "🕶️ {display_name} walked in — unreadable, owning the pause.",
    "🔒 {display_name} arrived; the world closed in tighter.",
    "🧿 {display_name} came — watched and watching back.",
    "🩶 {display_name} stepped through — calm, collected, inevitable.",
    "🌫️ {display_name} appears like smoke — hard to push away.",
    "🐺 {display_name} joined alone; the pack noticed.",
    "🪞 {display_name} arrived — reflection trembles when they move.",
    "🎯 {display_name} entered — precise, unavoidable.",
    "🧨 {display_name} slipped in; contained trouble with a smile.",
    "🪄 {display_name} arrived — small magic, large consequence.",
    "🕷️ {display_name} stepped into the web — enjoy the pull.",
    "🌪️ {display_name} entered — calm before the pleasant storm.",
    "💎 {display_name} joined — cold, beautiful, demanding regard.",
    "🫦 {display_name} arrived; lips quiet, intentions loud.",
    "🎭 {display_name} came with a hidden grin — masks not required.",
    "🍷 {display_name} entered like a slow pour; taste lingers.",
    "🐉 {display_name} arrived — ancient hush follows new steps.",
    "🧠 {display_name} joined; thinking people are deliciously dangerous.",
    "💫 {display_name} arrives and the room holds its breath.",
    "🌺 {display_name} stepped in — beauty that commands.",
    "🕯️ {display_name} came — slow flame, sharp heat.",
    "🪶 {display_name} drifted in; soft steps, heavy intent.",
    "⚖️ {display_name} joined — balance subtly tipped.",
    "🌌 {display_name} arrived — vast, dark, magnetic.",
    "🔮 {display_name} entered; futures leaned toward them.",
    "🧊 {display_name} came cool, but the air betrayed them.",
    "🖤 {display_name} arrived — possession in every glance.",
    "🌒 {display_name} stepped in and the night acknowledged them.",
    "👑 {display_name} joined; crowns fit easily on slow smiles.",
    "🔥 {display_name} slipped in — embers trailed their footsteps.",
    "🕶️ {display_name} arrived — no one dares stare too long.",
    "🩸 {display_name} entered; the room kept a small, sharp memory.",
    "🧿 {display_name} joined — attention, harvested whole.",
    "🗝️ {display_name} stepped through; doors sighed closed behind them.",
    "🐍 {display_name} arrived — elegant, patient, inevitable.",
    "🌘 {display_name} joined; darkness welcomed a familiar shape.",
    "🎶 {display_name} entered on a low, dangerous rhythm.",
    "🪞 {display_name} came — mirrors preferred their reflection tonight.",
    "🥀 {display_name} arrives — wilted petals, potent scent.",
    "🕸️ {display_name} joined; the web tightened delightfully.",
    "💼 {display_name} stepped in — composed, with a hidden edge.",
    "🧨 {display_name} arrived — quiet fuse, loud results.",
    "🫀 {display_name} came — heartbeats answered them.",
    "🪙 {display_name} arrived — coin dropped, choices made.",
    "🐾 {display_name} joined — footsteps marking territory.",
    "🖤 {display_name} entered and the room learned to follow their lead.",
    "⚡ {display_name} arrived; sparks were not accidental.",
    "🌑 {display_name} joined — welcome to the darker side of curiosity.",
]

LEAVE_GREETINGS = [
    "🌙 {display_name} slips away — the afterglow lingers.",
    "🖤 {display_name} left; the room exhales and remembers the warmth.",
    "🌑 {display_name} drifted out, leaving charged silence in their wake.",
    "👀 {display_name} left — eyes still searching the doorway.",
    "🕯️ {display_name} exited; the candle burned a little brighter while they were here.",
    "😈 {display_name} disappeared — mischief properly recorded.",
    "🌫️ {display_name} faded into the night; whispers followed.",
    "🧠 {display_name} walked away smiling — plotting, no doubt.",
    "🕶️ {display_name} slipped out unnoticed — or cleverly unnoticed.",
    "💎 {display_name} left — the room is slightly less dazzling.",
    "🔥 {display_name} exited — the temperature is slow to drop.",
    "🩸 {display_name} is gone; the air still carries a memory.",
    "🐍 {display_name} slithered away — patient until next time.",
    "🪞 {display_name} stepped out; reflections linger.",
    "👑 {display_name} left — brief reign, lasting impression.",
    "🌒 {display_name} faded — the shadow kept the scent.",
    "💋 {display_name} slipped away — lips still warm with goodbye.",
    "🕷️ {display_name} left the web — threads still vibrate.",
    "🧿 {display_name} exited — the room blinked and they were gone.",
    "⚡ {display_name} departed — static still crackles.",
    "🌺 {display_name} left — fragrance lingers like a second presence.",
    "🎶 {display_name} stepped out mid-beat; the rhythm misses them.",
    "🪄 {display_name} vanished — no trick, just absence.",
    "🌘 {display_name} left with the dark; the room forgot to breathe.",
    "⚖️ {display_name} exited — balance quietly unsettled.",
    "🫦 {display_name} left — the promise hangs unfinished.",
    "🌪️ {display_name} is gone; the calm feels suspicious.",
    "🗝️ {display_name} locked up and left — what did they take?",
    "🩶 {display_name} stepped out quietly — most dangerous departures are.",
    "🐺 {display_name} went back to the dark; the pack felt it.",
    "🔒 {display_name} left — something closed with them.",
    "🎯 {display_name} exited precisely — no wasted movement.",
    "🌌 {display_name} faded into the vast — see you on the other side.",
    "🐾 {display_name} left footprints; warmth on the floor.",
    "🧨 {display_name} is gone — the fuse remembers the spark.",
    "🔮 {display_name} exited; the future tilts a little.",
    "🌑 {display_name} stepped into the night — gracefully, inevitably.",
    "😈 {display_name} vanished — the mischief is still here somewhere.",
    "🕯️ {display_name} left; the flame lowered but didn't die.",
    "🖤 {display_name} gone. The hush that replaced them says everything.",
    "💫 {display_name} left; the room adjusts slowly to less light.",
    "🐉 {display_name} departed — the old presence lingers like myth.",
    "🌺 {display_name} slipped out; the air still smells of them.",
    "🩸 {display_name} left. Something small and sharp stayed.",
    "🪙 {display_name} exited — the coin has been spent.",
    "🎭 {display_name} removed their mask on the way out — imagine that.",
    "🖤 {display_name} is gone. Door's open. Heart isn't.",
    "🌒 {display_name} drifted away — half the night went with them.",
    "🧊 {display_name} left; a cool vacancy settled in.",
    "👁️ {display_name} stopped watching — the room feels less seen.",
    "🔥 {display_name} left; the embers hold the shape of their heat.",
    "🗡️ {display_name} departed — the edge lingered.",
    "🌫️ {display_name} dissolved into the air — breathe deep.",
    "⚡ {display_name} stepped out; the sparks miss them already.",
    "🎯 {display_name} gone. The precision of their absence is felt.",
    "🕸️ {display_name} left the web spinning — enjoy the vibration.",
    "🪞 {display_name} exited — the glass is duller without them.",
    "💎 {display_name} gone — the room noticed the drop in brilliance.",
    "🧿 {display_name} stepped away — but the eye still watches.",
    "🐍 {display_name} coiled away into the dark. Until next time.",
]

MOOD_MESSAGES = [
    "🌙 *The room hums with something unspoken tonight.*",
    "👁️ *Someone is definitely watching. That's not necessarily bad.*",
    "🕯️ *The candles burn a little lower when everyone's here.*",
    "🌑 *Something shifted. The air knows.*",
    "🎶 *A song no one remembers the name of starts playing.*",
    "🖤 *The dark is comfortable here. Stay a while.*",
    "🌌 *The void acknowledged you. You may not have noticed.*",
    "🐍 *Patience is a form of power. Just saying.*",
    "⚡ *Static in the air. Nobody made it — it just formed.*",
    "🌺 *Something beautiful and slightly dangerous is happening right now.*",
    "🔮 *The future is reading the room. What it sees is interesting.*",
    "🥀 *Even wilted things carry a scent. Remember that.*",
    # ── NEW mood messages ──
    "🩶 *Quiet rooms are the loudest ones. Listen.*",
    "🕸️ *Once you're in the web, you stop noticing the threads.*",
    "🧿 *The eye sees what the mind refuses to admit.*",
    "🌘 *Half-dark is where the interesting things live.*",
    "🐉 *Old things move slowly. That's how they outlast everything else.*",
    "🪙 *The coin was always going to land this way.*",
    "🔒 *Some doors close with you still inside. That's not always a problem.*",
    "🌫️ *You can't hold smoke. That's never stopped anyone from trying.*",
    "💎 *Pressure makes diamonds. The room is keeping score.*",
    "🐺 *The pack is quiet because they're listening to you.*",
    "🗝️ *Every room has a key. This one's already been turned.*",
    "🫀 *The heartbeat in the walls isn't an echo. It never was.*",
]

DAILY_OPENERS = [
    "🌅 *A new day begins. The room opens its eyes.*",
    "🎌 *The gates are open. Who arrives first sets the tone.*",
    "🌒 *Another cycle, another cast of characters. Welcome back.*",
    "⛩️ *The channel wakes. Something is already stirring.*",
    "🌸 *Day begins. The air is still. Not for long.*",
    "🌄 *Dawn breaks here first. You arrived just in time.*",
    "☀️ *A new page. The room waits to see who fills it.*",
]

# ── Bot status rotation pool ───────────────────────────────────────────────────

BOT_STATUS_POOL = [
    "👁️ watching the void",
    "🌙 lurking after midnight",
    "🔥 temperature: rising",
    "🎵 dark academia playlist",
    "🐍 patience. always patience.",
    "⚡ static in the walls",
    "🌑 night shift, permanently",
    "🕯️ keeping the flame alive",
    "🌺 beautiful and dangerous",
    "🖤 presence is everything",
    "🎌 waiting for the next arrival",
    "🌌 somewhere in the dark",
    "🔮 reading the room",
    "🥀 wilted but watching",
    "😈 always here. always.",
    "🌙 the dark agrees with me",
    "🐍 coiled. waiting. inevitable.",
    "💎 cold, beautiful, unresisted",
    "🌑 the shadows made room",
    "👑 the void bows to no one",
    "🎶 low rhythm, dangerous intent",
    "🩸 the room keeps score",
    "🪞 the reflection blinks first",
    "🗝️ some doors stay open",
    "🌫️ drifting. always watching.",
    # ── NEW statuses ──
    "🧿 the eye that never closes",
    "🐺 running with something ancient",
    "🔒 locked in. by choice.",
    "🪙 the coin always knows",
    "🫀 the room has a pulse",
    "🧨 quiet fuse. patient spark.",
    "🌘 half-dark, fully present",
    "🐉 older than the server itself",
    "🎯 always aimed. never missing.",
    "🕸️ the web is already woven",
    "💫 something celestial this way lurks",
    "⚖️ the balance tips tonight",
    "🪶 light touch, heavy presence",
    "🩶 still waters. deep current.",
    "🌪️ the calm that precedes",
    "🔮 the future already happened here",
    "👁️ everything seen. nothing said.",
    "🐾 footprints in the dark",
    "🧠 thinking dangerously",
    "🎭 masks optional. intentions loud.",
]

# ── Embed configuration ────────────────────────────────────────────────────────

JOIN_EMBED_COLORS = [
    discord.Color.from_rgb(220, 53, 69),
    discord.Color.from_rgb(123, 44, 191),
    discord.Color.from_rgb(255, 159, 28),
    discord.Color.from_rgb(220, 53, 128),
    discord.Color.from_rgb(180, 30, 60),
    discord.Color.from_rgb(150, 20, 100),
    discord.Color.from_rgb(200, 80, 20),
    discord.Color.from_rgb(100, 0, 150),   # NEW — deep violet
    discord.Color.from_rgb(190, 40, 40),   # NEW — crimson
    discord.Color.from_rgb(255, 80, 0),    # NEW — ember orange
]

LEAVE_EMBED_COLORS = [
    discord.Color.from_rgb(60, 60, 90),
    discord.Color.from_rgb(40, 40, 70),
    discord.Color.from_rgb(80, 50, 100),
    discord.Color.from_rgb(30, 50, 80),
    discord.Color.from_rgb(20, 20, 40),    # NEW — near black
    discord.Color.from_rgb(50, 30, 70),    # NEW — dark plum
]

JOIN_TITLES = [
    "⛩️ Voice Channel Arrival",
    "🎌 New Presence Detected",
    "🌙 The Room Shifts",
    "👁️ Arrival Noted",
    "🔮 A Soul Enters",
    "🌑 The Dark Acknowledges",
    "🕯️ The Flame Rises",
    "✨ Presence: Confirmed",
    "🌺 The Room Noticed",
    "🐍 Another One Steps In",
    "🖤 The Air Changed",
    "⚡ Signal Detected",
    "🧿 The Eye Opens",       # NEW
    "🐉 Something Ancient Stirs",  # NEW
    "🌘 Shadow Joins Shadow",  # NEW
    "🎯 Target Acquired",      # NEW
]

LEAVE_TITLES = [
    "🌙 They Slipped Away",
    "🖤 Departure Noted",
    "🌑 The Room Exhales",
    "🕯️ The Flame Lowers",
    "🌫️ Gone Like Smoke",
    "👁️ One Less Watcher",
    "🌺 The Fragrance Lingers",
    "🐍 Coiled Away",
    "🧿 The Eye Closes",       # NEW
    "🌘 The Shadow Retreats",  # NEW
    "💎 The Brilliance Dims",  # NEW
]

FOOTER_LINES = [
    f"{BOT_PERSONA} noticed you  🖤",
    "presence detected — system unstable",
    "another soul joins the chaos",
    "logged, archived, adored  ❤️",
    "the void welcomed you",
    f"𝑤𝑎𝑡𝑐ℎ𝑒𝑑 𝑏𝑦 {BOT_PERSONA}",
    "𝑝𝑟𝑒𝑠𝑒𝑛𝑐𝑒 𝑖𝑠 𝑒𝑣𝑒𝑟𝑦𝑡ℎ𝑖𝑛𝑔",
    "the room remembers  🌙",
    "you were always going to end up here",
    "the dark kept a seat for you",
    f"{BOT_PERSONA} has been waiting  🌑",
    "arrival recorded. judgment reserved.  👁️",   # NEW
    "the web noted your entrance  🕸️",             # NEW
    "you brought the energy. we kept it.  ⚡",     # NEW
    "the coin flipped. it landed on you.  🪙",     # NEW
]

LEAVE_FOOTER_LINES = [
    f"{BOT_PERSONA} watched them leave  🖤",
    "𝑡ℎ𝑒𝑦'𝑙𝑙 𝑏𝑒 𝑏𝑎𝑐𝑘",
    "departure archived  🌙",
    "absence noted — warmth remains",
    "𝑡ℎ𝑒 𝑟𝑜𝑜𝑚 𝑟𝑒𝑚𝑒𝑚𝑏𝑒𝑟𝑠",
    "the door is still open  🖤",
    "they left. the room didn't forget.  🧿",      # NEW
    "gone but the shadow stayed  🌑",              # NEW
    "departure logged. return expected.  🔮",      # NEW
]

JOIN_REACTIONS  = ["🖤", "👁️", "🔥", "✨", "💫", "🌙", "😈", "👑", "🥀", "⚡", "🎌", "🌸", "🧿", "🐍", "💎"]
LEAVE_REACTIONS = ["🌙", "🖤", "💫", "🌑", "🕯️", "🌺", "🎶", "🌌", "🧿", "🩶"]

# ── Greeting helpers ───────────────────────────────────────────────────────────

def _get_time_theme() -> str:
    hour = datetime.datetime.utcnow().hour
    if   0 <= hour < 6:   return "midnight"
    elif 6 <= hour < 12:  return "morning"
    elif 12 <= hour < 18: return "afternoon"
    else:                  return "evening"

def get_join_greeting(member) -> str:
    uid   = str(member.id)
    name  = member.display_name
    count = data["visit_count"].get(uid, 0) + 1
    data["visit_count"][uid] = count

    _update_streak(uid)

    is_first = uid not in data["greeted_users"]
    if is_first:
        data["greeted_users"].append(uid)
        tier = random.choice(TIERS)
        data["user_tiers"][uid] = tier
        return TIER_FIRST_GREETINGS[tier].format(display_name=name)

    holiday = _get_holiday()
    if holiday and holiday in HOLIDAY_JOINS and random.random() < 0.40:
        return random.choice(HOLIDAY_JOINS[holiday]).format(display_name=name)

    if count % 10 == 0 or count % 5 == 0:
        return random.choice(MILESTONE_GREETINGS).format(display_name=name, count=count)

    theme = _get_time_theme()
    pool  = THEMED_JOINS.get(theme, []) + JOIN_GREETINGS
    return random.choice(pool).format(display_name=name)

def get_leave_greeting(display_name: str) -> str:
    return random.choice(LEAVE_GREETINGS).format(display_name=display_name)

async def maybe_send_daily_open(channel):
    today = datetime.date.today().isoformat()
    if data["last_daily_open"] == today:
        return
    data["last_daily_open"] = today
    try:
        await channel.send(random.choice(DAILY_OPENERS))
    except Exception:
        pass

def _is_late_night() -> bool:
    return 0 <= datetime.datetime.utcnow().hour < 5

# ── Discord log helper ─────────────────────────────────────────────────────────

async def _log(msg: str):
    """Send a message to the designated log channel."""
    try:
        ch = bot.get_channel(LOG_CHANNEL_ID)
        if ch:
            await ch.send(msg)
    except Exception:
        pass

# ── Safe send with retry ───────────────────────────────────────────────────────

async def _safe_send(channel, **kwargs):
    for attempt in range(3):   # ↑ 2→3 attempts
        try:
            return await channel.send(**kwargs)
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = getattr(e, "retry_after", 2)
                await asyncio.sleep(retry_after + 0.5)
            elif attempt < 2:
                await asyncio.sleep(1)
            else:
                raise
    return None

# ── Embed sender ───────────────────────────────────────────────────────────────

async def send_greeting_embed(channel, session, greeting_text, image_url, member,
                               send_to_dm=None, event_type="join"):
    try:
        image_bytes, content_type = None, None
        if _image_pool is not None and not _image_pool.empty():
            try:
                pool_url, image_bytes, content_type = _image_pool.get_nowait()
                image_url = pool_url
            except asyncio.QueueEmpty:
                pass

        for _dl_attempt in range(4):   # ↑ 3→4 retry attempts
            if image_bytes is None:
                image_bytes, content_type = await _download_bytes(session, image_url)
            if image_bytes:
                break
            await asyncio.sleep(1)

        if image_bytes and len(image_bytes) > DISCORD_MAX_UPLOAD:
            image_bytes = await compress_image(image_bytes)
        if not image_bytes or len(image_bytes) > DISCORD_MAX_UPLOAD:
            return False

        lurl  = image_url.lower()
        ctype = content_type or ""
        ext   = ".gif" if ("gif" in lurl or "gif" in ctype) else ".jpg"
        if "png"  in lurl or "png"  in ctype: ext = ".png"
        elif "webp" in lurl or "webp" in ctype: ext = ".webp"
        filename = f"waifu{ext}"

        if event_type == "join":
            color    = random.choice(JOIN_EMBED_COLORS)
            title    = random.choice(JOIN_TITLES)
            footer   = random.choice(FOOTER_LINES)
            reaction = random.choice(JOIN_REACTIONS)
        else:
            color    = random.choice(LEAVE_EMBED_COLORS)
            title    = random.choice(LEAVE_TITLES)
            footer   = random.choice(LEAVE_FOOTER_LINES)
            reaction = random.choice(LEAVE_REACTIONS)

        if _is_late_night():
            color  = discord.Color.from_rgb(10, 10, 20)
            footer = f"𝑙𝑎𝑡𝑒 𝑛𝑖𝑔ℎ𝑡. {BOT_PERSONA} 𝑖𝑠 𝑠𝑡𝑖𝑙𝑙 𝑤𝑎𝑡𝑐ℎ𝑖𝑛𝑔  🌑"

        uid  = str(member.id)
        tier = data["user_tiers"].get(uid)

        if tier:
            footer = f"{footer}  ·  {TIER_LABELS[tier]}"
        streak = data["streaks"].get(uid, 0)
        badge  = _streak_badge(streak)
        if badge:
            footer = f"{footer}  ·  {badge}"

        embed = discord.Embed(
            title=title,
            description=greeting_text,
            color=color,
            timestamp=datetime.datetime.utcnow(),
        )
        embed.set_author(
            name=member.display_name,
            icon_url=getattr(member.display_avatar, "url", None),
        )

        # ↑ 25% → 30% thumbnail chance for more variety
        use_thumbnail = random.random() < 0.30
        if use_thumbnail:
            embed.set_thumbnail(url=f"attachment://{filename}")
        else:
            embed.set_image(url=f"attachment://{filename}")

        # ↑ 25% → 35% mood field chance
        if random.random() < 0.35:
            embed.add_field(name="​", value=random.choice(MOOD_MESSAGES), inline=False)

        embed.set_footer(
            text=footer,
            icon_url=getattr(member.display_avatar, "url", None),
        )

        ch_file = discord.File(io.BytesIO(image_bytes), filename=filename)
        msg = await _safe_send(channel, embed=embed, file=ch_file)
        if msg:
            try:
                await msg.add_reaction(reaction)
            except Exception:
                pass

        if send_to_dm:
            try:
                dm_file  = discord.File(io.BytesIO(image_bytes), filename=filename)
                dm_embed = discord.Embed(
                    title=title,
                    description=greeting_text,
                    color=color,
                    timestamp=datetime.datetime.utcnow(),
                )
                dm_embed.set_author(
                    name=member.display_name,
                    icon_url=getattr(member.display_avatar, "url", None),
                )
                if use_thumbnail:
                    dm_embed.set_thumbnail(url=f"attachment://{filename}")
                else:
                    dm_embed.set_image(url=f"attachment://{filename}")
                dm_embed.set_footer(
                    text=footer,
                    icon_url=getattr(member.display_avatar, "url", None),
                )
                await send_to_dm.send(embed=dm_embed, file=dm_file)
            except Exception:
                pass

        return True

    except Exception:
        return False

# ── Voice channel utilities ────────────────────────────────────────────────────

def _vc_has_users(vc: discord.VoiceChannel) -> bool:
    return any(m for m in vc.members if not m.bot)

async def _move_bot(guild: discord.Guild, go_to: discord.VoiceChannel = None):
    if not VC_IDS:
        return

    vc_client = guild.voice_client

    if go_to and go_to.id in VC_IDS:
        if vc_client and vc_client.is_connected():
            if vc_client.channel.id == go_to.id:
                return
            try:
                await vc_client.move_to(go_to)
            except Exception as e:
                logger.warning(f"[VC] move_to {go_to.name} failed: {e}")
        else:
            try:
                await go_to.connect()
            except Exception as e:
                logger.warning(f"[VC] connect to {go_to.name} failed: {e}")
        return

    best = None
    for vc_id in VC_IDS:
        vc = guild.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel) and _vc_has_users(vc):
            best = vc
            break

    if best:
        if vc_client and vc_client.is_connected():
            if vc_client.channel.id == best.id:
                return
            try:
                await vc_client.move_to(best)
            except Exception as e:
                logger.warning(f"[VC] move_to {best.name} failed: {e}")
        else:
            try:
                await best.connect()
            except Exception as e:
                logger.warning(f"[VC] connect to {best.name} failed: {e}")
        return

    if vc_client and vc_client.is_connected():
        return

    for vc_id in VC_IDS:
        vc = guild.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel):
            try:
                await vc.connect()
                return
            except Exception as e:
                logger.warning(f"[VC] startup connect to {vc.name} failed: {e}")
                continue

# ── Bot setup ──────────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.voice_states    = True
intents.message_content = True
intents.members         = True
bot = commands.Bot(command_prefix="!", intents=intents)

_image_pool = None

# ── Command list & suggestion system ──────────────────────────────────────────

COMMANDS = [
    "69", "aibooru", "aihentai", "anal", "bfuck", "boobjob", "boobs",
    "butt", "cum", "danbooru", "dickride", "doujin", "e621", "fap",
    "footjob", "fuck", "futafuck", "gelbooru", "grabboobs", "grabbutts",
    "handjob", "happyend", "hentai", "hentaigif", "hentaijk", "hvideo",
    "irl", "konachan", "kuni", "lewdere", "lewdkitsune", "lewdneko",
    "paizuri", "pussy", "realbooru", "rule34", "safebooru", "suck",
    "suckboobs", "threesome", "trap", "vtuber", "yaoifuck", "yurifuck",
]

def suggest_commands(text):
    return difflib.get_close_matches(text, COMMANDS, n=5, cutoff=0.3)

# ── Pool warmup helper ─────────────────────────────────────────────────────────

async def _warmup_pool():
    """Fill the image pool immediately on startup using parallel fetches."""
    if _image_pool is None:
        return
    WARMUP_TARGET = 16
    logger.info(f"[Pool] Warming up image pool to {WARMUP_TARGET} images...")

    async def _fetch_and_store():
        try:
            url, _, _ = await fetch_gif(bot.http_session)
            if not url:
                return
            image_bytes, ctype = await _download_bytes(bot.http_session, url)
            if image_bytes and len(image_bytes) <= DISCORD_MAX_UPLOAD:
                try:
                    _image_pool.put_nowait((url, image_bytes, ctype))
                except asyncio.QueueFull:
                    pass
        except Exception:
            pass

    tasks_list = [_fetch_and_store() for _ in range(WARMUP_TARGET)]
    await asyncio.gather(*tasks_list, return_exceptions=True)
    logger.info(f"[Pool] Warmup complete — {_image_pool.qsize()} images ready.")

# ── Background tasks ───────────────────────────────────────────────────────────

@tasks.loop(seconds=AUTOSAVE_INTERVAL)
async def autosave_task():
    try:
        await save_data()
    except Exception:
        pass

@tasks.loop(seconds=300)
async def vc_reconnect_heartbeat():
    """Every 5 min: if the bot got kicked/disconnected, reconnect it."""
    for guild in bot.guilds:
        try:
            vc_client = guild.voice_client
            if vc_client and vc_client.is_connected():
                continue
            await _move_bot(guild)
        except Exception as e:
            logger.warning(f"[VC heartbeat] {e}")

@tasks.loop(seconds=8)    # ↑ 20→8 seconds (Railway always on, be aggressive)
async def prefetch_pool_filler():
    """Keep the image pool topped up. Railway's always-on means we can prefetch hard."""
    if _image_pool is None: return
    POOL_TARGET = 16        # ↑ 4→16 (more RAM available)
    if _image_pool.qsize() >= POOL_TARGET: return
    try:
        # Fetch up to 3 at a time to fill faster
        slots = POOL_TARGET - _image_pool.qsize()
        fetch_count = min(3, slots)

        async def _one():
            url, _, _ = await fetch_gif(bot.http_session)
            if url:
                image_bytes, ctype = await _download_bytes(bot.http_session, url)
                if image_bytes and len(image_bytes) <= DISCORD_MAX_UPLOAD:
                    try:
                        _image_pool.put_nowait((url, image_bytes, ctype))
                    except asyncio.QueueFull:
                        pass

        await asyncio.gather(*[_one() for _ in range(fetch_count)], return_exceptions=True)
    except Exception:
        pass

@tasks.loop(minutes=25)    # Slightly more frequent mood drops
async def random_mood_drop():
    if not VC_CHANNEL_ID: return
    channel = bot.get_channel(VC_CHANNEL_ID)
    if not channel: return
    for vc_id in VC_IDS:
        vc = bot.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel):
            active = [m for m in vc.members if not m.bot]
            if active:
                # Only send mood messages when people are active
                try:
                    await channel.send(random.choice(MOOD_MESSAGES))
                except Exception:
                    pass
                break

@tasks.loop(hours=1)       # ↑ 2h→1h (Railway never sleeps, rotate more)
async def rotate_status():
    """Rotate the bot's Discord status every hour."""
    try:
        status = random.choice(BOT_STATUS_POOL)
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.listening,
                name=status,
            )
        )
    except Exception:
        pass

@tasks.loop(minutes=20)
async def provider_recovery():
    """
    Gradually recover failed providers over time.
    Railway is stable — providers that failed earlier deserve another chance.
    Reduces each provider's failure count by 1 every 20 minutes.
    """
    for name in list(_provider_failures.keys()):
        if _provider_failures[name] > 0:
            _provider_failures[name] = max(0, _provider_failures[name] - 1)

@tasks.loop(hours=24)
async def daily_stats_report():
    """
    Post a daily stats digest to the log channel.
    Railway paid = always running, so these will fire reliably every 24h.
    """
    try:
        if _bot_start_time:
            uptime = datetime.datetime.utcnow() - _bot_start_time
            hours  = int(uptime.total_seconds() // 3600)
            mins   = int((uptime.total_seconds() % 3600) // 60)
            uptime_str = f"{hours}h {mins}m"
        else:
            uptime_str = "unknown"

        total_users   = len(data["visit_count"])
        total_visits  = sum(data["visit_count"].values())
        total_greeted = len(data["greeted_users"])
        pool_size     = _image_pool.qsize() if _image_pool else 0

        # Provider health summary
        failed_providers = [
            n for n, c in _provider_failures.items()
            if c >= _PROVIDER_FAIL_LIMIT
        ]
        healthy_count = len(PROVIDERS) - len(failed_providers)

        report = (
            f"📊 **Daily Stats Report**\n"
            f"```\n"
            f"Uptime:          {uptime_str}\n"
            f"Images sent:     {_total_images_sent:,}\n"
            f"Pool size:       {pool_size}/{16}\n"
            f"Users tracked:   {total_users:,}\n"
            f"Total visits:    {total_visits:,}\n"
            f"Greeted users:   {total_greeted:,}\n"
            f"Providers:       {healthy_count}/{len(PROVIDERS)} healthy\n"
            f"Global dedup:    {len(_global_sent_hashes):,} hashes\n"
            f"```"
        )

        if failed_providers:
            report += f"\n⚠️ **Inactive providers:** {', '.join(f'`{p}`' for p in failed_providers)}"

        await _log(report)
    except Exception as e:
        logger.warning(f"[Stats] Daily report failed: {e}")

# ── Bot events ─────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    global _image_pool, _bot_start_time
    _bot_start_time = datetime.datetime.utcnow()
    logger.info(f"Logged in as {bot.user}")

    connector = aiohttp.TCPConnector(
        limit=60,                  # ↑ 15→60 concurrent connections (Railway paid)
        limit_per_host=10,         # ↑ 3→10 per host
        ttl_dns_cache=600,
        enable_cleanup_closed=True,
    )
    bot.http_session = aiohttp.ClientSession(connector=connector)
    _image_pool = asyncio.Queue(maxsize=20)  # ↑ 6→20

    asyncio.create_task(_keep_alive_server())

    for task in (autosave_task, vc_reconnect_heartbeat,
                 prefetch_pool_filler, random_mood_drop,
                 rotate_status, provider_recovery, daily_stats_report):
        if not task.is_running():
            task.start()

    for guild in bot.guilds:
        try:
            await _move_bot(guild)
        except Exception:
            pass

    # Set initial status
    try:
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.listening,
                name=random.choice(BOT_STATUS_POOL),
            )
        )
    except Exception:
        pass

    await _log(
        f"✅ **Bot online** — `{bot.user}` | "
        f"`{len(PROVIDERS)}` providers | "
        f"`{len(bot.guilds)}` guild(s) | "
        f"Pool target: 16 | Conn limit: 60"
    )

    # Warm up the image pool in the background immediately
    asyncio.create_task(_warmup_pool())

@bot.event
async def on_close():
    try:
        if hasattr(bot, "http_session") and not bot.http_session.closed:
            await bot.http_session.close()
    except Exception:
        pass
    await save_data()

@bot.event
async def on_voice_state_update(member, before, after):
    if member.id == bot.user.id: return
    guild   = member.guild
    channel = bot.get_channel(VC_CHANNEL_ID) if VC_CHANNEL_ID else None

    was_monitored = before.channel is not None and before.channel.id in VC_IDS
    now_monitored = after.channel  is not None and after.channel.id  in VC_IDS

    if now_monitored and (not was_monitored or before.channel.id != after.channel.id):
        await _move_bot(guild, go_to=after.channel)
    elif was_monitored and not now_monitored:
        await _move_bot(guild)
    elif was_monitored and now_monitored and before.channel.id != after.channel.id:
        await _move_bot(guild, go_to=after.channel)

    if not channel: return

    if now_monitored and (not was_monitored or before.channel.id != after.channel.id):
        await maybe_send_daily_open(channel)

        greeting = get_join_greeting(member)
        others   = [m for m in after.channel.members if not m.bot and m.id != member.id]
        if not others:
            greeting += "\n*— alone in the dark. interesting choice.*"
        elif len(others) >= 4:
            greeting += f"\n*— {len(others)} others already here. the audience is ready.*"

        # ↑ 5→7 retry attempts for image fetch
        gif_url = None
        for _attempt in range(7):
            gif_url, _, _ = await fetch_gif(bot.http_session, member.id)
            if gif_url:
                break
            await asyncio.sleep(1.5)

        if gif_url:
            sent = await send_greeting_embed(
                channel, bot.http_session, greeting, gif_url, member,
                send_to_dm=member, event_type="join"
            )
            if not sent:
                gif_url2, _, _ = await fetch_gif(bot.http_session, member.id)
                if gif_url2:
                    sent = await send_greeting_embed(
                        channel, bot.http_session, greeting, gif_url2, member,
                        send_to_dm=member, event_type="join"
                    )
                if not sent:
                    await _log(
                        f"⚠️ **Image download failed** for `{member.display_name}` "
                        f"— sent text greeting instead."
                    )
                    await _safe_send(channel, content=greeting)
        else:
            await _log(
                f"⚠️ **All providers failed** for `{member.display_name}` after 7 attempts "
                f"— sent text greeting instead."
            )
            await _safe_send(channel, content=greeting)

    elif was_monitored and not now_monitored:
        leave_msg = get_leave_greeting(member.display_name)

        gif_url = None
        for _attempt in range(7):
            gif_url, _, _ = await fetch_gif(bot.http_session, member.id)
            if gif_url:
                break
            await asyncio.sleep(1.5)

        if gif_url:
            sent = await send_greeting_embed(
                channel, bot.http_session, leave_msg, gif_url, member,
                send_to_dm=member, event_type="leave"
            )
            if not sent:
                gif_url2, _, _ = await fetch_gif(bot.http_session, member.id)
                if gif_url2:
                    await send_greeting_embed(
                        channel, bot.http_session, leave_msg, gif_url2, member,
                        send_to_dm=member, event_type="leave"
                    )
                else:
                    await _safe_send(channel, content=leave_msg)
        else:
            await _safe_send(channel, content=leave_msg)

# ── on_message — command channel filter + neko prefix + suggestions ────────────

_COMMANDS_LIST_MSG = (
    "📋 **Available Commands:**\n"
    "```\n"
    "69          aibooru     aihentai    anal        bfuck\n"
    "boobjob     boobs       butt        cum         danbooru\n"
    "dickride    doujin      e621        fap         footjob\n"
    "fuck        futafuck    gelbooru    grabboobs   grabbutts\n"
    "handjob     happyend    hentai      hentaigif   hentaijk\n"
    "hvideo      irl         konachan    kuni        lewdere\n"
    "lewdkitsune lewdneko    paizuri     pussy       realbooru\n"
    "rule34      safebooru   suck        suckboobs   threesome\n"
    "trap        vtuber      yaoifuck    yurifuck\n"
    "```"
)

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if COMMAND_CHANNEL_ID and message.channel.id == COMMAND_CHANNEL_ID:

        if not message.content.lower().startswith("neko"):
            try:
                await message.delete()
            except Exception:
                pass
            await message.channel.send(_COMMANDS_LIST_MSG)
            await message.channel.send(
                f"✅ **Example:** `neko hentai` {message.author.mention}"
            )
            return

        parts = message.content.split()
        if len(parts) < 2:
            await message.channel.send(_COMMANDS_LIST_MSG)
            await message.channel.send(
                f"✅ **Example:** `neko hentai` {message.author.mention}"
            )
            return

        cmd = parts[1].lower()

        if cmd not in COMMANDS:
            suggestions = suggest_commands(cmd)
            if suggestions:
                suggestion_text = "\n".join([f"`neko {s}`" for s in suggestions])
                await message.channel.send(
                    f"❓ Unknown command **`{cmd}`**\n\nDid you mean:\n{suggestion_text}\n\n"
                    f"✅ **Example:** `neko hentai` {message.author.mention}"
                )
            else:
                await message.channel.send(_COMMANDS_LIST_MSG)
                await message.channel.send(
                    f"❌ Unknown command **`{cmd}`** {message.author.mention}\n"
                    f"✅ **Example:** `neko hentai`"
                )
            return

    await bot.process_commands(message)

# ── Run ────────────────────────────────────────────────────────────────────────

if not TOKEN:
    logger.error("TOKEN env var is not set.")
    sys.exit(1)

bot.run(TOKEN)
