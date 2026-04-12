"""
Discord-бот: основной форум аниме (/forum_add), личные списки (форум из DISCORD_LIST_FORUM_CHANNEL_ID),
/mylist_show, MAL (/mal_bind, /mal_import, /mal_show), YummyAnime (/yummy_link, /yummy_sync, …),
фоновый опрос Yummy, /admin, /adminpanel, /update_topics.
Справка: DISCORD_BOT_INFO_THREAD_ID. Токен бота: DISCORD_BOT_TOKEN. Рекомендуется DISCORD_GUILD_ID.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import time
import re
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import aiohttp
import discord
import yummy_api
from aiohttp import web
from dotenv import load_dotenv
from discord import app_commands
from discord.ext import commands

load_dotenv()

logger = logging.getLogger(__name__)


def _env_channel_id(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Некорректный %s=%r — используется значение по умолчанию.", name, raw)
        return default


# ID каналов-форумов настраиваются в .env (см. .env.example).
FORUM_CHANNEL_ID = _env_channel_id("DISCORD_ANIME_FORUM_CHANNEL_ID", 1393418241468141580)
# Форум «личные списки»: по одной теме на пользователя.
LIST_FORUM_CHANNEL_ID = _env_channel_id("DISCORD_LIST_FORUM_CHANNEL_ID", 1491208484245602385)
# Ветка со справкой по командам.
BOT_INFO_THREAD_ID = _env_channel_id("DISCORD_BOT_INFO_THREAD_ID", 1490073562122289276)
BASE = "https://en.yummyani.me"
API_SEARCH = f"{BASE}/api/search"
API_ANIME = f"{BASE}/api/anime"
JIKAN_ANIME = "https://api.jikan.moe/v4/anime/{id}"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

ITEM_PATH_RE = re.compile(
    r"(?:https?://)?(?:en\.)?yummyani\.me/catalog/item/([^/?#\s]+)", re.I
)
DATE_SUFFIX_RE = re.compile(r"-\d{4}-\d{2}-\d{2}$")

MAL_LIST_OR_PROFILE_RE = re.compile(
    r"myanimelist\.net/(?:animelist|profile)/([\w-]+)", re.I
)
MAL_ANIME_PAGE_RE = re.compile(
    r"myanimelist\.net/anime/(\d+)", re.I
)

DATA_DIR = Path(__file__).resolve().parent / "data"
STATE_PATH = DATA_DIR / "mal_state.json"
YUMMY_LINK_STATE_PATH = DATA_DIR / "yummy_link_state.json"

MAL_STATUS_ALL = 7
MAL_STATUS_NAMES: dict[int, str] = {
    1: "Смотрю",
    2: "Просмотрено",
    3: "Отложено",
    4: "Брошено",
    6: "В планах",
}

# Защита от слишком долгого импорта, если почти всё уже есть в форуме
CONNECT_MAX_MERGES_PER_RUN = 50

DISCORD_CONTENT_LIMIT = 2000
EMBED_DESC_LIMIT = 4096
EMBED_FIELD_LIMIT = 1024
MAX_SCREENSHOTS = 4
EMBED_COLOR = 0xE67E22

STATUS_REACTIONS: tuple[str, ...] = ("📺", "✅", "📋", "⏸️", "❌")
STATUS_HINT = (
    "**Статус просмотра** — нажмите реакцию под этим сообщением "
    "(снимите старую, если хотите сменить):\n"
    "📺 смотрю · ✅ просмотрено · 📋 в планах · ⏸️ отложено · ❌ брошено"
)

RATING_PANEL_TITLE = "⭐ Оценка аниме (1–10)"
RATING_PANEL_INTRO = (
    "**Как оценить:** нажмите кнопку **«Оценить»** и введите целое число от **1** до **10**.\n\n"
    "Ниже — баллы участников сервера, которые уже выставили оценку."
)

RECOMMEND_PANEL_TITLE = "📣 Порекомендовать аниме"
RECOMMEND_PANEL_DESC = (
    "Выберите участника сервера в списке ниже — ему придёт сообщение **в этой теме** "
    "с упоминанием и названием аниме."
)


def _clean_slug(slug: str) -> str:
    return DATE_SUFFIX_RE.sub("", slug.strip())


def slug_from_text(text: str) -> str | None:
    text = text.strip()
    m = ITEM_PATH_RE.search(text)
    if m:
        return _clean_slug(m.group(1))
    return None


def _abs_media(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http"):
        return u
    return None


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _screenshot_urls_from_api(raw: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw, list):
        return out
    for item in raw[:MAX_SCREENSHOTS]:
        if not isinstance(item, dict):
            continue
        sizes = item.get("sizes")
        if not isinstance(sizes, dict):
            continue
        u = sizes.get("full") or sizes.get("small")
        if not u or not isinstance(u, str):
            continue
        u = u.strip()
        if not u.startswith("http"):
            u = _abs_media(u) or u
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= MAX_SCREENSHOTS:
            break
    return out


def _franchise_field(viewing_order: Any, current_slug: str) -> str | None:
    if not isinstance(viewing_order, list) or len(viewing_order) <= 1:
        return None
    lines: list[str] = []
    for i, vo in enumerate(viewing_order, 1):
        if not isinstance(vo, dict):
            continue
        slug = (vo.get("anime_url") or "").strip()
        title = (vo.get("title") or slug or "—").strip()
        year = vo.get("year")
        y = str(year) if year is not None else "—"
        type_d = vo.get("type") if isinstance(vo.get("type"), dict) else {}
        type_short = (type_d.get("shortname") or type_d.get("name") or "").strip()
        data = vo.get("data") if isinstance(vo.get("data"), dict) else {}
        rel = (data.get("text") or "").strip()
        link = f"{BASE}/catalog/item/{slug}" if slug else BASE
        mark = "📍 " if slug == current_slug else ""
        rel_part = f" — _{rel}_" if rel else ""
        type_part = f" · {type_short}" if type_short else ""
        lines.append(f"**{i}.** {mark}[{title}]({link}) · {y}{type_part}{rel_part}")
    if not lines:
        return None
    text = "\n".join(lines)
    return _truncate(text, EMBED_FIELD_LIMIT)


def _build_embed(info: dict[str, Any]) -> discord.Embed:
    title = info["title"]
    page_url = info["page_url"]
    desc = (info.get("description") or "").strip()
    embed = discord.Embed(
        title=f"📺 {title}",
        url=page_url,
        color=EMBED_COLOR,
    )
    if desc:
        embed.description = _truncate(desc, EMBED_DESC_LIMIT)

    genres = info.get("genres") or []
    if isinstance(genres, list) and genres:
        gtxt = ", ".join(str(g) for g in genres if g)
        if gtxt:
            embed.add_field(name="🎭 Жанры", value=_truncate(gtxt, EMBED_FIELD_LIMIT), inline=False)

    meta_bits: list[str] = []
    y = info.get("year")
    if y is not None:
        meta_bits.append(f"📅 {y}")
    st = (info.get("status_title") or "").strip()
    if st:
        meta_bits.append(f"📌 {st}")
    tn = (info.get("type_name") or "").strip()
    if tn:
        meta_bits.append(f"🎬 {tn}")
    ep = info.get("episodes")
    if isinstance(ep, dict):
        cnt = ep.get("count")
        aired = ep.get("aired")
        if cnt is not None and aired is not None:
            meta_bits.append(f"🎞️ Эпизоды: {aired}/{cnt}")
    if meta_bits:
        embed.add_field(
            name="ℹ️ Информация",
            value=" · ".join(meta_bits),
            inline=False,
        )

    rt = info.get("rating_avg")
    if isinstance(rt, (int, float)):
        embed.add_field(name="⭐ Рейтинг", value=f"{rt:.2f}", inline=True)

    franchise = _franchise_field(info.get("viewing_order"), info.get("anime_url") or "")
    if franchise:
        embed.add_field(
            name="🔗 Связанные сезоны и порядок просмотра",
            value=franchise,
            inline=False,
        )

    embed.set_footer(text="YummyAnime · en.yummyani.me")
    return embed


async def api_search_slug(session: aiohttp.ClientSession, q: str) -> str | None:
    params = {"q": q.strip()}
    async with session.get(API_SEARCH, params=params) as resp:
        if resp.status != 200:
            return None
        data: dict[str, Any] = await resp.json()
    items = data.get("response") or []
    if not items:
        return None
    u = items[0].get("anime_url")
    return _clean_slug(u) if u else None


async def api_fetch_anime(
    session: aiohttp.ClientSession, slug: str
) -> dict[str, Any] | None:
    slug = _clean_slug(slug)
    async with session.get(f"{API_ANIME}/{quote(slug, safe='')}") as resp:
        if resp.status != 200:
            return None
        data = await resp.json()
    r = data.get("response")
    if not isinstance(r, dict):
        return None
    title = (r.get("title") or "").strip()
    if not title:
        return None
    poster = r.get("poster") or {}
    img = _abs_media(
        poster.get("fullsize") or poster.get("big") or poster.get("huge")
    )
    anime_url = (r.get("anime_url") or slug).strip()
    page_url = f"{BASE}/catalog/item/{anime_url}"
    genres_raw = r.get("genres") or []
    genres: list[str] = []
    if isinstance(genres_raw, list):
        for g in genres_raw:
            if isinstance(g, dict) and g.get("title"):
                genres.append(str(g["title"]))
    rating = r.get("rating")
    rating_avg = None
    if isinstance(rating, dict):
        try:
            rating_avg = float(rating.get("average"))
        except (TypeError, ValueError):
            rating_avg = None
    st = r.get("anime_status") if isinstance(r.get("anime_status"), dict) else {}
    type_d = r.get("type") if isinstance(r.get("type"), dict) else {}

    return {
        "title": title,
        "page_url": page_url,
        "poster_url": img,
        "anime_url": anime_url,
        "description": (r.get("description") or "").strip(),
        "screenshot_urls": _screenshot_urls_from_api(r.get("random_screenshots")),
        "viewing_order": r.get("viewing_order"),
        "genres": genres,
        "year": r.get("year"),
        "rating_avg": rating_avg,
        "status_title": (st.get("title") or "").strip(),
        "type_name": (type_d.get("name") or "").strip(),
        "episodes": r.get("episodes") if isinstance(r.get("episodes"), dict) else {},
    }


async def jikan_fetch_anime(
    session: aiohttp.ClientSession, mal_id: int
) -> dict[str, Any] | None:
    """Постер и средний балл с Jikan (MAL id)."""
    url = JIKAN_ANIME.format(id=mal_id)
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            raw = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None
    d = raw.get("data")
    if not isinstance(d, dict):
        return None
    title = (d.get("title") or d.get("title_english") or "").strip()
    if not title:
        return None
    score = d.get("score")
    score_s = f"{float(score):.2f}" if isinstance(score, (int, float)) else None
    imgs = d.get("images") if isinstance(d.get("images"), dict) else {}
    jpg = imgs.get("jpg") if isinstance(imgs.get("jpg"), dict) else {}
    poster = (jpg.get("large_url") or jpg.get("image_url") or "").strip() or None
    mal_url = (d.get("url") or f"https://myanimelist.net/anime/{mal_id}").strip()
    return {
        "title": title,
        "poster_url": poster,
        "page_url": mal_url,
        "global_score": score_s,
        "source": "mal",
    }


async def download_image(session: aiohttp.ClientSession, url: str) -> bytes | None:
    async with session.get(url) as resp:
        if resp.status != 200:
            return None
        return await resp.read()


def image_filename(url: str, fallback: str) -> str:
    base = url.rsplit("/", 1)[-1].split("?", 1)[0]
    if "." not in base:
        return fallback
    ext = base.rsplit(".", 1)[-1].lower()
    if ext not in ("jpg", "jpeg", "png", "webp", "gif", "avif"):
        return fallback
    return base[:80]


async def build_attachment_files(
    session: aiohttp.ClientSession,
    poster_url: str | None,
    screenshot_urls: list[str],
) -> tuple[list[discord.File], list[str]]:
    """Возвращает файлы и список заметок о пропусках (для текста сообщения)."""
    tasks: list[tuple[str, str]] = []
    if poster_url:
        tasks.append(("poster", poster_url))
    for i, u in enumerate(screenshot_urls[:MAX_SCREENSHOTS], 1):
        tasks.append((f"screen_{i}", u))

    if not tasks:
        return [], []

    async def grab(name: str, url: str) -> tuple[str, str, bytes | None]:
        data = await download_image(session, url)
        return name, url, data

    results = await asyncio.gather(*[grab(n, u) for n, u in tasks])
    files: list[discord.File] = []
    warnings: list[str] = []
    for name, url, data in results:
        if not data:
            warnings.append(name)
            continue
        if len(data) > 25 * 1024 * 1024:
            warnings.append(name)
            continue
        fn = (
            "poster.jpg"
            if name == "poster"
            else image_filename(url, f"{name}.jpg")
        )
        files.append(discord.File(io.BytesIO(data), filename=fn))
    return files, warnings


def _dl_label(name: str) -> str:
    if name == "poster":
        return "постер"
    if name.startswith("screen_"):
        return f"кадр {name.replace('screen_', '')}"
    return name


def format_adders_line(adder_ids: list[int]) -> str:
    """Строка «Добавил(и): @a и @b» для подписи в теме."""
    seen: list[int] = []
    for uid in adder_ids:
        if uid not in seen:
            seen.append(uid)
    mentions = [f"<@{i}>" for i in seen]
    if not mentions:
        return "Добавили: —"
    prefix = "Добавил:" if len(mentions) == 1 else "Добавили:"
    if len(mentions) == 1:
        return f"{prefix} {mentions[0]}"
    if len(mentions) == 2:
        return f"{prefix} {mentions[0]} и {mentions[1]}"
    return prefix + " " + ", ".join(mentions[:-1]) + f" и {mentions[-1]}"


def _mention_ids_near_adders(content: str) -> list[int]:
    """ID из блока с «Добавил(и)», чтобы не цеплять лишние упоминания."""
    if "Добавил" not in content:
        return []
    i = content.find("Добавил")
    chunk = content[i : i + 500]
    out: list[int] = []
    for m in re.finditer(r"<@!?(\d+)>", chunk):
        try:
            out.append(int(m.group(1)))
        except ValueError:
            continue
    seen: set[int] = set()
    uniq: list[int] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def _topic_key_from_starter_text(text: str) -> tuple[str | None, str, str]:
    """
    Ключ anime_topics, kind (yummy|mal), ссылка для подписи.
    """
    s = slug_from_text(text)
    if s:
        ck = _clean_slug(s)
        return ck, "yummy", f"{BASE}/catalog/item/{ck}"
    mm = MAL_ANIME_PAGE_RE.search(text)
    if mm:
        aid = int(mm.group(1))
        return f"mal:{aid}", "mal", f"https://myanimelist.net/anime/{aid}"
    return None, "", ""


def build_message_content(
    page_url: str, adder_ids: list[int], image_notes: list[str]
) -> str:
    lines = [
        f"**Ссылка на YummyAnime**\n<{page_url}>",
        "",
        STATUS_HINT,
        "",
        format_adders_line(adder_ids),
    ]
    if image_notes:
        missed = ", ".join(_dl_label(x) for x in image_notes)
        lines.insert(2, f"_(Не загрузилось: {missed})_")
        lines.insert(2, "")
    text = "\n".join(lines)
    return _truncate(text, DISCORD_CONTENT_LIMIT)


_state_lock = asyncio.Lock()
_personal_list_thread_locks: dict[int, asyncio.Lock] = {}
_yummy_link_state_lock = asyncio.Lock()
YUMMY_LINK_PENDING_TTL_SEC = 900
YUMMY_LINK_READY_TTL_SEC = 600


def _personal_list_thread_lock(user_id: int) -> asyncio.Lock:
    lock = _personal_list_thread_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _personal_list_thread_locks[user_id] = lock
    return lock


def _default_yummy_link_state() -> dict[str, Any]:
    return {"pending": {}, "ready": {}}


def _load_yummy_link_state_raw() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not YUMMY_LINK_STATE_PATH.is_file():
        return _default_yummy_link_state()
    try:
        raw = YUMMY_LINK_STATE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return _default_yummy_link_state()
    if not isinstance(data, dict):
        return _default_yummy_link_state()
    data.setdefault("pending", {})
    data.setdefault("ready", {})
    if not isinstance(data["pending"], dict):
        data["pending"] = {}
    if not isinstance(data["ready"], dict):
        data["ready"] = {}
    return data


def _save_yummy_link_state_raw(data: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    YUMMY_LINK_STATE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _yummy_link_prune_pending_unlocked(pending: dict[str, Any], now: float) -> None:
    for k, v in list(pending.items()):
        if not isinstance(v, dict):
            pending.pop(k, None)
            continue
        try:
            exp = float(v.get("exp", 0))
        except (TypeError, ValueError):
            exp = 0
        if exp < now:
            pending.pop(k, None)


async def yummy_link_create_session(discord_user_id: int) -> str:
    import secrets

    token = secrets.token_urlsafe(32)
    now = time.time()
    async with _yummy_link_state_lock:
        st = _load_yummy_link_state_raw()
        pending = st["pending"]
        assert isinstance(pending, dict)
        _yummy_link_prune_pending_unlocked(pending, now)
        pending[token] = {"user_id": int(discord_user_id), "exp": now + YUMMY_LINK_PENDING_TTL_SEC}
        _save_yummy_link_state_raw(st)
    return token


async def yummy_link_consume_ready(discord_user_id: int) -> dict[str, Any] | None:
    uid_s = str(int(discord_user_id))
    now = time.time()
    async with _yummy_link_state_lock:
        st = _load_yummy_link_state_raw()
        ready = st["ready"]
        assert isinstance(ready, dict)
        for k, v in list(ready.items()):
            if not isinstance(v, dict):
                ready.pop(k, None)
                continue
            try:
                exp = float(v.get("exp", 0))
            except (TypeError, ValueError):
                exp = 0
            if exp < now:
                ready.pop(k, None)
        ent = ready.pop(uid_s, None)
        if isinstance(ent, dict):
            _save_yummy_link_state_raw(st)
            return ent
        _save_yummy_link_state_raw(st)
    return None


async def yummy_link_store_ready(
    discord_user_id: int, *, access_token: str, yummy_user_id: int, nickname: str
) -> None:
    uid_s = str(int(discord_user_id))
    now = time.time()
    async with _yummy_link_state_lock:
        st = _load_yummy_link_state_raw()
        ready = st["ready"]
        assert isinstance(ready, dict)
        ready[uid_s] = {
            "access_token": access_token.strip(),
            "yummy_user_id": int(yummy_user_id),
            "nickname": (nickname or "").strip(),
            "exp": now + YUMMY_LINK_READY_TTL_SEC,
        }
        _save_yummy_link_state_raw(st)


async def yummy_link_pending_user_id(token: str) -> int | None:
    """Возвращает discord user_id для действующей сессии привязки (без удаления)."""
    tok = (token or "").strip()
    if not tok:
        return None
    now = time.time()
    async with _yummy_link_state_lock:
        st = _load_yummy_link_state_raw()
        pending = st["pending"]
        assert isinstance(pending, dict)
        _yummy_link_prune_pending_unlocked(pending, now)
        ent = pending.get(tok)
        if not isinstance(ent, dict):
            _save_yummy_link_state_raw(st)
            return None
        try:
            exp = float(ent.get("exp", 0))
        except (TypeError, ValueError):
            exp = 0
        if exp < now:
            pending.pop(tok, None)
            _save_yummy_link_state_raw(st)
            return None
        try:
            uid = int(ent.get("user_id"))
        except (TypeError, ValueError):
            pending.pop(tok, None)
            _save_yummy_link_state_raw(st)
            return None
        _save_yummy_link_state_raw(st)
        return uid


async def yummy_link_remove_pending(token: str) -> None:
    tok = (token or "").strip()
    if not tok:
        return
    async with _yummy_link_state_lock:
        st = _load_yummy_link_state_raw()
        pending = st["pending"]
        assert isinstance(pending, dict)
        pending.pop(tok, None)
        _save_yummy_link_state_raw(st)


def _default_state() -> dict[str, Any]:
    return {
        "mal_accounts": {},
        "yummy_accounts": {},
        "imported_mal": {},
        "imported_yummy": {},
        "threads": {},
        "ratings": {},
        "anime_topics": {},
        "personal_lists": {},
        "slug_titles": {},
        "meta": {"bot_info_thread_id": None},
    }


def _load_state() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_PATH.is_file():
        return _default_state()
    try:
        raw = STATE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return _default_state()
    if not isinstance(data, dict):
        return _default_state()
    for key in (
        "mal_accounts",
        "yummy_accounts",
        "threads",
        "ratings",
        "imported_mal",
        "imported_yummy",
        "anime_topics",
        "personal_lists",
        "slug_titles",
        "meta",
    ):
        if key not in data or not isinstance(data[key], dict):
            data[key] = {}
    if "bot_info_thread_id" not in data.get("meta", {}):
        data.setdefault("meta", {})["bot_info_thread_id"] = None
    return data


def _write_state(data: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def mal_username_from_url(text: str) -> str | None:
    text = text.strip()
    m = MAL_LIST_OR_PROFILE_RE.search(text)
    return m.group(1) if m else None


async def mal_fetch_list_page(
    session: aiohttp.ClientSession, username: str, status: int, offset: int
) -> tuple[list[dict[str, Any]], int]:
    url = (
        f"https://myanimelist.net/animelist/{quote(username, safe='')}"
        f"/load.json?offset={offset}&status={status}"
    )
    async with session.get(url) as resp:
        http = resp.status
        if http != 200:
            return [], http
        raw = await resp.json()
    if not isinstance(raw, list):
        return [], http
    return [x for x in raw if isinstance(x, dict)], http


async def mal_fetch_full_list(
    session: aiohttp.ClientSession, username: str, status: int
) -> tuple[list[dict[str, Any]], int]:
    out: list[dict[str, Any]] = []
    offset = 0
    last_http = 200
    while True:
        chunk, last_http = await mal_fetch_list_page(session, username, status, offset)
        if last_http != 200 and offset == 0:
            return [], last_http
        if not chunk:
            break
        out.extend(chunk)
        offset += len(chunk)
        if len(chunk) < 50:
            break
        await asyncio.sleep(0.35)
    return out, last_http


def mal_item_title(entry: dict[str, Any]) -> str:
    t = (entry.get("anime_title_eng") or entry.get("anime_title") or "").strip()
    return t or "Без названия"


def mal_item_url(entry: dict[str, Any]) -> str:
    path = (entry.get("anime_url") or "").strip()
    if path.startswith("http"):
        return path
    if path.startswith("/"):
        return f"https://myanimelist.net{path}"
    aid = entry.get("anime_id")
    if aid is not None:
        return f"https://myanimelist.net/anime/{aid}"
    return "https://myanimelist.net"


def mal_status_label(entry: dict[str, Any]) -> str:
    st = entry.get("status")
    if st in MAL_STATUS_NAMES:
        return MAL_STATUS_NAMES[int(st)]
    return "Список"


async def register_thread_meta(
    thread_id: int,
    *,
    title: str,
    mal_id: int | None = None,
    yummy_slug: str | None = None,
) -> None:
    async with _state_lock:
        data = _load_state()
        tid = str(thread_id)
        prev = data.get("threads", {}).get(tid)
        extra: dict[str, Any] = {}
        if isinstance(prev, dict):
            for k in ("rating_message_id", "recommend_message_id", "add_to_list_message_id"):
                if k in prev and prev[k] is not None:
                    extra[k] = prev[k]
        data["threads"][tid] = {
            "title": title[:500],
            "mal_id": mal_id,
            "yummy_slug": yummy_slug,
            **extra,
        }
        _write_state(data)


async def set_user_rating(thread_id: int, user_id: int, score: int) -> None:
    async with _state_lock:
        data = _load_state()
        tid = str(thread_id)
        if tid not in data["ratings"]:
            data["ratings"][tid] = {}
        data["ratings"][tid][str(user_id)] = score
        _write_state(data)


async def save_rating_board_message_id(thread_id: int, message_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        tid = str(thread_id)
        slot = data.get("threads", {}).get(tid)
        if not isinstance(slot, dict):
            return
        slot["rating_message_id"] = message_id
        data["threads"][tid] = slot
        _write_state(data)


async def save_recommend_board_message_id(thread_id: int, message_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        tid = str(thread_id)
        slot = data.get("threads", {}).get(tid)
        if not isinstance(slot, dict):
            return
        slot["recommend_message_id"] = message_id
        data["threads"][tid] = slot
        _write_state(data)


async def save_add_to_list_board_message_id(thread_id: int, message_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        tid = str(thread_id)
        slot = data.get("threads", {}).get(tid)
        if not isinstance(slot, dict):
            return
        slot["add_to_list_message_id"] = message_id
        data["threads"][tid] = slot
        _write_state(data)


def _parse_thread_ratings(state: dict[str, Any], thread_id: int) -> list[tuple[int, int]]:
    tid_s = str(thread_id)
    raw = state.get("ratings", {}).get(tid_s, {})
    if not isinstance(raw, dict):
        return []
    pairs: list[tuple[int, int]] = []
    for uid_s, sc in raw.items():
        try:
            uid = int(uid_s)
            score = int(sc)
        except (TypeError, ValueError):
            continue
        if 1 <= score <= 10:
            pairs.append((uid, score))
    pairs.sort(key=lambda x: (-x[1], x[0]))
    return pairs


def _build_rating_panel_embed(
    state: dict[str, Any], thread_id: int, guild: discord.Guild | None
) -> discord.Embed:
    pairs = _parse_thread_ratings(state, thread_id)
    embed = discord.Embed(
        title=RATING_PANEL_TITLE,
        description=RATING_PANEL_INTRO,
        color=0xF1C40F,
    )
    if pairs:
        lines: list[str] = []
        for uid, score in pairs:
            if guild:
                m = guild.get_member(uid)
                label = m.display_name if m else f"<@{uid}>"
            else:
                label = f"<@{uid}>"
            lines.append(f"• {label} — **{score}**/10")
        body = "\n".join(lines)
        embed.add_field(
            name="Оценки участников",
            value=_truncate(body, EMBED_FIELD_LIMIT),
            inline=False,
        )
        avg = sum(p[1] for p in pairs) / len(pairs)
        embed.set_footer(text=f"Средняя: {avg:.2f}/10 · голосов: {len(pairs)}")
    else:
        embed.add_field(
            name="Оценки участников",
            value="_Пока никто не оценил — нажмите **«Оценить»**._",
            inline=False,
        )
        embed.set_footer(text="Шкала 1–10")
    return embed


async def bind_mal_account(
    discord_user_id: int, username: str, list_url: str
) -> None:
    async with _state_lock:
        data = _load_state()
        data["mal_accounts"][str(discord_user_id)] = {
            "username": username,
            "list_url": list_url.strip(),
        }
        _write_state(data)


async def mark_mal_imported(discord_user_id: int, mal_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        key = str(discord_user_id)
        cur = data["imported_mal"].get(key)
        if not isinstance(cur, list):
            cur = []
        if mal_id not in cur:
            cur.append(mal_id)
        data["imported_mal"][key] = cur
        _write_state(data)


async def bind_yummy_account(
    discord_user_id: int,
    access_token: str,
    yummy_user_id: int,
    nickname: str = "",
) -> None:
    async with _state_lock:
        data = _load_state()
        data.setdefault("yummy_accounts", {})[str(discord_user_id)] = {
            "access_token": access_token.strip(),
            "yummy_user_id": int(yummy_user_id),
            "nickname": (nickname or "").strip(),
        }
        _write_state(data)


async def update_yummy_access_token(discord_user_id: int, new_token: str) -> None:
    async with _state_lock:
        data = _load_state()
        key = str(discord_user_id)
        acc = data.get("yummy_accounts", {}).get(key)
        if not isinstance(acc, dict):
            return
        acc["access_token"] = new_token.strip()
        data.setdefault("yummy_accounts", {})[key] = acc
        _write_state(data)


async def unbind_yummy_account(discord_user_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        data.setdefault("yummy_accounts", {}).pop(str(discord_user_id), None)
        _write_state(data)


async def mark_yummy_imported(discord_user_id: int, anime_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        key = str(discord_user_id)
        cur = data.setdefault("imported_yummy", {}).get(key)
        if not isinstance(cur, list):
            cur = []
        if anime_id not in cur:
            cur.append(anime_id)
        data["imported_yummy"][key] = cur
        _write_state(data)


def _is_bot_admin(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    rid = (os.environ.get("DISCORD_ADMIN_ROLE_ID") or "").strip()
    if not rid or not member.guild:
        return False
    try:
        role = member.guild.get_role(int(rid))
    except ValueError:
        return False
    return bool(role and role in member.roles)


def _primary_guild_for_yummy_poll() -> discord.Guild | None:
    gid = (os.environ.get("DISCORD_GUILD_ID") or "").strip()
    if gid:
        try:
            g = bot.get_guild(int(gid))
            if g:
                return g
        except ValueError:
            pass
    return bot.guilds[0] if bot.guilds else None


def _admin_member_ok(interaction: discord.Interaction) -> tuple[bool, str | None]:
    if not interaction.guild:
        return False, "Команду можно использовать только на сервере."
    user = interaction.user
    if not isinstance(user, discord.Member):
        return False, "Не удалось определить участника."
    if not _is_bot_admin(user):
        return (
            False,
            "Нужны права **администратора** сервера или роль из **DISCORD_ADMIN_ROLE_ID**.",
        )
    return True, None


async def read_state_copy() -> dict[str, Any]:
    async with _state_lock:
        return json.loads(json.dumps(_load_state()))


PERSONAL_REBUILD_DELAY_SEC = 4.0
_personal_rebuild_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}


def schedule_personal_list_refresh(guild_id: int, user_id: int) -> None:
    """Debounce пересборки карточек, чтобы не спамить API/Discord при серии добавлений."""
    key = (guild_id, user_id)
    task = _personal_rebuild_tasks.get(key)
    if task and not task.done():
        task.cancel()

    async def _runner() -> None:
        try:
            await asyncio.sleep(PERSONAL_REBUILD_DELAY_SEC)
            await rebuild_personal_list_display(
                bot, guild_id, user_id, session=bot.session
            )
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Отложенное обновление личного списка: %s", key)
        finally:
            cur = _personal_rebuild_tasks.get(key)
            if cur is asyncio.current_task():
                _personal_rebuild_tasks.pop(key, None)

    _personal_rebuild_tasks[key] = asyncio.create_task(_runner())


@dataclass
class _DuplicateGroup:
    thread_ids: frozenset[int]
    hints: list[tuple[str, str | int]] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)


def _collect_duplicate_groups(state: dict[str, Any]) -> list[_DuplicateGroup]:
    """Несколько тем в форуме на одно аниме (одинаковый slug YummyAnime или один mal_id)."""
    threads_raw = state.get("threads", {})
    if not isinstance(threads_raw, dict):
        return []

    slug_to: dict[str, list[int]] = defaultdict(list)
    mal_to: dict[int, list[int]] = defaultdict(list)

    for tid_s, meta in threads_raw.items():
        if not isinstance(meta, dict):
            continue
        try:
            tid = int(tid_s)
        except (TypeError, ValueError):
            continue
        slug = (meta.get("yummy_slug") or "").strip()
        if slug:
            slug_to[_clean_slug(slug)].append(tid)
        mid = meta.get("mal_id")
        if isinstance(mid, int) and mid > 0:
            mal_to[mid].append(tid)

    merged: dict[frozenset[int], _DuplicateGroup] = {}

    for slug, tids in slug_to.items():
        if len(tids) < 2:
            continue
        fs = frozenset(tids)
        if fs not in merged:
            merged[fs] = _DuplicateGroup(thread_ids=fs)
        merged[fs].hints.append(("slug", slug))
        merged[fs].labels.append(f"**YummyAnime** · `{slug}` — тем: **{len(tids)}**")

    for mid, tids in mal_to.items():
        if len(tids) < 2:
            continue
        fs = frozenset(tids)
        if fs not in merged:
            merged[fs] = _DuplicateGroup(thread_ids=fs)
        merged[fs].hints.append(("mal", mid))
        merged[fs].labels.append(f"**MAL** · id `{mid}` — тем: **{len(tids)}**")

    return list(merged.values())


def _pick_keeper_thread_id(
    group: _DuplicateGroup, topics_raw: Any
) -> int:
    tids = set(group.thread_ids)
    if not isinstance(topics_raw, dict):
        return min(tids)
    for kind, val in group.hints:
        if kind == "slug":
            key = str(val)
        elif kind == "mal":
            key = f"mal:{int(val)}"
        else:
            continue
        ent = topics_raw.get(key)
        if not isinstance(ent, dict):
            continue
        try:
            tid = int(ent["thread_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if tid in tids:
            return tid
    return min(tids)


async def purge_thread_from_state(thread_id: int) -> None:
    async with _state_lock:
        data = _load_state()
        data["threads"].pop(str(thread_id), None)
        data["ratings"].pop(str(thread_id), None)
        topics = data.setdefault("anime_topics", {})
        drop_keys = [
            k
            for k, v in topics.items()
            if isinstance(v, dict)
            and int(v.get("thread_id") or 0) == thread_id
        ]
        for k in drop_keys:
            del topics[k]
        _write_state(data)


def thread_has_rating_slot(state: dict[str, Any], thread_id: int) -> bool:
    tid = str(thread_id)
    return tid in state.get("threads", {})


async def register_anime_topic_entry(
    key: str,
    thread_id: int,
    starter_message_id: int,
    adder_id: int,
    *,
    kind: str,
    page_url: str = "",
    mal_page: str = "",
    image_notes: list[str] | None = None,
) -> None:
    async with _state_lock:
        data = _load_state()
        topics = data.setdefault("anime_topics", {})
        topics[key] = {
            "thread_id": thread_id,
            "starter_message_id": starter_message_id,
            "adders": [adder_id],
            "kind": kind,
            "page_url": page_url,
            "mal_page": mal_page,
            "image_notes": list(image_notes or []),
        }
        _write_state(data)


def _parse_adder_ids(raw: Any) -> list[int]:
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for x in raw:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


async def merge_adder_into_existing_topic(
    client: discord.Client,
    key: str,
    adder_id: int,
) -> tuple[discord.Thread | None, str]:
    """status: '' нет записи, merged, already, edit_failed, fetch_failed."""
    async with _state_lock:
        data = _load_state()
        topics = data.setdefault("anime_topics", {})
        entry = topics.get(key)
        if not isinstance(entry, dict):
            return None, ""
        cur = _parse_adder_ids(entry.get("adders"))
        thread_id = int(entry["thread_id"])
        starter_id = int(entry["starter_message_id"])
        kind = str(entry.get("kind") or "yummy")
        page_url = str(entry.get("page_url") or "")
        mal_page = str(entry.get("mal_page") or "")
        image_notes = entry.get("image_notes")
        if not isinstance(image_notes, list):
            image_notes = []
        notes_str = [str(x) for x in image_notes]

    if adder_id in cur:
        thread = client.get_channel(thread_id)
        if thread is None:
            try:
                thread = await client.fetch_channel(thread_id)
            except (discord.NotFound, discord.Forbidden):
                return None, "fetch_failed"
        if isinstance(thread, discord.Thread):
            return thread, "already"
        return None, "fetch_failed"

    new_adders = cur + [adder_id]

    thread = client.get_channel(thread_id)
    if thread is None:
        try:
            thread = await client.fetch_channel(thread_id)
        except (discord.NotFound, discord.Forbidden):
            return None, "fetch_failed"
    if not isinstance(thread, discord.Thread):
        return None, "fetch_failed"

    try:
        starter = await thread.fetch_message(starter_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None, "fetch_failed"

    if kind == "mal":
        body = _mal_thread_body(mal_page or page_url, new_adders)
    else:
        body = build_message_content(page_url, new_adders, notes_str)

    try:
        await starter.edit(
            content=_truncate(body, DISCORD_CONTENT_LIMIT),
            embeds=starter.embeds,
        )
    except discord.HTTPException:
        return None, "edit_failed"

    async with _state_lock:
        data = _load_state()
        topics = data.setdefault("anime_topics", {})
        ent = topics.get(key)
        if isinstance(ent, dict):
            ent["adders"] = new_adders
            topics[key] = ent
            _write_state(data)

    try:
        await ensure_topic_side_panels(client, thread.id)
    except Exception:
        pass
    return thread, "merged"


async def _ingest_forum_thread_from_discord(
    client: discord.Client,
    forum: discord.ForumChannel,
    thread: discord.Thread,
    session: aiohttp.ClientSession | None,
) -> bool:
    """
    Обновляет anime_topics и threads по первому сообщению темы (ссылка YummyAnime / MAL).
    """
    if thread.id == BOT_INFO_THREAD_ID:
        return False
    async with _state_lock:
        raw_info_tid = _load_state().get("meta", {}).get("bot_info_thread_id")
    try:
        if raw_info_tid is not None and thread.id == int(raw_info_tid):
            return False
    except (TypeError, ValueError):
        pass
    starter = thread.starter_message
    if starter is None:
        try:
            async for m in thread.history(limit=1, oldest_first=True):
                starter = m
                break
        except discord.HTTPException:
            return False
    if starter is None:
        return False
    blob = _starter_text_blob(starter)
    key, kind, page_url = _topic_key_from_starter_text(blob)
    if not key or kind not in ("yummy", "mal"):
        return False
    adders = _mention_ids_near_adders(starter.content or "")
    mal_page = page_url if kind == "mal" else ""
    pu = page_url if kind == "yummy" else ""
    topics_changed = False
    async with _state_lock:
        data = _load_state()
        topics = data.setdefault("anime_topics", {})
        ent = topics.get(key)
        if isinstance(ent, dict) and int(ent.get("thread_id", 0)) != thread.id:
            return False
        if isinstance(ent, dict):
            cur = _parse_adder_ids(ent.get("adders"))
            merged: list[int] = []
            for u in cur + adders:
                if u not in merged:
                    merged.append(u)
            if merged != cur or int(ent.get("starter_message_id", 0)) != starter.id:
                ent["adders"] = merged
                ent["starter_message_id"] = starter.id
                ent["thread_id"] = thread.id
                topics[key] = ent
                topics_changed = True
        else:
            topics[key] = {
                "thread_id": thread.id,
                "starter_message_id": starter.id,
                "adders": list(adders),
                "kind": kind,
                "page_url": pu,
                "mal_page": mal_page,
                "image_notes": [],
            }
            topics_changed = True
        _write_state(data)
    mid = int(key.split(":")[1]) if kind == "mal" else None
    ys = key if kind == "yummy" else None
    await register_thread_meta(
        thread.id,
        title=thread.name[:500],
        mal_id=mid,
        yummy_slug=ys,
    )
    return topics_changed


async def sync_forum_threads_with_state(
    client: discord.Client,
    guild: discord.Guild,
    session: aiohttp.ClientSession | None,
    *,
    archived_limit: int = 100,
) -> tuple[int, int]:
    """(просмотрено тем, обновлено записей)."""
    forum = await resolve_forum_channel(client)
    if not forum:
        return 0, 0
    seen: set[int] = set()
    scanned = 0
    updated = 0

    async def one(th: discord.Thread) -> None:
        nonlocal scanned, updated
        if th.id in seen or th.parent_id != forum.id:
            return
        seen.add(th.id)
        scanned += 1
        if await _ingest_forum_thread_from_discord(client, forum, th, session):
            updated += 1

    for th in forum.threads:
        await one(th)
    guild_threads = guild.threads
    th_seq = (
        guild_threads.values()
        if hasattr(guild_threads, "values")
        else guild_threads
    )
    for th in th_seq:
        if th.parent_id == forum.id:
            await one(th)
    try:
        async for th in forum.archived_threads(limit=archived_limit):
            await one(th)
    except discord.HTTPException as e:
        logger.warning("Архив форума недоступен: %s", e)
    return scanned, updated


def list_discord_added_anime_for_user(
    state: dict[str, Any], guild_id: int, user_id: int
) -> list[tuple[str, str]]:
    """(название, URL темы) для embed."""
    out: list[tuple[str, str]] = []
    uid = user_id
    topics = state.get("anime_topics", {})
    threads_raw = state.get("threads", {})
    for _key, ent in topics.items():
        if not isinstance(ent, dict):
            continue
        if uid not in _parse_adder_ids(ent.get("adders")):
            continue
        tid = int(ent.get("thread_id") or 0)
        if not tid:
            continue
        title = ""
        meta = threads_raw.get(str(tid))
        if isinstance(meta, dict):
            title = str(meta.get("title") or "").strip()
        if not title:
            title = str(_key) if not str(_key).startswith("mal:") else f"MAL {_key}"
        jump = f"https://discord.com/channels/{guild_id}/{tid}"
        out.append((title, jump))
    out.sort(key=lambda x: x[0].lower())
    return out


async def repair_single_forum_thread(
    client: discord.Client,
    forum: discord.ForumChannel,
    thread: discord.Thread,
    session: aiohttp.ClientSession | None,
) -> list[str]:
    """Добавить реакции, панели, при необходимости обновить embed YummyAnime."""
    notes: list[str] = []
    if thread.parent_id != forum.id:
        return notes
    if thread.id == BOT_INFO_THREAD_ID:
        return notes
    async with _state_lock:
        raw_info_tid = _load_state().get("meta", {}).get("bot_info_thread_id")
    try:
        if raw_info_tid is not None and thread.id == int(raw_info_tid):
            return notes
    except (TypeError, ValueError):
        pass
    await _ingest_forum_thread_from_discord(client, forum, thread, session)
    starter = thread.starter_message
    if starter is None:
        try:
            async for m in thread.history(limit=1, oldest_first=True):
                starter = m
                break
        except discord.HTTPException:
            starter = None
    if starter:
        for emoji in STATUS_REACTIONS:
            try:
                await starter.add_reaction(emoji)
            except discord.HTTPException:
                break
        if session and starter.embeds:
            blob = _starter_text_blob(starter)
            key, kind, _ = _topic_key_from_starter_text(blob)
            if kind == "yummy" and key:
                info = await api_fetch_anime(session, key)
                if info:
                    try:
                        ne = _build_embed(info)
                        await starter.edit(content=starter.content, embeds=[ne])
                        notes.append("embed")
                    except discord.HTTPException as e:
                        notes.append(f"embed:{e}")
    try:
        await ensure_topic_side_panels(client, thread.id)
        notes.append("panels")
    except Exception as e:
        notes.append(f"panels:{e}")
    return notes


def _build_bot_commands_embed() -> discord.Embed:
    e = discord.Embed(
        title="📌 YummyAnime-бот — команды",
        description="Все slash-команды ниже. Можно также писать в чат: `!aa запрос`, `!animeadd запрос`, `/aa запрос` (как текст).",
        color=EMBED_COLOR,
    )
    rows = [
        ("**Форум** `/forum_add` (`/aa`, `/animeadd`)", "Добавить аниме в **основной** форум и личный список."),
        ("**Мой список** `/mylist_show`", "Личный Discord-список (без обхода всего форума)."),
        ("`/mylist_edit` / `/mylist_top`", "Название темы и первый пост; топ-5 на карточках."),
        ("`/mylist_panel`", "Панель кнопок в личной теме (после сбоев)."),
        ("**MAL** `/mal_bind` · `/mal_import` · `/mal_show`", "Привязка, импорт в форум, просмотр списка с сайта."),
        ("**Yummy** `/yummy_link` · `/yummy_token` · `/yummy_unbind`", "Привязка (веб или токен вручную), отвязка."),
        ("`/yummy_sync` · `/yummy_list` · `/yummy_status`", "Импорт в Discord, просмотр списка API, статус привязки."),
        ("**Админ** `/mylist_admin_sync`", "Пересобрать личный список участника из тем основного форума."),
        ("`/admin` …", "`yummy_resync`, `yummy_status`, `forum_scan`, `personal_rebuild`, `repair_topics`."),
        ("`/adminpanel`", "Меню быстрых действий."),
        ("`/rateanime`", "Оценка 1–10 в теме основного форума."),
        ("`/checkduplicates`", "Дубликаты тем и удаление лишних."),
        ("`/update_topics`", "**Админы:** реакции, панели, карточка в старых темах."),
    ]
    for name, desc in rows:
        e.add_field(name=name, value=desc, inline=False)
    e.set_footer(text="Панели «Оценка» и «Рекомендация» создаются под первым сообщением новых тем автоматически.")
    return e


async def ensure_bot_info_thread(client: discord.Client) -> None:
    """Фиксированная ветка справки: BOT_INFO_THREAD_ID (редактируйте посты там вручную)."""
    async with _state_lock:
        data = _load_state()
        data.setdefault("meta", {})["bot_info_thread_id"] = BOT_INFO_THREAD_ID
        _write_state(data)
    ch = client.get_channel(BOT_INFO_THREAD_ID)
    if ch is None:
        try:
            ch = await client.fetch_channel(BOT_INFO_THREAD_ID)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            ch = None
    if not isinstance(ch, discord.Thread):
        logger.warning(
            "Справочная ветка %s не найдена — создайте её или проверьте ID и права бота.",
            BOT_INFO_THREAD_ID,
        )
        return
    if ch.archived:
        logger.info("Справочная ветка %s в архиве — разархивируйте при необходимости.", BOT_INFO_THREAD_ID)


async def resolve_forum_channel(client: discord.Client) -> discord.ForumChannel | None:
    ch = client.get_channel(FORUM_CHANNEL_ID)
    if ch is None:
        try:
            ch = await client.fetch_channel(FORUM_CHANNEL_ID)
        except (discord.NotFound, discord.Forbidden):
            return None
    return ch if isinstance(ch, discord.ForumChannel) else None


async def resolve_list_forum_channel(client: discord.Client) -> discord.ForumChannel | None:
    ch = client.get_channel(LIST_FORUM_CHANNEL_ID)
    if ch is None:
        try:
            ch = await client.fetch_channel(LIST_FORUM_CHANNEL_ID)
        except (discord.NotFound, discord.Forbidden):
            return None
    return ch if isinstance(ch, discord.ForumChannel) else None


def _title_for_list_key(state: dict[str, Any], key: str) -> str:
    st = state.get("slug_titles", {})
    if isinstance(st, dict):
        t = (st.get(key) or "").strip()
        if t:
            return t
    topics = state.get("anime_topics", {})
    ent = topics.get(key) if isinstance(topics, dict) else None
    if isinstance(ent, dict):
        tid = str(ent.get("thread_id") or "")
        meta = state.get("threads", {}).get(tid)
        if isinstance(meta, dict):
            tt = (meta.get("title") or "").strip()
            if tt:
                return tt
    return key if not str(key).startswith("mal:") else f"MAL {key}"


def _jump_for_list_key(state: dict[str, Any], guild_id: int, key: str) -> str:
    topics = state.get("anime_topics", {})
    ent = topics.get(key) if isinstance(topics, dict) else None
    if isinstance(ent, dict):
        tid = int(ent.get("thread_id") or 0)
        if tid:
            return f"https://discord.com/channels/{guild_id}/{tid}"
    if str(key).startswith("mal:"):
        rest = key.split(":", 1)[-1]
        try:
            mid = int(rest)
            return f"https://myanimelist.net/anime/{mid}"
        except ValueError:
            pass
    return f"{BASE}/catalog/item/{_clean_slug(key)}"


def _ordered_keys_for_personal(pl: dict[str, Any]) -> list[str]:
    order = pl.get("order")
    if not isinstance(order, list):
        order = []
    top5 = pl.get("top5")
    if not isinstance(top5, list):
        top5 = []
    seen: set[str] = set()
    out: list[str] = []
    for k in top5:
        ks = str(k).strip()
        if ks and ks in order and ks not in seen:
            seen.add(ks)
            out.append(ks)
    for k in order:
        ks = str(k).strip()
        if ks and ks not in seen:
            seen.add(ks)
            out.append(ks)
    return out


async def apply_personal_list_permissions(
    thread: discord.Thread, guild: discord.Guild, owner_id: int
) -> None:
    """Только владелец списка и бот могут писать в личной теме."""
    everyone = guild.default_role
    over_everyone = discord.PermissionOverwrite(
        send_messages=False,
        add_reactions=True,
        read_message_history=True,
        view_channel=True,
    )
    over_owner = discord.PermissionOverwrite(
        send_messages=True,
        add_reactions=True,
        read_message_history=True,
        view_channel=True,
    )
    me = guild.me
    if me:
        over_bot = discord.PermissionOverwrite(
            send_messages=True,
            manage_messages=True,
            embed_links=True,
            attach_files=True,
            read_message_history=True,
            view_channel=True,
        )
        try:
            await thread.set_permissions(me, overwrite=over_bot)
        except discord.HTTPException:
            pass
    try:
        await thread.set_permissions(everyone, overwrite=over_everyone)
    except discord.HTTPException:
        pass
    owner = guild.get_member(owner_id)
    if owner:
        try:
            await thread.set_permissions(owner, overwrite=over_owner)
        except discord.HTTPException:
            pass


async def _get_list_thread_starter_message(thread: discord.Thread) -> discord.Message | None:
    starter = thread.starter_message
    if starter is not None:
        return starter
    try:
        async for m in thread.history(limit=1, oldest_first=True):
            return m
    except discord.HTTPException:
        return None
    return None


def _owner_id_from_list_starter_message(message: discord.Message | None) -> int | None:
    """Владелец личного списка — первое не-бот упоминание в стартовом сообщении темы."""
    if message is None:
        return None
    for u in message.mentions:
        if not u.bot:
            return u.id
    for m in re.finditer(r"<@!?(\d+)>", message.content or ""):
        try:
            uid = int(m.group(1))
        except ValueError:
            continue
        if uid > 0:
            return uid
    return None


async def persist_personal_thread_binding(
    owner_id: int,
    thread: discord.Thread,
    starter: discord.Message | None,
) -> None:
    """Сохраняет thread_id и starter_message_id для личного списка (привязка темы)."""
    async with _state_lock:
        data = _load_state()
        uid = str(owner_id)
        pl = data.setdefault("personal_lists", {}).setdefault(uid, {})
        pl["thread_id"] = thread.id
        if starter is not None:
            pl["starter_message_id"] = starter.id
        pl.setdefault("anime_messages", {})
        pl.setdefault("order", [])
        pl.setdefault("top5", [])
        pl.setdefault("accent_color", EMBED_COLOR)
        pl.setdefault("show_numbers", False)
        pl.setdefault("compact_cards", False)
        data["personal_lists"][uid] = pl
        _write_state(data)


PERSONAL_ACCENT_PALETTE: tuple[int, ...] = (
    0xE67E22,
    0x9B59B6,
    0x3498DB,
    0x1ABC9C,
    0xE74C3C,
    0x2ECC71,
    0xF1C40F,
    0xE91E63,
)


async def resolve_personal_list_owner_for_interaction(
    interaction: discord.Interaction,
) -> tuple[int, dict[str, Any]] | None:
    """
    Владелец темы из state или авто-привязка по @ в первом сообщении темы (форум личных списков).
    При ошибках отправляет ephemeral и возвращает None.
    """
    ch = interaction.channel
    if not isinstance(ch, discord.Thread):
        await interaction.response.send_message(
            "Панель работает только в **личной теме** списка.", ephemeral=True
        )
        return None
    if ch.parent_id != LIST_FORUM_CHANNEL_ID:
        await interaction.response.send_message(
            "Это не форум **личных списков**. Откройте свою тему там, где канал личных списков "
            f"(id `{LIST_FORUM_CHANNEL_ID}`), а не основной форум с аниме.",
            ephemeral=True,
        )
        return None

    state = await read_state_copy()
    oid = _list_owner_id_by_thread_id(state, ch.id)
    if oid is not None:
        pl = (state.get("personal_lists") or {}).get(str(oid))
        if isinstance(pl, dict):
            return oid, pl
        await interaction.response.send_message("Нет данных списка.", ephemeral=True)
        return None

    starter = await _get_list_thread_starter_message(ch)
    inferred = _owner_id_from_list_starter_message(starter)
    if inferred is None:
        await interaction.response.send_message(
            "Тема не была в базе бота. В **самом первом** сообщении темы должен быть **ваш @ник** "
            "(как когда бот создаёт тему: «@вы — личный список…»). "
            "Добавьте себя в начало первого поста и снова нажмите кнопку или **Синхронизировать**.",
            ephemeral=True,
        )
        return None
    if interaction.user.id != inferred:
        await interaction.response.send_message(
            f"По первому сообщению тема для <@{inferred}>. Войдите с того аккаунта или попросите владельца.",
            ephemeral=True,
        )
        return None

    await persist_personal_thread_binding(inferred, ch, starter)
    state2 = await read_state_copy()
    pl2 = (state2.get("personal_lists") or {}).get(str(inferred))
    if not isinstance(pl2, dict):
        await interaction.response.send_message(
            "Не удалось сохранить привязку темы.", ephemeral=True
        )
        return None
    return inferred, pl2


class PersonalTopicHubView(discord.ui.View):
    """Постоянные кнопки панели (custom_id фиксированы — работают после перезапуска бота)."""

    def __init__(self) -> None:
        super().__init__(timeout=None)

    async def _resolve_owner(
        self, interaction: discord.Interaction
    ) -> tuple[int, dict[str, Any]] | None:
        return await resolve_personal_list_owner_for_interaction(interaction)

    @discord.ui.button(
        label="Обновить",
        style=discord.ButtonStyle.primary,
        emoji="🔄",
        custom_id="plist:hub:ref",
        row=0,
    )
    async def hub_refresh(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, _pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Только **владелец** списка может обновлять карточки.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        client = interaction.client
        sess = getattr(client, "session", None)
        try:
            await rebuild_personal_list_display(
                client, interaction.guild.id, owner_id, session=sess
            )
        except Exception:
            logger.exception("plist hub refresh")
            await interaction.followup.send("Ошибка при обновлении.", ephemeral=True)
            return
        await interaction.followup.send("Карточки пересобраны.", ephemeral=True)

    @discord.ui.button(
        label="Тема",
        style=discord.ButtonStyle.secondary,
        emoji="🎨",
        custom_id="plist:hub:accent",
        row=0,
    )
    async def hub_accent(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Только **владелец** может менять тему.", ephemeral=True
            )
            return
        cur = int(pl.get("accent_color") or EMBED_COLOR)
        try:
            idx = PERSONAL_ACCENT_PALETTE.index(cur)
        except ValueError:
            idx = -1
        nxt = PERSONAL_ACCENT_PALETTE[(idx + 1) % len(PERSONAL_ACCENT_PALETTE)]
        await _set_personal_list_fields(owner_id, accent_color=nxt)
        st = await read_state_copy()
        pl2 = (st.get("personal_lists") or {}).get(str(owner_id), pl)
        mem = interaction.guild.get_member(owner_id) if interaction.guild else None
        dn = mem.display_name if mem else str(owner_id)
        hub_embed = _personal_hub_embed(pl2 if isinstance(pl2, dict) else pl, dn)
        try:
            await interaction.response.edit_message(
                embed=hub_embed, view=PersonalTopicHubView()
            )
            await interaction.followup.send(
                f"Акцент `#{nxt:06x}`. Нажми **Обновить**, чтобы перекрасить карточки.",
                ephemeral=True,
            )
        except discord.HTTPException:
            await interaction.response.send_message(
                f"Цвет карточек: `#{nxt:06x}`. Нажми **Обновить**, чтобы применить к аниме.",
                ephemeral=True,
            )

    @discord.ui.button(
        label="Нумерация",
        style=discord.ButtonStyle.secondary,
        emoji="🔢",
        custom_id="plist:hub:num",
        row=0,
    )
    async def hub_numbers(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Только **владелец** может менять отображение.", ephemeral=True
            )
            return
        new_val = not bool(pl.get("show_numbers"))
        await _set_personal_list_fields(owner_id, show_numbers=new_val)
        await interaction.response.send_message(
            f"Нумерация карточек: **{'вкл.' if new_val else 'выкл.'}** "
            "— нажми **Обновить**, чтобы применить.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Компакт",
        style=discord.ButtonStyle.secondary,
        emoji="📦",
        custom_id="plist:hub:cmp",
        row=1,
    )
    async def hub_compact(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Только **владелец** может менять вид.", ephemeral=True
            )
            return
        new_val = not bool(pl.get("compact_cards"))
        await _set_personal_list_fields(owner_id, compact_cards=new_val)
        await interaction.response.send_message(
            f"Компактные карточки: **{'вкл.' if new_val else 'выкл.'}** "
            "— нажми **Обновить**.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Статистика",
        style=discord.ButtonStyle.success,
        emoji="📊",
        custom_id="plist:hub:stats",
        row=1,
    )
    async def hub_stats(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        state = await read_state_copy()
        keys = _ordered_keys_for_personal(pl)
        top5 = pl.get("top5") if isinstance(pl.get("top5"), list) else []
        accent = int(pl.get("accent_color") or EMBED_COLOR)
        rated_n = sum(
            1
            for k in keys
            if _user_thread_rating_for_key(state, owner_id, k) is not None
        )
        lines = [
            f"**Всего тайтлов:** {len(keys)}",
            f"**С вашей оценкой в темах:** {rated_n}",
            f"**В топе (слоты):** {len([x for x in top5 if str(x).strip() in keys])}",
            f"**Акцент:** `#{accent:06x}`",
            f"**Нумерация:** {'да' if pl.get('show_numbers') else 'нет'}",
            f"**Компакт:** {'да' if pl.get('compact_cards') else 'нет'}",
        ]
        await interaction.response.send_message(
            "\n".join(lines), ephemeral=True
        )

    @discord.ui.button(
        label="Справка",
        style=discord.ButtonStyle.secondary,
        emoji="❓",
        custom_id="plist:hub:help",
        row=1,
    )
    async def hub_help(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        _, _pl = resolved
        text = (
            "**Личный топик**\n"
            "· Карточки подтягиваются из основного форума; средняя оценка — с YummyAnime или MAL.\n"
            "· **Ваша оценка** — из панели «Оценить» в **теме этого аниме** на основном форуме.\n"
            "· `/mylist_top` — закрепить топ-5 (звезда на карточке).\n"
            "· `/mylist_edit` — название темы и первый пост.\n"
            "· **Синхронизировать** — как `/syncanimelist` для вас: парсинг основного форума + список в теме.\n"
            "· **Yummy ↻** — подтянуть новые тайтлы с YummyAnime (нужны `/yummy_link` или `/yummy_token` и токен приложения у бота).\n"
            "· **Экспорт** — Markdown-список с ссылками (только вам).\n"
            "· `/mylist_panel` — восстановить панель после сбоев.\n"
            "· Jikan — публичный API; при лимитах постер MAL может не подгрузиться.\n"
        )
        await interaction.response.send_message(text, ephemeral=True)

    @discord.ui.button(
        label="Синхронизировать",
        style=discord.ButtonStyle.success,
        emoji="🔗",
        custom_id="plist:hub:sync",
        row=2,
    )
    async def hub_sync(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, _pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Синхронизация только для **владельца** списка.", ephemeral=True
            )
            return
        if not interaction.guild:
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        client = interaction.client
        sess = getattr(client, "session", None)
        try:
            scanned, updated = await sync_forum_threads_with_state(
                client, interaction.guild, sess
            )
        except Exception:
            logger.exception("plist hub sync forum")
            await interaction.followup.send(
                "Ошибка при обходе основного форума.", ephemeral=True
            )
            return
        n, err = await sync_personal_list_from_anime_topics(
            interaction.guild, owner_id
        )
        try:
            await rebuild_personal_list_display(
                client, interaction.guild.id, owner_id, session=sess
            )
        except Exception:
            logger.exception("plist hub sync rebuild")
            await interaction.followup.send(
                "Список обновлён в базе, но не удалось пересобрать карточки. Нажми **Обновить**.",
                ephemeral=True,
            )
            return
        parts = [
            f"**Основной форум:** просмотрено веток **{scanned}**, записей **{updated}**.",
            f"**Ваш личный список:** **{n}** позиций, карточки пересобраны.",
        ]
        if err:
            parts.append(str(err))
        await interaction.followup.send(
            _truncate("\n".join(parts), DISCORD_CONTENT_LIMIT),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Yummy ↻",
        style=discord.ButtonStyle.primary,
        emoji="🍱",
        custom_id="plist:hub:yummy",
        row=2,
    )
    async def hub_yummy_sync(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, _pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Только **владелец** может синхронизировать YummyAnime.", ephemeral=True
            )
            return
        if not interaction.guild:
            return
        app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
        if not app:
            await interaction.response.send_message(
                "Синхронизация Yummy отключена: нет **YUMMY_APPLICATION_TOKEN** на стороне бота.",
                ephemeral=True,
            )
            return
        sess = getattr(interaction.client, "session", None)
        if not sess:
            await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            r = await run_yummy_list_import_for_member(
                interaction.guild,
                owner_id,
                list_filter="all",
                max_topics=25,
                session=sess,
                app_token=app,
            )
        except Exception:
            logger.exception("plist hub yummy sync")
            await interaction.followup.send(
                "Ошибка при синхронизации YummyAnime.", ephemeral=True
            )
            return
        if r.get("error"):
            await interaction.followup.send(
                _truncate(str(r["error"]), DISCORD_CONTENT_LIMIT), ephemeral=True
            )
            return
        lines = [
            f"**Новых тем:** **{r.get('n_new', 0)}**",
            f"**Дописано в существующие:** **{r.get('merge_ops', 0)}**",
        ]
        cr = r.get("created_urls") or []
        if cr:
            lines.append("Новые: " + ", ".join(cr[:6]))
            if len(cr) > 6:
                lines.append(f"_…ещё {len(cr) - 6}_")
        mer = r.get("merged_urls") or []
        if mer:
            lines.append("Объединено: " + ", ".join(mer[:4]))
        er = r.get("errors") or []
        if er:
            lines.append("Замечания: " + "; ".join(er[:3]))
        await interaction.followup.send(
            _truncate("\n".join(lines), DISCORD_CONTENT_LIMIT), ephemeral=True
        )

    @discord.ui.button(
        label="Экспорт",
        style=discord.ButtonStyle.secondary,
        emoji="📤",
        custom_id="plist:hub:exp",
        row=2,
    )
    async def hub_export(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Экспорт доступен только **владельцу** списка.", ephemeral=True
            )
            return
        if not interaction.guild:
            return
        state = await read_state_copy()
        lines: list[str] = []
        for i, k in enumerate(_ordered_keys_for_personal(pl), 1):
            t = _title_for_list_key(state, k)
            u = _jump_for_list_key(state, interaction.guild.id, k)
            lines.append(f"{i}. [{t}]({u})")
        body = "\n".join(lines) if lines else "_пусто_"
        body = _truncate(body, 1900)
        await interaction.response.send_message(
            f"**Markdown** (можно скопировать):\n```md\n{body}\n```",
            ephemeral=True,
        )


def _list_owner_id_by_thread_id(state: dict[str, Any], thread_id: int) -> int | None:
    for uid_s, pl in (state.get("personal_lists") or {}).items():
        if not isinstance(pl, dict):
            continue
        try:
            if int(pl.get("thread_id") or 0) == thread_id:
                return int(uid_s)
        except (TypeError, ValueError):
            continue
    return None


def _user_thread_rating_for_key(
    state: dict[str, Any], user_id: int, anime_key: str
) -> int | None:
    topics = state.get("anime_topics", {})
    ent = topics.get(anime_key) if isinstance(topics, dict) else None
    if not isinstance(ent, dict):
        return None
    tid = str(ent.get("thread_id") or "")
    if not tid:
        return None
    raw = state.get("ratings", {}).get(tid, {})
    if not isinstance(raw, dict):
        return None
    sc = raw.get(str(user_id))
    try:
        n = int(sc)
    except (TypeError, ValueError):
        return None
    if 1 <= n <= 10:
        return n
    return None


async def _fetch_personal_card_meta(
    session: aiohttp.ClientSession | None,
    state: dict[str, Any],
    anime_key: str,
) -> dict[str, Any]:
    """title, poster_url, page_url, global_score (str|None), source."""
    key = str(anime_key).strip()
    fallback_title = _title_for_list_key(state, key)
    if key.startswith("mal:"):
        rest = key.split(":", 1)[-1]
        try:
            mid = int(rest)
        except ValueError:
            return {
                "title": fallback_title,
                "poster_url": None,
                "page_url": f"https://myanimelist.net/anime/{rest}",
                "global_score": None,
                "source": "mal",
            }
        if session:
            j = await jikan_fetch_anime(session, mid)
            if j:
                return j
        return {
            "title": fallback_title,
            "poster_url": None,
            "page_url": f"https://myanimelist.net/anime/{mid}",
            "global_score": None,
            "source": "mal",
        }
    if not session:
        return {
            "title": fallback_title,
            "poster_url": None,
            "page_url": f"{BASE}/catalog/item/{_clean_slug(key)}",
            "global_score": None,
            "source": "yummy",
        }
    info = await api_fetch_anime(session, _clean_slug(key))
    if not info:
        return {
            "title": fallback_title,
            "poster_url": None,
            "page_url": f"{BASE}/catalog/item/{_clean_slug(key)}",
            "global_score": None,
            "source": "yummy",
        }
    rt = info.get("rating_avg")
    gs = f"{float(rt):.2f}" if isinstance(rt, (int, float)) else None
    return {
        "title": (info.get("title") or fallback_title).strip(),
        "poster_url": info.get("poster_url"),
        "page_url": info.get("page_url") or f"{BASE}/catalog/item/{key}",
        "global_score": gs,
        "source": "yummy",
    }


def _build_personal_anime_card_embed(
    state: dict[str, Any],
    guild_id: int,
    owner_id: int,
    anime_key: str,
    *,
    display_index: int,
    in_top: bool,
    meta: dict[str, Any],
    accent: int,
    compact: bool,
    show_numbers: bool,
) -> discord.Embed:
    jump = _jump_for_list_key(state, guild_id, anime_key)
    title = (meta.get("title") or _title_for_list_key(state, anime_key)).strip()
    prefix = f"`#{display_index}` · " if show_numbers else ""
    top_badge = "⭐ **В вашем топе** · " if in_top else ""
    embed = discord.Embed(
        title=_truncate(f"{prefix}{top_badge}{title}", 256),
        url=jump,
        color=accent,
    )
    poster = meta.get("poster_url")
    if isinstance(poster, str) and poster.startswith("http"):
        embed.set_image(url=poster)

    src = meta.get("source") or "yummy"
    src_label = "YummyAnime" if src == "yummy" else ("MyAnimeList" if src == "mal" else "—")

    gscore = meta.get("global_score")
    global_line = f"**{gscore}**/10" if gscore else "_нет данных_"

    ur = _user_thread_rating_for_key(state, owner_id, anime_key)
    if ur is not None:
        user_line = f"**{ur}**/10"
    else:
        user_line = "_не ставили в теме основного форума_"

    if compact:
        embed.description = _truncate(
            f"📊 {src_label}: {global_line} · Ваша оценка: {user_line}",
            EMBED_DESC_LIMIT,
        )
    else:
        embed.add_field(name="📊 Средняя оценка", value=global_line, inline=True)
        embed.add_field(name="✏️ Ваша оценка", value=user_line, inline=True)
        embed.add_field(name="🔗 Источник", value=src_label, inline=True)
        embed.set_footer(text="Нажмите заголовок — открыть ветку или страницу")

    return embed


def _personal_hub_embed(pl: dict[str, Any], display_name: str) -> discord.Embed:
    accent = int(pl.get("accent_color") or EMBED_COLOR)
    if accent < 0 or accent > 0xFFFFFF:
        accent = EMBED_COLOR
    nums = "вкл." if pl.get("show_numbers") else "выкл."
    comp = "вкл." if pl.get("compact_cards") else "выкл."
    e = discord.Embed(
        title="🎛️ Панель топика",
        description=(
            f"**{display_name}** — настройки и действия.\n\n"
            "· **Синхронизировать** — обход **основного** форума аниме + пересбор вашего списка и карточек.\n"
            "· **Обновить** — только пересобрать карточки из уже сохранённых данных.\n"
            "· **Тема** — цвет карточек.\n"
            "· **# Нумерация** — порядковые номера в заголовках.\n"
            "· **Компакт** — короткий вид карточек.\n"
            "· **Статистика** / **Справка** / **Экспорт** — сводка и Markdown.\n\n"
            f"_Сейчас: нумерация **{nums}**, компакт **{comp}**._"
        ),
        color=accent,
    )
    e.set_footer(text="Только владелец может менять настройки и обновлять карточки.")
    return e


async def _save_personal_thread_meta(
    user_id: int,
    *,
    thread_id: int,
    starter_message_id: int,
    control_message_id: int | None = None,
) -> None:
    async with _state_lock:
        data = _load_state()
        uid = str(user_id)
        pl = data.setdefault("personal_lists", {}).setdefault(uid, {})
        pl["thread_id"] = thread_id
        pl["starter_message_id"] = starter_message_id
        if control_message_id is not None:
            pl["control_message_id"] = control_message_id
        pl.setdefault("anime_messages", {})
        pl.setdefault("accent_color", EMBED_COLOR)
        pl.setdefault("show_numbers", False)
        pl.setdefault("compact_cards", False)
        data["personal_lists"][uid] = pl
        _write_state(data)


async def _set_personal_list_fields(user_id: int, **fields: Any) -> None:
    async with _state_lock:
        data = _load_state()
        uid = str(user_id)
        pl = data.setdefault("personal_lists", {}).setdefault(uid, {})
        for k, v in fields.items():
            pl[k] = v
        data["personal_lists"][uid] = pl
        _write_state(data)


async def rebuild_personal_list_display(
    client: discord.Client,
    guild_id: int,
    user_id: int,
    *,
    session: aiohttp.ClientSession | None,
) -> None:
    """Удаляет старые карточки, шлёт новые (1 аниме = 1 сообщение), обновляет панель."""
    state = await read_state_copy()
    uid_s = str(user_id)
    pl = (state.get("personal_lists") or {}).get(uid_s)
    if not isinstance(pl, dict):
        return
    try:
        tid = int(pl.get("thread_id") or 0)
    except (TypeError, ValueError):
        return
    if not tid:
        return

    guild = client.get_guild(guild_id)
    if guild is None:
        try:
            guild = await client.fetch_guild(guild_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return
    member = guild.get_member(user_id)
    display_name = member.display_name if member else str(user_id)

    thread = client.get_channel(tid)
    if thread is None:
        try:
            thread = await client.fetch_channel(tid)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return
    if not isinstance(thread, discord.Thread):
        return

    # миграция: одно старое embed-сообщение
    leg_mid = pl.get("list_message_id")
    if leg_mid and not pl.get("control_message_id"):
        try:
            lm = await thread.fetch_message(int(leg_mid))
            await lm.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
        async with _state_lock:
            data = _load_state()
            pl2 = data.setdefault("personal_lists", {}).setdefault(uid_s, {})
            pl2.pop("list_message_id", None)
            data["personal_lists"][uid_s] = pl2
            _write_state(data)
        pl = (await read_state_copy()).get("personal_lists", {}).get(uid_s, pl)

    am_raw = pl.get("anime_messages")
    if not isinstance(am_raw, dict):
        am_raw = {}
    old_ids = []
    for _k, mid in am_raw.items():
        try:
            old_ids.append(int(mid))
        except (TypeError, ValueError):
            continue
    for mid in old_ids:
        try:
            m = await thread.fetch_message(mid)
            await m.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
        await asyncio.sleep(0.05)

    hub_view = PersonalTopicHubView()
    ctrl_id = pl.get("control_message_id")
    hub_embed = _personal_hub_embed(pl, display_name)
    if ctrl_id:
        try:
            hub_msg = await thread.fetch_message(int(ctrl_id))
            await hub_msg.edit(embed=hub_embed, view=hub_view)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            try:
                hub_msg = await thread.send(embed=hub_embed, view=hub_view)
            except discord.HTTPException:
                hub_msg = None
            if hub_msg:
                await _set_personal_list_fields(user_id, control_message_id=hub_msg.id)
    else:
        try:
            hub_msg = await thread.send(embed=hub_embed, view=hub_view)
        except discord.HTTPException:
            hub_msg = None
        if hub_msg:
            await _set_personal_list_fields(user_id, control_message_id=hub_msg.id)

    pl = (await read_state_copy()).get("personal_lists", {}).get(uid_s, pl)
    if not isinstance(pl, dict):
        return

    accent = int(pl.get("accent_color") or EMBED_COLOR)
    if accent < 0 or accent > 0xFFFFFF:
        accent = EMBED_COLOR
    compact = bool(pl.get("compact_cards"))
    show_numbers = bool(pl.get("show_numbers"))

    top5_raw = pl.get("top5") if isinstance(pl.get("top5"), list) else []
    top5_set = {str(x).strip() for x in top5_raw if str(x).strip()}

    keys = _ordered_keys_for_personal(pl)
    new_map: dict[str, int] = {}

    st_cards = await read_state_copy()
    for i, anime_key in enumerate(keys, start=1):
        meta = await _fetch_personal_card_meta(session, st_cards, anime_key)
        emb = _build_personal_anime_card_embed(
            st_cards,
            guild_id,
            user_id,
            anime_key,
            display_index=i,
            in_top=anime_key in top5_set,
            meta=meta,
            accent=accent,
            compact=compact,
            show_numbers=show_numbers,
        )
        try:
            msg = await thread.send(embed=emb)
            new_map[anime_key] = msg.id
        except discord.HTTPException as e:
            logger.warning("Карточка списка %s: %s", anime_key, e)
        await asyncio.sleep(0.35)

    async with _state_lock:
        data = _load_state()
        pl3 = data.setdefault("personal_lists", {}).setdefault(uid_s, {})
        pl3["anime_messages"] = new_map
        data["personal_lists"][uid_s] = pl3
        _write_state(data)


async def ensure_personal_list_thread(
    client: discord.Client,
    guild: discord.Guild,
    member: discord.Member,
    *,
    session: aiohttp.ClientSession | None,
) -> discord.Thread | None:
    """Одна тема LIST_FORUM на пользователя; защита от гонок и «битого» thread_id в базе."""
    uid = member.id
    async with _personal_list_thread_lock(uid):
        async with _state_lock:
            data = _load_state()
            pl_raw = (data.get("personal_lists") or {}).get(str(uid))
            existing_id = pl_raw.get("thread_id") if isinstance(pl_raw, dict) else None

        if existing_id:
            try:
                eid = int(existing_id)
            except (TypeError, ValueError):
                eid = 0
            if eid:
                ch = client.get_channel(eid)
                if ch is None:
                    try:
                        ch = await client.fetch_channel(eid)
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        ch = None
                if isinstance(ch, discord.Thread) and ch.parent_id == LIST_FORUM_CHANNEL_ID:
                    await apply_personal_list_permissions(ch, guild, uid)
                    return ch
                async with _state_lock:
                    data = _load_state()
                    pl = data.setdefault("personal_lists", {}).setdefault(str(uid), {})
                    for k in (
                        "thread_id",
                        "starter_message_id",
                        "control_message_id",
                        "list_message_id",
                    ):
                        pl.pop(k, None)
                    data["personal_lists"][str(uid)] = pl
                    _write_state(data)

        async with _state_lock:
            data = _load_state()
            pl_raw2 = (data.get("personal_lists") or {}).get(str(uid))
            again = pl_raw2.get("thread_id") if isinstance(pl_raw2, dict) else None
        if again:
            try:
                eid2 = int(again)
            except (TypeError, ValueError):
                eid2 = 0
            if eid2:
                ch2 = client.get_channel(eid2)
                if ch2 is None:
                    try:
                        ch2 = await client.fetch_channel(eid2)
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        ch2 = None
                if isinstance(ch2, discord.Thread) and ch2.parent_id == LIST_FORUM_CHANNEL_ID:
                    await apply_personal_list_permissions(ch2, guild, uid)
                    return ch2

        forum = await resolve_list_forum_channel(client)
        if not forum:
            logger.warning("Канал личных списков %s не найден.", LIST_FORUM_CHANNEL_ID)
            return None

        name = f"{member.display_name} anime list"[:100]
        intro = (
            f"{member.mention} — **личный список аниме**.\n\n"
            "Название темы и текст: `/mylist_edit` · топ-5: `/mylist_top`\n"
            "Панель **кнопок** под этим сообщением — тема, нумерация, обновление карточек.\n\n"
            "_Ниже — по одному сообщению на каждое аниме (обложка, оценки)._"
        )
        try:
            twm = await forum.create_thread(name=name, content=intro)
            thread = twm.thread
            starter = twm.message
        except discord.Forbidden:
            logger.warning("Нет прав создавать темы в форуме личных списков.")
            return None
        except discord.HTTPException as e:
            logger.warning("Создание личной темы: %s", e)
            return None

        stub_pl = {
            "accent_color": EMBED_COLOR,
            "show_numbers": False,
            "compact_cards": False,
        }
        hub_embed = _personal_hub_embed(stub_pl, member.display_name)
        hub_view = PersonalTopicHubView()
        try:
            hub_msg = await thread.send(embed=hub_embed, view=hub_view)
        except discord.HTTPException:
            hub_msg = None

        await apply_personal_list_permissions(thread, guild, uid)

        if starter and hub_msg:
            await _save_personal_thread_meta(
                uid,
                thread_id=thread.id,
                starter_message_id=starter.id,
                control_message_id=hub_msg.id,
            )
        return thread


async def append_user_anime_to_personal_state(
    guild: discord.Guild,
    user_id: int,
    key: str,
    title: str,
) -> None:
    """Добавляет ключ в порядок списка и кэш названий; затем обновляет сообщение в личной теме."""
    key = str(key).strip()
    if not key:
        return
    title = (title or "").strip() or _title_for_list_key(await read_state_copy(), key)
    async with _state_lock:
        data = _load_state()
        uid = str(user_id)
        pl = data.setdefault("personal_lists", {}).setdefault(uid, {})
        order = pl.get("order")
        if not isinstance(order, list):
            order = []
        if key not in order:
            order.append(key)
        pl["order"] = order
        st = data.setdefault("slug_titles", {})
        st[key] = title[:500]
        data["slug_titles"] = st
        data["personal_lists"][uid] = pl
        _write_state(data)

    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logger.warning("Не удалось получить участника %s для личного списка.", user_id)
            return
    await ensure_personal_list_thread(bot, guild, member, session=bot.session)
    schedule_personal_list_refresh(guild.id, user_id)


def list_personal_anime_pairs(
    state: dict[str, Any], guild_id: int, user_id: int
) -> list[tuple[str, str]]:
    uid_s = str(user_id)
    pl = (state.get("personal_lists") or {}).get(uid_s)
    if not isinstance(pl, dict):
        return []
    out: list[tuple[str, str]] = []
    for k in _ordered_keys_for_personal(pl):
        t = _title_for_list_key(state, k)
        u = _jump_for_list_key(state, guild_id, k)
        out.append((t, u))
    return out


async def sync_personal_list_from_anime_topics(
    guild: discord.Guild, target_id: int
) -> tuple[int, str | None]:
    """
    Строит order из anime_topics (где пользователь в adders). Возвращает (число ключей, ошибка).
    """
    async with _state_lock:
        data = _load_state()
        topics = data.get("anime_topics", {})
        if not isinstance(topics, dict):
            return 0, "Нет данных anime_topics."
        keys: list[str] = []
        for key, ent in topics.items():
            if not isinstance(ent, dict):
                continue
            if target_id not in _parse_adder_ids(ent.get("adders")):
                continue
            ks = str(key).strip()
            if ks:
                keys.append(ks)
        keys.sort(key=lambda x: _title_for_list_key(data, x).lower())
        uid = str(target_id)
        pl = data.setdefault("personal_lists", {}).setdefault(uid, {})
        old_top = pl.get("top5")
        if not isinstance(old_top, list):
            old_top = []
        new_top = [str(x).strip() for x in old_top if str(x).strip() in keys][:5]
        pl["order"] = keys
        pl["top5"] = new_top
        data["personal_lists"][uid] = pl
        # обновить кэш названий из threads
        threads_raw = data.get("threads", {})
        st = data.setdefault("slug_titles", {})
        for k in keys:
            ent = topics.get(k)
            if not isinstance(ent, dict):
                continue
            tid = str(ent.get("thread_id") or "")
            meta = threads_raw.get(tid) if isinstance(threads_raw, dict) else None
            if isinstance(meta, dict):
                tt = (meta.get("title") or "").strip()
                if tt:
                    st[k] = tt[:500]
        data["slug_titles"] = st
        _write_state(data)
    return len(keys), None


async def run_yummy_list_import_for_member(
    guild: discord.Guild,
    discord_user_id: int,
    *,
    list_filter: str,
    max_topics: int,
    session: aiohttp.ClientSession,
    app_token: str,
) -> dict[str, Any]:
    """Новые позиции из API YummyAnime → основной форум и личный список."""
    empty: dict[str, Any] = {
        "ok": False,
        "error": None,
        "n_new": 0,
        "merge_ops": 0,
        "merged_urls": [],
        "created_urls": [],
        "errors": [],
    }
    state = await read_state_copy()
    acc = (state.get("yummy_accounts") or {}).get(str(discord_user_id))
    if not isinstance(acc, dict):
        empty["error"] = "Аккаунт YummyAnime не привязан (`/yummy_link` или `/yummy_token`)."
        return empty
    yuid = acc.get("yummy_user_id")
    bearer = (acc.get("access_token") or "").strip()
    if yuid is None or not bearer:
        empty["error"] = "Неполная привязка YummyAnime."
        return empty
    try:
        yuid_i = int(yuid)
    except (TypeError, ValueError):
        empty["error"] = "Некорректный yummy_user_id в базе."
        return empty

    items, new_tok, err_msg = await yummy_api.yani_fetch_lists_with_token_refresh(
        session, app_token, bearer, yuid_i, USER_AGENT
    )
    if err_msg:
        empty["error"] = err_msg
        return empty
    if new_tok:
        await update_yummy_access_token(discord_user_id, new_tok)

    entries = yummy_api.filter_yummy_entries_by_status(items or [], list_filter)

    raw_imp = state.get("imported_yummy", {}).get(str(discord_user_id), [])
    imported_ids: set[int] = set()
    if isinstance(raw_imp, list):
        for x in raw_imp:
            try:
                imported_ids.add(int(x))
            except (TypeError, ValueError):
                continue

    forum = await resolve_forum_channel(bot)
    if not forum:
        empty["error"] = "Канал основного форума не найден."
        return empty

    uid = discord_user_id
    created_urls: list[str] = []
    merged_urls: list[str] = []
    errors: list[str] = []
    n_new = 0
    merge_ops = 0

    for entry in entries:
        if n_new >= max_topics:
            break
        aid = yummy_api.yummy_entry_anime_id(entry)
        if aid is None:
            continue
        if aid in imported_ids:
            continue

        err: str | None = None
        thread: discord.Thread | None = None

        query = yummy_api.yummy_entry_title(entry)
        slug_raw = yummy_api.yummy_entry_anime_url(entry)
        slug = _clean_slug(slug_raw) if slug_raw else ""
        if not slug:
            slug = await api_search_slug(session, query) or ""
        info = await api_fetch_anime(session, slug) if slug else None

        rem = entry.get("remote_ids") if isinstance(entry.get("remote_ids"), dict) else {}
        mal_ref = rem.get("myanimelist_id")
        mal_id_opt: int | None
        try:
            mal_id_opt = (
                int(mal_ref)
                if mal_ref is not None and str(mal_ref).strip() != ""
                else None
            )
        except (TypeError, ValueError):
            mal_id_opt = None

        if info:
            slug_key = _clean_slug((info.get("anime_url") or "").strip())
            thread, mst = await merge_adder_into_existing_topic(bot, slug_key, uid)
            if mst == "merged":
                await mark_yummy_imported(uid, aid)
                imported_ids.add(aid)
                merge_ops += 1
                if thread:
                    ju = (
                        thread.jump_url
                        if hasattr(thread, "jump_url")
                        else f"<#{thread.id}>"
                    )
                    merged_urls.append(ju)
                    try:
                        tnm = thread.name[:200] if thread else query
                        await append_user_anime_to_personal_state(
                            guild, uid, slug_key, tnm
                        )
                    except Exception:
                        logger.exception("Личный список после merge Yummy→Discord")
                if merge_ops >= CONNECT_MAX_MERGES_PER_RUN:
                    errors.append(
                        "Достигнут лимит дописываний за один запуск; запустите снова."
                    )
                    break
                await asyncio.sleep(0.35)
                continue
            if mst == "already":
                await mark_yummy_imported(uid, aid)
                imported_ids.add(aid)
                merge_ops += 1
                try:
                    await append_user_anime_to_personal_state(
                        guild, uid, slug_key, query[:500]
                    )
                except Exception:
                    logger.exception("Личный список после already Yummy")
                if merge_ops >= CONNECT_MAX_MERGES_PER_RUN:
                    errors.append(
                        "Достигнут лимит дописываний за один запуск; запустите снова."
                    )
                    break
                continue
            if mst in ("edit_failed", "fetch_failed"):
                errors.append(f"{query}: не удалось обновить существующую тему")
                continue

            thread, _st, err = await create_yummy_forum_thread(
                forum,
                session,
                info,
                uid,
                mal_id=mal_id_opt,
                resolved_slug=slug or "",
            )
        else:
            errors.append(
                f"{query}: нет карточки en.yummyani.me для slug `{slug or '—'}`"
            )
            continue

        if err:
            errors.append(f"{query}: {err}")
            continue
        if not thread:
            errors.append(f"{query}: неизвестная ошибка")
            continue

        try:
            pk = _clean_slug((info.get("anime_url") or "").strip())
            pt = str(info.get("title") or query)[:500]
            await append_user_anime_to_personal_state(guild, uid, pk, pt)
        except Exception:
            logger.exception("Личный список после новой темы Yummy import")

        await mark_yummy_imported(uid, aid)
        imported_ids.add(aid)
        n_new += 1
        ju = thread.jump_url if hasattr(thread, "jump_url") else f"<#{thread.id}>"
        created_urls.append(ju)
        schedule_personal_list_refresh(guild.id, uid)
        await asyncio.sleep(1.25)

    return {
        "ok": True,
        "error": None,
        "n_new": n_new,
        "merge_ops": merge_ops,
        "merged_urls": merged_urls,
        "created_urls": created_urls,
        "errors": errors,
    }


async def _write_yummy_poll_meta(
    *,
    users_checked: int,
    total_new: int,
    errors: list[str],
) -> None:
    from datetime import datetime, timezone

    async with _state_lock:
        data = _load_state()
        meta = data.setdefault("meta", {})
        meta["yummy_poll"] = {
            "last_run_utc": datetime.now(timezone.utc).isoformat(),
            "users_checked": users_checked,
            "imports_new": total_new,
            "errors": errors[:8],
        }
        _write_state(data)


async def yummy_background_poll_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        interval = max(
            60,
            int((os.environ.get("YUMMY_SYNC_INTERVAL_SEC") or "600").strip() or "600"),
        )
        app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
        if app and bot.session:
            g = _primary_guild_for_yummy_poll()
            if g:
                state = await read_state_copy()
                accounts = state.get("yummy_accounts") or {}
                err_buf: list[str] = []
                n_checked = 0
                total_new = 0
                for uid_s, acc in accounts.items():
                    if not isinstance(acc, dict):
                        continue
                    try:
                        duid = int(uid_s)
                    except ValueError:
                        continue
                    if not g.get_member(duid):
                        continue
                    n_checked += 1
                    try:
                        r = await run_yummy_list_import_for_member(
                            g,
                            duid,
                            list_filter="all",
                            max_topics=15,
                            session=bot.session,
                            app_token=app,
                        )
                        if r.get("error"):
                            err_buf.append(f"{duid}: {r['error']}")
                        else:
                            total_new += int(r.get("n_new") or 0)
                    except Exception as e:
                        logger.exception("yummy poll user %s", duid)
                        err_buf.append(f"{duid}: {e}")
                try:
                    await _write_yummy_poll_meta(
                        users_checked=n_checked,
                        total_new=total_new,
                        errors=err_buf,
                    )
                except Exception:
                    logger.exception("yummy poll meta")
        await asyncio.sleep(interval)
        if bot.is_closed():
            break


async def create_yummy_forum_thread(
    forum: discord.ForumChannel,
    session: aiohttp.ClientSession,
    info: dict[str, Any],
    adder_id: int,
    *,
    mal_id: int | None = None,
    resolved_slug: str = "",
) -> tuple[discord.Thread | None, discord.Message | None, str | None]:
    title = (info.get("title") or "Аниме")[:100]
    page_url = info["page_url"]
    poster_url = info.get("poster_url")
    shot_urls = info.get("screenshot_urls") or []
    files, dl_warnings = await build_attachment_files(
        session, poster_url, shot_urls if isinstance(shot_urls, list) else []
    )
    body = build_message_content(page_url, [adder_id], dl_warnings)
    embed = _build_embed(info)
    kwargs: dict[str, Any] = {
        "name": title,
        "content": body,
        "embeds": [embed],
    }
    if files:
        kwargs["files"] = files
    try:
        twm = await forum.create_thread(**kwargs)
        thread = twm.thread
        starter = twm.message
    except discord.Forbidden:
        return None, None, "Нет прав создавать темы в этом форуме."
    except discord.HTTPException as e:
        return None, None, f"Discord отклонил создание темы: {e}"

    raw_slug = (info.get("anime_url") or resolved_slug or "").strip()
    slug_key = _clean_slug(raw_slug) if raw_slug else ""
    yummy_slug_store = slug_key if slug_key else None
    await register_thread_meta(
        thread.id, title=title, mal_id=mal_id, yummy_slug=yummy_slug_store
    )
    if slug_key:
        await register_anime_topic_entry(
            slug_key,
            thread.id,
            starter.id,
            adder_id,
            kind="yummy",
            page_url=page_url,
            mal_page="",
            image_notes=dl_warnings,
        )

    for emoji in STATUS_REACTIONS:
        try:
            await starter.add_reaction(emoji)
        except discord.HTTPException:
            break
    try:
        await ensure_topic_side_panels(bot, thread.id)
    except Exception as e:
        logger.warning("Панели оценок/рекомендаций после создания темы: %s", e)
    return thread, starter, None


def _build_mal_embed(entry: dict[str, Any], mal_page: str) -> discord.Embed:
    title = mal_item_title(entry)
    embed = discord.Embed(
        title=f"📋 {title}",
        url=mal_page,
        color=0x2E51A2,
    )
    lines: list[str] = []
    lines.append(f"**Статус в списке:** {mal_status_label(entry)}")
    ep = entry.get("anime_num_episodes")
    watched = entry.get("num_watched_episodes")
    if isinstance(ep, int) and ep > 0 and watched is not None:
        lines.append(f"**Прогресс:** {watched}/{ep}")
    sc = entry.get("score")
    if isinstance(sc, int) and sc > 0:
        lines.append(f"**Ваша оценка на MAL:** {sc}/10")
    avg = entry.get("anime_score_val")
    if isinstance(avg, (int, float)):
        lines.append(f"**Средняя на MAL:** {avg:.2f}")
    embed.description = "\n".join(lines)
    img = entry.get("anime_image_path")
    if isinstance(img, str) and img.startswith("http"):
        embed.set_image(url=img)
    embed.set_footer(text="MyAnimeList · импорт из списка")
    return embed


def _mal_thread_body(mal_page: str, adder_ids: list[int]) -> str:
    lines = [
        f"**Ссылка на MyAnimeList**\n<{mal_page}>",
        "",
        STATUS_HINT,
        "",
        format_adders_line(adder_ids),
        "",
        "_Тема создана из привязанного списка MAL._",
    ]
    return _truncate("\n".join(lines), DISCORD_CONTENT_LIMIT)


async def create_mal_only_forum_thread(
    forum: discord.ForumChannel,
    entry: dict[str, Any],
    adder_id: int,
    mal_id: int,
) -> tuple[discord.Thread | None, discord.Message | None, str | None]:
    title = mal_item_title(entry)[:100]
    mal_page = mal_item_url(entry)
    body = _mal_thread_body(mal_page, [adder_id])
    embed = _build_mal_embed(entry, mal_page)
    try:
        twm = await forum.create_thread(
            name=title,
            content=body,
            embeds=[embed],
        )
        thread = twm.thread
        starter = twm.message
    except discord.Forbidden:
        return None, None, "Нет прав создавать темы в этом форуме."
    except discord.HTTPException as e:
        return None, None, f"Discord отклонил создание темы: {e}"

    await register_thread_meta(thread.id, title=title, mal_id=mal_id, yummy_slug=None)
    await register_anime_topic_entry(
        f"mal:{mal_id}",
        thread.id,
        starter.id,
        adder_id,
        kind="mal",
        page_url="",
        mal_page=mal_page,
        image_notes=[],
    )
    for emoji in STATUS_REACTIONS:
        try:
            await starter.add_reaction(emoji)
        except discord.HTTPException:
            break
    try:
        await ensure_topic_side_panels(bot, thread.id)
    except Exception as e:
        logger.warning("Панели после создания темы MAL: %s", e)
    return thread, starter, None


def _starter_text_blob(starter: discord.Message) -> str:
    parts = [starter.content or ""]
    for e in starter.embeds:
        if e.url:
            parts.append(str(e.url))
        if e.description:
            parts.append(e.description)
    return "\n".join(parts)


class AnimeRatingModal(discord.ui.Modal, title="Оценка аниме"):
    score = discord.ui.TextInput(
        label="Оценка от 1 до 10",
        placeholder="Например: 8",
        min_length=1,
        max_length=2,
        required=True,
    )

    def __init__(self, thread_id: int) -> None:
        super().__init__()
        self.thread_id = thread_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.score.value.strip()
        try:
            n = int(raw)
        except ValueError:
            await interaction.response.send_message(
                "Нужно целое число от 1 до 10.", ephemeral=True
            )
            return
        if not 1 <= n <= 10:
            await interaction.response.send_message(
                "Допустимы только целые числа **от 1 до 10**.", ephemeral=True
            )
            return
        await set_user_rating(self.thread_id, interaction.user.id, n)
        await refresh_rating_panel(interaction.client, self.thread_id)
        await interaction.response.send_message(
            f"Оценка **{n}/10** сохранена. Панель в теме обновлена.", ephemeral=True
        )


class RateAnimePanelView(discord.ui.View):
    def __init__(self, *, thread_id: int) -> None:
        super().__init__(timeout=None)
        self.thread_id = thread_id

    @discord.ui.button(
        label="Оценить",
        style=discord.ButtonStyle.primary,
        emoji="✏️",
    )
    async def open_rating_modal(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        ch = interaction.channel
        if not isinstance(ch, discord.Thread) or ch.id != self.thread_id:
            await interaction.response.send_message(
                "Используйте кнопку **в этой теме** форума.", ephemeral=True
            )
            return
        state = await read_state_copy()
        if not thread_has_rating_slot(state, self.thread_id):
            await interaction.response.send_message(
                "Эта ветка не зарегистрирована для оценок.", ephemeral=True
            )
            return
        await interaction.response.send_modal(AnimeRatingModal(self.thread_id))


async def refresh_rating_panel(client: discord.Client, thread_id: int) -> None:
    state = await read_state_copy()
    if not thread_has_rating_slot(state, thread_id):
        return
    thread = client.get_channel(thread_id)
    if thread is None:
        try:
            thread = await client.fetch_channel(thread_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logger.warning("Панель оценок: не удалось получить ветку %s", thread_id)
            return
    if not isinstance(thread, discord.Thread):
        return
    guild = thread.guild
    embed = _build_rating_panel_embed(state, thread_id, guild)
    view = RateAnimePanelView(thread_id=thread_id)
    tid_s = str(thread_id)
    slot = state.get("threads", {}).get(tid_s, {})
    msg_id: int | None = None
    if isinstance(slot, dict) and slot.get("rating_message_id") is not None:
        try:
            msg_id = int(slot["rating_message_id"])
        except (TypeError, ValueError):
            msg_id = None
    if msg_id:
        try:
            msg = await thread.fetch_message(msg_id)
            await msg.edit(embed=embed, view=view)
            return
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logger.info("Панель оценок: сообщение %s недоступно, создаём новое", msg_id)
    try:
        msg = await thread.send(embed=embed, view=view)
    except discord.HTTPException as e:
        logger.warning("Панель оценок: не удалось отправить сообщение: %s", e)
        return
    await save_rating_board_message_id(thread_id, msg.id)


class RecommendPanelView(discord.ui.View):
    def __init__(self, *, thread_id: int) -> None:
        super().__init__(timeout=None)
        self.thread_id = thread_id

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="Кому порекомендовать?",
        min_values=1,
        max_values=1,
    )
    async def pick_user_for_recommend(
        self, interaction: discord.Interaction, select: discord.ui.UserSelect
    ) -> None:
        ch = interaction.channel
        if not isinstance(ch, discord.Thread) or ch.id != self.thread_id:
            await interaction.response.send_message(
                "Выберите участника **в этой теме** форума.", ephemeral=True
            )
            return
        target = select.values[0]
        if target.bot:
            await interaction.response.send_message(
                "Нужно выбрать человека, не бота.", ephemeral=True
            )
            return
        anime_title = ch.name[:200] or "аниме"
        line = (
            f"{target.mention}, тебе порекомендовал(а) {interaction.user.mention} "
            f"аниме **{anime_title}**."
        )
        await interaction.response.defer(ephemeral=True)
        await ch.send(
            line,
            allowed_mentions=discord.AllowedMentions(users=[target, interaction.user]),
        )
        await interaction.followup.send("Сообщение отправлено в тему.", ephemeral=True)


async def refresh_recommend_panel(client: discord.Client, thread_id: int) -> None:
    state = await read_state_copy()
    if not thread_has_rating_slot(state, thread_id):
        return
    thread = client.get_channel(thread_id)
    if thread is None:
        try:
            thread = await client.fetch_channel(thread_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logger.warning("Панель рекомендаций: ветка %s недоступна", thread_id)
            return
    if not isinstance(thread, discord.Thread):
        return
    embed = discord.Embed(
        title=RECOMMEND_PANEL_TITLE,
        description=RECOMMEND_PANEL_DESC,
        color=0x9B59B6,
    )
    view = RecommendPanelView(thread_id=thread_id)
    tid_s = str(thread_id)
    slot = state.get("threads", {}).get(tid_s, {})
    msg_id: int | None = None
    if isinstance(slot, dict) and slot.get("recommend_message_id") is not None:
        try:
            msg_id = int(slot["recommend_message_id"])
        except (TypeError, ValueError):
            msg_id = None
    if msg_id:
        try:
            msg = await thread.fetch_message(msg_id)
            await msg.edit(embed=embed, view=view)
            return
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logger.info(
                "Панель рекомендаций: сообщение %s недоступно, создаём новое", msg_id
            )
    try:
        msg = await thread.send(embed=embed, view=view)
    except discord.HTTPException as e:
        logger.warning("Панель рекомендаций: не отправить: %s", e)
        return
    await save_recommend_board_message_id(thread_id, msg.id)


class AddToMyListPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Добавить в мой список",
        style=discord.ButtonStyle.success,
        emoji="➕",
        custom_id="anime:panel:add_to_my_list",
    )
    async def add_to_my_list(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        ch = interaction.channel
        if not isinstance(ch, discord.Thread):
            await interaction.response.send_message(
                "Нажмите кнопку в теме конкретного аниме.", ephemeral=True
            )
            return
        if not interaction.guild:
            await interaction.response.send_message(
                "Команда работает только на сервере.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)

        starter = await _get_list_thread_starter_message(ch)
        if starter is None:
            await interaction.followup.send("Не нашёл стартовое сообщение темы.", ephemeral=True)
            return
        key, kind, _ = _topic_key_from_starter_text(_starter_text_blob(starter))
        if not key:
            forum = await resolve_forum_channel(interaction.client)
            if forum:
                try:
                    await _ingest_forum_thread_from_discord(
                        interaction.client, forum, ch, getattr(interaction.client, "session", None)
                    )
                except Exception:
                    pass
            starter2 = await _get_list_thread_starter_message(ch)
            key, kind, _ = _topic_key_from_starter_text(_starter_text_blob(starter2 or starter))
        if not key:
            await interaction.followup.send(
                "Не удалось определить аниме в этой теме.", ephemeral=True
            )
            return

        thread, st = await merge_adder_into_existing_topic(
            interaction.client, key, interaction.user.id
        )
        if st in ("merged", "already"):
            try:
                await append_user_anime_to_personal_state(
                    interaction.guild, interaction.user.id, key, ch.name[:500]
                )
            except Exception:
                logger.exception("Кнопка add_to_my_list: личный список")
            txt = "Добавлено в ваш личный список." if st == "merged" else "Уже было в теме — докинул в ваш личный список."
            link = thread.jump_url if thread and hasattr(thread, "jump_url") else ch.jump_url
            await interaction.followup.send(f"{txt} {link}", ephemeral=True)
            return
        if st in ("edit_failed", "fetch_failed"):
            await interaction.followup.send(
                "Не удалось обновить подпись в стартовом посте темы.", ephemeral=True
            )
            return
        await interaction.followup.send(
            "Эта тема не найдена в базе. Нажмите «Синхронизировать» в личной теме и повторите.",
            ephemeral=True,
        )


async def refresh_add_to_list_panel(client: discord.Client, thread_id: int) -> None:
    state = await read_state_copy()
    if not thread_has_rating_slot(state, thread_id):
        return
    thread = client.get_channel(thread_id)
    if thread is None:
        try:
            thread = await client.fetch_channel(thread_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return
    if not isinstance(thread, discord.Thread):
        return
    embed = discord.Embed(
        title="📥 Личный список",
        description="Нажмите кнопку ниже, чтобы добавить это аниме в ваш личный список.",
        color=0x2ECC71,
    )
    view = AddToMyListPanelView()
    slot = state.get("threads", {}).get(str(thread_id), {})
    msg_id: int | None = None
    if isinstance(slot, dict) and slot.get("add_to_list_message_id") is not None:
        try:
            msg_id = int(slot["add_to_list_message_id"])
        except (TypeError, ValueError):
            msg_id = None
    if msg_id:
        try:
            msg = await thread.fetch_message(msg_id)
            await msg.edit(embed=embed, view=view)
            return
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    try:
        msg = await thread.send(embed=embed, view=view)
    except discord.HTTPException:
        return
    await save_add_to_list_board_message_id(thread_id, msg.id)


async def ensure_topic_side_panels(client: discord.Client, thread_id: int) -> None:
    await refresh_rating_panel(client, thread_id)
    await refresh_recommend_panel(client, thread_id)
    await refresh_add_to_list_panel(client, thread_id)


class DuplicateCleanupView(discord.ui.View):
    def __init__(
        self,
        *,
        requester_id: int,
        victims: list[int],
    ) -> None:
        super().__init__(timeout=900.0)
        self.requester_id = requester_id
        self.victims = list(victims)

    @discord.ui.button(
        label="Удалить лишние темы",
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
    )
    async def delete_duplicates(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message(
                "Эта кнопка только у того, кто запускал проверку.", ephemeral=True
            )
            return
        if not interaction.permissions.manage_threads:
            await interaction.response.send_message(
                "Нужно право **Управлять ветками**, чтобы удалять темы.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        deleted: list[int] = []
        cleaned_state: list[int] = []
        errors: list[str] = []
        client = interaction.client
        for tid in self.victims:
            ch = client.get_channel(tid)
            if ch is None:
                try:
                    ch = await client.fetch_channel(tid)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    ch = None
            if ch is None:
                await purge_thread_from_state(tid)
                cleaned_state.append(tid)
                continue
            if not isinstance(ch, discord.Thread):
                errors.append(f"{tid} (не ветка форума)")
                continue
            try:
                await ch.delete()
            except discord.Forbidden:
                errors.append(f"{tid} (нет прав на удаление)")
                continue
            except discord.HTTPException as e:
                errors.append(f"{tid} ({e})")
                continue
            await purge_thread_from_state(tid)
            deleted.append(tid)

        for child in self.children:
            child.disabled = True
        try:
            await interaction.edit_original_response(view=self)
        except discord.HTTPException:
            pass

        parts = [f"Удалено тем в Discord: **{len(deleted)}**."]
        if deleted:
            parts.append("ID: " + ", ".join(str(x) for x in deleted))
        if cleaned_state:
            parts.append(
                f"Тем уже не было в Discord, очищена запись в базе бота: **{len(cleaned_state)}**."
            )
        if errors:
            parts.append("Не удалось: " + "; ".join(errors[:5]))
            if len(errors) > 5:
                parts.append(f"_…и ещё {len(errors) - 5}_")
        await interaction.followup.send("\n".join(parts), ephemeral=True)


def _yummy_link_hcaptcha_site_key() -> str:
    return (
        os.environ.get("YUMMY_HCAPTCHA_SITE_KEY") or "b1847961-208e-4a90-9671-1e6bba9e0b36"
    ).strip()


def _html_esc(s: str) -> str:
    import html

    return html.escape(s, quote=True)


def _page_yummy_link_form(*, token: str, error: str | None = None) -> str:
    sk = _yummy_link_hcaptcha_site_key()
    err_block = ""
    if error:
        err_block = f'<p class="err">{_html_esc(error)}</p>'
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>YummyAnime — привязка</title>
<style>
body {{ font-family: system-ui,sans-serif; max-width: 28rem; margin: 2rem auto; padding: 0 1rem; }}
.err {{ color: #c0392b; }}
label {{ display:block; margin-top:1rem; }}
button {{ margin-top:1.25rem; padding: .5rem 1rem; }}
</style>
</head>
<body>
<h1>Вход YummyAnime</h1>
<p>Данные отправляются на официальный API YummyAnime. После успеха вернитесь в Discord и нажмите кнопку завершения.</p>
{err_block}
<form method="post" action="/yummy-link">
<input type="hidden" name="token" value="{_html_esc(token)}"/>
<label>E-mail <input type="email" name="email" required autocomplete="username"/></label>
<label>Пароль <input type="password" name="password" required autocomplete="current-password"/></label>
<div class="h-captcha" data-sitekey="{_html_esc(sk)}"></div>
<script src="https://js.hcaptcha.com/1/api.js" async defer></script>
<button type="submit">Войти и привязать</button>
</form>
</body>
</html>"""


def _page_yummy_link_ok() -> str:
    return """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"/><title>Готово</title></head>
<body style="font-family:system-ui,sans-serif;max-width:28rem;margin:3rem auto;padding:0 1rem">
<h1>Вход выполнен</h1>
<p>Вернитесь в Discord и нажмите кнопку <strong>«Завершить привязку YummyAnime»</strong>.</p>
</body></html>"""


def _page_yummy_link_bad() -> str:
    return """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"/><title>Ссылка недействительна</title></head>
<body style="font-family:system-ui,sans-serif;max-width:28rem;margin:3rem auto;padding:0 1rem">
<p>Сессия истекла или ссылка неверная. Запросите новую командой <code>/yummy_link</code> в Discord.</p>
</body></html>"""


async def _yummy_link_web_get(request: web.Request) -> web.StreamResponse:
    token = (request.query.get("token") or "").strip()
    if not token:
        return web.Response(
            text=_page_yummy_link_bad(),
            content_type="text/html; charset=utf-8",
            status=400,
        )
    uid = await yummy_link_pending_user_id(token)
    if uid is None:
        return web.Response(
            text=_page_yummy_link_bad(),
            content_type="text/html; charset=utf-8",
            status=400,
        )
    return web.Response(
        text=_page_yummy_link_form(token=token),
        content_type="text/html; charset=utf-8",
    )


async def _yummy_link_web_post(request: web.Request) -> web.StreamResponse:
    bot = request.app["bot"]
    app_tok = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
    if not app_tok or not bot.session:
        return web.Response(
            text="<html><body><p>Сервер бота не настроен (YUMMY_APPLICATION_TOKEN).</p></body></html>",
            content_type="text/html; charset=utf-8",
            status=503,
        )
    data = await request.post()
    token = str(data.get("token") or "").strip()
    email = str(data.get("email") or "").strip()
    password = str(data.get("password") or "")
    captcha = str(data.get("h-captcha-response") or "").strip()
    uid = await yummy_link_pending_user_id(token)
    if uid is None:
        return web.Response(
            text=_page_yummy_link_bad(),
            content_type="text/html; charset=utf-8",
            status=400,
        )
    access, err_msg, _st = await yummy_api.yani_login(
        bot.session, app_tok, email, password, captcha or None, USER_AGENT
    )
    if not access:
        detail = err_msg or "Не удалось войти."
        return web.Response(
            text=_page_yummy_link_form(token=token, error=detail),
            content_type="text/html; charset=utf-8",
            status=200,
        )
    prof = await yummy_api.yani_get_profile(bot.session, app_tok, access, USER_AGENT)
    if not prof:
        return web.Response(
            text=_page_yummy_link_form(
                token=token, error="Токен получен, но профиль не загрузился."
            ),
            content_type="text/html; charset=utf-8",
        )
    yid = prof.get("id")
    if yid is None:
        return web.Response(
            text=_page_yummy_link_form(token=token, error="Ответ профиля без id."),
            content_type="text/html; charset=utf-8",
        )
    try:
        yid_i = int(yid)
    except (TypeError, ValueError):
        return web.Response(
            text=_page_yummy_link_form(token=token, error="Некорректный id в профиле."),
            content_type="text/html; charset=utf-8",
        )
    nick = str(prof.get("nickname") or "")
    await yummy_link_remove_pending(token)
    await yummy_link_store_ready(
        uid, access_token=access, yummy_user_id=yid_i, nickname=nick
    )
    return web.Response(
        text=_page_yummy_link_ok(), content_type="text/html; charset=utf-8"
    )


def create_yummy_link_web_app(bot: commands.Bot) -> web.Application:
    app = web.Application(client_max_size=1024**2)
    app["bot"] = bot
    app.router.add_get("/yummy-link", _yummy_link_web_get)
    app.router.add_post("/yummy-link", _yummy_link_web_post)
    return app


class YummyLinkVerifyView(discord.ui.View):
    """Постоянная кнопка: забирает токен из data/yummy_link_state после веб-входа."""

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Завершить привязку YummyAnime",
        style=discord.ButtonStyle.primary,
        custom_id="yummy_link:verify",
    )
    async def verify(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        ent = await yummy_link_consume_ready(interaction.user.id)
        if not ent:
            await interaction.response.send_message(
                "Нет готовой привязки. Сначала откройте ссылку из `/yummy_link` и войдите на сайте, "
                "либо срок ожидания истёк — запросите ссылку снова.",
                ephemeral=True,
            )
            return
        tok = (ent.get("access_token") or "").strip()
        if not tok:
            await interaction.response.send_message(
                "Ошибка данных привязки. Повторите `/yummy_link`.", ephemeral=True
            )
            return
        try:
            yuid = int(ent.get("yummy_user_id"))
        except (TypeError, ValueError):
            await interaction.response.send_message(
                "Ошибка данных привязки. Повторите `/yummy_link`.", ephemeral=True
            )
            return
        nick = str(ent.get("nickname") or "")
        await bind_yummy_account(interaction.user.id, tok, yuid, nick)
        await interaction.response.send_message(
            f"YummyAnime привязан (**{nick or yuid}**). Импорт: `/yummy_sync` или кнопка **Yummy ↻** в личной теме.\n"
            "_Токен хранится на сервере с ботом._",
            ephemeral=True,
        )


class YummyBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        # Для !aa / !animeadd в чате включите в Portal «Message Content Intent» и оставьте 1 (по умолчанию).
        _mc = (os.environ.get("DISCORD_MESSAGE_CONTENT_INTENT") or "1").strip().lower()
        intents.message_content = _mc not in ("0", "false", "no", "off")
        super().__init__(command_prefix="!", intents=intents)
        self.session: aiohttp.ClientSession | None = None
        self._yummy_poll_task: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        self.session = aiohttp.ClientSession(headers={"User-Agent": USER_AGENT})
        self.add_view(PersonalTopicHubView())
        self.add_view(AddToMyListPanelView())
        self.add_view(YummyLinkVerifyView())
        port_raw = (os.environ.get("YUMMY_LINK_HTTP_PORT") or "").strip()
        self._yummy_link_runner: web.AppRunner | None = None
        if port_raw:
            try:
                port = int(port_raw)
                host = (os.environ.get("YUMMY_LINK_HTTP_HOST") or "127.0.0.1").strip()
                if not host:
                    host = "127.0.0.1"
                web_app = create_yummy_link_web_app(self)
                runner = web.AppRunner(web_app)
                await runner.setup()
                site = web.TCPSite(runner, host=host, port=port)
                await site.start()
                self._yummy_link_runner = runner
                logger.info("Веб-вход YummyAnime: http://%s:%s/yummy-link", host, port)
            except Exception:
                logger.exception("Не удалось запустить YUMMY_LINK_HTTP_PORT")
        guild_id = (os.environ.get("DISCORD_GUILD_ID") or "").strip()

        async def _sync_global() -> None:
            synced = await self.tree.sync()
            logger.info(
                "Глобальная синхронизация slash-команд: %s шт. "
                "Появление в серверах Discord может занять до ~1 часа.",
                len(synced),
            )
            logger.info("Имена команд: %s", [c.name for c in synced])

        try:
            if guild_id:
                g = discord.Object(id=int(guild_id))
                self.tree.copy_global_to(guild=g)
                try:
                    synced = await self.tree.sync(guild=g)
                    logger.info(
                        "Slash-команды на сервере %s (%s): %s",
                        guild_id,
                        len(synced),
                        [c.name for c in synced],
                    )
                    self.tree.clear_commands(guild=None)
                    try:
                        await self.tree.sync()
                    except discord.HTTPException as e2:
                        logger.warning(
                            "Не удалось очистить глобальные дубликаты команд: %s",
                            e2.text,
                        )
                except discord.HTTPException as e:
                    logger.error(
                        "Синхронизация на сервер %s не удалась (HTTP %s): %s. "
                        "Проверьте DISCORD_GUILD_ID и права бота. Пробую глобальную регистрацию…",
                        guild_id,
                        e.status,
                        e.text,
                    )
                    await _sync_global()
            else:
                await _sync_global()
        except discord.HTTPException as e:
            logger.error(
                "Slash-команды не зарегистрированы (HTTP %s): %s. "
                "Бот всё равно запустится — исправьте права/ID и перезапустите.",
                e.status,
                e.text,
            )

    async def close(self) -> None:
        runner = getattr(self, "_yummy_link_runner", None)
        if runner is not None:
            await runner.cleanup()
            self._yummy_link_runner = None
        if self.session:
            await self.session.close()
        await super().close()


bot = YummyBot()

TEXT_ANIMEADD_RE = re.compile(
    r"^(?:!aa|!animeadd|!forum_add|/aa|/animeadd|/forum_add)\s+(.+)$",
    re.I | re.DOTALL,
)


async def run_animeadd_for_user(guild: discord.Guild, user_id: int, query: str) -> str:
    """Текст ответа для slash или сообщения в чате."""
    if not bot.session:
        return "Сессия HTTP не готова."
    q = query.strip()
    if not q:
        return "Пустой запрос."
    slug = slug_from_text(q)
    if not slug:
        slug = await api_search_slug(bot.session, q)
    if not slug:
        return "Не нашёл аниме. Уточните запрос или вставьте ссылку с en.yummyani.me."
    info = await api_fetch_anime(bot.session, slug)
    if not info:
        return "Не удалось загрузить карточку аниме (API вернул ошибку)."
    ch = await resolve_forum_channel(bot)
    if not ch:
        return "Канал форума не найден или бот не видит его. Проверьте ID и права бота."
    slug_key = _clean_slug((info.get("anime_url") or slug or "").strip())
    thread, merge_st = await merge_adder_into_existing_topic(bot, slug_key, user_id)
    if merge_st == "merged":
        link = thread.jump_url if thread and hasattr(thread, "jump_url") else f"<#{thread.id}>"
        tname = thread.name[:200] if thread else (info.get("title") or slug_key)
        try:
            await append_user_anime_to_personal_state(guild, user_id, slug_key, tname)
        except Exception:
            logger.exception("Личный список после merge animeadd")
        return f"Тема уже была — добавил вас в подпись: {link}"
    if merge_st == "already":
        link = thread.jump_url if thread and hasattr(thread, "jump_url") else f"<#{thread.id}>"
        try:
            tname = thread.name[:200] if thread else (info.get("title") or slug_key)
            await append_user_anime_to_personal_state(guild, user_id, slug_key, tname)
        except Exception:
            logger.exception("Личный список после already animeadd")
        return f"Эта тема уже есть, вы уже среди добавивших: {link}"
    if merge_st in ("edit_failed", "fetch_failed"):
        return (
            "Тема с этим аниме уже есть в базе бота, но не удалось обновить сообщение "
            "(права или тема удалена). Обратитесь к администратору."
        )
    thread, _starter, err = await create_yummy_forum_thread(
        ch, bot.session, info, user_id, mal_id=None, resolved_slug=slug
    )
    if err or not thread:
        return err or "Не удалось создать тему."
    try:
        await append_user_anime_to_personal_state(
            guild, user_id, slug_key, str(info.get("title") or "")[:500]
        )
    except Exception:
        logger.exception("Личный список после создания темы animeadd")
    link = thread.jump_url if hasattr(thread, "jump_url") else f"<#{thread.id}>"
    return f"Готово: {link}"


async def build_mal_list_embed_for_member(
    session: aiohttp.ClientSession,
    state: dict[str, Any],
    member: discord.Member,
) -> tuple[discord.Embed | None, str | None]:
    """Список MAL с сайта. (embed, err_text)."""
    acc = state.get("mal_accounts", {}).get(str(member.id))
    if not isinstance(acc, dict):
        return None, f"{member.mention} ещё не привязал список MAL (`/mal_bind`, затем `/mal_import`)."
    username = (acc.get("username") or "").strip()
    list_url = (acc.get("list_url") or "").strip()
    if not username:
        return None, "В сохранённой привязке нет имени пользователя MAL."
    entries, http_st = await mal_fetch_full_list(session, username, MAL_STATUS_ALL)
    if http_st != 200:
        return None, "Не удалось загрузить список с MyAnimeList (список закрыт или MAL недоступен)."
    by_status: dict[int, list[str]] = {}
    for e in entries:
        st = e.get("status")
        try:
            sk = int(st) if st is not None else 0
        except (TypeError, ValueError):
            sk = 0
        line = _format_mal_entry_line(e)
        by_status.setdefault(sk, []).append(line)
    embed = discord.Embed(
        title=f"MyAnimeList — {member.display_name}",
        url=list_url or f"https://myanimelist.net/animelist/{username}",
        color=0x2E51A2,
        description=f"**MAL:** [{username}]({list_url or f'https://myanimelist.net/animelist/{username}'}) · "
        f"записей: **{len(entries)}**",
    )
    order = (1, 6, 2, 3, 4)
    for sk in order:
        lines = by_status.get(sk, [])
        if not lines:
            continue
        name = MAL_STATUS_NAMES.get(sk, "Другое")
        chunk = lines[:35]
        val = "\n".join(chunk)
        if len(lines) > 35:
            val += f"\n_…и ещё {len(lines) - 35}_"
        embed.add_field(
            name=f"{name} ({len(lines)})",
            value=_truncate(val, EMBED_FIELD_LIMIT),
            inline=False,
        )
    leftover = [sk for sk in sorted(by_status.keys()) if sk not in order and by_status[sk]]
    for sk in leftover:
        lines = by_status[sk]
        val = "\n".join(lines[:20])
        if len(lines) > 20:
            val += f"\n_…и ещё {len(lines) - 20}_"
        embed.add_field(
            name=f"Статус {sk} ({len(lines)})",
            value=_truncate(val, EMBED_FIELD_LIMIT),
            inline=False,
        )
    return embed, None


async def run_animelist_discord_topics(
    guild: discord.Guild, target: discord.Member
) -> tuple[discord.Embed | None, str | None, int, int]:
    """Аниме из сохранённого личного списка (без полного сканирования форума)."""
    state = await read_state_copy()
    pairs = list_personal_anime_pairs(state, guild.id, target.id)
    if not pairs:
        return (
            None,
            f"{target.mention} — в личном списке пока нет записей. "
            "Они появляются при добавлении аниме в основной форум (`/addanime`). "
            "Если вы уже добавляли раньше, админ может выполнить `/mylist_admin_sync` для вашего профиля.",
            0,
            0,
        )
    lines = [f"• [{t}]({u})" for t, u in pairs[:60]]
    body = "\n".join(lines)
    if len(pairs) > 60:
        body += f"\n_…и ещё {len(pairs) - 60}_"
    embed = discord.Embed(
        title=f"Discord-лист — {target.display_name}",
        description=_truncate(body, EMBED_DESC_LIMIT),
        color=EMBED_COLOR,
    )
    embed.set_footer(text="Данные из личной темы списков · /mylist_show")
    return embed, None, len(pairs), 0


@bot.event
async def on_ready() -> None:
    assert bot.user is not None
    logger.info("Бот онлайн: %s (%s)", bot.user, bot.user.id)
    try:
        await ensure_bot_info_thread(bot)
    except Exception as e:
        logger.warning("Справочная тема форума: %s", e)
    t = bot._yummy_poll_task
    if t is None or t.done():
        bot._yummy_poll_task = asyncio.create_task(yummy_background_poll_loop())


@bot.event
async def on_disconnect() -> None:
    logger.warning("Соединение с Discord разорвано (on_disconnect).")


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot or not message.guild:
        await bot.process_commands(message)
        return
    raw = (message.content or "").strip()
    m = TEXT_ANIMEADD_RE.match(raw)
    if not m:
        await bot.process_commands(message)
        return
    query = (m.group(1) or "").strip()
    if not query:
        await message.channel.send(
            "Укажите ссылку или название: `!aa название` или `!animeadd ссылка`",
            reference=message,
            mention_author=False,
        )
        return
    async with message.channel.typing():
        try:
            out = await run_animeadd_for_user(message.guild, message.author.id, query)
        except Exception:
            logger.exception("Текстовый animeadd")
            out = "Произошла ошибка при добавлении. Попробуйте `/forum_add`."
    await message.reply(_truncate(out, DISCORD_CONTENT_LIMIT), mention_author=False)


async def _cmd_forum_add(interaction: discord.Interaction, query: str) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    out = await run_animeadd_for_user(interaction.guild, interaction.user.id, query)
    await interaction.followup.send(_truncate(out, DISCORD_CONTENT_LIMIT), ephemeral=True)


@bot.tree.command(
    name="forum_add",
    description="[Форум] Добавить аниме YummyAnime в основной форум и личный список",
)
@app_commands.describe(query="Ссылка на страницу аниме или поисковый запрос")
async def forum_add(interaction: discord.Interaction, query: str) -> None:
    await _cmd_forum_add(interaction, query)


@bot.tree.command(name="animeadd", description="Алиас /forum_add — добавить аниме в форум")
@app_commands.describe(query="Ссылка на страницу аниме или поисковый запрос")
async def animeadd(interaction: discord.Interaction, query: str) -> None:
    await _cmd_forum_add(interaction, query)


@bot.tree.command(name="addanime", description="Алиас /forum_add")
@app_commands.describe(query="Ссылка на аниме или название")
async def addanime(interaction: discord.Interaction, query: str) -> None:
    await _cmd_forum_add(interaction, query)


@bot.tree.command(name="aa", description="Короткий алиас /forum_add")
@app_commands.describe(query="Ссылка на страницу аниме или поисковый запрос")
async def aa(interaction: discord.Interaction, query: str) -> None:
    await _cmd_forum_add(interaction, query)


def _mal_choice_to_status(choice: str) -> int:
    return {
        "all": MAL_STATUS_ALL,
        "watching": 1,
        "completed": 2,
        "on_hold": 3,
        "dropped": 4,
        "plan_to_watch": 6,
    }.get(choice, MAL_STATUS_ALL)


def _format_mal_entry_line(entry: dict[str, Any]) -> str:
    t = mal_item_title(entry)
    ep = entry.get("anime_num_episodes")
    watched = entry.get("num_watched_episodes")
    prog = ""
    if isinstance(ep, int) and ep > 0 and watched is not None:
        prog = f" ({watched}/{ep})"
    sc = entry.get("score")
    star = ""
    if isinstance(sc, int) and sc > 0:
        star = f" · **{sc}/10**"
    return f"• {t}{prog}{star}"


async def _mal_bind_impl(interaction: discord.Interaction, list_url: str) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    username = mal_username_from_url(list_url)
    if not username:
        await interaction.response.send_message(
            "Нужна ссылка вида `https://myanimelist.net/animelist/ник` или `https://myanimelist.net/profile/ник`.",
            ephemeral=True,
        )
        return
    norm_url = f"https://myanimelist.net/animelist/{username}"
    await bind_mal_account(interaction.user.id, username, norm_url)
    await interaction.response.send_message(
        f"MAL привязан: [{username}]({norm_url}). Можно перепривязать этой же командой.",
        ephemeral=True,
    )


@bot.tree.command(
    name="mal_bind",
    description="[MAL] Привязать или перепривязать ваш MyAnimeList",
)
@app_commands.describe(list_url="Ссылка на ваш MAL animelist или профиль")
async def mal_bind(interaction: discord.Interaction, list_url: str) -> None:
    await _mal_bind_impl(interaction, list_url)


@bot.tree.command(
    name="malbind",
    description="Устаревшее имя — используйте /mal_bind",
)
@app_commands.describe(list_url="Ссылка на ваш MAL animelist или профиль")
async def malbind(interaction: discord.Interaction, list_url: str) -> None:
    await _mal_bind_impl(interaction, list_url)


@bot.tree.command(
    name="mal_import",
    description="[MAL] Импортировать аниме из привязанного списка в основной форум",
)
@app_commands.describe(
    list_url="(необязательно) новая ссылка MAL для перепривязки перед импортом",
    mal_status="Какие позиции брать из списка",
    max_topics="Сколько новых тем создать за один раз (1–25)",
)
@app_commands.choices(
    mal_status=[
        app_commands.Choice(name="Все записи", value="all"),
        app_commands.Choice(name="Смотрю", value="watching"),
        app_commands.Choice(name="В планах", value="plan_to_watch"),
        app_commands.Choice(name="Просмотрено", value="completed"),
        app_commands.Choice(name="Отложено", value="on_hold"),
        app_commands.Choice(name="Брошено", value="dropped"),
    ]
)
async def _run_mal_import(
    interaction: discord.Interaction,
    mal_status: str,
    list_url: str | None = None,
    max_topics: app_commands.Range[int, 1, 25] = 10,
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    if not bot.session:
        await interaction.followup.send("Сессия HTTP не готова.", ephemeral=True)
        return

    username = ""
    norm_url = ""
    if list_url:
        username = mal_username_from_url(list_url) or ""
        if not username:
            await interaction.followup.send(
                "Нужна ссылка вида `https://myanimelist.net/animelist/ник` "
                "или `https://myanimelist.net/profile/ник`.",
                ephemeral=True,
            )
            return
        norm_url = f"https://myanimelist.net/animelist/{username}"
        await bind_mal_account(interaction.user.id, username, norm_url)
    else:
        state0 = await read_state_copy()
        acc = state0.get("mal_accounts", {}).get(str(interaction.user.id))
        if not isinstance(acc, dict):
            await interaction.followup.send(
                "Сначала привяжите MAL: `/mal_bind` (или укажите ссылку в этой команде).",
                ephemeral=True,
            )
            return
        username = (acc.get("username") or "").strip()
        norm_url = (acc.get("list_url") or "").strip() or f"https://myanimelist.net/animelist/{username}"
        if not username:
            await interaction.followup.send(
                "В привязке MAL нет имени пользователя. Выполните `/mal_bind` заново.",
                ephemeral=True,
            )
            return

    status_int = _mal_choice_to_status(mal_status)
    entries, http_st = await mal_fetch_full_list(bot.session, username, status_int)
    if http_st != 200:
        await interaction.followup.send(
            "Не удалось открыть список на MyAnimeList (проверьте ник и что список **публичный**).",
            ephemeral=True,
        )
        return

    forum = await resolve_forum_channel(bot)
    if not forum:
        await interaction.followup.send(
            "Канал форума не найден. Аккаунт MAL сохранён; импорт можно повторить позже.",
            ephemeral=True,
        )
        return

    state = await read_state_copy()
    key = str(interaction.user.id)
    raw_imp = state.get("imported_mal", {}).get(key, [])
    imported_ids: set[int] = set()
    if isinstance(raw_imp, list):
        for x in raw_imp:
            try:
                imported_ids.add(int(x))
            except (TypeError, ValueError):
                continue

    uid = interaction.user.id
    created_urls: list[str] = []
    merged_urls: list[str] = []
    errors: list[str] = []
    n_new = 0
    merge_ops = 0

    for entry in entries:
        if n_new >= max_topics:
            break
        aid = entry.get("anime_id")
        if not isinstance(aid, int):
            continue
        if aid in imported_ids:
            continue

        query = mal_item_title(entry)
        slug = await api_search_slug(bot.session, query)
        info = await api_fetch_anime(bot.session, slug) if slug else None

        if info:
            slug_key = _clean_slug((info.get("anime_url") or "").strip())
            thread, mst = await merge_adder_into_existing_topic(bot, slug_key, uid)
            if mst == "merged":
                await mark_mal_imported(interaction.user.id, aid)
                imported_ids.add(aid)
                merge_ops += 1
                if thread:
                    ju = (
                        thread.jump_url
                        if hasattr(thread, "jump_url")
                        else f"<#{thread.id}>"
                    )
                    merged_urls.append(ju)
                    try:
                        tnm = thread.name[:200] if thread else query
                        await append_user_anime_to_personal_state(
                            interaction.guild, uid, slug_key, tnm
                        )
                    except Exception:
                        logger.exception("Личный список после merge MAL→Yummy")
                if merge_ops >= CONNECT_MAX_MERGES_PER_RUN:
                    errors.append(
                        "Достигнут лимит дописываний в существующие темы за один запуск; "
                        "запустите команду ещё раз."
                    )
                    break
                await asyncio.sleep(0.35)
                continue
            if mst == "already":
                await mark_mal_imported(interaction.user.id, aid)
                imported_ids.add(aid)
                merge_ops += 1
                try:
                    await append_user_anime_to_personal_state(
                        interaction.guild, uid, slug_key, query[:500]
                    )
                except Exception:
                    logger.exception("Личный список после already MAL→Yummy")
                if merge_ops >= CONNECT_MAX_MERGES_PER_RUN:
                    errors.append(
                        "Достигнут лимит дописываний в существующие темы за один запуск; "
                        "запустите команду ещё раз."
                    )
                    break
                continue
            if mst in ("edit_failed", "fetch_failed"):
                errors.append(f"{query}: тема уже есть, не удалось обновить подпись")
                continue

            thread, _st, err = await create_yummy_forum_thread(
                forum,
                bot.session,
                info,
                uid,
                mal_id=aid,
                resolved_slug=slug or "",
            )
        else:
            thread, mst = await merge_adder_into_existing_topic(bot, f"mal:{aid}", uid)
            if mst == "merged":
                await mark_mal_imported(interaction.user.id, aid)
                imported_ids.add(aid)
                merge_ops += 1
                if thread:
                    ju = (
                        thread.jump_url
                        if hasattr(thread, "jump_url")
                        else f"<#{thread.id}>"
                    )
                    merged_urls.append(ju)
                    try:
                        tnm = thread.name[:200] if thread else query
                        await append_user_anime_to_personal_state(
                            interaction.guild, uid, f"mal:{aid}", tnm
                        )
                    except Exception:
                        logger.exception("Личный список после merge MAL-only")
                if merge_ops >= CONNECT_MAX_MERGES_PER_RUN:
                    errors.append(
                        "Достигнут лимит дописываний в существующие темы за один запуск; "
                        "запустите команду ещё раз."
                    )
                    break
                await asyncio.sleep(0.35)
                continue
            if mst == "already":
                await mark_mal_imported(interaction.user.id, aid)
                imported_ids.add(aid)
                merge_ops += 1
                try:
                    await append_user_anime_to_personal_state(
                        interaction.guild, uid, f"mal:{aid}", query[:500]
                    )
                except Exception:
                    logger.exception("Личный список после already MAL-only")
                if merge_ops >= CONNECT_MAX_MERGES_PER_RUN:
                    errors.append(
                        "Достигнут лимит дописываний в существующие темы за один запуск; "
                        "запустите команду ещё раз."
                    )
                    break
                continue
            if mst in ("edit_failed", "fetch_failed"):
                errors.append(f"{query}: тема MAL уже есть, не удалось обновить подпись")
                continue

            thread, _st, err = await create_mal_only_forum_thread(
                forum, entry, uid, aid
            )

        if err:
            errors.append(f"{query}: {err}")
            continue
        if not thread:
            errors.append(f"{query}: неизвестная ошибка")
            continue

        try:
            if info:
                pk = _clean_slug((info.get("anime_url") or "").strip())
                pt = str(info.get("title") or query)[:500]
            else:
                pk = f"mal:{aid}"
                pt = mal_item_title(entry)
            await append_user_anime_to_personal_state(interaction.guild, uid, pk, pt)
        except Exception:
            logger.exception("Личный список после новой темы из connectmyanimelist")

        await mark_mal_imported(interaction.user.id, aid)
        imported_ids.add(aid)
        n_new += 1
        ju = thread.jump_url if hasattr(thread, "jump_url") else f"<#{thread.id}>"
        created_urls.append(ju)
        await asyncio.sleep(1.25)

    lines = [
        f"Аккаунт **MAL** привязан: [{username}]({norm_url}).",
        f"Создано **новых** тем: **{n_new}**.",
    ]
    if merged_urls:
        lines.append(
            f"Дописаны в уже существующие темы ({len(merged_urls)}): "
            + ", ".join(merged_urls[:8])
        )
        if len(merged_urls) > 8:
            lines.append(f"_…и ещё ссылок: {len(merged_urls) - 8}_")
    if created_urls:
        lines.append("Новые темы: " + ", ".join(created_urls[:10]))
        if len(created_urls) > 10:
            lines.append(f"_…и ещё {len(created_urls) - 10}_")
    if errors:
        lines.append("Проблемы: " + "; ".join(errors[:3]))
        if len(errors) > 3:
            lines.append(f"_…и ещё {len(errors) - 3}_")
    await interaction.followup.send(
        _truncate("\n".join(lines), DISCORD_CONTENT_LIMIT), ephemeral=True
    )


@bot.tree.command(
    name="connectmyanimelist",
    description="Устаревшее имя — используйте /mal_import",
)
@app_commands.describe(
    list_url="(необязательно) новая ссылка MAL для перепривязки перед импортом",
    mal_status="Какие позиции брать из списка",
    max_topics="Сколько новых тем создать за один раз (1–25)",
)
@app_commands.choices(
    mal_status=[
        app_commands.Choice(name="Все записи", value="all"),
        app_commands.Choice(name="Смотрю", value="watching"),
        app_commands.Choice(name="В планах", value="plan_to_watch"),
        app_commands.Choice(name="Просмотрено", value="completed"),
        app_commands.Choice(name="Отложено", value="on_hold"),
        app_commands.Choice(name="Брошено", value="dropped"),
    ]
)
async def connectmyanimelist(
    interaction: discord.Interaction,
    mal_status: str,
    list_url: str | None = None,
    max_topics: app_commands.Range[int, 1, 25] = 10,
) -> None:
    await _run_mal_import(interaction, mal_status, list_url, max_topics)


@bot.tree.command(
    name="rateanime",
    description="Поставить оценку 1–10 аниме в этой теме форума (отдельное окно)",
)
async def rateanime(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    ch = interaction.channel
    if not isinstance(ch, discord.Thread):
        await interaction.response.send_message(
            "Откройте команду **внутри темы** форума с аниме.", ephemeral=True
        )
        return

    state = await read_state_copy()
    if not thread_has_rating_slot(state, ch.id):
        await interaction.response.send_message(
            "Эта тема не зарегистрирована для оценок. "
            "Создайте её через `/forum_add` или импорт из MAL (`/mal_import`).",
            ephemeral=True,
        )
        return

    try:
        await ensure_topic_side_panels(interaction.client, ch.id)
    except Exception as e:
        logger.warning("Панели перед /rateanime: %s", e)
    await interaction.response.send_modal(AnimeRatingModal(ch.id))


async def _mal_show_impl(
    interaction: discord.Interaction, member: discord.Member | None
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    if not bot.session:
        await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
        return
    raw_target = member or interaction.user
    target = (
        raw_target
        if isinstance(raw_target, discord.Member)
        else interaction.guild.get_member(raw_target.id)
    )
    if target is None:
        await interaction.response.send_message(
            "Укажите участника этого сервера.", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    state = await read_state_copy()
    embed, err = await build_mal_list_embed_for_member(bot.session, state, target)
    if err:
        await interaction.followup.send(err)
        return
    assert embed is not None
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="mal_show",
    description="[MAL] Список с сайта MyAnimeList (нужна привязка /mal_bind)",
)
@app_commands.describe(member="Чей список (по умолчанию ваш)")
async def mal_show(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mal_show_impl(interaction, member)


@bot.tree.command(
    name="checkanimelist",
    description="Устар.: используйте /mal_show",
)
@app_commands.describe(member="Чей список показать (по умолчанию вы)")
async def checkanimelist(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mal_show_impl(interaction, member)


@bot.tree.command(
    name="myanimelist",
    description="Устар.: используйте /mal_show",
)
@app_commands.describe(member="Чей список MAL (необязательно)")
async def myanimelist(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mal_show_impl(interaction, member)


async def _mylist_show_impl(
    interaction: discord.Interaction, member: discord.Member | None
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    raw_target = member or interaction.user
    target = (
        raw_target
        if isinstance(raw_target, discord.Member)
        else interaction.guild.get_member(raw_target.id)
    )
    if target is None:
        await interaction.response.send_message(
            "Укажите участника этого сервера.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    embed, err, _s, _u = await run_animelist_discord_topics(interaction.guild, target)
    if err:
        await interaction.followup.send(_truncate(err, DISCORD_CONTENT_LIMIT), ephemeral=True)
        return
    assert embed is not None
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(
    name="mylist_show",
    description="[Мой список] Личный Discord-лист (как в теме форума списков)",
)
@app_commands.describe(member="Чей список (если не указано — ваш)")
async def mylist_show(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mylist_show_impl(interaction, member)


@bot.tree.command(
    name="animelist",
    description="Устар.: используйте /mylist_show",
)
@app_commands.describe(member="Чей список (если не указано — ваш)")
async def animelist(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mylist_show_impl(interaction, member)


@bot.tree.command(
    name="mylist",
    description="Устар.: используйте /mylist_show",
)
@app_commands.describe(member="Чей список показать (если не указано — ваш)")
async def mylist(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mylist_show_impl(interaction, member)


async def _personal_slug_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    state = await read_state_copy()
    uid = str(interaction.user.id)
    pl = (state.get("personal_lists") or {}).get(uid)
    if not isinstance(pl, dict):
        return []
    order = pl.get("order")
    if not isinstance(order, list):
        return []
    cur = (current or "").strip().lower()
    choices: list[app_commands.Choice[str]] = []
    for k in order:
        ks = str(k).strip()
        if not ks:
            continue
        title = _title_for_list_key(state, ks)
        if cur and cur not in title.lower() and cur not in ks.lower():
            continue
        label = _truncate(f"{title} ({ks})", 100)
        choices.append(app_commands.Choice(name=label, value=ks))
        if len(choices) >= 25:
            break
    return choices


@bot.tree.command(
    name="checkanime",
    description="Устар.: используйте /mylist_show",
)
@app_commands.describe(member="Чей список показать (по умолчанию ваш)")
async def checkanime(
    interaction: discord.Interaction, member: discord.Member | None = None
) -> None:
    await _mylist_show_impl(interaction, member)


async def _admin_sync_personal_from_forum(
    interaction: discord.Interaction, member: discord.Member
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Команда только для **администраторов** сервера.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    scanned, updated = await sync_forum_threads_with_state(
        bot, interaction.guild, bot.session if bot.session else None
    )
    n, err = await sync_personal_list_from_anime_topics(interaction.guild, member.id)
    try:
        await ensure_personal_list_thread(
            bot, interaction.guild, member, session=bot.session
        )
        await rebuild_personal_list_display(
            bot, interaction.guild.id, member.id, session=bot.session
        )
    except Exception:
        logger.exception("syncanimelist: обновление личной темы")
    parts = [
        f"Основной форум: просмотрено веток **{scanned}**, обновлено записей **{updated}**.",
        f"В личном списке **{member.display_name}**: **{n}** позиций.",
    ]
    if err:
        parts.append(str(err))
    await interaction.followup.send("\n".join(parts), ephemeral=True)


@bot.tree.command(
    name="mylist_admin_sync",
    description="[Админ] Синхронизировать личный список участника с темами основного форума",
)
@app_commands.describe(member="Чей список пересобрать из базы бота")
@app_commands.default_permissions(administrator=True)
async def mylist_admin_sync(
    interaction: discord.Interaction, member: discord.Member
) -> None:
    await _admin_sync_personal_from_forum(interaction, member)


@bot.tree.command(
    name="syncanimelist",
    description="Устар.: используйте /mylist_admin_sync",
)
@app_commands.describe(member="Чей список пересобрать из базы бота")
@app_commands.default_permissions(administrator=True)
async def syncanimelist(
    interaction: discord.Interaction, member: discord.Member
) -> None:
    await _admin_sync_personal_from_forum(interaction, member)


@bot.tree.command(
    name="yummy_link",
    description="[Yummy] Ссылка для входа на сайте — токен привяжется после кнопки в Discord",
)
async def yummy_link(interaction: discord.Interaction) -> None:
    app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
    if not app:
        await interaction.response.send_message(
            "Владелец бота должен задать **YUMMY_APPLICATION_TOKEN** (приложение на https://yummyani.me/dev/applications).",
            ephemeral=True,
        )
        return
    public = (os.environ.get("YUMMY_LINK_PUBLIC_URL") or "").strip().rstrip("/")
    port = (os.environ.get("YUMMY_LINK_HTTP_PORT") or "").strip()
    if not public or not port:
        await interaction.response.send_message(
            "Веб-вход не настроен: в `.env` укажите **YUMMY_LINK_PUBLIC_URL** (как вас видит интернет, "
            "например `https://bot.example.com`) и **YUMMY_LINK_HTTP_PORT** (локальный порт, на котором "
            "бот слушает HTTP; прокси nginx/caddy перенаправляет на него `/yummy-link`).\n\n"
            "Пока можно привязать аккаунт вручную: **`/yummy_token`** с Bearer из DevTools.",
            ephemeral=True,
        )
        return
    tok = await yummy_link_create_session(interaction.user.id)
    url = f"{public}/yummy-link?token={quote(tok, safe='')}"
    view = YummyLinkVerifyView()
    await interaction.response.send_message(
        "1. Откройте ссылку и войдите в аккаунт YummyAnime (как на сайте).\n"
        f"2. После успешного входа нажмите кнопку ниже.\n\n{url}",
        view=view,
        ephemeral=True,
    )


async def _yummy_token_impl(interaction: discord.Interaction, bearer_token: str) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)
    app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
    if not app:
        await interaction.followup.send(
            "Владелец бота должен задать **YUMMY_APPLICATION_TOKEN** (приложение на yummyani.me/dev).",
            ephemeral=True,
        )
        return
    t = (bearer_token or "").strip()
    if t.lower().startswith("bearer "):
        t = t[7:].strip()
    if len(t) < 12:
        await interaction.followup.send("Токен слишком короткий.", ephemeral=True)
        return
    if not bot.session:
        await interaction.followup.send("Сессия HTTP не готова.", ephemeral=True)
        return
    prof = await yummy_api.yani_get_profile(bot.session, app, t, USER_AGENT)
    if not prof:
        await interaction.followup.send(
            "Не удалось получить профиль. Проверьте токен (скопируйте только часть после `Bearer `).",
            ephemeral=True,
        )
        return
    yid = prof.get("id")
    if yid is None:
        await interaction.followup.send("Ответ API без id пользователя.", ephemeral=True)
        return
    try:
        yid_i = int(yid)
    except (TypeError, ValueError):
        await interaction.followup.send("Некорректный id в ответе API.", ephemeral=True)
        return
    nick = str(prof.get("nickname") or "")
    await bind_yummy_account(interaction.user.id, t, yid_i, nick)
    await interaction.followup.send(
        f"YummyAnime привязан (**{nick or yid_i}**). Импорт: `/yummy_sync` или кнопка **Yummy ↻** в личной теме.\n"
        "_Токен хранится на сервере с ботом; не пересылайте его третьим лицам._",
        ephemeral=True,
    )


@bot.tree.command(
    name="yummy_token",
    description="[Yummy] Привязать Bearer-токен вручную (из DevTools браузера после входа на сайт)",
)
@app_commands.describe(
    bearer_token="Токен: Network → запрос к api.yani.tv → Authorization: Bearer …"
)
async def yummy_token(interaction: discord.Interaction, bearer_token: str) -> None:
    await _yummy_token_impl(interaction, bearer_token)


@bot.tree.command(
    name="yummybind",
    description="Устар.: используйте /yummy_link или /yummy_token",
)
@app_commands.describe(
    bearer_token="Токен: вкладка Сеть (Network) → любой запрос к api.yani.tv → Authorization: Bearer …"
)
async def yummybind(interaction: discord.Interaction, bearer_token: str) -> None:
    await _yummy_token_impl(interaction, bearer_token)


async def _yummy_unbind_impl(interaction: discord.Interaction) -> None:
    await unbind_yummy_account(interaction.user.id)
    await interaction.response.send_message(
        "Привязка YummyAnime снята. История импортов (`imported_yummy`) сохранена — повторный импорт не продублирует темы.",
        ephemeral=True,
    )


@bot.tree.command(
    name="yummy_unbind",
    description="[Yummy] Отвязать аккаунт YummyAnime от Discord-профиля",
)
async def yummy_unbind(interaction: discord.Interaction) -> None:
    await _yummy_unbind_impl(interaction)


@bot.tree.command(
    name="yummyunbind",
    description="Устар.: используйте /yummy_unbind",
)
async def yummyunbind(interaction: discord.Interaction) -> None:
    await _yummy_unbind_impl(interaction)


async def _run_yummy_sync(
    interaction: discord.Interaction,
    yummy_list: str,
    max_topics: app_commands.Range[int, 1, 25],
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
    if not app:
        await interaction.response.send_message(
            "Не задан **YUMMY_APPLICATION_TOKEN** на стороне бота.", ephemeral=True
        )
        return
    if not bot.session:
        await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    r = await run_yummy_list_import_for_member(
        interaction.guild,
        interaction.user.id,
        list_filter=yummy_list,
        max_topics=int(max_topics),
        session=bot.session,
        app_token=app,
    )
    if r.get("error"):
        await interaction.followup.send(
            _truncate(str(r["error"]), DISCORD_CONTENT_LIMIT), ephemeral=True
        )
        return
    lines = [
        f"**Новых тем:** **{r.get('n_new', 0)}**",
        f"**Дописано в существующие:** **{r.get('merge_ops', 0)}**",
    ]
    cr = r.get("created_urls") or []
    if cr:
        lines.append("Новые: " + ", ".join(cr[:8]))
        if len(cr) > 8:
            lines.append(f"_…ещё {len(cr) - 8}_")
    mer = r.get("merged_urls") or []
    if mer:
        lines.append("Объединено: " + ", ".join(mer[:6]))
    er = r.get("errors") or []
    if er:
        lines.append("Замечания: " + "; ".join(er[:4]))
    await interaction.followup.send(
        _truncate("\n".join(lines), DISCORD_CONTENT_LIMIT), ephemeral=True
    )


@bot.tree.command(
    name="yummy_sync",
    description="[Yummy] Импорт новых аниме из списка YummyAnime в основной форум и личный топик",
)
@app_commands.describe(
    yummy_list="Какой список на Yummy учитывать",
    max_topics="Максимум новых тем за один раз (1–25)",
)
@app_commands.choices(
    yummy_list=[
        app_commands.Choice(name="Все списки", value="all"),
        app_commands.Choice(name="Смотрю", value="watching"),
        app_commands.Choice(name="В планах", value="plan_to_watch"),
        app_commands.Choice(name="Просмотрено", value="completed"),
        app_commands.Choice(name="Отложено", value="on_hold"),
        app_commands.Choice(name="Брошено", value="dropped"),
    ]
)
async def yummy_sync(
    interaction: discord.Interaction,
    yummy_list: str,
    max_topics: app_commands.Range[int, 1, 25] = 15,
) -> None:
    await _run_yummy_sync(interaction, yummy_list, max_topics)


@bot.tree.command(
    name="syncyummy",
    description="Устар.: используйте /yummy_sync",
)
@app_commands.describe(
    yummy_list="Какой список на Yummy учитывать",
    max_topics="Максимум новых тем за один раз (1–25)",
)
@app_commands.choices(
    yummy_list=[
        app_commands.Choice(name="Все списки", value="all"),
        app_commands.Choice(name="Смотрю", value="watching"),
        app_commands.Choice(name="В планах", value="plan_to_watch"),
        app_commands.Choice(name="Просмотрено", value="completed"),
        app_commands.Choice(name="Отложено", value="on_hold"),
        app_commands.Choice(name="Брошено", value="dropped"),
    ]
)
async def syncyummy(
    interaction: discord.Interaction,
    yummy_list: str,
    max_topics: app_commands.Range[int, 1, 25] = 15,
) -> None:
    await _run_yummy_sync(interaction, yummy_list, max_topics)


@bot.tree.command(
    name="yummy_status",
    description="[Yummy] Показать, привязан ли аккаунт YummyAnime к вашему Discord",
)
async def yummy_status(interaction: discord.Interaction) -> None:
    state = await read_state_copy()
    acc = (state.get("yummy_accounts") or {}).get(str(interaction.user.id))
    if not isinstance(acc, dict):
        await interaction.response.send_message(
            "Аккаунт YummyAnime **не привязан**. Используйте `/yummy_link` (веб-вход) или `/yummy_token`.",
            ephemeral=True,
        )
        return
    nick = str(acc.get("nickname") or "")
    yid = acc.get("yummy_user_id")
    await interaction.response.send_message(
        f"Привязан: **{nick or yid}** (id Yummy: `{yid}`). Импорт: `/yummy_sync`.",
        ephemeral=True,
    )


@bot.tree.command(
    name="yummy_list",
    description="[Yummy] Показать записи из вашего списка на YummyAnime (без создания тем в Discord)",
)
@app_commands.describe(
    yummy_list="Какой список показать",
    limit="Сколько строк вывести (1–40)",
)
@app_commands.choices(
    yummy_list=[
        app_commands.Choice(name="Все списки", value="all"),
        app_commands.Choice(name="Смотрю", value="watching"),
        app_commands.Choice(name="В планах", value="plan_to_watch"),
        app_commands.Choice(name="Просмотрено", value="completed"),
        app_commands.Choice(name="Отложено", value="on_hold"),
        app_commands.Choice(name="Брошено", value="dropped"),
    ]
)
async def yummy_list_cmd(
    interaction: discord.Interaction,
    yummy_list: str,
    limit: app_commands.Range[int, 1, 40] = 20,
) -> None:
    app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
    if not app:
        await interaction.response.send_message(
            "Не задан **YUMMY_APPLICATION_TOKEN**.", ephemeral=True
        )
        return
    if not bot.session:
        await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
        return
    state = await read_state_copy()
    acc = (state.get("yummy_accounts") or {}).get(str(interaction.user.id))
    if not isinstance(acc, dict):
        await interaction.response.send_message(
            "Сначала привяжите аккаунт: `/yummy_link` или `/yummy_token`.", ephemeral=True
        )
        return
    yuid = acc.get("yummy_user_id")
    bearer = (acc.get("access_token") or "").strip()
    if yuid is None or not bearer:
        await interaction.response.send_message("Неполная привязка YummyAnime.", ephemeral=True)
        return
    try:
        yuid_i = int(yuid)
    except (TypeError, ValueError):
        await interaction.response.send_message("Некорректный yummy_user_id в базе.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    items, new_tok, err_msg = await yummy_api.yani_fetch_lists_with_token_refresh(
        bot.session, app, bearer, yuid_i, USER_AGENT
    )
    if err_msg:
        await interaction.followup.send(
            _truncate(err_msg, DISCORD_CONTENT_LIMIT), ephemeral=True
        )
        return
    if new_tok:
        await update_yummy_access_token(interaction.user.id, new_tok)
    entries = yummy_api.filter_yummy_entries_by_status(items or [], yummy_list)
    lines: list[str] = []
    for e in entries[: int(limit)]:
        title = yummy_api.yummy_entry_title(e)
        url = yummy_api.yummy_entry_anime_url(e)
        if url:
            lines.append(f"• [{title}]({url})")
        else:
            lines.append(f"• {title}")
    body = "\n".join(lines) if lines else "_Пусто для выбранного фильтра._"
    if len(entries) > int(limit):
        body += f"\n_…всего записей: **{len(entries)}**_"
    nick = str(acc.get("nickname") or "")
    embed = discord.Embed(
        title=f"YummyAnime — {nick or yuid_i}",
        description=_truncate(body, EMBED_DESC_LIMIT),
        color=EMBED_COLOR,
    )
    embed.set_footer(text="Только просмотр; темы в Discord создаёт /yummy_sync")
    await interaction.followup.send(embed=embed, ephemeral=True)


admin_cmd_group = app_commands.Group(
    name="admin",
    description="Админ: YummyAnime, скан форума, личные списки, обновление тем",
)


@admin_cmd_group.command(
    name="yummy_resync",
    description="Принудительно синхронизировать список Yummy участника с форумом",
)
@app_commands.describe(
    member="Участник с привязкой Yummy (/yummy_link или /yummy_token)",
    max_topics="Максимум новых тем за запуск (1–25)",
)
async def admin_yummy_resync(
    interaction: discord.Interaction,
    member: discord.Member,
    max_topics: app_commands.Range[int, 1, 25] = 20,
) -> None:
    ok, err = _admin_member_ok(interaction)
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
    if not app:
        await interaction.response.send_message(
            "Нет **YUMMY_APPLICATION_TOKEN**.", ephemeral=True
        )
        return
    if not bot.session:
        await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    r = await run_yummy_list_import_for_member(
        interaction.guild,
        member.id,
        list_filter="all",
        max_topics=int(max_topics),
        session=bot.session,
        app_token=app,
    )
    if r.get("error"):
        await interaction.followup.send(
            _truncate(str(r["error"]), DISCORD_CONTENT_LIMIT), ephemeral=True
        )
        return
    msg = (
        f"**{member.display_name}:** новых тем **{r.get('n_new', 0)}**, "
        f"дописано **{r.get('merge_ops', 0)}**."
    )
    er = r.get("errors") or []
    if er:
        msg += "\n" + "; ".join(er[:3])
    await interaction.followup.send(_truncate(msg, DISCORD_CONTENT_LIMIT), ephemeral=True)


@admin_cmd_group.command(
    name="yummy_status",
    description="Статус последнего фонового опроса YummyAnime",
)
async def admin_yummy_status(interaction: discord.Interaction) -> None:
    ok, err = _admin_member_ok(interaction)
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    state = await read_state_copy()
    poll = (state.get("meta") or {}).get("yummy_poll")
    if not isinstance(poll, dict):
        embed = discord.Embed(
            title="Фон YummyAnime",
            description="Ещё не было успешного цикла (или опрос отключён — нет токена приложения).",
            color=EMBED_COLOR,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    desc = (
        f"**Время (UTC):** {poll.get('last_run_utc', '—')}\n"
        f"**Участников проверено:** {poll.get('users_checked', 0)}\n"
        f"**Новых тем за цикл:** {poll.get('imports_new', 0)}\n"
    )
    errs = poll.get("errors")
    if isinstance(errs, list) and errs:
        desc += "**Ошибки:**\n" + "\n".join(f"· {_truncate(str(e), 200)}" for e in errs[:6])
    embed = discord.Embed(title="Фон YummyAnime", description=desc, color=EMBED_COLOR)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@admin_cmd_group.command(
    name="forum_scan",
    description="Обновить anime_topics по веткам основного форума (как при старте синка списков)",
)
async def admin_forum_scan(interaction: discord.Interaction) -> None:
    ok, err = _admin_member_ok(interaction)
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    if not bot.session:
        await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    assert interaction.guild is not None
    scanned, updated = await sync_forum_threads_with_state(
        bot, interaction.guild, bot.session
    )
    await interaction.followup.send(
        f"Просмотрено веток: **{scanned}**, обновлено записей в состоянии: **{updated}**.",
        ephemeral=True,
    )


@admin_cmd_group.command(
    name="personal_rebuild",
    description="Пересоздать карточки в личной теме участника",
)
@app_commands.describe(member="Участник")
async def admin_personal_rebuild(
    interaction: discord.Interaction, member: discord.Member
) -> None:
    ok, err = _admin_member_ok(interaction)
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    assert interaction.guild is not None
    try:
        await ensure_personal_list_thread(
            bot, interaction.guild, member, session=bot.session
        )
        await rebuild_personal_list_display(
            bot, interaction.guild.id, member.id, session=bot.session
        )
    except Exception:
        logger.exception("admin personal_rebuild")
        await interaction.followup.send("Ошибка при пересборке.", ephemeral=True)
        return
    await interaction.followup.send(
        f"Личная тема **{member.display_name}** обновлена.", ephemeral=True
    )


@admin_cmd_group.command(
    name="repair_topics",
    description="Досинхронизировать темы основного форума (реакции, панели, карточка Yummy)",
)
async def admin_repair_topics(interaction: discord.Interaction) -> None:
    ok, err = _admin_member_ok(interaction)
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    if not bot.session:
        await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    assert interaction.guild is not None
    forum = await resolve_forum_channel(bot)
    if not forum:
        await interaction.followup.send("Канал форума не найден.", ephemeral=True)
        return
    seen: set[int] = set()
    ok_n = 0
    err_n = 0

    async def run(th: discord.Thread) -> None:
        nonlocal ok_n, err_n
        if th.id in seen or th.parent_id != forum.id:
            return
        seen.add(th.id)
        try:
            await repair_single_forum_thread(bot, forum, th, bot.session)
            ok_n += 1
        except Exception:
            logger.exception("admin repair_topics %s", th.id)
            err_n += 1

    for th in forum.threads:
        await run(th)
    gt = interaction.guild.threads
    seq = gt.values() if hasattr(gt, "values") else gt
    for th in seq:
        if th.parent_id == forum.id:
            await run(th)
    try:
        async for th in forum.archived_threads(limit=100):
            await run(th)
    except discord.HTTPException as e:
        logger.warning("Архив форума admin: %s", e)
    await interaction.followup.send(
        f"Готово. Веток: **{len(seen)}**, успешно **{ok_n}**"
        + (f", ошибок **{err_n}**" if err_n else "")
        + ".",
        ephemeral=True,
    )


bot.tree.add_command(admin_cmd_group)


class AdminPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=300)

    @discord.ui.select(
        placeholder="Выберите действие…",
        custom_id="adminpanel:menu",
        options=[
            discord.SelectOption(
                label="Статус фона Yummy",
                value="yummy_status",
                description="Последний автоматический опрос списков",
            ),
            discord.SelectOption(
                label="Скан основного форума",
                value="forum_scan",
                description="Обновить anime_topics из веток",
            ),
            discord.SelectOption(
                label="Обновить темы форума",
                value="repair_topics",
                description="Реакции, панели, описание Yummy",
            ),
        ],
    )
    async def admin_menu(
        self, interaction: discord.Interaction, select: discord.ui.Select
    ) -> None:
        ok, err = _admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        choice = select.values[0]
        if choice == "yummy_status":
            state = await read_state_copy()
            poll = (state.get("meta") or {}).get("yummy_poll")
            if not isinstance(poll, dict):
                await interaction.response.send_message(
                    "Фоновый опрос ещё не выполнялся или нет токена приложения.",
                    ephemeral=True,
                )
                return
            desc = (
                f"**UTC:** {poll.get('last_run_utc', '—')}\n"
                f"**Проверено пользователей:** {poll.get('users_checked', 0)}\n"
                f"**Новых тем:** {poll.get('imports_new', 0)}\n"
            )
            errs = poll.get("errors")
            if isinstance(errs, list) and errs:
                desc += "\n".join(f"· {_truncate(str(x), 180)}" for x in errs[:5])
            embed = discord.Embed(title="Yummy — фон", description=desc, color=EMBED_COLOR)
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        assert interaction.guild is not None
        if choice == "forum_scan":
            if not bot.session:
                await interaction.followup.send("Нет HTTP сессии.", ephemeral=True)
                return
            scanned, updated = await sync_forum_threads_with_state(
                bot, interaction.guild, bot.session
            )
            await interaction.followup.send(
                f"Скан: веток **{scanned}**, обновлено записей **{updated}**.",
                ephemeral=True,
            )
            return
        if choice == "repair_topics":
            if not bot.session:
                await interaction.followup.send("Нет HTTP сессии.", ephemeral=True)
                return
            forum = await resolve_forum_channel(bot)
            if not forum:
                await interaction.followup.send("Форум не найден.", ephemeral=True)
                return
            seen: set[int] = set()
            ok_n = err_n = 0

            async def run(th: discord.Thread) -> None:
                nonlocal ok_n, err_n
                if th.id in seen or th.parent_id != forum.id:
                    return
                seen.add(th.id)
                try:
                    await repair_single_forum_thread(bot, forum, th, bot.session)
                    ok_n += 1
                except Exception:
                    logger.exception("adminpanel repair %s", th.id)
                    err_n += 1

            for th in forum.threads:
                await run(th)
            gt = interaction.guild.threads
            seq = gt.values() if hasattr(gt, "values") else gt
            for th in seq:
                if th.parent_id == forum.id:
                    await run(th)
            try:
                async for th in forum.archived_threads(limit=80):
                    await run(th)
            except discord.HTTPException as e:
                logger.warning("adminpanel архив: %s", e)
            await interaction.followup.send(
                f"Тем обработано: **{len(seen)}**, ок **{ok_n}**"
                + (f", ошибок **{err_n}**" if err_n else "")
                + ".",
                ephemeral=True,
            )


@bot.tree.command(
    name="adminpanel",
    description="[Админы] Панель быстрых действий бота",
)
async def adminpanel(interaction: discord.Interaction) -> None:
    ok, err = _admin_member_ok(interaction)
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    embed = discord.Embed(
        title="Админ-панель",
        description=(
            "Меню слева — быстрые действия.\n"
            "Полный набор: **`/admin yummy_resync`**, **`/admin forum_scan`**, "
            "**`/admin repair_topics`**, **`/admin personal_rebuild`**, **`/admin yummy_status`**."
        ),
        color=EMBED_COLOR,
    )
    await interaction.response.send_message(
        embed=embed, view=AdminPanelView(), ephemeral=True
    )


async def _mylist_top_impl(
    interaction: discord.Interaction,
    slot1: str | None = None,
    slot2: str | None = None,
    slot3: str | None = None,
    slot4: str | None = None,
    slot5: str | None = None,
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    raw_slots = [slot1, slot2, slot3, slot4, slot5]
    slots = [str(s).strip() for s in raw_slots if s and str(s).strip()]
    await interaction.response.defer(ephemeral=True, thinking=True)

    uid = interaction.user.id
    uid_s = str(uid)
    state = await read_state_copy()
    pl = (state.get("personal_lists") or {}).get(uid_s)
    if not isinstance(pl, dict):
        await interaction.followup.send(
            "Личного списка ещё нет — сначала добавьте аниме через `/forum_add`.",
            ephemeral=True,
        )
        return
    order = pl.get("order")
    if not isinstance(order, list):
        order = []

    if not slots:
        async with _state_lock:
            data = _load_state()
            pl2 = data.setdefault("personal_lists", {}).setdefault(uid_s, {})
            pl2["top5"] = []
            data["personal_lists"][uid_s] = pl2
            _write_state(data)
        await rebuild_personal_list_display(
            bot, interaction.guild.id, uid, session=bot.session
        )
        await interaction.followup.send("Топ очищен. Карточки в теме пересобраны.", ephemeral=True)
        return

    uniq = list(dict.fromkeys(slots))[:5]
    for s in uniq:
        if s not in order:
            await interaction.followup.send(
                f"В вашем списке нет ключа `{s}`. Выберите значения из автодополнения.",
                ephemeral=True,
            )
            return

    async with _state_lock:
        data = _load_state()
        pl2 = data.setdefault("personal_lists", {}).setdefault(uid_s, {})
        pl2["top5"] = uniq
        data["personal_lists"][uid_s] = pl2
        _write_state(data)
    await rebuild_personal_list_display(
        bot, interaction.guild.id, uid, session=bot.session
    )
    await interaction.followup.send(
        f"Топ сохранён (**{len(uniq)}**). Карточки пересобраны.",
        ephemeral=True,
    )


@bot.tree.command(
    name="mylist_top",
    description="[Мой список] До 5 аниме для блока «Топ» в личной теме",
)
@app_commands.describe(
    slot1="1-е место топа",
    slot2="2-е место",
    slot3="3-е место",
    slot4="4-е место",
    slot5="5-е место",
)
@app_commands.autocomplete(
    slot1=_personal_slug_autocomplete,
    slot2=_personal_slug_autocomplete,
    slot3=_personal_slug_autocomplete,
    slot4=_personal_slug_autocomplete,
    slot5=_personal_slug_autocomplete,
)
async def mylist_top(
    interaction: discord.Interaction,
    slot1: str | None = None,
    slot2: str | None = None,
    slot3: str | None = None,
    slot4: str | None = None,
    slot5: str | None = None,
) -> None:
    await _mylist_top_impl(
        interaction, slot1, slot2, slot3, slot4, slot5
    )


@bot.tree.command(
    name="settopanime",
    description="Устар.: используйте /mylist_top",
)
@app_commands.describe(
    slot1="1-е место топа",
    slot2="2-е место",
    slot3="3-е место",
    slot4="4-е место",
    slot5="5-е место",
)
@app_commands.autocomplete(
    slot1=_personal_slug_autocomplete,
    slot2=_personal_slug_autocomplete,
    slot3=_personal_slug_autocomplete,
    slot4=_personal_slug_autocomplete,
    slot5=_personal_slug_autocomplete,
)
async def settopanime(
    interaction: discord.Interaction,
    slot1: str | None = None,
    slot2: str | None = None,
    slot3: str | None = None,
    slot4: str | None = None,
    slot5: str | None = None,
) -> None:
    await _mylist_top_impl(
        interaction, slot1, slot2, slot3, slot4, slot5
    )


async def _mylist_panel_impl(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    ch = interaction.channel
    if not isinstance(ch, discord.Thread) or ch.parent_id != LIST_FORUM_CHANNEL_ID:
        await interaction.response.send_message(
            "Вызовите команду **внутри своей личной темы** в форуме списков.",
            ephemeral=True,
        )
        return
    resolved = await resolve_personal_list_owner_for_interaction(interaction)
    if not resolved:
        return
    oid, pl = resolved
    if oid != interaction.user.id:
        await interaction.response.send_message(
            f"Эта тема в базе за <@{oid}>. Войдите с того аккаунта.",
            ephemeral=True,
        )
        return
    cid = pl.get("control_message_id")
    if cid:
        try:
            await ch.fetch_message(int(cid))
            await interaction.response.send_message(
                "Панель уже на месте. Если кнопки «мёртвые», нажмите **Обновить** на панели "
                "или перезапустите бота (команды регистрируются в setup_hook).",
                ephemeral=True,
            )
            return
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    await interaction.response.defer(ephemeral=True, thinking=True)
    hub_embed = _personal_hub_embed(pl, interaction.user.display_name)
    try:
        hub_msg = await ch.send(embed=hub_embed, view=PersonalTopicHubView())
    except discord.HTTPException as e:
        await interaction.followup.send(f"Не удалось отправить панель: {e}", ephemeral=True)
        return
    await _set_personal_list_fields(oid, control_message_id=hub_msg.id)
    await interaction.followup.send(
        "Панель отправлена **вниз темы**. При необходимости удалите дубликат вручную.",
        ephemeral=True,
    )


@bot.tree.command(
    name="mylist_panel",
    description="[Мой список] Восстановить панель кнопок в личной теме",
)
async def mylist_panel(interaction: discord.Interaction) -> None:
    await _mylist_panel_impl(interaction)


@bot.tree.command(
    name="mytopicpanel",
    description="Устар.: используйте /mylist_panel",
)
async def mytopicpanel(interaction: discord.Interaction) -> None:
    await _mylist_panel_impl(interaction)


async def _mylist_edit_impl(
    interaction: discord.Interaction,
    name: str | None,
    description: str | None,
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    if not name and not description:
        await interaction.response.send_message(
            "Укажите **name** и/или **description**.", ephemeral=True
        )
        return

    uid = interaction.user.id
    uid_s = str(uid)
    state = await read_state_copy()
    pl = (state.get("personal_lists") or {}).get(uid_s)
    if not isinstance(pl, dict) or not pl.get("thread_id"):
        await interaction.response.send_message(
            "Личной темы ещё нет — она создаётся при первом добавлении аниме (`/forum_add`).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    tid = int(pl["thread_id"])
    thread = interaction.client.get_channel(tid)
    if thread is None:
        try:
            thread = await interaction.client.fetch_channel(tid)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            thread = None
    if not isinstance(thread, discord.Thread):
        await interaction.followup.send("Личная тема не найдена (проверьте ID).", ephemeral=True)
        return

    if name:
        try:
            await thread.edit(name=name.strip()[:100])
        except discord.HTTPException as e:
            await interaction.followup.send(f"Не удалось сменить название: {e}", ephemeral=True)
            return

    if description:
        sid = int(pl.get("starter_message_id") or 0)
        if not sid:
            await interaction.followup.send(
                "В базе нет id первого сообщения — удалите тему и дайте боту создать заново.",
                ephemeral=True,
            )
            return
        try:
            msg = await thread.fetch_message(sid)
            await msg.edit(content=_truncate(description.strip(), 2000))
        except discord.HTTPException as e:
            await interaction.followup.send(f"Не удалось изменить пост: {e}", ephemeral=True)
            return

    await interaction.followup.send("Готово.", ephemeral=True)


@bot.tree.command(
    name="mylist_edit",
    description="[Мой список] Изменить название и первый пост личной темы",
)
@app_commands.describe(
    name="Новое название темы форума",
    description="Новый текст первого сообщения в теме",
)
async def mylist_edit(
    interaction: discord.Interaction,
    name: str | None = None,
    description: str | None = None,
) -> None:
    await _mylist_edit_impl(interaction, name, description)


@bot.tree.command(
    name="editmyanimelist",
    description="Устар.: используйте /mylist_edit",
)
@app_commands.describe(
    name="Новое название темы форума",
    description="Новый текст первого сообщения в теме",
)
async def editmyanimelist(
    interaction: discord.Interaction,
    name: str | None = None,
    description: str | None = None,
) -> None:
    await _mylist_edit_impl(interaction, name, description)


@bot.tree.command(
    name="update_topics",
    description="[Администраторы] Досинхронизировать старые темы: реакции, панели, описание YummyAnime",
)
@app_commands.default_permissions(administrator=True)
async def update_topics(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Команда только для **администраторов** сервера.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    forum = await resolve_forum_channel(bot)
    if not forum:
        await interaction.followup.send("Канал форума не найден.", ephemeral=True)
        return

    seen: set[int] = set()
    ok = 0
    err = 0

    async def run(th: discord.Thread) -> None:
        nonlocal ok, err
        if th.id in seen or th.parent_id != forum.id:
            return
        seen.add(th.id)
        try:
            await repair_single_forum_thread(
                bot, forum, th, bot.session if bot.session else None
            )
            ok += 1
        except Exception:
            logger.exception("update_topics: ветка %s", th.id)
            err += 1

    for th in forum.threads:
        await run(th)
    gt = interaction.guild.threads
    seq = gt.values() if hasattr(gt, "values") else gt
    for th in seq:
        if th.parent_id == forum.id:
            await run(th)
    try:
        async for th in forum.archived_threads(limit=100):
            await run(th)
    except discord.HTTPException as e:
        logger.warning("Архив форума: %s", e)

    await interaction.followup.send(
        f"Готово. Обработано уникальных веток: **{len(seen)}** (успешных проходов **{ok}**"
        + (f", ошибок **{err}**" if err else "")
        + ").",
        ephemeral=True,
    )


@bot.tree.command(
    name="checkduplicates",
    description="Найти дубликаты тем форума (одно аниме — несколько веток) и при необходимости удалить лишние",
)
async def checkduplicates(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    state = await read_state_copy()
    groups = _collect_duplicate_groups(state)
    if not groups:
        await interaction.response.send_message(
            "Дубликатов не найдено: у каждого slug YummyAnime и каждого MAL id не больше одной зарегистрированной темы.",
            ephemeral=True,
        )
        return

    topics = state.get("anime_topics", {})
    lines: list[str] = [
        "Несколько **веток форума** привязаны к **одному и тому же** аниме "
        "(одинаковый каталог YummyAnime или один id на MAL). "
        "Оставляется тема, которая записана в базе бота; остальные можно снять кнопкой ниже.",
        "",
    ]
    victims: list[int] = []
    for i, g in enumerate(groups, 1):
        keeper = _pick_keeper_thread_id(g, topics)
        extra = sorted(x for x in g.thread_ids if x != keeper)
        victims.extend(extra)
        lines.append(f"**Группа {i}**")
        for lab in g.labels:
            lines.append(f"· {lab}")
        lines.append(f"· Оставить: <#{keeper}>")
        lines.append(
            "· Удалить: "
            + (", ".join(f"<#{x}>" for x in extra) if extra else "—")
        )
        lines.append("")

    victims = list(dict.fromkeys(victims))
    text = _truncate("\n".join(lines).rstrip(), DISCORD_CONTENT_LIMIT)

    embed = discord.Embed(
        title="Дубликаты тем",
        description=text,
        color=0xE74C3C,
    )
    embed.set_footer(
        text="Удаление требует права «Управлять ветками». Кнопка доступна только вам."
    )

    view = DuplicateCleanupView(
        requester_id=interaction.user.id,
        victims=victims,
    )
    await interaction.response.send_message(
        embed=embed,
        view=view,
        ephemeral=True,
    )


def _normalize_discord_token(raw: str | None) -> str:
    if not raw:
        return ""
    t = str(raw).strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in "'\"":
        t = t[1:-1].strip()
    return t


def main() -> None:
    load_dotenv()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(stream=sys.stdout),
            logging.FileHandler(
                DATA_DIR / "bot.log", encoding="utf-8", mode="a"
            ),
        ],
        force=True,
    )
    token = _normalize_discord_token(os.environ.get("DISCORD_BOT_TOKEN"))
    if not token:
        err = (
            "Ошибка: не задан DISCORD_BOT_TOKEN.\n"
            "Добавьте строку в файл .env рядом с bot.py:\n"
            "  DISCORD_BOT_TOKEN=ваш_токен\n"
            "Токен: Discord Developer Portal → ваше приложение → Bot → Reset Token / скопировать."
        )
        print(err, file=sys.stderr)
        raise SystemExit(1)
    if len(token) < 50:
        print(
            "Предупреждение: токен выглядит слишком коротким. Проверьте, что в .env нет лишних пробелов и кавычек.",
            file=sys.stderr,
        )

    try:
        bot.run(token)
    except discord.LoginFailure:
        print(
            "\n=== Вход не удался (неверный или отозванный токен) ===\n"
            "Создайте новый токен: https://discord.com/developers/applications\n"
            "→ ваше приложение → Bot → Reset Token, вставьте в .env как DISCORD_BOT_TOKEN=...\n"
            "Подробности в файле data/bot.log\n",
            file=sys.stderr,
        )
        logger.exception("LoginFailure")
        raise SystemExit(1) from None
    except discord.HTTPException as e:
        if e.status == 429:
            print(
                "\n=== Discord: 429 Too Many Requests ===\n"
                "Слишком много попыток входа с этого IP (частые перезапуски на хостинге).\n"
                "Подождите 15–60 минут, уменьшите частоту рестартов, смените IP/хостинг при необходимости.\n",
                file=sys.stderr,
            )
        else:
            print(f"\n=== Ошибка HTTP Discord: {e.status} ===\n{e}\n", file=sys.stderr)
        logger.exception("HTTPException при запуске")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
