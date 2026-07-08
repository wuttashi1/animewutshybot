"""
Discord bot: YummyAnime — каталог аниме на форуме, личные списки, MAL/Yummy импорт.
Настройка на сервере: `/bot setup` (категория + форумы). Команды сгруппированы: /anime, /list, /mal, /yummy, /admin, /bot, /owner.
Токен: DISCORD_BOT_TOKEN. Владелец бота: DISCORD_BOT_OWNER_ID (@wutshy).
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import aiohttp
import discord
import guild_config
import personal_display
import roaster
import roaster_automation
import yummy_api
from dotenv import load_dotenv
from discord import app_commands
from discord.ext import commands

BOT_OWNER_USERNAME = roaster.OWNER_USERNAME
PERSONAL_PAGE_SIZE = personal_display.PAGE_SIZE
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

load_dotenv()

logger = logging.getLogger(__name__)

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
        "guilds": {},
        "meta": {
            "bot_info_thread_id": None,
            "roaster_global_enabled": False,
        },
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
        "guilds",
        "meta",
    ):
        if key not in data or not isinstance(data[key], dict):
            data[key] = {}
    meta = data.setdefault("meta", {})
    if "bot_info_thread_id" not in meta:
        meta["bot_info_thread_id"] = None
    if "roaster_global_enabled" not in meta:
        meta["roaster_global_enabled"] = False
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


def schedule_personal_list_refresh(
    guild_id: int, user_id: int, *, defer_ui: bool = False
) -> None:
    """Debounce пересборки карточек; defer_ui — только state, без заливки канала (массовый импорт)."""
    if defer_ui:
        async def _mark_deferred() -> None:
            await _set_personal_list_fields(user_id, ui_deferred=True)

        asyncio.create_task(_mark_deferred())
        return

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
    if await is_bot_info_thread(forum.guild.id, thread.id):
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
    forum = await resolve_forum_channel(client, guild.id)
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
    if await is_bot_info_thread(forum.guild.id, thread.id):
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
        description=(
            "Настройка сервера: **`/bot setup`** (категория + форумы).\n"
            "Текст в чате: `!aa запрос`, `!animeadd`."
        ),
        color=EMBED_COLOR,
    )
    rows = [
        ("**`/anime`**", "`add` · `rate` · `duplicates`"),
        ("**`/list`**", "`show` · `top` · `panel` · `edit` · `mode` (сводка/страницы/галерея)"),
        ("**`/mal`**", "`bind` · `import` · `show`"),
        ("**`/yummy`**", "`bind` · `unbind` · `sync`"),
        ("**`/admin`**", "скан форума, синк Yummy, ремонт тем, **`roaster_enable`**"),
        ("**`/bot`**", "`setup` · `status`"),
        ("**`/owner`**", "`on` / `off` — глобальный «Обзыватель» (@wutshy)"),
        ("**`/roast`**", "Сатирическая шутка (и **авто** в чате + раз в 1–6 ч)"),
    ]
    for name, desc in rows:
        e.add_field(name=name, value=desc, inline=False)
    e.set_footer(text="Панели «Оценка» и «Рекомендация» создаются под первым сообщением новых тем автоматически.")
    return e


async def ensure_bot_info_thread(client: discord.Client, guild_id: int) -> None:
    """Справочная ветка в форуме каталога (создаётся при /bot setup)."""
    cfg = await get_guild_cfg(guild_id)
    tid = guild_config.bot_info_thread_id(cfg)
    if not tid:
        return
    async with _state_lock:
        data = _load_state()
        data.setdefault("meta", {})["bot_info_thread_id"] = tid
        _write_state(data)
    ch = client.get_channel(tid)
    if ch is None:
        try:
            ch = await client.fetch_channel(tid)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            ch = None
    if not isinstance(ch, discord.Thread):
        logger.warning(
            "Справочная ветка %s не найдена — выполните /bot setup или проверьте права.",
            tid,
        )
        return
    if ch.archived:
        logger.info("Справочная ветка %s в архиве — разархивируйте при необходимости.", tid)


async def get_guild_cfg(guild_id: int) -> dict[str, Any] | None:
    state = await read_state_copy()
    return guild_config.guild_config_from_state(state, guild_id)


async def save_guild_cfg(guild_id: int, cfg: dict[str, Any]) -> None:
    async with _state_lock:
        data = _load_state()
        data.setdefault("guilds", {})[str(guild_id)] = cfg
        _write_state(data)


def _bot_owner_ids() -> set[int]:
    raw = (os.environ.get("DISCORD_BOT_OWNER_ID") or "").strip()
    ids: set[int] = set()
    if raw:
        for part in raw.replace(",", " ").split():
            try:
                ids.add(int(part.strip()))
            except ValueError:
                continue
    return ids


def is_bot_owner(user: discord.abc.User) -> bool:
    if user.id in _bot_owner_ids():
        return True
    return user.name.lower() == BOT_OWNER_USERNAME.lower()


async def is_roaster_active(guild_id: int) -> bool:
    state = await read_state_copy()
    global_on = bool((state.get("meta") or {}).get("roaster_global_enabled"))
    if not global_on:
        return False
    cfg = guild_config.guild_config_from_state(state, guild_id)
    return guild_config.is_guild_roaster_enabled(cfg)


def pick_roast_titles(
    state: dict[str, Any], user_id: int, *, limit: int = 8
) -> list[str]:
    titles: list[str] = []
    pl = (state.get("personal_lists") or {}).get(str(user_id))
    if isinstance(pl, dict):
        order = pl.get("order")
        if isinstance(order, list):
            for k in order:
                ks = str(k).strip()
                if not ks:
                    continue
                titles.append(_title_for_list_key(state, ks))
                if len(titles) >= limit:
                    return titles
    topics = state.get("anime_topics") or {}
    if isinstance(topics, dict):
        for _key, ent in topics.items():
            if not isinstance(ent, dict):
                continue
            if user_id not in _parse_adder_ids(ent.get("adders")):
                continue
            tid = int(ent.get("thread_id") or 0)
            meta = (state.get("threads") or {}).get(str(tid))
            if isinstance(meta, dict):
                t = str(meta.get("title") or "").strip()
                if t:
                    titles.append(t)
            if len(titles) >= limit:
                break
    return titles


def _legacy_forum_id() -> int | None:
    raw = (os.environ.get("DISCORD_FORUM_CHANNEL_ID") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return None


def _legacy_list_forum_id() -> int | None:
    raw = (os.environ.get("DISCORD_LIST_FORUM_CHANNEL_ID") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return None


async def resolve_forum_channel(
    client: discord.Client, guild_id: int | None = None
) -> discord.ForumChannel | None:
    if guild_id is not None:
        cfg = await get_guild_cfg(guild_id)
        ch = await guild_config.resolve_forum_channel(client, guild_id, cfg)
        if ch:
            return ch
    legacy = _legacy_forum_id()
    if legacy:
        ch = client.get_channel(legacy)
        if ch is None:
            try:
                ch = await client.fetch_channel(legacy)
            except (discord.NotFound, discord.Forbidden):
                return None
        return ch if isinstance(ch, discord.ForumChannel) else None
    if guild_id is not None:
        return None
    for g in client.guilds:
        ch = await resolve_forum_channel(client, g.id)
        if ch:
            return ch
    return None


async def resolve_list_forum_channel(
    client: discord.Client, guild_id: int | None = None
) -> discord.ForumChannel | None:
    if guild_id is not None:
        cfg = await get_guild_cfg(guild_id)
        ch = await guild_config.resolve_list_forum_channel(client, guild_id, cfg)
        if ch:
            return ch
    legacy = _legacy_list_forum_id()
    if legacy:
        ch = client.get_channel(legacy)
        if ch is None:
            try:
                ch = await client.fetch_channel(legacy)
            except (discord.NotFound, discord.Forbidden):
                return None
        return ch if isinstance(ch, discord.ForumChannel) else None
    return None


async def list_forum_parent_id(guild_id: int) -> int | None:
    cfg = await get_guild_cfg(guild_id)
    return guild_config.list_forum_channel_id(cfg) or _legacy_list_forum_id()


async def is_bot_info_thread(guild_id: int, thread_id: int) -> bool:
    cfg = await get_guild_cfg(guild_id)
    tid = guild_config.bot_info_thread_id(cfg)
    if tid and thread_id == tid:
        return True
    state = await read_state_copy()
    raw = (state.get("meta") or {}).get("bot_info_thread_id")
    try:
        return raw is not None and int(raw) == thread_id
    except (TypeError, ValueError):
        return False


async def guild_not_configured_message(guild_id: int) -> str:
    return (
        "Бот не настроен на этом сервере. Администратор должен выполнить **`/bot setup`** "
        "(создаст категорию и форумы автоматически)."
    )


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
        pl.setdefault("display_mode", "summary")
        pl.setdefault("current_page", 0)
        pl.setdefault("recent_keys", [])
        pl.setdefault("card_cache", {})
        pl.setdefault("ui_deferred", False)
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
    list_parent = await list_forum_parent_id(interaction.guild.id)
    if list_parent is None:
        await interaction.response.send_message(
            await guild_not_configured_message(interaction.guild.id),
            ephemeral=True,
        )
        return None
    if ch.parent_id != list_parent:
        await interaction.response.send_message(
            "Это не форум **личных списков**. Откройте свою тему в канале личных списков, "
            "а не в основном каталоге аниме.",
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
        await _set_personal_list_fields(owner_id, ui_deferred=False)
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
            f"**Режим:** {personal_display.normalize_display_mode(pl.get('display_mode'))}",
        ]
        await interaction.response.send_message(
            "\n".join(lines), ephemeral=True
        )

    @discord.ui.button(
        label="Режим",
        style=discord.ButtonStyle.secondary,
        emoji="📑",
        custom_id="plist:hub:mode",
        row=1,
    )
    async def hub_mode(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        resolved = await self._resolve_owner(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Только **владелец** может менять режим.", ephemeral=True
            )
            return
        order_modes = ("summary", "paged", "gallery")
        cur = personal_display.normalize_display_mode(pl.get("display_mode"))
        try:
            idx = order_modes.index(cur)
        except ValueError:
            idx = 0
        nxt = order_modes[(idx + 1) % len(order_modes)]
        labels = {"summary": "Сводка", "paged": "Страницы", "gallery": "Галерея"}
        await _set_personal_list_fields(owner_id, display_mode=nxt, current_page=0)
        await interaction.response.send_message(
            f"Режим: **{labels[nxt]}**. Нажмите **Обновить**, чтобы применить.",
            ephemeral=True,
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
            "· **`/list top`** — топ-5 · **`/list edit`** — название темы.\n"
            "· **Режим** — сводка (лёгкий) / страницы / галерея (тяжёлый).\n"
            "· **Синхронизировать** — обход основного форума.\n"
            "· **Yummy ↻** — импорт с YummyAnime (`/yummy bind`).\n"
            "· **`/list show`** — полный список без скролла темы.\n"
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
        client = interaction.client
        if not isinstance(client, YummyBot):
            await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
            return
        try:
            sess = await client.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (plist hub yummy)")
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
    mode = personal_display.normalize_display_mode(pl.get("display_mode"))
    mode_ru = {"summary": "Сводка", "paged": "Страницы", "gallery": "Галерея"}.get(mode, mode)
    e = discord.Embed(
        title="🎛️ Панель топика",
        description=(
            f"**{display_name}** — настройки и действия.\n\n"
            "· **Синхронизировать** — обход основного форума + ваш список.\n"
            "· **Обновить** — перерисовать из сохранённых данных.\n"
            "· **Режим** — сводка / страницы / галерея.\n"
            "· **Тема** · **#** · **Компакт** — оформление.\n"
            "· **Yummy ↻** · **Экспорт** · **Статистика** · **Справка**.\n\n"
            f"_Нумерация **{nums}**, компакт **{comp}**, режим **{mode_ru}**._"
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
        pl.setdefault("display_mode", "summary")
        pl.setdefault("current_page", 0)
        pl.setdefault("recent_keys", [])
        pl.setdefault("card_cache", {})
        pl.setdefault("ui_deferred", False)
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
    incremental: bool = False,
) -> None:
    """Пересборка личного списка: summary / paged / gallery."""
    await personal_display.rebuild_display(
        client,
        guild_id,
        user_id,
        session=session,
        incremental=incremental,
        read_state=read_state_copy,
        write_personal_fields=_set_personal_list_fields,
        title_for_key=_title_for_list_key,
        jump_for_key=_jump_for_list_key,
        ordered_keys=_ordered_keys_for_personal,
        fetch_meta=_fetch_personal_card_meta,
        build_card_embed=_build_personal_anime_card_embed,
        hub_embed_builder=_personal_hub_embed,
        hub_view_factory=PersonalTopicHubView,
        accent_palette=PERSONAL_ACCENT_PALETTE,
        default_accent=EMBED_COLOR,
    )


async def ensure_personal_list_thread(
    client: discord.Client,
    guild: discord.Guild,
    member: discord.Member,
    *,
    session: aiohttp.ClientSession | None,
) -> discord.Thread | None:
    """Создаёт тему в LIST_FORUM при первом добавлении, если её ещё нет."""
    uid = member.id
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
            list_parent = await list_forum_parent_id(guild.id)
            if (
                isinstance(ch, discord.Thread)
                and list_parent
                and ch.parent_id == list_parent
            ):
                await apply_personal_list_permissions(ch, guild, uid)
                return ch

    forum = await resolve_list_forum_channel(client, guild.id)
    if not forum:
        logger.warning("Канал личных списков не настроен на сервере %s.", guild.id)
        return None

    name = f"{member.display_name} anime list"[:100]
    intro = (
        f"{member.mention} — **личный список аниме**.\n\n"
        "Название темы и текст: **`/list edit`** · топ-5: **`/list top`**\n"
        "Панель кнопок — режим отображения, обновление, синхронизация.\n\n"
        "_Список показывается компактно (сводка / страницы). Режим «Галерея» — на панели._"
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
    *,
    defer_ui: bool = False,
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
        recent = pl.get("recent_keys")
        if not isinstance(recent, list):
            recent = []
        recent = [key] + [k for k in recent if k != key]
        pl["recent_keys"] = recent[: personal_display.RECENT_COUNT]
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
    schedule_personal_list_refresh(guild.id, user_id, defer_ui=defer_ui)


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
        empty["error"] = "Аккаунт YummyAnime не привязан (`/yummybind`)."
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

    forum = await resolve_forum_channel(bot, guild.id)
    if not forum:
        empty["error"] = "Канал основного форума не найден. Админ: `/bot setup`."
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
                            guild, uid, slug_key, tnm, defer_ui=True
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
                        guild, uid, slug_key, query[:500], defer_ui=True
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
            await append_user_anime_to_personal_state(guild, uid, pk, pt, defer_ui=True)
        except Exception:
            logger.exception("Личный список после новой темы Yummy import")

        await mark_yummy_imported(uid, aid)
        imported_ids.add(aid)
        n_new += 1
        ju = thread.jump_url if hasattr(thread, "jump_url") else f"<#{thread.id}>"
        created_urls.append(ju)
        await asyncio.sleep(1.25)

    if n_new or merge_ops:
        schedule_personal_list_refresh(guild.id, uid)

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
            forum = await resolve_forum_channel(
                interaction.client, interaction.guild.id if interaction.guild else None
            )
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
            try:
                scan_session = await bot.ensure_http_session()
            except Exception:
                logger.exception("HTTP session (forum_scan)")
                await interaction.followup.send("Нет HTTP сессии.", ephemeral=True)
                return
            scanned, updated = await sync_forum_threads_with_state(
                bot, interaction.guild, scan_session
            )
            await interaction.followup.send(
                f"Скан: веток **{scanned}**, обновлено записей **{updated}**.",
                ephemeral=True,
            )
            return
        if choice == "repair_topics":
            try:
                repair_session = await bot.ensure_http_session()
            except Exception:
                logger.exception("HTTP session (repair_topics)")
                await interaction.followup.send("Нет HTTP сессии.", ephemeral=True)
                return
            forum = await resolve_forum_channel(bot, interaction.guild.id)
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
                    await repair_single_forum_thread(bot, forum, th, repair_session)
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


class YummyBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        # Для !aa / !animeadd в чате включите в Portal «Message Content Intent» и оставьте 1 (по умолчанию).
        _mc = (os.environ.get("DISCORD_MESSAGE_CONTENT_INTENT") or "1").strip().lower()
        intents.message_content = _mc not in ("0", "false", "no", "off")
        super().__init__(command_prefix="!", intents=intents)
        self.session: aiohttp.ClientSession | None = None
        self._yummy_poll_task: asyncio.Task[None] | None = None
        self._roaster_poll_task: asyncio.Task[None] | None = None

    async def ensure_http_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            if self.session is not None and self.session.closed:
                logger.warning("HTTP-сессия закрыта, пересоздаём")
            self.session = aiohttp.ClientSession(headers={"User-Agent": USER_AGENT})
        return self.session

    async def setup_hook(self) -> None:
        await self.ensure_http_session()
        self.add_view(PersonalTopicHubView())
        self.add_view(AddToMyListPanelView())
        self.add_view(personal_display.PersonalPagerView())
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
        if self.session:
            await self.session.close()
        await super().close()


bot = YummyBot()

TEXT_ANIMEADD_RE = re.compile(
    r"^(?:!aa|!animeadd|/aa|/animeadd)\s+(.+)$",
    re.I | re.DOTALL,
)


async def run_animeadd_for_user(guild: discord.Guild, user_id: int, query: str) -> str:
    """Текст ответа для slash или сообщения в чате."""
    try:
        session = await bot.ensure_http_session()
    except Exception:
        logger.exception("Не удалось создать HTTP-сессию")
        return "Сессия HTTP не готова."
    q = query.strip()
    if not q:
        return "Пустой запрос."
    slug = slug_from_text(q)
    if not slug:
        slug = await api_search_slug(session, q)
    if not slug:
        return "Не нашёл аниме. Уточните запрос или вставьте ссылку с en.yummyani.me."
    info = await api_fetch_anime(session, slug)
    if not info:
        return "Не удалось загрузить карточку аниме (API вернул ошибку)."
    ch = await resolve_forum_channel(bot, guild.id)
    if not ch:
        return "Канал форума не настроен. Админ сервера: **`/bot setup`**."
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
        ch, session, info, user_id, mal_id=None, resolved_slug=slug
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
        return None, f"{member.mention} ещё не привязал список MAL (`/connectmyanimelist`)."
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
            "Если вы уже добавляли раньше, админ может выполнить `/syncmylist` для вашего профиля.",
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
    embed.set_footer(text="Данные из личной темы списков · команда /checkanime")
    return embed, None, len(pairs), 0


@bot.event
async def on_ready() -> None:
    assert bot.user is not None
    logger.info("Бот онлайн: %s (%s)", bot.user, bot.user.id)
    state = await read_state_copy()
    guilds_cfg = state.get("guilds") or {}
    if isinstance(guilds_cfg, dict):
        for gid_s in guilds_cfg:
            try:
                await ensure_bot_info_thread(bot, int(gid_s))
            except Exception as e:
                logger.warning("Справочная тема guild %s: %s", gid_s, e)
    t = bot._yummy_poll_task
    if t is None or t.done():
        bot._yummy_poll_task = asyncio.create_task(yummy_background_poll_loop())
    rt = bot._roaster_poll_task
    if rt is None or rt.done():
        bot._roaster_poll_task = asyncio.create_task(
            roaster_automation.roaster_background_loop(bot)
        )


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
        try:
            await roaster_automation.maybe_roast_on_message(message)
        except Exception:
            logger.exception("roaster on_message")
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
            out = "Произошла ошибка при добавлении. Попробуйте `/anime add`."
    await message.reply(_truncate(out, DISCORD_CONTENT_LIMIT), mention_author=False)



# --- Slash-команды регистрируются в register_commands.py ---
import register_commands  # noqa: E402
register_commands.setup(bot)

def _normalize_discord_token(raw: str | None) -> str:
    if not raw:
        return ""
    t = str(raw).strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in "'\"":
        t = t[1:-1].strip()
    return t


def main() -> None:
    # python bot.py загружает файл как __main__; import bot as core иначе даёт второй экземпляр YummyBot без session.
    if __name__ == "__main__":
        sys.modules["bot"] = sys.modules[__name__]

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
