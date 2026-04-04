"""
Discord bot: форум YummyAnime (/animeadd), MAL, оценки, рекомендации, /myanimelist, /update_topics.
Токен: DISCORD_BOT_TOKEN. Рекомендуется DISCORD_GUILD_ID — иначе глобальные slash-команды дублировались бы с гильдейскими.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import aiohttp
import discord
from dotenv import load_dotenv
from discord import app_commands
from discord.ext import commands

FORUM_CHANNEL_ID = 1393418241468141580
BASE = "https://en.yummyani.me"
API_SEARCH = f"{BASE}/api/search"
API_ANIME = f"{BASE}/api/anime"
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
        "threads": {},
        "ratings": {},
        "imported_mal": {},
        "anime_topics": {},
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
    for key in ("mal_accounts", "threads", "ratings", "imported_mal", "anime_topics", "meta"):
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
            for k in ("rating_message_id", "recommend_message_id"):
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


async def read_state_copy() -> dict[str, Any]:
    async with _state_lock:
        return json.loads(json.dumps(_load_state()))


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
        description="Все slash-команды ниже. Используйте их на **этом сервере**.",
        color=EMBED_COLOR,
    )
    rows = [
        ("`/animeadd`", "Добавить аниме с YummyAnime в форум (ссылка или поиск). Повторное добавление того же тайтла дописывает вас в «Добавили»."),
        ("`/connectmyanimelist`", "Привязать MAL и создать темы из списка (ещё не импортированные)."),
        ("`/checkanimelist`", "Показать список с сайта MyAnimeList у привязанного пользователя."),
        ("`/myanimelist`", "Список аниме, которые пользователь **добавил на сервер** в топики форума (после синхронизации с форумом)."),
        ("`/rateanime`", "Оценка 1–10 внутри темы форума (или кнопка «Оценить» под постом)."),
        ("`/checkduplicates`", "Найти дубликаты тем (одно аниме — несколько веток) и удалить лишние."),
        ("`/update_topics`", "**Только админы:** обновить старые темы — реакции, панели оценок/рекомендаций, при возможности карточку YummyAnime."),
    ]
    for name, desc in rows:
        e.add_field(name=name, value=desc, inline=False)
    e.set_footer(text="Панели «Оценка» и «Рекомендация» создаются под первым сообщением новых тем автоматически.")
    return e


async def ensure_bot_info_thread(client: discord.Client) -> None:
    forum = await resolve_forum_channel(client)
    if not forum:
        return
    async with _state_lock:
        data = _load_state()
        meta = data.setdefault("meta", {})
        raw_id = meta.get("bot_info_thread_id")
        try:
            existing_id = int(raw_id) if raw_id is not None else None
        except (TypeError, ValueError):
            existing_id = None
    if existing_id:
        ch = client.get_channel(existing_id)
        if ch is None:
            try:
                ch = await client.fetch_channel(existing_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                ch = None
        if isinstance(ch, discord.Thread) and not ch.archived:
            return
    embed = _build_bot_commands_embed()
    try:
        twm = await forum.create_thread(
            name="📌 Команды бота и справка",
            embeds=[embed],
            content="Закрепите это сообщение (📌), чтобы быстро находить список команд.",
        )
        th = twm.thread
        msg = twm.message
    except discord.HTTPException as e:
        logger.warning("Не удалось создать справочную тему: %s", e)
        return
    try:
        await msg.pin()
    except discord.HTTPException:
        logger.info("Не удалось закрепить сообщение в справочной теме (права).")
    async with _state_lock:
        data = _load_state()
        data.setdefault("meta", {})["bot_info_thread_id"] = th.id
        _write_state(data)


async def resolve_forum_channel(client: discord.Client) -> discord.ForumChannel | None:
    ch = client.get_channel(FORUM_CHANNEL_ID)
    if ch is None:
        try:
            ch = await client.fetch_channel(FORUM_CHANNEL_ID)
        except (discord.NotFound, discord.Forbidden):
            return None
    return ch if isinstance(ch, discord.ForumChannel) else None


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


async def ensure_topic_side_panels(client: discord.Client, thread_id: int) -> None:
    await refresh_rating_panel(client, thread_id)
    await refresh_recommend_panel(client, thread_id)


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


class YummyBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.session: aiohttp.ClientSession | None = None

    async def setup_hook(self) -> None:
        self.session = aiohttp.ClientSession(headers={"User-Agent": USER_AGENT})
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


@bot.event
async def on_ready() -> None:
    assert bot.user is not None
    logger.info("Бот онлайн: %s (%s)", bot.user, bot.user.id)
    try:
        await ensure_bot_info_thread(bot)
    except Exception as e:
        logger.warning("Справочная тема форума: %s", e)


@bot.event
async def on_disconnect() -> None:
    logger.warning("Соединение с Discord разорвано (on_disconnect).")


@bot.tree.command(name="animeadd", description="Добавить аниме с YummyAnime в форум (ссылка или название)")
@app_commands.describe(query="Ссылка на страницу аниме или поисковый запрос")
async def animeadd(interaction: discord.Interaction, query: str) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    if not bot.session:
        await interaction.followup.send("Сессия HTTP не готова.", ephemeral=True)
        return

    slug = slug_from_text(query)
    if not slug:
        slug = await api_search_slug(bot.session, query)
    if not slug:
        await interaction.followup.send(
            "Не нашёл аниме. Уточните запрос или вставьте прямую ссылку с en.yummyani.me.",
            ephemeral=True,
        )
        return

    info = await api_fetch_anime(bot.session, slug)
    if not info:
        await interaction.followup.send(
            "Не удалось загрузить карточку аниме (API вернул ошибку).",
            ephemeral=True,
        )
        return

    ch = await resolve_forum_channel(bot)
    if not ch:
        await interaction.followup.send(
            "Канал форума не найден или бот не видит его. Проверьте ID и права бота.",
            ephemeral=True,
        )
        return

    slug_key = _clean_slug((info.get("anime_url") or slug or "").strip())
    uid = interaction.user.id
    thread, merge_st = await merge_adder_into_existing_topic(bot, slug_key, uid)
    if merge_st == "merged":
        link = thread.jump_url if thread and hasattr(thread, "jump_url") else f"<#{thread.id}>"
        await interaction.followup.send(
            f"Тема уже была — добавил вас в подпись: {link}", ephemeral=True
        )
        return
    if merge_st == "already":
        link = thread.jump_url if thread and hasattr(thread, "jump_url") else f"<#{thread.id}>"
        await interaction.followup.send(
            f"Эта тема уже есть, вы уже среди добавивших: {link}", ephemeral=True
        )
        return
    if merge_st in ("edit_failed", "fetch_failed"):
        await interaction.followup.send(
            "Тема с этим аниме уже есть в базе бота, но не удалось обновить "
            "сообщение (права или тема удалена). Обратитесь к администратору.",
            ephemeral=True,
        )
        return

    thread, _starter, err = await create_yummy_forum_thread(
        ch, bot.session, info, uid, mal_id=None, resolved_slug=slug
    )
    if err or not thread:
        await interaction.followup.send(err or "Не удалось создать тему.", ephemeral=True)
        return

    link = thread.jump_url if hasattr(thread, "jump_url") else f"<#{thread.id}>"
    await interaction.followup.send(f"Готово: {link}", ephemeral=True)


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


@bot.tree.command(
    name="connectmyanimelist",
    description="Привязать MyAnimeList и создать темы из списка (ещё не импортированные)",
)
@app_commands.describe(
    list_url="Ссылка на animelist или профиль MAL",
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
    list_url: str,
    mal_status: str,
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

    username = mal_username_from_url(list_url)
    if not username:
        await interaction.followup.send(
            "Нужна ссылка вида `https://myanimelist.net/animelist/ник` "
            "или `https://myanimelist.net/profile/ник`.",
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

    norm_url = f"https://myanimelist.net/animelist/{username}"
    await bind_mal_account(interaction.user.id, username, norm_url)

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
            "Создайте её через `/animeadd` или импорт из MAL (`/connectmyanimelist`).",
            ephemeral=True,
        )
        return

    try:
        await ensure_topic_side_panels(interaction.client, ch.id)
    except Exception as e:
        logger.warning("Панели перед /rateanime: %s", e)
    await interaction.response.send_modal(AnimeRatingModal(ch.id))


@bot.tree.command(
    name="checkanimelist",
    description="Показать привязанный список MyAnimeList пользователя",
)
@app_commands.describe(member="Чей список показать")
async def checkanimelist(interaction: discord.Interaction, member: discord.Member) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    if not bot.session:
        await interaction.response.send_message(
            "Сессия HTTP не готова.", ephemeral=True
        )
        return

    await interaction.response.defer(thinking=True)

    state = await read_state_copy()
    acc = state.get("mal_accounts", {}).get(str(member.id))
    if not isinstance(acc, dict):
        await interaction.followup.send(
            f"{member.mention} ещё не привязал список MAL (`/connectmyanimelist`)."
        )
        return

    username = (acc.get("username") or "").strip()
    list_url = (acc.get("list_url") or "").strip()
    if not username:
        await interaction.followup.send("В сохранённой привязке нет имени пользователя MAL.")
        return

    entries, http_st = await mal_fetch_full_list(bot.session, username, MAL_STATUS_ALL)
    if http_st != 200:
        await interaction.followup.send(
            "Не удалось загрузить список с MyAnimeList (список закрыт или MAL недоступен)."
        )
        return

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
        title=f"Список аниме — {member.display_name}",
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

    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="myanimelist",
    description="Аниме, добавленные пользователем в топики форума на этом сервере (с синхронизацией форума)",
)
@app_commands.describe(member="Чей список (если не указано — ваш)")
async def myanimelist(interaction: discord.Interaction, member: discord.Member | None = None) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Команду можно использовать только на сервере.", ephemeral=True
        )
        return

    target = member or interaction.user
    await interaction.response.defer(ephemeral=True, thinking=True)

    scanned, updated = await sync_forum_threads_with_state(
        bot, interaction.guild, bot.session if bot.session else None
    )
    state = await read_state_copy()
    pairs = list_discord_added_anime_for_user(state, interaction.guild.id, target.id)
    if not pairs:
        await interaction.followup.send(
            f"{target.mention} пока не числится среди добавивших ни в одной теме форума "
            f"(или темы не удалось разобрать). Просмотрено веток: **{scanned}**, "
            f"обновлено записей в базе: **{updated}**.",
            ephemeral=True,
        )
        return

    lines = [f"• [{t}]({u})" for t, u in pairs[:60]]
    body = "\n".join(lines)
    if len(pairs) > 60:
        body += f"\n_…и ещё {len(pairs) - 60}_"
    embed = discord.Embed(
        title=f"Добавлено в Discord — {target.display_name}",
        description=_truncate(body, EMBED_DESC_LIMIT),
        color=EMBED_COLOR,
    )
    embed.set_footer(
        text=f"Форум синхронизирован: веток {scanned}, записей обновлено {updated}"
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


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


def main() -> None:
    load_dotenv()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                DATA_DIR / "bot.log", encoding="utf-8", mode="a"
            ),
        ],
        force=True,
    )
    token = os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("Задайте переменную окружения DISCORD_BOT_TOKEN.")
    bot.run(token)


if __name__ == "__main__":
    main()
