"""Клиент официального API YummyAnime (api.yani.tv)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

YANI_BASE = "https://api.yani.tv"


def build_yani_headers(
    app_token: str, bearer: str | None, user_agent: str
) -> dict[str, str]:
    h: dict[str, str] = {
        "X-Application": app_token.strip(),
        "Accept": "application/json",
        "Lang": "en",
        "User-Agent": user_agent.strip() or "YummyDiscordBot/1.0",
    }
    b = (bearer or "").strip()
    if b:
        h["Authorization"] = f"Bearer {b}"
    return h


async def _yani_request(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    *,
    app_token: str,
    bearer: str | None,
    user_agent: str,
) -> tuple[Any, int]:
    url = f"{YANI_BASE}{path}"
    headers = build_yani_headers(app_token, bearer, user_agent)
    try:
        async with session.request(method, url, headers=headers) as resp:
            status = resp.status
            if status == 204:
                return None, status
            try:
                data = await resp.json(content_type=None)
            except (aiohttp.ContentTypeError, ValueError):
                text = await resp.text()
                logger.warning("YANI non-JSON %s %s: %s", method, path, text[:200])
                return None, status
            return data, status
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning("YANI request failed %s %s: %s", method, path, e)
        return None, 0


async def yani_get_profile(
    session: aiohttp.ClientSession,
    app_token: str,
    bearer: str,
    user_agent: str,
) -> dict[str, Any] | None:
    data, status = await _yani_request(
        session, "GET", "/profile", app_token=app_token, bearer=bearer, user_agent=user_agent
    )
    if status != 200 or not isinstance(data, dict):
        return None
    inner = data.get("response")
    return inner if isinstance(inner, dict) else None


async def yani_refresh_access_token(
    session: aiohttp.ClientSession,
    app_token: str,
    bearer: str,
    user_agent: str,
) -> str | None:
    data, status = await _yani_request(
        session,
        "GET",
        "/profile/token",
        app_token=app_token,
        bearer=bearer,
        user_agent=user_agent,
    )
    if status != 200 or not isinstance(data, dict):
        return None
    inner = data.get("response")
    if not isinstance(inner, dict):
        return None
    tok = inner.get("token")
    return tok.strip() if isinstance(tok, str) and tok.strip() else None


async def yani_get_user_lists(
    session: aiohttp.ClientSession,
    app_token: str,
    bearer: str,
    yummy_user_id: int,
    user_agent: str,
) -> tuple[list[dict[str, Any]] | None, int]:
    path = f"/users/{int(yummy_user_id)}/lists"
    data, status = await _yani_request(
        session, "GET", path, app_token=app_token, bearer=bearer, user_agent=user_agent
    )
    if status != 200 or not isinstance(data, dict):
        return None, status
    items = data.get("response")
    if not isinstance(items, list):
        return [], status
    out: list[dict[str, Any]] = [x for x in items if isinstance(x, dict)]
    return out, status


async def yani_fetch_lists_with_token_refresh(
    session: aiohttp.ClientSession,
    app_token: str,
    access_token: str,
    yummy_user_id: int,
    user_agent: str,
) -> tuple[list[dict[str, Any]] | None, str | None, str | None]:
    """
    Загружает все списки пользователя.
    Возвращает (items, new_access_token_если_обновили, текст_ошибки).
    """
    bearer = access_token.strip()
    if not bearer:
        return None, None, "Пустой токен"

    items, st = await yani_get_user_lists(
        session, app_token, bearer, yummy_user_id, user_agent
    )
    if st == 200 and items is not None:
        return items, None, None

    if st == 401:
        new_tok = await yani_refresh_access_token(session, app_token, bearer, user_agent)
        if not new_tok:
            return None, None, "Сессия YummyAnime недействительна — выполните `/yummybind` снова."
        items2, st2 = await yani_get_user_lists(
            session, app_token, new_tok, yummy_user_id, user_agent
        )
        if st2 == 200 and items2 is not None:
            return items2, new_tok, None
        return None, new_tok, f"Не удалось загрузить список после обновления токена (HTTP {st2})."

    return None, None, f"API YummyAnime: HTTP {st}" if st else "Сеть недоступна"


def yummy_entry_list_href(entry: dict[str, Any]) -> str:
    user = entry.get("user") if isinstance(entry.get("user"), dict) else {}
    ul = user.get("list") if isinstance(user.get("list"), dict) else {}
    inner = ul.get("list") if isinstance(ul.get("list"), dict) else {}
    return str(inner.get("href") or "").strip()


def yummy_entry_anime_id(entry: dict[str, Any]) -> int | None:
    aid = entry.get("anime_id")
    if isinstance(aid, int):
        return aid
    try:
        return int(aid)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def yummy_entry_anime_url(entry: dict[str, Any]) -> str:
    u = entry.get("anime_url")
    return str(u).strip() if isinstance(u, str) else ""


def yummy_entry_title(entry: dict[str, Any]) -> str:
    t = entry.get("title")
    return str(t).strip() if isinstance(t, str) else "Без названия"


def filter_yummy_entries_by_status(
    entries: list[dict[str, Any]], list_filter: str
) -> list[dict[str, Any]]:
    """list_filter: all | watching | plan_to_watch | completed | on_hold | dropped"""
    if list_filter == "all":
        return entries
    want = {
        "watching": "watch_now",
        "plan_to_watch": "will",
        "completed": "watched",
        "on_hold": "postpone",
        "dropped": "lost",
    }.get(list_filter)
    if not want:
        return entries
    return [e for e in entries if yummy_entry_list_href(e) == want]
