#!/usr/bin/env python3
"""Диагностика: .env, YummyAnime API, привязка каналов Discord."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import aiohttp
import discord

BASE = "https://en.yummyani.me"
API_SEARCH = f"{BASE}/api/search"
API_ANIME = f"{BASE}/api/anime"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
STATE_PATH = ROOT / "data" / "mal_state.json"


def check_env() -> list[str]:
    lines: list[str] = []
    token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    guild_id = (os.environ.get("DISCORD_GUILD_ID") or "").strip()
    owner_id = (os.environ.get("DISCORD_BOT_OWNER_ID") or "").strip()
    yummy_app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()

    lines.append("=== ENV ===")
    lines.append(f"DISCORD_BOT_TOKEN: {'OK (' + str(len(token)) + ' chars)' if len(token) >= 50 else 'MISSING/SHORT'}")
    lines.append(f"DISCORD_GUILD_ID: {guild_id or 'NOT SET (global slash sync)'}")
    lines.append(f"DISCORD_BOT_OWNER_ID: {owner_id or 'NOT SET (/owner needs owner check)'}")
    lines.append(
        f"YUMMY_APPLICATION_TOKEN: {'OK' if yummy_app else 'NOT SET (Yummy bind/import disabled)'}"
    )
    return lines


async def check_yummy_api() -> list[str]:
    lines: list[str] = ["", "=== YummyAnime API ==="]
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        async with session.get(API_SEARCH, params={"q": "naruto"}) as resp:
            lines.append(f"GET /api/search?q=naruto → HTTP {resp.status}")
            if resp.status == 200:
                data = await resp.json()
                items = data.get("response") or []
                slug = None
                if items and isinstance(items[0], dict):
                    slug = (items[0].get("anime_url") or "").strip()
                lines.append(f"  results: {len(items)}, first slug: {slug or '—'}")
                if slug:
                    async with session.get(f"{API_ANIME}/{slug}") as r2:
                        lines.append(f"GET /api/anime/{slug} → HTTP {r2.status}")
                        if r2.status == 200:
                            d2 = await r2.json()
                            title = (d2.get("response") or {}).get("title") or "?"
                            lines.append(f"  title: {title}")
            else:
                lines.append("  FAIL: search unavailable")
    return lines


async def check_discord_channels() -> list[str]:
    lines: list[str] = ["", "=== Discord channels (guild config) ==="]
    token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    if not token:
        lines.append("SKIP: no token")
        return lines

    if not STATE_PATH.is_file():
        lines.append(f"SKIP: no state file at {STATE_PATH}")
        return lines

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    guilds = state.get("guilds") or {}
    if not guilds:
        lines.append("No guilds configured — run /bot setup on server")
        return lines

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    ready = asyncio.Event()

    @client.event
    async def on_ready() -> None:
        ready.set()

    async def _run() -> None:
        await client.login(token)
        await client.connect(reconnect=False)

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(ready.wait(), timeout=30)
    except asyncio.TimeoutError:
        lines.append("FAIL: Discord login timeout")
        await client.close()
        task.cancel()
        return lines

    for gid_s, cfg in guilds.items():
        if not isinstance(cfg, dict):
            continue
        lines.append(f"Guild {gid_s}:")
        try:
            gid = int(gid_s)
        except ValueError:
            lines.append("  invalid guild id")
            continue
        guild = client.get_guild(gid)
        lines.append(f"  guild in cache: {'yes' if guild else 'no (bot may not be on server)'}")

        for label, key, forum in (
            ("category", "category_id", False),
            ("main forum", "forum_channel_id", True),
            ("list forum", "list_forum_channel_id", True),
            ("info thread", "bot_info_thread_id", False),
        ):
            raw = cfg.get(key)
            if not raw:
                lines.append(f"  {label}: not set")
                continue
            try:
                cid = int(raw)
            except (TypeError, ValueError):
                lines.append(f"  {label}: invalid id {raw!r}")
                continue
            ch = client.get_channel(cid)
            if ch is None and guild:
                ch = guild.get_channel(cid)
            if ch is None:
                try:
                    ch = await client.fetch_channel(cid)
                except discord.NotFound:
                    lines.append(f"  {label} ({cid}): NOT FOUND")
                    continue
                except discord.Forbidden:
                    lines.append(f"  {label} ({cid}): FORBIDDEN")
                    continue
            kind = type(ch).__name__
            ok_forum = not forum or isinstance(ch, discord.ForumChannel)
            status = "OK" if ok_forum else f"WRONG TYPE ({kind})"
            lines.append(f"  {label} ({cid}): {status} — #{getattr(ch, 'name', '?')}")

    await client.close()
    try:
        await asyncio.wait_for(task, timeout=5)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    return lines


async def main() -> None:
    out: list[str] = check_env()
    out.extend(await check_yummy_api())
    out.extend(await check_discord_channels())
    text = "\n".join(out)
    print(text)
    report_path = ROOT / "data" / "health_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(text + "\n", encoding="utf-8")
    print(f"\nReport saved: {report_path}")


if __name__ == "__main__":
    asyncio.run(main())
