"""Авто-«обзыватель»: реакция на сообщения и таймер 1–6 ч."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any

import discord
import guild_config
import roaster

logger = logging.getLogger(__name__)

# ~3.5% сообщений в активных каналах (настраивается через .env)
MESSAGE_ROAST_CHANCE = float(
    (os.environ.get("ROASTER_MESSAGE_CHANCE") or "0.035").strip() or "0.035"
)
# Не чаще одного подкола одному человеку на сервере за этот интервал
MESSAGE_COOLDOWN_SEC = max(
    60,
    int((os.environ.get("ROASTER_MESSAGE_COOLDOWN_SEC") or "900").strip() or "900"),
)
TIMER_MIN_SEC = max(3600, int((os.environ.get("ROASTER_TIMER_MIN_SEC") or "3600").strip() or "3600"))
TIMER_MAX_SEC = max(
    TIMER_MIN_SEC,
    int((os.environ.get("ROASTER_TIMER_MAX_SEC") or "21600").strip() or "21600"),
)

_last_message_roast: dict[tuple[int, int], float] = {}


def _chance_ok() -> bool:
    return random.random() < max(0.0, min(MESSAGE_ROAST_CHANCE, 1.0))


def _cooldown_ok(guild_id: int, user_id: int) -> bool:
    key = (guild_id, user_id)
    last = _last_message_roast.get(key, 0.0)
    return (time.monotonic() - last) >= MESSAGE_COOLDOWN_SEC


def _mark_roasted(guild_id: int, user_id: int) -> None:
    _last_message_roast[(guild_id, user_id)] = time.monotonic()


async def _pick_delivery_channel(
    guild: discord.Guild, cfg: dict[str, Any] | None, victim_id: int
) -> discord.abc.Messageable | None:
    import bot as core

    cat_id = guild_config.category_id(cfg)
    if cat_id:
        cat = guild.get_channel(cat_id)
        if isinstance(cat, discord.CategoryChannel):
            for ch in cat.text_channels:
                me = guild.me
                if me and ch.permissions_for(me).send_messages:
                    return ch

    me = guild.me
    if me:
        for ch in guild.text_channels:
            if ch.permissions_for(me).send_messages:
                return ch

    state = await core.read_state_copy()
    pl = core.guild_personal_list(state, guild.id, victim_id)
    if isinstance(pl, dict) and pl.get("thread_id"):
        try:
            tid = int(pl["thread_id"])
        except (TypeError, ValueError):
            tid = 0
        if tid:
            th = guild.get_thread(tid) or core.bot.get_channel(tid)
            if th is None:
                try:
                    th = await core.bot.fetch_channel(tid)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    th = None
            if isinstance(th, discord.Thread):
                return th
    return None


async def send_roast_to_channel(
    guild: discord.Guild,
    member: discord.Member,
    channel: discord.abc.Messageable,
    *,
    reference: discord.Message | None = None,
) -> bool:
    import bot as core

    if not await core.is_roaster_active(guild.id):
        return False
    state = await core.read_state_copy()
    titles = core.pick_roast_titles(state, member.id, guild.id)
    text = roaster.build_roast_message(member.mention, titles)
    text = core._truncate(text, core.DISCORD_CONTENT_LIMIT)
    allowed = discord.AllowedMentions(users=[member], roles=False, everyone=False)
    try:
        kwargs: dict[str, Any] = {
            "allowed_mentions": allowed,
        }
        if reference is not None:
            kwargs["reference"] = reference
            kwargs["mention_author"] = False
        await channel.send(text, **kwargs)
        _mark_roasted(guild.id, member.id)
        return True
    except discord.HTTPException as e:
        logger.warning("roast send failed guild=%s: %s", guild.id, e)
        return False


async def maybe_roast_on_message(message: discord.Message) -> None:
    """С шансом ответить на сообщение сатирическим «обзывом»."""
    import bot as core

    if message.author.bot or not message.guild:
        return
    if not isinstance(message.author, discord.Member):
        return
    if not await core.is_roaster_active(message.guild.id):
        return
    content = (message.content or "").strip()
    if len(content) < 4:
        return
    if not _chance_ok():
        return
    if not _cooldown_ok(message.guild.id, message.author.id):
        return

    me = message.guild.me
    if me is None:
        return
    ch = message.channel
    perms = ch.permissions_for(me) if hasattr(ch, "permissions_for") else None
    if perms is not None and not perms.send_messages:
        return

    await send_roast_to_channel(
        message.guild,
        message.author,
        ch,
        reference=message,
    )


def _members_with_list(guild: discord.Guild, state: dict[str, Any]) -> list[discord.Member]:
    import bot as core

    out: list[discord.Member] = []
    for m in guild.members:
        if m.bot:
            continue
        if core.pick_roast_titles(state, m.id, guild.id):
            out.append(m)
    return out


async def _timer_roast_once() -> None:
    import bot as core

    state = await core.read_state_copy()
    meta = state.get("meta") or {}
    if meta.get("roaster_global_enabled") is False:
        return

    guilds_raw = state.get("guilds") or {}
    if not isinstance(guilds_raw, dict):
        return

    active_guilds: list[tuple[discord.Guild, dict[str, Any]]] = []
    for gid_s, cfg in guilds_raw.items():
        if not isinstance(cfg, dict) or not cfg.get("roaster_enabled"):
            continue
        try:
            gid = int(gid_s)
        except (TypeError, ValueError):
            continue
        g = core.bot.get_guild(gid)
        if g:
            active_guilds.append((g, cfg))

    if not active_guilds:
        return

    guild, cfg = random.choice(active_guilds)
    pool = _members_with_list(guild, state)
    if not pool:
        pool = [m for m in guild.members if not m.bot]
    if not pool:
        return

    victim = random.choice(pool)
    channel = await _pick_delivery_channel(guild, cfg, victim.id)
    if channel is None:
        logger.info("roaster timer: нет канала для guild %s", guild.id)
        return

    ok = await send_roast_to_channel(guild, victim, channel)
    if ok:
        logger.info(
            "roaster timer: guild=%s victim=%s channel=%s",
            guild.id,
            victim.id,
            getattr(channel, "id", "?"),
        )


async def roaster_background_loop(client: discord.Client) -> None:
    """Случайный подкол кого-то на сервере каждые 1–6 часов."""
    await client.wait_until_ready()
    while not client.is_closed():
        delay = random.randint(TIMER_MIN_SEC, TIMER_MAX_SEC)
        logger.debug("roaster timer: sleep %s sec", delay)
        await asyncio.sleep(delay)
        if client.is_closed():
            break
        try:
            await _timer_roast_once()
        except Exception:
            logger.exception("roaster timer cycle")
