"""Конфигурация сервера: категория, форумы, привязка к guild_id."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import discord

logger = logging.getLogger(__name__)

DEFAULT_CATEGORY_NAME = "Anime Bot"
MAIN_FORUM_NAME = "📺 Каталог аниме"
LIST_FORUM_NAME = "📋 Личные списки"
INFO_THREAD_NAME = "📌 Справка и команды"


def guild_config_from_state(state: dict[str, Any], guild_id: int) -> dict[str, Any] | None:
    raw = (state.get("guilds") or {}).get(str(guild_id))
    return raw if isinstance(raw, dict) else None


def forum_channel_id(cfg: dict[str, Any] | None) -> int | None:
    if not cfg:
        return None
    try:
        v = int(cfg.get("forum_channel_id") or 0)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def list_forum_channel_id(cfg: dict[str, Any] | None) -> int | None:
    if not cfg:
        return None
    try:
        v = int(cfg.get("list_forum_channel_id") or 0)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def bot_info_thread_id(cfg: dict[str, Any] | None) -> int | None:
    if not cfg:
        return None
    try:
        v = int(cfg.get("bot_info_thread_id") or 0)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def category_id(cfg: dict[str, Any] | None) -> int | None:
    if not cfg:
        return None
    try:
        v = int(cfg.get("category_id") or 0)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def is_guild_roaster_enabled(cfg: dict[str, Any] | None) -> bool:
    return bool(cfg and cfg.get("roaster_enabled"))


async def resolve_forum_channel(
    client: discord.Client, guild_id: int, cfg: dict[str, Any] | None
) -> discord.ForumChannel | None:
    fid = forum_channel_id(cfg)
    if not fid:
        return None
    ch: discord.abc.GuildChannel | None = client.get_channel(fid)
    if ch is None:
        guild = client.get_guild(guild_id)
        if guild is not None:
            ch = guild.get_channel(fid)
    if ch is None and client.is_ready():
        try:
            ch = await client.fetch_channel(fid)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    return ch if isinstance(ch, discord.ForumChannel) else None


async def resolve_list_forum_channel(
    client: discord.Client, guild_id: int, cfg: dict[str, Any] | None
) -> discord.ForumChannel | None:
    fid = list_forum_channel_id(cfg)
    if not fid:
        return None
    ch: discord.abc.GuildChannel | None = client.get_channel(fid)
    if ch is None:
        guild = client.get_guild(guild_id)
        if guild is not None:
            ch = guild.get_channel(fid)
    if ch is None and client.is_ready():
        try:
            ch = await client.fetch_channel(fid)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    return ch if isinstance(ch, discord.ForumChannel) else None


async def setup_guild_channels(
    guild: discord.Guild,
    *,
    category_name: str = DEFAULT_CATEGORY_NAME,
    commands_embed: discord.Embed | None = None,
) -> tuple[dict[str, Any], str | None]:
    """
    Создаёт категорию и два форум-канала. Возвращает (config_dict, error).
    """
    me = guild.me
    if me is None:
        return {}, "Бот не видит себя на сервере."

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=False,
            create_public_threads=True,
            send_messages_in_threads=True,
            read_message_history=True,
        ),
        me: discord.PermissionOverwrite(
            view_channel=True,
            manage_channels=True,
            manage_threads=True,
            send_messages=True,
            embed_links=True,
            attach_files=True,
            read_message_history=True,
        ),
    }

    try:
        category = await guild.create_category(name=category_name[:100], overwrites=overwrites)
    except discord.Forbidden:
        return {}, "Нет прав **Управление каналами** для создания категории."
    except discord.HTTPException as e:
        return {}, f"Не удалось создать категорию: {e}"

    try:
        main_forum = await guild.create_forum(
            name=MAIN_FORUM_NAME[:100],
            category=category,
            topic="Основной каталог аниме — одна тема на тайтл.",
            reason="Anime bot auto-setup",
        )
    except (discord.Forbidden, discord.HTTPException) as e:
        try:
            await category.delete(reason="Rollback failed main forum")
        except discord.HTTPException:
            pass
        return {}, f"Не удалось создать форум каталога: {e}"

    try:
        list_forum = await guild.create_forum(
            name=LIST_FORUM_NAME[:100],
            category=category,
            topic="Личные списки участников — по одной теме на человека.",
            reason="Anime bot auto-setup",
        )
    except (discord.Forbidden, discord.HTTPException) as e:
        try:
            await main_forum.delete(reason="Rollback failed list forum")
            await category.delete(reason="Rollback failed list forum")
        except discord.HTTPException:
            pass
        return {}, f"Не удалось создать форум личных списков: {e}"

    info_tid: int | None = None
    if commands_embed is not None:
        try:
            twm = await main_forum.create_thread(
                name=INFO_THREAD_NAME[:100],
                content="Справочная ветка бота. Команды ниже.",
                embeds=[commands_embed],
            )
            info_tid = twm.thread.id
        except discord.HTTPException as e:
            logger.warning("Info thread creation failed: %s", e)

    cfg: dict[str, Any] = {
        "category_id": category.id,
        "forum_channel_id": main_forum.id,
        "list_forum_channel_id": list_forum.id,
        "bot_info_thread_id": info_tid,
        "roaster_enabled": False,
        "setup_at": datetime.now(timezone.utc).isoformat(),
    }
    return cfg, None


def format_guild_status(cfg: dict[str, Any] | None) -> str:
    if not cfg:
        return "Бот **не настроен** на этом сервере. Админ: `/bot setup`."
    lines = [
        f"**Категория:** <#{cfg.get('category_id')}>",
        f"**Каталог аниме:** <#{cfg.get('forum_channel_id')}>",
        f"**Личные списки:** <#{cfg.get('list_forum_channel_id')}>",
    ]
    itid = cfg.get("bot_info_thread_id")
    if itid:
        lines.append(f"**Справка:** <#{itid}>")
    lines.append(
        f"**Обзыватель на сервере:** {'вкл.' if cfg.get('roaster_enabled') else 'выкл.'}"
    )
    if cfg.get("setup_at"):
        lines.append(f"_Настроено: {cfg['setup_at']}_")
    return "\n".join(lines)
