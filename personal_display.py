"""Отображение личного списка: summary / paged / gallery."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable

import aiohttp
import discord

logger = logging.getLogger(__name__)

PAGE_SIZE = 20
RECENT_COUNT = 10
DISPLAY_MODES = ("summary", "paged", "gallery")

EmbedBuilder = Callable[..., discord.Embed]
MetaFetcher = Callable[[aiohttp.ClientSession | None, dict[str, Any], str], Awaitable[dict[str, Any]]]
TitleForKey = Callable[[dict[str, Any], str], str]
JumpForKey = Callable[[dict[str, Any], int, str], str]
OrderedKeys = Callable[[dict[str, Any]], list[str]]
CardEmbedBuilder = Callable[..., discord.Embed]


def normalize_display_mode(raw: Any) -> str:
    m = str(raw or "summary").strip().lower()
    return m if m in DISPLAY_MODES else "summary"


def page_count(n_items: int) -> int:
    if n_items <= 0:
        return 1
    return (n_items + PAGE_SIZE - 1) // PAGE_SIZE


def slice_page(keys: list[str], page: int) -> tuple[list[str], int, int]:
    total_pages = page_count(len(keys))
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    return keys[start:end], page, total_pages


def build_summary_embed(
    pl: dict[str, Any],
    display_name: str,
    *,
    n_items: int,
    title_for_key: TitleForKey,
    state: dict[str, Any],
    accent: int,
    mode: str,
) -> discord.Embed:
    top5 = pl.get("top5") if isinstance(pl.get("top5"), list) else []
    top_lines: list[str] = []
    for k in top5[:5]:
        ks = str(k).strip()
        if ks:
            top_lines.append(f"⭐ {_truncate(title_for_key(state, ks), 60)}")
    top_block = "\n".join(top_lines) if top_lines else "_Топ не задан — `/list top`_"
    mode_labels = {"summary": "Сводка", "paged": "Страницы", "gallery": "Галерея"}
    e = discord.Embed(
        title=f"📋 Список — {display_name}",
        description=(
            f"**Всего тайтлов:** {n_items}\n"
            f"**Режим:** {mode_labels.get(mode, mode)}\n\n"
            f"**Топ:**\n{top_block}\n\n"
            "· **Страницы** — кнопки ◀ ▶ (режим «Страницы»)\n"
            "· **Обновить** — перерисовать без лишних API\n"
            "· **Галерея** — карточки с постерами (тяжёлый режим)\n"
            "· Полный текст: **`/list show`** или **Экспорт**"
        ),
        color=accent,
    )
    e.set_footer(text=f"Страниц: {page_count(n_items)} · по {PAGE_SIZE} на страницу")
    return e


def build_text_list_embed(
    keys: list[str],
    *,
    state: dict[str, Any],
    guild_id: int,
    title_for_key: TitleForKey,
    jump_for_key: JumpForKey,
    accent: int,
    page: int,
    total_pages: int,
    header: str,
) -> discord.Embed:
    lines: list[str] = []
    base = page * PAGE_SIZE
    for i, k in enumerate(keys, start=1):
        t = title_for_key(state, k)
        u = jump_for_key(state, guild_id, k)
        lines.append(f"**{base + i}.** [{_truncate(t, 70)}]({u})")
    body = "\n".join(lines) if lines else "_Пусто_"
    e = discord.Embed(
        title=header,
        description=_truncate(body, 3900),
        color=accent,
    )
    e.set_footer(text=f"Страница {page + 1}/{total_pages}")
    return e


def build_recent_embed(
    recent_keys: list[str],
    *,
    state: dict[str, Any],
    guild_id: int,
    title_for_key: TitleForKey,
    jump_for_key: JumpForKey,
    accent: int,
) -> discord.Embed | None:
    if not recent_keys:
        return None
    lines = []
    for i, k in enumerate(recent_keys[:RECENT_COUNT], 1):
        lines.append(
            f"**{i}.** [{_truncate(title_for_key(state, k), 70)}]({jump_for_key(state, guild_id, k)})"
        )
    return discord.Embed(
        title="🆕 Недавно добавленные",
        description="\n".join(lines),
        color=accent,
    )


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


class PersonalPagerView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="◀",
        style=discord.ButtonStyle.secondary,
        custom_id="plist:page:prev",
    )
    async def prev_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._shift(interaction, -1)

    @discord.ui.button(
        label="▶",
        style=discord.ButtonStyle.secondary,
        custom_id="plist:page:next",
    )
    async def next_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._shift(interaction, 1)

    async def _shift(self, interaction: discord.Interaction, delta: int) -> None:
        from bot import (  # noqa: PLC0415 — runtime import avoids cycle at load
            resolve_personal_list_owner_for_interaction,
            read_state_copy,
            _set_personal_list_fields,
            rebuild_personal_list_display,
        )

        resolved = await resolve_personal_list_owner_for_interaction(interaction)
        if not resolved:
            return
        owner_id, pl = resolved
        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Листать может только **владелец** списка.", ephemeral=True
            )
            return
        mode = normalize_display_mode(pl.get("display_mode"))
        if mode != "paged":
            await interaction.response.send_message(
                "Пагинация активна в режиме **Страницы**. Смените режим на панели.",
                ephemeral=True,
            )
            return
        cur = int(pl.get("current_page") or 0)
        await _set_personal_list_fields(
            interaction.guild.id, owner_id, current_page=max(0, cur + delta)
        )
        await interaction.response.defer(ephemeral=True)
        await rebuild_personal_list_display(
            interaction.client,
            interaction.guild.id,
            owner_id,
            session=getattr(interaction.client, "session", None),
            incremental=True,
        )
        await interaction.followup.send("Страница обновлена.", ephemeral=True)


async def rebuild_display(
    client: discord.Client,
    guild_id: int,
    user_id: int,
    *,
    session: aiohttp.ClientSession | None,
    incremental: bool = False,
    skip_deferred: bool = False,
    # injected helpers from bot
    read_state: Callable[[], Awaitable[dict[str, Any]]],
    write_personal_fields: Callable[..., Awaitable[None]],
    title_for_key: TitleForKey,
    jump_for_key: JumpForKey,
    ordered_keys: OrderedKeys,
    fetch_meta: MetaFetcher,
    build_card_embed: CardEmbedBuilder,
    hub_embed_builder: Callable[[dict[str, Any], str], discord.Embed],
    hub_view_factory: Callable[[], discord.ui.View],
    accent_palette: tuple[int, ...],
    default_accent: int,
) -> None:
    """Пересборка UI личного списка с режимами summary / paged / gallery."""
    from bot import guild_personal_list  # noqa: PLC0415

    state = await read_state()
    uid_s = str(user_id)
    pl = guild_personal_list(state, guild_id, user_id)
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

    mode = normalize_display_mode(pl.get("display_mode"))
    if skip_deferred and pl.get("ui_deferred"):
        return

    accent = int(pl.get("accent_color") or default_accent)
    if accent < 0 or accent > 0xFFFFFF:
        accent = default_accent
    compact = bool(pl.get("compact_cards"))
    show_numbers = bool(pl.get("show_numbers"))
    keys = ordered_keys(pl)
    n_items = len(keys)

    top5_raw = pl.get("top5") if isinstance(pl.get("top5"), list) else []
    top5_set = {str(x).strip() for x in top5_raw if str(x).strip()}

    # Reconcile known messages. A failed fetch must never create a duplicate.
    async def upsert(mid, embed, view=None):
        if mid:
            try:
                message = await thread.fetch_message(int(mid))
            except discord.NotFound:
                message = None
            else:
                same_embed = [e.to_dict() for e in message.embeds] == [embed.to_dict()]
                if not same_embed or view is not None:
                    await message.edit(embed=embed, view=view,
                                       allowed_mentions=discord.AllowedMentions.none())
                return message.id
        message = await thread.send(embed=embed, view=view, silent=True,
                                    allowed_mentions=discord.AllowedMentions.none())
        return message.id

    async def block(field, embed, view=None):
        mid = await upsert(pl.get(field), embed, view)
        pl[field] = mid
        # Persist each send immediately, including when a later API request fails.
        await write_personal_fields(user_id, **{field: mid})

    async def remove(mid):
        try:
            message = await thread.fetch_message(int(mid))
            await message.delete()
        except discord.NotFound:
            pass

    await block('control_message_id', hub_embed_builder(pl, display_name), hub_view_factory())
    active = {'control_message_id'}
    if mode in ('summary', 'paged'):
        await block('summary_message_id', build_summary_embed(
            pl, display_name, n_items=n_items, title_for_key=title_for_key,
            state=state, accent=accent, mode=mode))
        active.add('summary_message_id')
    if mode == 'summary':
        recent = [str(k) for k in (pl.get('recent_keys') or []) if str(k) in keys]
        embed = build_recent_embed(recent, state=state, guild_id=guild_id,
            title_for_key=title_for_key, jump_for_key=jump_for_key, accent=accent)
        if embed:
            await block('recent_message_id', embed)
            active.add('recent_message_id')
    elif mode == 'paged':
        page_keys, current, total = slice_page(keys, int(pl.get('current_page') or 0))
        await block('page_message_id', build_text_list_embed(page_keys,
            state=state, guild_id=guild_id, title_for_key=title_for_key,
            jump_for_key=jump_for_key, accent=accent, page=current,
            total_pages=total, header='📄 Страница списка'), PersonalPagerView())
        active.add('page_message_id')
        await write_personal_fields(user_id, current_page=current)

    cards = dict(pl.get('anime_messages') or {})
    cache = dict(pl.get('card_cache') or {})
    if mode == 'gallery':
        for i, key in enumerate(keys, 1):
            meta = cache.get(key)
            if not isinstance(meta, dict) or not meta.get('title'):
                meta = await fetch_meta(session, state, key)
                cache[key] = meta
            embed = build_card_embed(state, guild_id, user_id, key,
                display_index=i, in_top=key in top5_set, meta=meta,
                accent=accent, compact=compact, show_numbers=show_numbers)
            cards[key] = await upsert(cards.get(key), embed)
            await write_personal_fields(user_id, anime_messages=dict(cards), card_cache=cache)
    for key, mid in list(cards.items()):
        if mode != 'gallery' or key not in keys:
            await remove(mid)
            del cards[key]
            await write_personal_fields(user_id, anime_messages=dict(cards))
    for field in ('summary_message_id', 'page_message_id', 'recent_message_id',
                  'top_block_message_id', 'list_message_id'):
        if field not in active and pl.get(field):
            if pl[field] != pl.get('control_message_id'):
                await remove(pl[field])
            await write_personal_fields(user_id, **{field: None})
    await write_personal_fields(user_id, ui_deferred=False)
