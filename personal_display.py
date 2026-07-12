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

    # Удаляем только устаревшие gallery-карточки при смене режима или full rebuild
    msg_ids_to_delete: list[int] = []
    if not incremental or mode != "gallery":
        am_raw = pl.get("anime_messages")
        if isinstance(am_raw, dict):
            for mid in am_raw.values():
                try:
                    msg_ids_to_delete.append(int(mid))
                except (TypeError, ValueError):
                    pass

    block_ids = [
        pl.get("summary_message_id"),
        pl.get("page_message_id"),
        pl.get("recent_message_id"),
        pl.get("top_block_message_id"),
    ]
    if not incremental:
        for bid in block_ids:
            if bid:
                try:
                    msg_ids_to_delete.append(int(bid))
                except (TypeError, ValueError):
                    pass

    seen_del: set[int] = set()
    for mid in msg_ids_to_delete:
        if mid in seen_del:
            continue
        seen_del.add(mid)
        try:
            m = await thread.fetch_message(mid)
            await m.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
        await asyncio.sleep(0.04)

    hub_view = hub_view_factory()
    hub_embed = hub_embed_builder(pl, display_name)
    ctrl_id = pl.get("control_message_id")
    if ctrl_id:
        try:
            hub_msg = await thread.fetch_message(int(ctrl_id))
            await hub_msg.edit(embed=hub_embed, view=hub_view)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            try:
                hub_msg = await thread.send(embed=hub_embed, view=hub_view)
                await write_personal_fields(user_id, control_message_id=hub_msg.id)
            except discord.HTTPException:
                hub_msg = None
    else:
        try:
            hub_msg = await thread.send(embed=hub_embed, view=hub_view)
            await write_personal_fields(user_id, control_message_id=hub_msg.id)
        except discord.HTTPException:
            hub_msg = None

    pl = guild_personal_list(await read_state(), guild_id, user_id) or pl
    if not isinstance(pl, dict):
        return

    new_fields: dict[str, Any] = {
        "anime_messages": {},
        "ui_deferred": False,
    }

    if mode == "summary":
        emb = build_summary_embed(
            pl, display_name, n_items=n_items,
            title_for_key=title_for_key, state=state, accent=accent, mode=mode,
        )
        try:
            sm = await thread.send(embed=emb)
            new_fields["summary_message_id"] = sm.id
        except discord.HTTPException as e:
            logger.warning("summary embed: %s", e)

        recent_raw = pl.get("recent_keys")
        recent: list[str] = []
        if isinstance(recent_raw, list):
            for k in recent_raw:
                ks = str(k).strip()
                if ks in keys:
                    recent.append(ks)
        rem = build_recent_embed(
            recent, state=state, guild_id=guild_id,
            title_for_key=title_for_key, jump_for_key=jump_for_key, accent=accent,
        )
        if rem:
            try:
                rm = await thread.send(embed=rem)
                new_fields["recent_message_id"] = rm.id
            except discord.HTTPException:
                pass

    elif mode == "paged":
        cur_page = int(pl.get("current_page") or 0)
        page_keys, cur_page, total_pages = slice_page(keys, cur_page)
        await write_personal_fields(user_id, current_page=cur_page)

        sum_emb = build_summary_embed(
            pl, display_name, n_items=n_items,
            title_for_key=title_for_key, state=state, accent=accent, mode=mode,
        )
        try:
            sm = await thread.send(embed=sum_emb)
            new_fields["summary_message_id"] = sm.id
        except discord.HTTPException:
            pass

        list_emb = build_text_list_embed(
            page_keys,
            state=state,
            guild_id=guild_id,
            title_for_key=title_for_key,
            jump_for_key=jump_for_key,
            accent=accent,
            page=cur_page,
            total_pages=total_pages,
            header="📄 Страница списка",
        )
        pager = PersonalPagerView()
        try:
            pm = await thread.send(embed=list_emb, view=pager)
            new_fields["page_message_id"] = pm.id
        except discord.HTTPException as e:
            logger.warning("page embed: %s", e)

    else:  # gallery
        st_cards = await read_state()
        new_map: dict[str, int] = {}
        card_cache: dict[str, Any] = dict(pl.get("card_cache") or {})
        for i, anime_key in enumerate(keys, start=1):
            cached = card_cache.get(anime_key) if isinstance(card_cache, dict) else None
            if isinstance(cached, dict) and cached.get("title"):
                meta = cached
            else:
                meta = await fetch_meta(session, st_cards, anime_key)
                card_cache[anime_key] = meta
            emb = build_card_embed(
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
                logger.warning("gallery card %s: %s", anime_key, e)
            await asyncio.sleep(0.25)
        new_fields["anime_messages"] = new_map
        new_fields["card_cache"] = card_cache

    await write_personal_fields(user_id, **new_fields)
