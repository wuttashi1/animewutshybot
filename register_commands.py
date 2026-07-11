"""Регистрация сгруппированных slash-команд на bot.tree."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import discord
import guild_config
import personal_display
import roaster
import yummy_api
from discord import app_commands

logger = logging.getLogger(__name__)


def _mal_choice_to_status(choice: str) -> int:
    import bot as core

    return {
        "all": core.MAL_STATUS_ALL,
        "watching": 1,
        "completed": 2,
        "on_hold": 3,
        "dropped": 4,
        "plan_to_watch": 6,
    }.get(choice, core.MAL_STATUS_ALL)


async def _personal_slug_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    import bot as core

    state = await core.read_state_copy()
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
        title = core._title_for_list_key(state, ks)
        if cur and cur not in title.lower() and cur not in ks.lower():
            continue
        label = core._truncate(f"{title} ({ks})", 100)
        choices.append(app_commands.Choice(name=label, value=ks))
        if len(choices) >= 25:
            break
    return choices


_GUILD_ONLY_MSG = "Команду можно использовать только на сервере."


def setup(bot_instance: discord.Client) -> None:
    """Регистрирует группы /anime, /mal, /yummy, /list, /admin, /bot, /owner, /roast."""
    import bot as core

    # --- anime ---
    anime_group = app_commands.Group(name="anime", description="Каталог аниме на основном форуме")

    async def _anime_add_impl(interaction: discord.Interaction, query: str) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            out = await core.run_animeadd_for_user(
                interaction.guild,
                interaction.user.id,
                query,
                client=bot_instance,
            )
        except Exception:
            logger.exception("anime add")
            out = "Произошла ошибка при добавлении."
        await interaction.followup.send(
            core._truncate(out, core.DISCORD_CONTENT_LIMIT), ephemeral=True
        )

    @anime_group.command(name="add", description="Добавить аниме в основной форум и личный список")
    @app_commands.describe(query="Ссылка en.yummyani.me или название")
    async def anime_add(interaction: discord.Interaction, query: str) -> None:
        await _anime_add_impl(interaction, query)

    async def _anime_rate_impl(interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        ch = interaction.channel
        if not isinstance(ch, discord.Thread):
            await interaction.response.send_message(
                "Откройте команду **внутри темы** форума с аниме.", ephemeral=True
            )
            return
        state = await core.read_state_copy()
        if not core.thread_has_rating_slot(state, ch.id):
            await interaction.response.send_message(
                "Эта тема не зарегистрирована для оценок. "
                "Создайте её через `/anime add` или импорт из MAL (`/mal import`).",
                ephemeral=True,
            )
            return
        try:
            await core.ensure_topic_side_panels(interaction.client, ch.id)
        except Exception as e:
            logger.warning("Панели перед /anime rate: %s", e)
        await interaction.response.send_modal(core.AnimeRatingModal(ch.id))

    @anime_group.command(
        name="rate",
        description="Поставить оценку 1–10 аниме в этой теме форума",
    )
    async def anime_rate(interaction: discord.Interaction) -> None:
        await _anime_rate_impl(interaction)

    @anime_group.command(
        name="duplicates",
        description="Найти дубликаты тем форума и при необходимости удалить лишние",
    )
    async def anime_duplicates(interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        state = await core.read_state_copy()
        groups = core._collect_duplicate_groups(state)
        if not groups:
            await interaction.response.send_message(
                "Дубликатов не найдено: у каждого slug YummyAnime и каждого MAL id "
                "не больше одной зарегистрированной темы.",
                ephemeral=True,
            )
            return
        topics = state.get("anime_topics", {})
        lines: list[str] = [
            "Несколько **веток форума** привязаны к **одному и тому же** аниме. "
            "Оставляется тема из базы бота; остальные можно снять кнопкой ниже.",
            "",
        ]
        victims: list[int] = []
        for i, g in enumerate(groups, 1):
            keeper = core._pick_keeper_thread_id(g, topics)
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
        text = core._truncate("\n".join(lines).rstrip(), core.DISCORD_CONTENT_LIMIT)
        embed = discord.Embed(
            title="Дубликаты тем",
            description=text,
            color=0xE74C3C,
        )
        embed.set_footer(
            text="Удаление требует права «Управлять ветками». Кнопка доступна только вам."
        )
        view = core.DuplicateCleanupView(
            requester_id=interaction.user.id,
            victims=victims,
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    # --- mal ---
    mal_group = app_commands.Group(name="mal", description="MyAnimeList: привязка и импорт")

    @mal_group.command(
        name="bind",
        description="Привязать или перепривязать ваш MyAnimeList",
    )
    @app_commands.describe(list_url="Ссылка на animelist или профиль MAL")
    async def mal_bind(interaction: discord.Interaction, list_url: str) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        username = core.mal_username_from_url(list_url)
        if not username:
            await interaction.response.send_message(
                "Нужна ссылка вида `https://myanimelist.net/animelist/ник` "
                "или `https://myanimelist.net/profile/ник`.",
                ephemeral=True,
            )
            return
        norm_url = f"https://myanimelist.net/animelist/{username}"
        await core.bind_mal_account(interaction.user.id, username, norm_url)
        await interaction.response.send_message(
            f"MAL привязан: [{username}]({norm_url}). Можно перепривязать этой же командой.",
            ephemeral=True,
        )

    @mal_group.command(
        name="import",
        description="Импортировать аниме из привязанного MAL в основной форум",
    )
    @app_commands.describe(
        mal_status="Какие позиции брать из списка",
        list_url="(необязательно) новая ссылка MAL для перепривязки перед импортом",
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
    async def mal_import(
        interaction: discord.Interaction,
        mal_status: str,
        list_url: str | None = None,
        max_topics: app_commands.Range[int, 1, 25] = 10,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (mal import)")
            await interaction.followup.send("Сессия HTTP не готова.", ephemeral=True)
            return

        username = ""
        norm_url = ""
        if list_url:
            username = core.mal_username_from_url(list_url) or ""
            if not username:
                await interaction.followup.send(
                    "Нужна ссылка вида `https://myanimelist.net/animelist/ник` "
                    "или `https://myanimelist.net/profile/ник`.",
                    ephemeral=True,
                )
                return
            norm_url = f"https://myanimelist.net/animelist/{username}"
            await core.bind_mal_account(interaction.user.id, username, norm_url)
        else:
            state0 = await core.read_state_copy()
            acc = state0.get("mal_accounts", {}).get(str(interaction.user.id))
            if not isinstance(acc, dict):
                await interaction.followup.send(
                    "Сначала привяжите MAL командой `/mal bind` "
                    "(или передайте ссылку в `/mal import`).",
                    ephemeral=True,
                )
                return
            username = (acc.get("username") or "").strip()
            norm_url = (acc.get("list_url") or "").strip() or (
                f"https://myanimelist.net/animelist/{username}"
            )
            if not username:
                await interaction.followup.send(
                    "В привязке MAL нет имени пользователя. Выполните `/mal bind` заново.",
                    ephemeral=True,
                )
                return

        status_int = _mal_choice_to_status(mal_status)
        entries, http_st = await core.mal_fetch_full_list(
            session, username, status_int
        )
        if http_st != 200:
            await interaction.followup.send(
                "Не удалось открыть список на MyAnimeList "
                "(проверьте ник и что список **публичный**).",
                ephemeral=True,
            )
            return

        forum = await core.resolve_forum_channel(bot_instance, interaction.guild.id)
        if not forum:
            await interaction.followup.send(
                "Канал форума не найден. Аккаунт MAL сохранён; импорт можно повторить позже.",
                ephemeral=True,
            )
            return

        state = await core.read_state_copy()
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

            query = core.mal_item_title(entry)
            slug = await core.api_search_slug(session, query)
            info = await core.api_fetch_anime(session, slug) if slug else None

            if info:
                slug_key = core._clean_slug((info.get("anime_url") or "").strip())
                thread, mst = await core.merge_adder_into_existing_topic(
                    bot_instance, slug_key, uid
                )
                if mst == "merged":
                    await core.mark_mal_imported(interaction.user.id, aid)
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
                            await core.append_user_anime_to_personal_state(
                                interaction.guild,
                                uid,
                                slug_key,
                                tnm,
                                defer_ui=True,
                            )
                        except Exception:
                            logger.exception("Личный список после merge MAL→Yummy")
                    if merge_ops >= core.CONNECT_MAX_MERGES_PER_RUN:
                        errors.append(
                            "Достигнут лимит дописываний за один запуск; запустите снова."
                        )
                        break
                    await asyncio.sleep(0.35)
                    continue
                if mst == "already":
                    await core.mark_mal_imported(interaction.user.id, aid)
                    imported_ids.add(aid)
                    merge_ops += 1
                    try:
                        await core.append_user_anime_to_personal_state(
                            interaction.guild,
                            uid,
                            slug_key,
                            query[:500],
                            defer_ui=True,
                        )
                    except Exception:
                        logger.exception("Личный список после already MAL→Yummy")
                    if merge_ops >= core.CONNECT_MAX_MERGES_PER_RUN:
                        errors.append(
                            "Достигнут лимит дописываний за один запуск; запустите снова."
                        )
                        break
                    continue
                if mst in ("edit_failed", "fetch_failed"):
                    errors.append(f"{query}: тема уже есть, не удалось обновить подпись")
                    continue

                thread, _st, err = await core.create_yummy_forum_thread(
                    forum,
                    session,
                    info,
                    uid,
                    mal_id=aid,
                    resolved_slug=slug or "",
                )
            else:
                thread, mst = await core.merge_adder_into_existing_topic(
                    bot_instance, f"mal:{aid}", uid
                )
                if mst == "merged":
                    await core.mark_mal_imported(interaction.user.id, aid)
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
                            await core.append_user_anime_to_personal_state(
                                interaction.guild,
                                uid,
                                f"mal:{aid}",
                                tnm,
                                defer_ui=True,
                            )
                        except Exception:
                            logger.exception("Личный список после merge MAL-only")
                    if merge_ops >= core.CONNECT_MAX_MERGES_PER_RUN:
                        errors.append(
                            "Достигнут лимит дописываний за один запуск; запустите снова."
                        )
                        break
                    await asyncio.sleep(0.35)
                    continue
                if mst == "already":
                    await core.mark_mal_imported(interaction.user.id, aid)
                    imported_ids.add(aid)
                    merge_ops += 1
                    try:
                        await core.append_user_anime_to_personal_state(
                            interaction.guild,
                            uid,
                            f"mal:{aid}",
                            query[:500],
                            defer_ui=True,
                        )
                    except Exception:
                        logger.exception("Личный список после already MAL-only")
                    if merge_ops >= core.CONNECT_MAX_MERGES_PER_RUN:
                        errors.append(
                            "Достигнут лимит дописываний за один запуск; запустите снова."
                        )
                        break
                    continue
                if mst in ("edit_failed", "fetch_failed"):
                    errors.append(f"{query}: тема MAL уже есть, не удалось обновить подпись")
                    continue

                thread, _st, err = await core.create_mal_only_forum_thread(
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
                    pk = core._clean_slug((info.get("anime_url") or "").strip())
                    pt = str(info.get("title") or query)[:500]
                else:
                    pk = f"mal:{aid}"
                    pt = core.mal_item_title(entry)
                await core.append_user_anime_to_personal_state(
                    interaction.guild, uid, pk, pt, defer_ui=True
                )
            except Exception:
                logger.exception("Личный список после новой темы из mal import")

            await core.mark_mal_imported(interaction.user.id, aid)
            imported_ids.add(aid)
            n_new += 1
            ju = thread.jump_url if hasattr(thread, "jump_url") else f"<#{thread.id}>"
            created_urls.append(ju)
            await asyncio.sleep(1.25)

        if n_new or merge_ops:
            core.schedule_personal_list_refresh(interaction.guild.id, uid)

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
            core._truncate("\n".join(lines), core.DISCORD_CONTENT_LIMIT), ephemeral=True
        )

    @mal_group.command(
        name="show",
        description="Показать список MyAnimeList (ваш или выбранного участника)",
    )
    @app_commands.describe(member="Чей список MAL (необязательно)")
    async def mal_show(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        try:
            mal_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (mal show)")
            await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
            return
        assert interaction.guild is not None
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
        state = await core.read_state_copy()
        embed, err = await core.build_mal_list_embed_for_member(
            mal_session, state, target
        )
        if err:
            await interaction.followup.send(err)
            return
        assert embed is not None
        await interaction.followup.send(embed=embed)

    async def _bind_yummy_with_token(
        interaction: discord.Interaction,
        access_token: str,
    ) -> None:
        app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
        if not app:
            await interaction.followup.send(
                "Владелец бота должен задать **YUMMY_APPLICATION_TOKEN**.",
                ephemeral=True,
            )
            return
        try:
            bind_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (yummy bind)")
            await interaction.followup.send("Сессия HTTP не готова.", ephemeral=True)
            return
        t = (access_token or "").strip()
        if t.lower().startswith("bearer "):
            t = t[7:].strip()
        if len(t) < 12:
            await interaction.followup.send("Получен слишком короткий access token.", ephemeral=True)
            return
        prof = await yummy_api.yani_get_profile(bind_session, app, t, core.USER_AGENT)
        if not prof:
            await interaction.followup.send(
                "Не удалось получить профиль. Проверьте токен или логин/пароль.",
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
        await core.bind_yummy_account(interaction.user.id, t, yid_i, nick)
        await interaction.followup.send(
            f"YummyAnime привязан (**{nick or yid_i}**). Импорт: `/yummy sync`.",
            ephemeral=True,
        )

    class YummyBindModal(discord.ui.Modal, title="YummyAnime авторизация"):
        login = discord.ui.TextInput(
            label="Логин или почта",
            placeholder="Введите логин или email",
            required=True,
            max_length=100,
        )
        password = discord.ui.TextInput(
            label="Пароль",
            placeholder="Введите пароль",
            required=True,
            style=discord.TextStyle.short,
            max_length=128,
        )

        async def on_submit(self, interaction: discord.Interaction) -> None:
            app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
            if not app:
                await interaction.response.send_message(
                    "Владелец бота должен задать **YUMMY_APPLICATION_TOKEN**.",
                    ephemeral=True,
                )
                return
            try:
                bind_session = await bot_instance.ensure_http_session()
            except Exception:
                logger.exception("HTTP session (yummy bind modal)")
                await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True, thinking=True)
            token, auth_err = await yummy_api.yani_login_password(
                bind_session,
                app,
                str(self.login.value or "").strip(),
                str(self.password.value or "").strip(),
                core.USER_AGENT,
            )
            if not token:
                await interaction.followup.send(
                    auth_err or "Не удалось выполнить вход в YummyAnime API.",
                    ephemeral=True,
                )
                return
            await _bind_yummy_with_token(interaction, token)

    # --- yummy ---
    yummy_group = app_commands.Group(name="yummy", description="YummyAnime: привязка и синхронизация")

    @yummy_group.command(
        name="bind",
        description="Привязать аккаунт YummyAnime (через окно входа)",
    )
    @app_commands.describe(
        bearer_token="(опционально) Bearer токен из DevTools, без окна входа",
    )
    async def yummy_bind(
        interaction: discord.Interaction,
        bearer_token: str | None = None,
    ) -> None:
        t = (bearer_token or "").strip()
        if t:
            await interaction.response.defer(ephemeral=True, thinking=True)
            await _bind_yummy_with_token(interaction, t)
            return
        await interaction.response.send_modal(YummyBindModal())

    @yummy_group.command(name="unbind", description="Отвязать аккаунт YummyAnime")
    async def yummy_unbind(interaction: discord.Interaction) -> None:
        await core.unbind_yummy_account(interaction.user.id)
        await interaction.response.send_message(
            "Привязка YummyAnime снята.",
            ephemeral=True,
        )

    @yummy_group.command(
        name="sync",
        description="Подтянуть новые аниме из YummyAnime в основной форум и личный топик",
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
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
        if not app:
            await interaction.response.send_message(
                "Не задан **YUMMY_APPLICATION_TOKEN** на стороне бота.", ephemeral=True
            )
            return
        try:
            yummy_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (yummy import)")
            await interaction.response.send_message("Сессия HTTP не готова.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        r = await core.run_yummy_list_import_for_member(
            interaction.guild,
            interaction.user.id,
            list_filter=yummy_list,
            max_topics=int(max_topics),
            session=yummy_session,
            app_token=app,
        )
        if r.get("error"):
            await interaction.followup.send(
                core._truncate(str(r["error"]), core.DISCORD_CONTENT_LIMIT), ephemeral=True
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
            core._truncate("\n".join(lines), core.DISCORD_CONTENT_LIMIT), ephemeral=True
        )

    # --- list ---
    list_group = app_commands.Group(name="list", description="Личный Discord-список аниме")

    async def _list_show_impl(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
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
        embed, err, _s, _u = await core.run_animelist_discord_topics(
            interaction.guild, target
        )
        if err:
            await interaction.followup.send(
                core._truncate(err, core.DISCORD_CONTENT_LIMIT), ephemeral=True
            )
            return
        assert embed is not None
        await interaction.followup.send(embed=embed, ephemeral=True)

    @list_group.command(
        name="show",
        description="Личный Discord-список (без полного сканирования форума)",
    )
    @app_commands.describe(member="Чей список (если не указано — ваш)")
    async def list_show(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        await _list_show_impl(interaction, member)

    @list_group.command(
        name="top",
        description="Задать до 5 аниме для блока «Топ» в личной теме",
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
    async def list_top(
        interaction: discord.Interaction,
        slot1: str | None = None,
        slot2: str | None = None,
        slot3: str | None = None,
        slot4: str | None = None,
        slot5: str | None = None,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        raw_slots = [slot1, slot2, slot3, slot4, slot5]
        slots = [str(s).strip() for s in raw_slots if s and str(s).strip()]
        await interaction.response.defer(ephemeral=True, thinking=True)

        uid = interaction.user.id
        uid_s = str(uid)
        state = await core.read_state_copy()
        pl = (state.get("personal_lists") or {}).get(uid_s)
        if not isinstance(pl, dict):
            await interaction.followup.send(
                "Личного списка ещё нет — сначала добавьте аниме через `/anime add`.",
                ephemeral=True,
            )
            return
        order = pl.get("order")
        if not isinstance(order, list):
            order = []

        if not slots:
            async with core._state_lock:
                data = core._load_state()
                pl2 = data.setdefault("personal_lists", {}).setdefault(uid_s, {})
                pl2["top5"] = []
                data["personal_lists"][uid_s] = pl2
                core._write_state(data)
            await core.rebuild_personal_list_display(
                bot_instance, interaction.guild.id, uid, session=bot_instance.session
            )
            await interaction.followup.send(
                "Топ очищен. Карточки в теме пересобраны.", ephemeral=True
            )
            return

        uniq = list(dict.fromkeys(slots))[:5]
        for s in uniq:
            if s not in order:
                await interaction.followup.send(
                    f"В вашем списке нет ключа `{s}`. Выберите значения из автодополнения.",
                    ephemeral=True,
                )
                return

        async with core._state_lock:
            data = core._load_state()
            pl2 = data.setdefault("personal_lists", {}).setdefault(uid_s, {})
            pl2["top5"] = uniq
            data["personal_lists"][uid_s] = pl2
            core._write_state(data)
        await core.rebuild_personal_list_display(
            bot_instance, interaction.guild.id, uid, session=bot_instance.session
        )
        await interaction.followup.send(
            f"Топ сохранён (**{len(uniq)}**). Карточки пересобраны.",
            ephemeral=True,
        )

    @list_group.command(
        name="panel",
        description="Восстановить панель кнопок в личной теме списка",
    )
    async def list_panel(interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        ch = interaction.channel
        list_parent = await core.list_forum_parent_id(interaction.guild.id)
        if not isinstance(ch, discord.Thread) or list_parent is None or ch.parent_id != list_parent:
            await interaction.response.send_message(
                "Вызовите команду **внутри своей личной темы** в форуме списков.",
                ephemeral=True,
            )
            return
        resolved = await core.resolve_personal_list_owner_for_interaction(interaction)
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
                    "Панель уже на месте. Если кнопки «мёртвые», нажмите **Обновить** "
                    "на панели или перезапустите бота.",
                    ephemeral=True,
                )
                return
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        await interaction.response.defer(ephemeral=True, thinking=True)
        hub_embed = core._personal_hub_embed(pl, interaction.user.display_name)
        try:
            hub_msg = await ch.send(embed=hub_embed, view=core.PersonalTopicHubView())
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"Не удалось отправить панель: {e}", ephemeral=True
            )
            return
        await core._set_personal_list_fields(oid, control_message_id=hub_msg.id)
        await interaction.followup.send(
            "Панель отправлена **вниз темы**. При необходимости удалите дубликат вручную.",
            ephemeral=True,
        )

    @list_group.command(
        name="edit",
        description="Изменить название и первый пост в личной теме списка",
    )
    @app_commands.describe(
        name="Новое название темы форума",
        description="Новый текст первого сообщения в теме",
    )
    async def list_edit(
        interaction: discord.Interaction,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        if not name and not description:
            await interaction.response.send_message(
                "Укажите **name** и/или **description**.", ephemeral=True
            )
            return

        uid = interaction.user.id
        uid_s = str(uid)
        state = await core.read_state_copy()
        pl = (state.get("personal_lists") or {}).get(uid_s)
        if not isinstance(pl, dict) or not pl.get("thread_id"):
            await interaction.response.send_message(
                "Личной темы ещё нет — она создаётся при первом добавлении аниме.",
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
            await interaction.followup.send(
                "Личная тема не найдена (проверьте ID).", ephemeral=True
            )
            return

        if name:
            try:
                await thread.edit(name=name.strip()[:100])
            except discord.HTTPException as e:
                await interaction.followup.send(
                    f"Не удалось сменить название: {e}", ephemeral=True
                )
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
                await msg.edit(content=core._truncate(description.strip(), 2000))
            except discord.HTTPException as e:
                await interaction.followup.send(
                    f"Не удалось изменить пост: {e}", ephemeral=True
                )
                return

        await interaction.followup.send("Готово.", ephemeral=True)

    @list_group.command(
        name="mode",
        description="Режим отображения личного списка: сводка, страницы или галерея",
    )
    @app_commands.describe(display_mode="Как показывать карточки в теме")
    @app_commands.choices(
        display_mode=[
            app_commands.Choice(name="Сводка", value="summary"),
            app_commands.Choice(name="Страницы", value="paged"),
            app_commands.Choice(name="Галерея", value="gallery"),
        ]
    )
    async def list_mode(interaction: discord.Interaction, display_mode: str) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        mode = personal_display.normalize_display_mode(display_mode)
        uid = interaction.user.id
        await core._set_personal_list_fields(uid, display_mode=mode, current_page=0)
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await core.rebuild_personal_list_display(
                bot_instance, interaction.guild.id, uid, session=bot_instance.session
            )
        except Exception:
            logger.exception("list mode rebuild")
            await interaction.followup.send(
                "Режим сохранён, но не удалось пересобрать карточки.", ephemeral=True
            )
            return
        labels = {"summary": "Сводка", "paged": "Страницы", "gallery": "Галерея"}
        await interaction.followup.send(
            f"Режим: **{labels.get(mode, mode)}**. Карточки пересобраны.",
            ephemeral=True,
        )

    # --- admin ---
    admin_group = app_commands.Group(
        name="admin",
        description="Админ: YummyAnime, скан форума, личные списки, обновление тем",
    )

    @admin_group.command(
        name="yummy_resync",
        description="Принудительно синхронизировать список Yummy участника с форумом",
    )
    @app_commands.describe(
        member="Участник с привязкой /yummy bind",
        max_topics="Максимум новых тем за запуск (1–25)",
    )
    async def admin_yummy_resync(
        interaction: discord.Interaction,
        member: discord.Member,
        max_topics: app_commands.Range[int, 1, 25] = 20,
    ) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        assert interaction.guild is not None
        app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
        if not app:
            await interaction.response.send_message(
                "Нет **YUMMY_APPLICATION_TOKEN**.", ephemeral=True
            )
            return
        try:
            admin_yummy_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (admin yummy sync)")
            await interaction.response.send_message(
                "Сессия HTTP не готова.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        r = await core.run_yummy_list_import_for_member(
            interaction.guild,
            member.id,
            list_filter="all",
            max_topics=int(max_topics),
            session=admin_yummy_session,
            app_token=app,
        )
        if r.get("error"):
            await interaction.followup.send(
                core._truncate(str(r["error"]), core.DISCORD_CONTENT_LIMIT), ephemeral=True
            )
            return
        msg = (
            f"**{member.display_name}:** новых тем **{r.get('n_new', 0)}**, "
            f"дописано **{r.get('merge_ops', 0)}**."
        )
        er = r.get("errors") or []
        if er:
            msg += "\n" + "; ".join(er[:3])
        await interaction.followup.send(
            core._truncate(msg, core.DISCORD_CONTENT_LIMIT), ephemeral=True
        )

    @admin_group.command(
        name="yummy_status",
        description="Статус последнего фонового опроса YummyAnime",
    )
    async def admin_yummy_status(interaction: discord.Interaction) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        state = await core.read_state_copy()
        poll = (state.get("meta") or {}).get("yummy_poll")
        if not isinstance(poll, dict):
            embed = discord.Embed(
                title="Фон YummyAnime",
                description="Ещё не было успешного цикла (или опрос отключён).",
                color=core.EMBED_COLOR,
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
            desc += "**Ошибки:**\n" + "\n".join(
                f"· {core._truncate(str(e), 200)}" for e in errs[:6]
            )
        embed = discord.Embed(title="Фон YummyAnime", description=desc, color=core.EMBED_COLOR)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @admin_group.command(
        name="forum_scan",
        description="Обновить anime_topics по веткам основного форума",
    )
    async def admin_forum_scan(interaction: discord.Interaction) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        try:
            scan_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (admin forum_scan)")
            await interaction.response.send_message(
                "Сессия HTTP не готова.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        assert interaction.guild is not None
        scanned, updated = await core.sync_forum_threads_with_state(
            bot_instance, interaction.guild, scan_session
        )
        await interaction.followup.send(
            f"Просмотрено веток: **{scanned}**, обновлено записей: **{updated}**.",
            ephemeral=True,
        )

    @admin_group.command(
        name="personal_rebuild",
        description="Пересоздать карточки в личной теме участника",
    )
    @app_commands.describe(member="Участник")
    async def admin_personal_rebuild(
        interaction: discord.Interaction, member: discord.Member
    ) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        assert interaction.guild is not None
        try:
            await core.ensure_personal_list_thread(
                bot_instance, interaction.guild, member, session=bot_instance.session
            )
            await core.rebuild_personal_list_display(
                bot_instance, interaction.guild.id, member.id, session=bot_instance.session
            )
        except Exception:
            logger.exception("admin personal_rebuild")
            await interaction.followup.send("Ошибка при пересборке.", ephemeral=True)
            return
        await interaction.followup.send(
            f"Личная тема **{member.display_name}** обновлена.", ephemeral=True
        )

    @admin_group.command(
        name="repair_topics",
        description="Досинхронизировать темы основного форума (реакции, панели, карточка)",
    )
    async def admin_repair_topics(interaction: discord.Interaction) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        try:
            repair_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (admin repair_topics)")
            await interaction.response.send_message(
                "Сессия HTTP не готова.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        assert interaction.guild is not None
        forum = await core.resolve_forum_channel(bot_instance, interaction.guild.id)
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
                await core.repair_single_forum_thread(
                    bot_instance, forum, th, repair_session
                )
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

    @admin_group.command(
        name="sync_list",
        description="Пересобрать личный список участника из тем основного форума",
    )
    @app_commands.describe(member="Чей список пересобрать")
    @app_commands.default_permissions(administrator=True)
    async def admin_sync_list(
        interaction: discord.Interaction, member: discord.Member
    ) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        assert interaction.guild is not None
        scanned, updated = await core.sync_forum_threads_with_state(
            bot_instance, interaction.guild, bot_instance.session if bot_instance.session else None
        )
        n, sync_err = await core.sync_personal_list_from_anime_topics(
            interaction.guild, member.id
        )
        try:
            await core.ensure_personal_list_thread(
                bot_instance, interaction.guild, member, session=bot_instance.session
            )
            await core.rebuild_personal_list_display(
                bot_instance, interaction.guild.id, member.id, session=bot_instance.session
            )
        except Exception:
            logger.exception("admin sync_list")
        parts = [
            f"Основной форум: просмотрено веток **{scanned}**, обновлено **{updated}**.",
            f"В личном списке **{member.display_name}**: **{n}** позиций.",
        ]
        if sync_err:
            parts.append(str(sync_err))
        await interaction.followup.send("\n".join(parts), ephemeral=True)

    @admin_group.command(
        name="panel",
        description="Панель быстрых админ-действий",
    )
    async def admin_panel(interaction: discord.Interaction) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        embed = discord.Embed(
            title="Админ-панель",
            description=(
                "Меню слева — быстрые действия.\n"
                "Полный набор: **`/admin yummy_resync`**, **`/admin forum_scan`**, "
                "**`/admin repair_topics`**, **`/admin personal_rebuild`**, "
                "**`/admin yummy_status`**."
            ),
            color=core.EMBED_COLOR,
        )
        await interaction.response.send_message(
            embed=embed, view=core.AdminPanelView(), ephemeral=True
        )

    @admin_group.command(
        name="roaster_enable",
        description="Включить сатирический «Обзыватель» на этом сервере",
    )
    async def admin_roaster_enable(interaction: discord.Interaction) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        assert interaction.guild is not None
        cfg = await core.get_guild_cfg(interaction.guild.id) or {}
        cfg["roaster_enabled"] = True
        await core.save_guild_cfg(interaction.guild.id, cfg)
        embed = discord.Embed(
            title="Режим «Обзыватель» включён на сервере",
            description=roaster.ROASTER_WARNING,
            color=core.EMBED_COLOR,
        )
        embed.set_footer(text="Админ сервера может выключить: /admin roaster_disable")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @admin_group.command(
        name="roaster_disable",
        description="Выключить «Обзыватель» на этом сервере",
    )
    async def admin_roaster_disable(interaction: discord.Interaction) -> None:
        ok, err = core._admin_member_ok(interaction)
        if not ok:
            await interaction.response.send_message(err, ephemeral=True)
            return
        assert interaction.guild is not None
        cfg = await core.get_guild_cfg(interaction.guild.id) or {}
        cfg["roaster_enabled"] = False
        await core.save_guild_cfg(interaction.guild.id, cfg)
        await interaction.response.send_message(
            "Обзыватель **выключен** на этом сервере.", ephemeral=True
        )

    # --- bot ---
    bot_group = app_commands.Group(
        name="bot",
        description="Настройка бота на сервере",
    )

    @bot_group.command(
        name="setup",
        description="Создать категорию и форумы аниме-бота на этом сервере",
    )
    @app_commands.describe(
        category_name="Имя категории Discord (необязательно, по умолчанию «Anime Bot»)",
    )
    @app_commands.default_permissions(administrator=True)
    async def bot_setup(
        interaction: discord.Interaction,
        category_name: str = guild_config.DEFAULT_CATEGORY_NAME,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Не удалось определить участника.", ephemeral=True
            )
            return
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "Нужны права **администратора** сервера.", ephemeral=True
            )
            return
        assert interaction.guild is not None
        await interaction.response.defer(ephemeral=True, thinking=True)
        cat_name = (category_name or guild_config.DEFAULT_CATEGORY_NAME).strip()
        cfg, err = await guild_config.setup_guild_channels(
            interaction.guild,
            category_name=cat_name or guild_config.DEFAULT_CATEGORY_NAME,
            commands_embed=core._build_bot_commands_embed(),
        )
        if err:
            await interaction.followup.send(err, ephemeral=True)
            return
        cfg["category_name"] = cat_name or guild_config.DEFAULT_CATEGORY_NAME
        await core.save_guild_cfg(interaction.guild.id, cfg)
        lines = [
            f"Сервер настроен. Категория: **{cfg['category_name']}**",
            f"**Каталог:** <#{cfg.get('forum_channel_id')}>",
            f"**Личные списки:** <#{cfg.get('list_forum_channel_id')}>",
        ]
        itid = cfg.get("bot_info_thread_id")
        if itid:
            lines.append(f"**Справка:** <#{itid}>")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @bot_group.command(name="status", description="Статус настройки бота на этом сервере")
    async def bot_status(interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        cfg = await core.get_guild_cfg(interaction.guild.id)
        await interaction.response.send_message(
            guild_config.format_guild_status(cfg), ephemeral=True
        )

    @bot_group.command(
        name="health",
        description="Диагностика: env, API, каналы, зарегистрированные команды",
    )
    @app_commands.default_permissions(administrator=True)
    async def bot_health(interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        await interaction.response.defer(ephemeral=True, thinking=True)

        lines: list[str] = ["**Диагностика бота**", ""]

        token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
        yummy_app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
        lines.append(
            f"DISCORD_BOT_TOKEN: {'OK' if len(token) >= 50 else 'MISSING/SHORT'}"
        )
        lines.append(
            f"YUMMY_APPLICATION_TOKEN: {'OK' if yummy_app else 'NOT SET'}"
        )

        cfg = await core.get_guild_cfg(interaction.guild.id)
        lines.append("")
        lines.append("**Конфигурация сервера:**")
        if not cfg:
            lines.append("Не настроен — выполните `/bot setup`.")
        else:
            for label, key, want_forum in (
                ("Категория", "category_id", False),
                ("Каталог", "forum_channel_id", True),
                ("Личные списки", "list_forum_channel_id", True),
                ("Справка", "bot_info_thread_id", False),
            ):
                raw = cfg.get(key)
                if not raw:
                    lines.append(f"· {label}: не задан")
                    continue
                try:
                    cid = int(raw)
                except (TypeError, ValueError):
                    lines.append(f"· {label}: неверный ID")
                    continue
                ch = interaction.guild.get_channel(cid) or bot_instance.get_channel(cid)
                if ch is None:
                    try:
                        ch = await bot_instance.fetch_channel(cid)
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        ch = None
                if ch is None:
                    lines.append(f"· {label} ({cid}): NOT FOUND")
                elif want_forum and not isinstance(ch, discord.ForumChannel):
                    lines.append(f"· {label} ({cid}): WRONG TYPE")
                else:
                    lines.append(f"· {label} ({cid}): OK — #{getattr(ch, 'name', '?')}")
            lines.append(
                f"· Обзыватель: {'вкл.' if cfg.get('roaster_enabled') else 'выкл.'}"
            )

        lines.append("")
        lines.append("**YummyAnime API:**")
        try:
            session = await bot_instance.ensure_http_session()
            async with session.get(
                f"{core.BASE}/api/search", params={"q": "naruto"}
            ) as resp:
                lines.append(f"GET /api/search → HTTP {resp.status}")
            if yummy_app:
                async with session.post(
                    "https://api.yani.tv/profile/login",
                    headers=yummy_api.build_yani_headers(yummy_app, None, core.USER_AGENT),
                    json={"login": "healthcheck", "password": "healthcheck"},
                ) as resp:
                    lines.append(
                        f"POST /profile/login → HTTP {resp.status} "
                        f"({'endpoint OK' if resp.status != 404 else 'NOT FOUND'})"
                    )
        except Exception as e:
            lines.append(f"API error: {e}")

        cmds = bot_instance.tree.get_commands()
        cmd_names: list[str] = []
        for c in cmds:
            if isinstance(c, app_commands.Group):
                for sub in c.commands:
                    cmd_names.append(f"/{c.name} {sub.name}")
            else:
                cmd_names.append(f"/{c.name}")
        lines.append("")
        lines.append(f"**Slash-команды:** {len(cmd_names)} шт.")
        preview = ", ".join(sorted(cmd_names)[:20])
        if len(cmd_names) > 20:
            preview += f" … (+{len(cmd_names) - 20})"
        lines.append(preview or "—")

        yummy_task = getattr(bot_instance, "_yummy_poll_task", None)
        roast_task = getattr(bot_instance, "_roaster_poll_task", None)
        lines.append("")
        lines.append("**Фоновые задачи:**")
        lines.append(
            f"· Yummy poll: {'running' if yummy_task and not yummy_task.done() else 'stopped'}"
        )
        lines.append(
            f"· Roaster timer: {'running' if roast_task and not roast_task.done() else 'stopped'}"
        )
        state = await core.read_state_copy()
        poll = (state.get("meta") or {}).get("yummy_poll")
        if isinstance(poll, dict) and poll.get("last_run_utc"):
            lines.append(f"· Последний Yummy poll: {poll.get('last_run_utc')}")

        await interaction.followup.send(
            core._truncate("\n".join(lines), core.DISCORD_CONTENT_LIMIT),
            ephemeral=True,
        )

    # --- owner ---
    owner_group = app_commands.Group(
        name="owner",
        description="Команды владельца бота",
    )

    @owner_group.command(
        name="on",
        description="(Устарело) Снять глобальный kill-switch «Обзывателя»",
    )
    async def owner_roaster_on(interaction: discord.Interaction) -> None:
        if not core.is_bot_owner(interaction.user):
            await interaction.response.send_message(
                "Только **владелец бота** может использовать эту команду.", ephemeral=True
            )
            return
        async with core._state_lock:
            data = core._load_state()
            data.setdefault("meta", {})["roaster_global_enabled"] = True
            core._write_state(data)
        await interaction.response.send_message(
            "Глобальный kill-switch снят. На каждом сервере админы включают "
            "обзывателя локально: `/admin roaster_enable`.",
            ephemeral=True,
        )

    @owner_group.command(
        name="off",
        description="Глобально выключить «Обзыватель» на всех серверах (kill-switch)",
    )
    async def owner_roaster_off(interaction: discord.Interaction) -> None:
        if not core.is_bot_owner(interaction.user):
            await interaction.response.send_message(
                "Только **владелец бота** может использовать эту команду.", ephemeral=True
            )
            return
        async with core._state_lock:
            data = core._load_state()
            data.setdefault("meta", {})["roaster_global_enabled"] = False
            core._write_state(data)
        await interaction.response.send_message(
            "Обзыватель **выключен глобально** (kill-switch). "
            "Снять блокировку: `/owner on`.",
            ephemeral=True,
        )

    # --- roast ---
    roast_group = app_commands.Group(
        name="roast",
        description="Сатирический обзыватель за аниме-вкус",
    )

    @roast_group.command(
        name="member",
        description="Сатирически подколоть участника за его аниме-вкус",
    )
    @app_commands.describe(member="Кого подколоть (по умолчанию — вы)")
    async def roast_member(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        assert interaction.guild is not None
        if not await core.is_roaster_active(interaction.guild.id):
            await interaction.response.send_message(
                "Обзыватель **не активен** на этом сервере. "
                "Админ сервера: `/admin roaster_enable`.",
                ephemeral=True,
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
        if target.bot:
            await interaction.response.send_message(
                "Нельзя обзывать ботов.", ephemeral=True
            )
            return
        state = await core.read_state_copy()
        titles = core.pick_roast_titles(state, target.id)
        msg = roaster.build_roast_message(target.mention, titles)
        await interaction.response.send_message(
            core._truncate(msg, core.DISCORD_CONTENT_LIMIT),
            allowed_mentions=discord.AllowedMentions(users=[target]),
        )

    @bot_instance.tree.command(
        name="adminpanel",
        description="Единая панель админа сервера",
    )
    @app_commands.describe(
        action="Действие администратора",
        member="Участник (нужен для sync_list/yummy_sync_member)",
    )
    @app_commands.choices(
        action=[
            app_commands.Choice(name="setup_channels", value="setup_channels"),
            app_commands.Choice(name="status", value="status"),
            app_commands.Choice(name="forum_scan", value="forum_scan"),
            app_commands.Choice(name="repair_topics", value="repair_topics"),
            app_commands.Choice(name="sync_list", value="sync_list"),
            app_commands.Choice(name="yummy_sync_member", value="yummy_sync_member"),
            app_commands.Choice(name="roaster_enable", value="roaster_enable"),
            app_commands.Choice(name="roaster_disable", value="roaster_disable"),
            app_commands.Choice(name="owner_roaster_on", value="owner_roaster_on"),
            app_commands.Choice(name="owner_roaster_off", value="owner_roaster_off"),
        ]
    )
    async def adminpanel(
        interaction: discord.Interaction,
        action: str,
        member: discord.Member | None = None,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(_GUILD_ONLY_MSG, ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Не удалось определить участника.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "Команда доступна только администратору сервера.",
                ephemeral=True,
            )
            return
        assert interaction.guild is not None

        if action == "status":
            cfg = await core.get_guild_cfg(interaction.guild.id)
            await interaction.response.send_message(guild_config.format_guild_status(cfg), ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        if action == "setup_channels":
            cfg, err = await guild_config.setup_guild_channels(
                interaction.guild, commands_embed=core._build_bot_commands_embed()
            )
            if err:
                await interaction.followup.send(err, ephemeral=True)
                return
            await core.save_guild_cfg(interaction.guild.id, cfg)
            await interaction.followup.send(guild_config.format_guild_status(cfg), ephemeral=True)
            return

        if action == "roaster_enable":
            cfg = await core.get_guild_cfg(interaction.guild.id) or {}
            cfg["roaster_enabled"] = True
            await core.save_guild_cfg(interaction.guild.id, cfg)
            await interaction.followup.send("Обзыватель включён на сервере.", ephemeral=True)
            return
        if action == "roaster_disable":
            cfg = await core.get_guild_cfg(interaction.guild.id) or {}
            cfg["roaster_enabled"] = False
            await core.save_guild_cfg(interaction.guild.id, cfg)
            await interaction.followup.send("Обзыватель выключен на сервере.", ephemeral=True)
            return
        if action == "owner_roaster_on":
            if not core.is_bot_owner(interaction.user):
                await interaction.followup.send("Только владелец бота.", ephemeral=True)
                return
            async with core._state_lock:
                data = core._load_state()
                data.setdefault("meta", {})["roaster_global_enabled"] = True
                core._write_state(data)
            await interaction.followup.send("Глобальный обзыватель включён.", ephemeral=True)
            return
        if action == "owner_roaster_off":
            if not core.is_bot_owner(interaction.user):
                await interaction.followup.send("Только владелец бота.", ephemeral=True)
                return
            async with core._state_lock:
                data = core._load_state()
                data.setdefault("meta", {})["roaster_global_enabled"] = False
                core._write_state(data)
            await interaction.followup.send("Глобальный обзыватель выключен.", ephemeral=True)
            return

        try:
            admin_session = await bot_instance.ensure_http_session()
        except Exception:
            logger.exception("HTTP session (adminpanel)")
            await interaction.followup.send("Сессия HTTP не готова.", ephemeral=True)
            return

        if action == "forum_scan":
            scanned, updated = await core.sync_forum_threads_with_state(
                bot_instance, interaction.guild, admin_session
            )
            await interaction.followup.send(
                f"Скан: веток **{scanned}**, обновлено записей **{updated}**.",
                ephemeral=True,
            )
            return
        if action == "repair_topics":
            forum = await core.resolve_forum_channel(bot_instance, interaction.guild.id)
            if not forum:
                await interaction.followup.send("Канал форума не найден.", ephemeral=True)
                return
            seen: set[int] = set()
            ok_n = err_n = 0
            for th in list(forum.threads):
                if th.id in seen or th.parent_id != forum.id:
                    continue
                seen.add(th.id)
                try:
                    await core.repair_single_forum_thread(bot_instance, forum, th, admin_session)
                    ok_n += 1
                except Exception:
                    err_n += 1
            await interaction.followup.send(
                f"Готово. Веток: **{len(seen)}**, успешно **{ok_n}**, ошибок **{err_n}**.",
                ephemeral=True,
            )
            return
        if action == "sync_list":
            if not member:
                await interaction.followup.send("Укажите участника через `member`.", ephemeral=True)
                return
            scanned, updated = await core.sync_forum_threads_with_state(
                bot_instance, interaction.guild, admin_session
            )
            n, sync_err = await core.sync_personal_list_from_anime_topics(interaction.guild, member.id)
            await interaction.followup.send(
                f"Форум: **{scanned}/{updated}**, список {member.display_name}: **{n}**."
                + (f"\n{sync_err}" if sync_err else ""),
                ephemeral=True,
            )
            return
        if action == "yummy_sync_member":
            if not member:
                await interaction.followup.send("Укажите участника через `member`.", ephemeral=True)
                return
            app = (os.environ.get("YUMMY_APPLICATION_TOKEN") or "").strip()
            if not app:
                await interaction.followup.send("Нет **YUMMY_APPLICATION_TOKEN**.", ephemeral=True)
                return
            r = await core.run_yummy_list_import_for_member(
                interaction.guild,
                member.id,
                list_filter="all",
                max_topics=10,
                session=admin_session,
                app_token=app,
            )
            if r.get("error"):
                await interaction.followup.send(str(r["error"]), ephemeral=True)
                return
            await interaction.followup.send(
                f"{member.display_name}: новых **{r.get('n_new', 0)}**, дописано **{r.get('merge_ops', 0)}**.",
                ephemeral=True,
            )
            return

        await interaction.followup.send("Неизвестное действие.", ephemeral=True)

    # --- register all groups ---
    for grp in (
        anime_group,
        mal_group,
        yummy_group,
        list_group,
        admin_group,
        bot_group,
        owner_group,
        roast_group,
    ):
        bot_instance.tree.add_command(grp)

    # --- legacy slash aliases (совместимость с main) ---
    @bot_instance.tree.command(
        name="animeadd",
        description="[legacy] Добавить аниме — используйте /anime add",
    )
    @app_commands.describe(query="Ссылка en.yummyani.me или название")
    async def legacy_animeadd(interaction: discord.Interaction, query: str) -> None:
        await _anime_add_impl(interaction, query)

    @bot_instance.tree.command(
        name="addanime",
        description="[legacy] Добавить аниме — используйте /anime add",
    )
    @app_commands.describe(query="Ссылка en.yummyani.me или название")
    async def legacy_addanime(interaction: discord.Interaction, query: str) -> None:
        await _anime_add_impl(interaction, query)

    @bot_instance.tree.command(
        name="aa",
        description="[legacy] Короткий алиас — используйте /anime add",
    )
    @app_commands.describe(query="Ссылка en.yummyani.me или название")
    async def legacy_aa(interaction: discord.Interaction, query: str) -> None:
        await _anime_add_impl(interaction, query)

    @bot_instance.tree.command(
        name="rateanime",
        description="[legacy] Оценить аниме — используйте /anime rate",
    )
    async def legacy_rateanime(interaction: discord.Interaction) -> None:
        await _anime_rate_impl(interaction)

    @bot_instance.tree.command(
        name="mylist",
        description="[legacy] Личный список — используйте /list show",
    )
    @app_commands.describe(member="Чей список (если не указано — ваш)")
    async def legacy_mylist(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        await list_show(interaction, member)

    @bot_instance.tree.command(
        name="animelist",
        description="[legacy] Личный список — используйте /list show",
    )
    @app_commands.describe(member="Чей список (если не указано — ваш)")
    async def legacy_animelist(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        await list_show(interaction, member)

    @bot_instance.tree.command(
        name="checkanime",
        description="[legacy] Личный список — используйте /list show",
    )
    @app_commands.describe(member="Чей список (если не указано — ваш)")
    async def legacy_checkanime(
        interaction: discord.Interaction, member: discord.Member | None = None
    ) -> None:
        await list_show(interaction, member)

    @bot_instance.tree.command(
        name="yummybind",
        description="[legacy] Привязать YummyAnime — используйте /yummy bind",
    )
    @app_commands.describe(bearer_token="Bearer-токен (опционально — иначе окно входа)")
    async def legacy_yummybind(
        interaction: discord.Interaction, bearer_token: str | None = None
    ) -> None:
        await yummy_bind(interaction, bearer_token)

    @bot_instance.tree.command(
        name="yummyunbind",
        description="[legacy] Отвязать YummyAnime — используйте /yummy unbind",
    )
    async def legacy_yummyunbind(interaction: discord.Interaction) -> None:
        await yummy_unbind(interaction)

    @bot_instance.tree.command(
        name="syncyummy",
        description="[legacy] Синхронизация Yummy — используйте /yummy sync",
    )
    @app_commands.describe(
        yummy_list="Какой список на Yummy учитывать",
        max_topics="Максимум новых тем за один раз (1–25)",
    )
    @app_commands.choices(
        yummy_list=[
            app_commands.Choice(name="all", value="all"),
            app_commands.Choice(name="watching", value="watching"),
            app_commands.Choice(name="plan_to_watch", value="plan_to_watch"),
            app_commands.Choice(name="completed", value="completed"),
            app_commands.Choice(name="on_hold", value="on_hold"),
            app_commands.Choice(name="dropped", value="dropped"),
        ]
    )
    async def legacy_syncyummy(
        interaction: discord.Interaction,
        yummy_list: str = "all",
        max_topics: app_commands.Range[int, 1, 25] = 15,
    ) -> None:
        await yummy_sync(interaction, yummy_list, max_topics)

    @bot_instance.tree.command(
        name="malbind",
        description="[legacy] Привязать MAL — используйте /mal bind",
    )
    @app_commands.describe(list_url="Ссылка на animelist или профиль MAL")
    async def legacy_malbind(interaction: discord.Interaction, list_url: str) -> None:
        await mal_bind(interaction, list_url)

    @bot_instance.tree.command(
        name="connectmyanimelist",
        description="[legacy] Импорт MAL — используйте /mal import",
    )
    @app_commands.describe(
        mal_status="Какой список MAL импортировать",
        max_topics="Максимум новых тем за один раз (1–25)",
    )
    @app_commands.choices(
        mal_status=[
            app_commands.Choice(name="all", value="all"),
            app_commands.Choice(name="watching", value="watching"),
            app_commands.Choice(name="completed", value="completed"),
            app_commands.Choice(name="on_hold", value="on_hold"),
            app_commands.Choice(name="dropped", value="dropped"),
            app_commands.Choice(name="plan_to_watch", value="plan_to_watch"),
        ]
    )
    async def legacy_connectmyanimelist(
        interaction: discord.Interaction,
        mal_status: str = "all",
        max_topics: app_commands.Range[int, 1, 25] = 25,
    ) -> None:
        await mal_import(interaction, mal_status, max_topics=max_topics)
