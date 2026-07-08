"""Сатирический «обзыватель» за аниме-вкус."""

from __future__ import annotations

import random
import re
from typing import Any

ROAST_TEMPLATES: tuple[str, ...] = (
    "{mention}, ты реально смотришь **{title}**? Это же peak «я смотрю всё подряд».",
    "{mention}, **{title}** в твоём списке — смелый выбор. Смелый, как пить чай без сахара в -30.",
    "Кто-то смотрел **{title}**, и этот кто-то — {mention}. Мы не осуждаем. Почти.",
    "{mention}, **{title}**? У тебя вкус как у человека, который выбирает персонажа по цвету волос.",
    "Статистика не врёт: {mention} и **{title}** — связь, которую психологи изучают.",
    "{mention}, после **{title}** тебе уже ничего не страшно. Кроме рекомендаций друзей.",
    "**{title}** + {mention} = комбо, от которого критики MAL плачут в подушку.",
    "{mention}, **{title}** — это не аниме, это испытание. Ты его прошёл. Зачем?",
    "Если бы плохой вкус был олимпийским видом, {mention} взял бы золото за **{title}**.",
    "{mention}, **{title}** в твоём списке — red flag размером с постер IMAX.",
    "Родители: «Смотри что-нибудь полезное». {mention}: *добавляет **{title}***.",
    "{mention}, **{title}** — proof that curiosity beats common sense.",
    "Твой список кричит «{title}», {mention}. Мы слышим. Discord слышит. Бог слышит.",
    "{mention}, **{title}** — аниме для тех, кто нажал «случайная серия» и не остановился.",
    "Вкус {mention} на **{title}** — как pineapple на пицце: спорно, но легально.",
    "{mention}, **{title}**? Это же классика… если ты живёшь в параллельной вселенной.",
    "Когда {mention} говорит «рекомендую **{title}**», комната становится тише.",
    "{mention}, **{title}** в списке — это не ошибка. Это lifestyle.",
    "Археологи будущего найдут твой список, {mention}, увидят **{title}** и напишут диссертацию.",
    "{mention}, **{title}** — must watch… если must = «надо же попробовать всё».",
    "У {mention} и **{title}** love story: он не понимает, оно не объясняет.",
    "{mention}, ты смотрел **{title}** и всё ещё здесь? Респект выносливости.",
    "**{title}** — выбор {mention}. Выбор смелый. Выбор… такой.",
    "{mention}, после **{title}** твой стандарт «норм аниме» сместился в unknown.",
    "Если бы {mention} был рецензентом, **{title}** получило бы «5/10, но я досмотрел».",
    "{mention}, **{title}** — аниме, которое смотрят, когда закончились все хорошие.",
    "Вселенная: миллиарды звёзд. {mention}: *смотрит **{title}***.",
    "{mention}, **{title}** в топе твоих просмотров — plot twist без plot.",
    "Когда {mention} добавляет **{title}**, алгоритмы рекомендаций уходят в отпуск.",
    "{mention}, **{title}** — это не filler, это whole season of questionable decisions.",
    "Твой вкус, {mention}, как **{title}** — не для всех. И, честно, иногда не для тебя.",
    "{mention}, **{title}**? Я бы спросил «зачем», но боюсь ответа.",
    "Hall of fame плохих идей: {mention}, **{title}**, и кнопка «ещё сезон».",
    "{mention}, **{title}** — аниме, после которого «ещё одну серию» звучит как угроза.",
    "Если смеяться над собой — искусство, {mention} мастер благодаря **{title}**.",
    "{mention}, **{title}** в списке — как socks with sandals: works for someone.",
    "Критик внутри {mention} после **{title}**: «мы обсудим это на исповеди».",
    "{mention}, **{title}** — must-see для изучения границ терпения.",
    "Ты и **{title}**, {mention} — история о том, что autoplay опасен.",
    "{mention}, **{title}**? Это же… ну… *аниме*. Технически.",
    "Когда {mention} рекомендует **{title}**, друзья включают режим «слышу, не слушаю».",
    "{mention}, **{title}** — proof that completionists deserve therapy.",
    "В parallel universe {mention} не смотрел **{title}**. Там он счастливее. Наверное.",
    "{mention}, **{title}** в истории просмотров — как typos в резюме: заметят все.",
    "Твой список: «я ценю разнообразие». Также твой список: **{title}**, {mention}.",
    "{mention}, **{title}** — аниме для людей, которые нажали «продолжить» из принципа.",
    "Если бы taste был currency, {mention} потратил бы всё на **{title}**.",
    "{mention}, **{title}**? Bold. Unhinged. Iconic. В плохом смысле все три.",
    "Netflix: «Are you still watching?» {mention} + **{title}**: «Unfortunately, yes.»",
    "{mention}, **{title}** — не worst, просто… aggressively mid.",
)

GENERIC_ROASTS: tuple[str, ...] = (
    "{mention}, твой список аниме — как playlist на вписке: все удивлены, никто не признаётся.",
    "{mention}, у тебя вкус как у человека, который читает описание и всё равно жмёт play.",
    "Если бы плохие решения были жанром, {mention} был бы главным героем.",
    "{mention}, твой MAL/Yummy — museum of «ну, раз уж начал».",
    "{mention}, ты смотришь так много, что статистически *что-то* обязано быть спорным.",
)

OWNER_USERNAME = "wutshy"
ROASTER_WARNING = (
    "⚠️ **Режим «Обзыватель»**\n\n"
    "Бот будет **сатирически** подкалывать участников за их аниме-вкус "
    "(с упоминанием тайтлов из их списка).\n\n"
    "**Автоматически:** с небольшим шансом ответы на сообщения в чате; "
    "раз в **1–6 часов** — случайный подкол кому-то на сервере "
    "(без команды `/roast`).\n\n"
    "Это **шутки**, не harassment — но реакция людей разная.\n"
    "**Вся ответственность** за включение на сервере лежит на **администрации сервера**.\n\n"
    "Глобально функцию включает только владелец бота **@wutshy**.\n"
    "Продолжая, вы подтверждаете, что понимаете риски."
)


def build_roast_message(
    mention: str,
    titles: list[str],
) -> str:
    if titles:
        title = random.choice(titles)
        template = random.choice(ROAST_TEMPLATES)
        msg = template.format(mention=mention, title=_truncate_title(title))
    else:
        msg = random.choice(GENERIC_ROASTS).format(mention=mention)
    return msg


def _truncate_title(title: str, limit: int = 80) -> str:
    t = re.sub(r"\s+", " ", title.strip())
    if len(t) <= limit:
        return t
    return t[: limit - 1].rstrip() + "…"
