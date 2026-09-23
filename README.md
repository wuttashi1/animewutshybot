<div align="center">

# Anime Wutshy Bot

Discord-бот для аниме-сообщества: карточки аниме, форумные темы, личные списки и интеграция с YummyAnime.

[Правила разработки](CONTRIBUTING.md) · [Ветки](https://github.com/wuttashi1/animewutshybot/branches)

</div>

---

## Возможности

- Карточки аниме с данными внешних API.
- Работа с форумными темами и личными аниме-списками.
- Панель администратора, обновление тем и поиск дубликатов.
- Интеграция с YummyAnime и получение данных через Jikan.

## Запуск

```bash
python -m venv .venv
# Активируйте .venv для вашей оболочки
python -m pip install -r requirements.txt
python bot.py
```

Перед запуском настройте локальный `.env` по `.env.example`. Бот читает `DISCORD_BOT_TOKEN`; набор параметров интеграций смотрите в примере конфигурации. Настройте приложение и разрешения бота в Discord Developer Portal.

## Навигация

- `bot.py` — команды, панели и работа с Discord.
- `yummy_api.py` — интеграция с YummyAnime.
- Рабочие версии и эксперименты доступны в списке веток репозитория; `main` остаётся основной веткой.

## Разработка

Соглашения по веткам и изменениям: [CONTRIBUTING.md](CONTRIBUTING.md).
