<div align="center">

# Anime Wutshy Bot

Discord anime community bot with anime cards, forum topics, personal lists and YummyAnime integration.

[Contributing](CONTRIBUTING.md) · [Branches](https://github.com/wuttashi1/animewutshybot/branches)

</div>

---

## Features

- Anime cards backed by external APIs.
- Forum topics and personal anime lists.
- Administrative panels, topic updates and duplicate detection.
- YummyAnime integration and Jikan data fetching.

## Quick start

Create and activate a Python virtual environment, then:

```bash
python -m pip install -r requirements.txt
python bot.py
```

Before starting, configure a local `.env` using `.env.example`. The bot reads `DISCORD_BOT_TOKEN`; see the example configuration for integration settings. Configure the application and bot permissions in the Discord Developer Portal.

## Project layout

- `bot.py` — Discord commands, panels and forum workflows.
- `yummy_api.py` — YummyAnime integration.
- Development versions are available in the repository's branch list. `main` remains the default branch.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch and contribution guidelines.
