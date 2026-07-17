# CasaOS — установка через Custom Install

Используйте файл **`docker-compose.casaos.yml`** (формат как у FunPay Cardinal):
базовый образ `python:3.12-slim-bookworm`, при первом старте `git clone` с GitHub.

## 1. GitHub PAT

1. GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate → scope ✅ **`repo`**
3. Скопируйте `ghp_...` или `github_pat_...`

## 2. Установка в CasaOS

1. Откройте `docker-compose.casaos.yml`
2. Замените **`GITHUB_PAT`** на свой токен
3. CasaOS → App Store → Custom Install → Docker Compose
4. Вставьте YAML → Install

Первый запуск дольше (clone + `pip install`). Дальше контейнер просто стартует `bot.py`.

## 3. Обновить код с GitHub

```bash
rm /DATA/AppData/animewutshybot/.installed
docker restart animewutshybot
```

Данные бота (`data/mal_state.json` и т.д.) останутся в `/DATA/AppData/animewutshybot/data`.

## 4. После запуска

В Discord: **`/bot setup`** → **`/anime add`**
