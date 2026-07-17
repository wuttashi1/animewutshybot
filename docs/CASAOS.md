# Установка бота на CasaOS + доступ к приватному GitHub

Бот — это Discord-приложение без веб-порта. На CasaOS его ставят через **Docker Compose** (Custom App / Docker Compose).

---

## 1. Токен GitHub для приватного репозитория

Нужен **Personal Access Token (classic)** или **fine-grained token** с правом читать репозиторий.

### Вариант A — Classic PAT (проще)

1. Откройте GitHub → аватар → **Settings**.
2. Слева внизу: **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. **Generate new token** → **Generate new token (classic)**.
4. Note: например `casaos-clone-animebot`.
5. Expiration: на ваш вкус (90 дней / No expiration).
6. Права (scopes):
   - ✅ **`repo`** — полный доступ к приватным репозиториям (для clone/pull).
7. **Generate token** → **сразу скопируйте** токен (`ghp_...`). Повторно его не показать.

### Вариант B — Fine-grained token (безопаснее)

1. **Developer settings** → **Personal access tokens** → **Fine-grained tokens**.
2. **Generate new token**.
3. Resource owner — ваш аккаунт (или организация).
4. Repository access → **Only select repositories** → выберите `animewutshybot`.
5. Permissions → Repository permissions:
   - **Contents**: **Read-only**
   - **Metadata**: Read-only (ставится само)
6. Generate → скопируйте токен (`github_pat_...`).

> Токен = пароль. Не коммитьте его в репозиторий и не кидайте в чаты.

---

## 2. Склонировать приватный репозиторий на сервер CasaOS

Подключитесь по SSH к машине с CasaOS (или откройте Terminal в CasaOS).

```bash
# Папка для приложений (пример)
mkdir -p ~/apps
cd ~/apps

# HTTPS + токен (подставьте USER и TOKEN)
git clone https://ВАШ_GITHUB_ЛОГИН:ВАШ_ТОКЕН@github.com/wuttashi1/animewutshybot.git

cd animewutshybot
# Если нужен конкретный бранч с Docker:
# git checkout cursor/docker-casaos-c91d
# или после мержа в main — просто main
```

Альтернатива без логина в URL (токен как пароль при запросе):

```bash
git clone https://github.com/wuttashi1/animewutshybot.git
# Username: ваш логин GitHub
# Password: ВСТАВЬТЕ ТОКЕН (не пароль от аккаунта)
```

Сохранить credentials, чтобы не вводить каждый раз:

```bash
git config --global credential.helper store
# следующий clone/pull спросит логин/токен один раз
```

---

## 3. Настроить `.env`

```bash
cd ~/apps/animewutshybot
cp .env.example .env
nano .env   # или любой редактор
```

Минимум:

```env
DISCORD_BOT_TOKEN=ваш_токен_бота_из_Discord_Developer_Portal
DISCORD_GUILD_ID=id_вашего_сервера
YUMMY_APPLICATION_TOKEN=токен_приложения_yani
# опционально
DISCORD_BOT_OWNER_ID=ваш_discord_user_id
```

Токен Discord-бота: [Discord Developer Portal](https://discord.com/developers/applications) → Application → Bot → Reset/Copy Token.

---

## 4. Запуск через Docker Compose (рекомендуется)

На сервере с Docker (CasaOS уже ставит Docker):

```bash
cd ~/apps/animewutshybot
mkdir -p data
docker compose up -d --build
docker compose logs -f
```

Проверка:

```bash
docker compose ps
# контейнер animewutshybot должен быть Up
```

Остановка / обновление:

```bash
docker compose pull   # если используете готовый image
git pull              # обновить код из GitHub
docker compose up -d --build
```

Данные бота лежат в `./data` (том `./data:/app/data`) — каталог аниме, списки, привязки не пропадут при пересборке.

---

## 5. Установка через UI CasaOS (Custom App)

1. CasaOS → **App Store** → **Custom Install** / **Install a customized app** (иконка `+` / Docker Compose).
2. Режим **Docker Compose**.
3. Вставьте содержимое `docker-compose.yml` из репозитория.
4. **Важно:** CasaOS Custom Install часто **не умеет `build: .`** из локальной папки.
   - Либо сначала соберите image вручную на хосте:
     ```bash
     cd ~/apps/animewutshybot
     docker compose build
     ```
     и в compose оставьте `image: animewutshybot:latest` (строка `build: .` можно убрать в UI).
   - Либо запускайте только из терминала: `docker compose up -d --build`.
5. В UI добавьте переменные окружения (`DISCORD_BOT_TOKEN` и др.) или смонтируйте `.env`.
6. Volume: хост-путь к `data`, например `/DATA/AppData/animewutshybot/data` → `/app/data`.
7. Install / Save.

Пример volume для CasaOS AppData:

```yaml
volumes:
  - /DATA/AppData/animewutshybot/data:/app/data
```

---

## 6. После запуска

1. В Discord на сервере: **`/bot setup`** (админ) — создаст категорию и форумы.
2. Проверка: **`/bot status`**, **`/bot health`**.
3. Добавление аниме: **`/anime add`**.

Каждый Discord-сервер — своя «база» (каталог и списки изолированы).

---

## 7. Частые проблемы

| Проблема | Что сделать |
|----------|-------------|
| `Authentication failed` при `git clone` | Токен просрочен / нет scope `repo` / неверный логин |
| Бот оффлайн | `docker compose logs` — нет `DISCORD_BOT_TOKEN` или Intent не включены |
| Slash-команд нет | Укажите `DISCORD_GUILD_ID`, подождите 1–2 минуты, перелогиньтесь в Discord |
| Состояние сбросилось | Том `data` не был смонтирован — проверьте volumes |
| CasaOS не билдит | Соберите image на хосте (`docker compose build`), в UI только `image:` |

---

## 8. Безопасность

- Не коммитьте `.env` и токены.
- Для GitHub лучше fine-grained token только на один репозиторий.
- При утечке токена — сразу **Revoke** в GitHub Settings → Tokens.
