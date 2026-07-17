# Установка бота с GitHub на CasaOS

Репозиторий приватный: для `git clone` / сборки нужен GitHub PAT.

---

## 1. Токен GitHub (чтобы тянуть приватный репо)

1. GitHub → **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**
2. **Generate new token (classic)**
3. Scope: ✅ **`repo`**
4. Скопируйте `ghp_...`

---

## 2. Рекомендуемый способ: clone с GitHub → compose

```bash
mkdir -p /DATA/AppData && cd /DATA/AppData

git clone https://ВАШ_ЛОГИН:ghp_ВАШ_ТОКЕН@github.com/wuttashi1/animewutshybot.git
cd animewutshybot
git checkout cursor/docker-casaos-c91d

mkdir -p data
docker compose up -d --build
docker compose logs -f
```

Переменные бота уже в `docker-compose.yml` — отдельный `.env` не нужен.

### Обновление с GitHub

```bash
cd /DATA/AppData/animewutshybot
git pull
docker compose up -d --build
```

---

## 3. Альтернатива: CasaOS UI без clone

1. CasaOS → **App Store** → **Custom Install** → Docker Compose
2. Вставьте `docker-compose.casaos.yml` из репозитория
3. В `build.context` замените `GITHUB_TOKEN` на ваш PAT
4. Volume уже: `/DATA/AppData/animewutshybot/data:/app/data`
5. Install

---

## 4. После запуска

В Discord: **`/bot setup`** → **`/bot status`** → **`/anime add`**

---

## 5. Если не собирается с приватного GitHub

На хосте один раз:

```bash
git config --global credential.helper store
git ls-remote https://ВАШ_ЛОГИН:ghp_ТОКЕН@github.com/wuttashi1/animewutshybot.git
```

Либо в URL compose:  
`https://ghp_ТОКЕН@github.com/wuttashi1/animewutshybot.git#cursor/docker-casaos-c91d`
