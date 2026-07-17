# Установка бота на CasaOS с GitHub

Ошибка `No such image: animewutshybot:latest` значит: CasaOS пытается
**скачать** образ, а не собрать его. Локального тега на Docker Hub нет.

Ниже два рабочих пути.

---

## Способ 1 — SSH / Terminal (работает сразу)

```bash
cd /DATA/AppData
git clone https://ВАШ_ЛОГИН:ghp_ТОКЕН@github.com/wuttashi1/animewutshybot.git
cd animewutshybot
git checkout cursor/docker-casaos-c91d

mkdir -p data
docker compose up -d --build
docker compose logs -f
```

Обновление:

```bash
cd /DATA/AppData/animewutshybot
git pull
docker compose up -d --build
```

---

## Способ 2 — CasaOS UI + образ из GHCR

CasaOS Custom Install умеет **pull**, а не `docker build`. Поэтому образ
собирается в GitHub Actions и лежит в:

`ghcr.io/wuttashi1/animewutshybot:latest`

### 1) Дождаться сборки образа

1. Откройте репозиторий → вкладка **Actions**
2. Workflow **Build and push Docker image** должен быть зелёным
3. Packages → `animewutshybot` (или `ghcr.io/wuttashi1/animewutshybot`)

Если пакет **Private**, на CasaOS один раз:

```bash
echo ghp_ВАШ_ТОКЕН | docker login ghcr.io -u ВАШ_ЛОГИН --password-stdin
```

PAT: scopes **`read:packages`** и **`repo`**.

Чтобы пакет был публичным (тогда login не нужен):  
GitHub → Packages → animewutshybot → Package settings → Change visibility → Public.

### 2) Установка в CasaOS

1. App Store → Custom Install → Docker Compose  
2. Вставьте содержимое **`docker-compose.casaos.yml`**  
3. Install  

Compose тянет `ghcr.io/wuttashi1/animewutshybot:latest` — не `animewutshybot:latest`.

---

## Токен GitHub для приватного репо

1. Settings → Developer settings → Personal access tokens → Tokens (classic)  
2. Generate → scope ✅ **`repo`** (для clone) и ✅ **`read:packages`** (для GHCR)  
3. Скопируйте `ghp_...`

---

## После запуска

В Discord: **`/bot setup`** → **`/anime add`**
