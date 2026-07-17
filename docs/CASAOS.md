# CasaOS Custom Install — формат как FunPay Cardinal

## Если контейнер сразу выключается

```bash
docker logs animewutshybot
# или
docker logs --tail 100 animewutshybot
```

Частые причины:
1. **Не заменён `REPLACE_ME_GITHUB_PAT`** на ваш GitHub token → clone падает
2. **Кривой/отозванный `DISCORD_BOT_TOKEN`** → LoginFailure
3. Битая первая установка → сброс:
   ```bash
   rm -f /DATA/AppData/animewutshybot/.installed
   # при полном сбросе кода (данные бота в data/ сохраните при необходимости):
   # rm -rf /DATA/AppData/animewutshybot/*
   docker restart animewutshybot
   ```

## Установка

1. В `docker-compose.casaos.yml` замените **`REPLACE_ME_GITHUB_PAT`** на PAT (`repo`)
2. CasaOS → Custom Install → вставьте YAML
3. Первый старт ~1–2 мин (apt + git clone + pip)
4. `docker logs -f animewutshybot` → ждите `Бот онлайн`

## Обновить код с GitHub

```bash
rm -f /DATA/AppData/animewutshybot/.installed
docker restart animewutshybot
```
