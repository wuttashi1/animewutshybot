# Docker / CasaOS

Скачайте репозиторий на сервер, скопируйте `.env.example` в `/DATA/AppData/animewutshybot/.env` и заполните собственные ключи. Ограничьте доступ: `chmod 600 /DATA/AppData/animewutshybot/.env`.

Из каталога с исходниками:

```bash
docker compose -f docker-compose.casaos.yml up -d --build
docker logs --tail 50 animewutshybot
```

Этот Compose собирается из checkout. Он не скачивает код с PAT при каждом запуске и не содержит токенов. Для Custom Install сначала соберите `animewutshybot:local` на сервере, затем уберите `build` из импортируемого Compose.

Перед заменой старого контейнера сохраните его настройки и каталог данных. Не удаляйте `.installed` у старой установки: старый bootstrap может перезаписать рабочие файлы. Новая сборка не использует этот маркер.

## Обновление

1. Остановите текущий контейнер и сохраните копию `data/` и `.env` вне репозитория.
2. Получите проверенный commit и соберите образ.
3. Запустите проверку: `docker compose run --rm anime-bot python scripts/health_check.py`.
4. При успешной проверке запустите сервис. `401` от Discord требует замены Bot Token.

При откате используйте предыдущий образ и сохранённую копию данных. Не выводите `docker inspect` или `docker compose config` в публичные журналы: они могут содержать ключи.
