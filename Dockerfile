FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py \
     http_client.py \
     guild_config.py \
     personal_display.py \
     register_commands.py \
     roaster.py \
     roaster_automation.py \
     yummy_api.py \
     ./
COPY scripts/ ./scripts/

RUN mkdir -p /app/data

# root — проще на CasaOS с внешним volume ./data (иначе bot:1000 не сможет писать)
VOLUME ["/app/data"]

CMD ["python", "-u", "bot.py"]
