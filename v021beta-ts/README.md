# YTWUTSHYBOT v0.2.1BETA (TypeScript Port)

This is a new beta branch of the bot migrated from Python to TypeScript (`discord.js`).

## What is included in 0.2.1BETA

- New TypeScript runtime (`discord.js` + `ts-node/tsx`)
- Slash command registration scoped to one guild
- `/ping`
- `/anime add`, `/anime rate`
- `/list show`
- `/mal bind`, `/mal import`
- `/yummy bind|unbind|sync`
- `/adminpanel` actions:
  - `setup_channels`
  - `status`
  - `forum_scan`
  - `repair_topics`
  - `sync_list`
  - `personal_rebuild`
  - `yummy_sync_member`
- JSON state storage for guild config, topics, ratings, personal lists and bindings in `data/v021beta-state.json`

## Environment

Copy `.env.example` and set:

- `DISCORD_BOT_TOKEN`
- `DISCORD_APPLICATION_ID`
- `DISCORD_GUILD_ID`
- `YUMMY_APPLICATION_TOKEN`

## Run

```bash
cd v021beta-ts
npm install
npm run dev
```

## Notes

This beta now includes the core user/admin command surface on TypeScript. Remaining parity work is mainly advanced UI panels, deeper thread metadata maintenance, and background automation loops.
