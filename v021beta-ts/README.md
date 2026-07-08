# YTWUTSHYBOT v0.2.1BETA (TypeScript Port)

This is a new beta branch of the bot migrated from Python to TypeScript (`discord.js`).

## What is included in 0.2.1BETA

- New TypeScript runtime (`discord.js` + `ts-node/tsx`)
- Slash command registration scoped to one guild
- `/ping`
- `/yummy bind` with modal login/password flow
- `/adminpanel` (admin-only gate)
- JSON state storage for Yummy bindings in `data/v021beta-state.json`

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

This is a migration baseline. Legacy Python features (forum automation, MAL sync, roaster automation, etc.) should be moved module-by-module on top of this beta.
