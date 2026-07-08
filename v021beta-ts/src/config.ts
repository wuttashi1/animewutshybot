import dotenv from "dotenv";

dotenv.config();

function readRequired(key: string): string {
  const value = process.env[key]?.trim();
  if (!value) {
    throw new Error(`Missing required env var: ${key}`);
  }
  return value;
}

export const config = {
  botToken: readRequired("DISCORD_BOT_TOKEN"),
  guildId: readRequired("DISCORD_GUILD_ID"),
  yummyApplicationToken: readRequired("YUMMY_APPLICATION_TOKEN"),
  userAgent: "YTWUTSHYBOT/0.2.1BETA (TypeScript port)"
};
