import {
  ButtonInteraction,
  Client,
  Events,
  GatewayIntentBits,
  Interaction,
  REST,
  Routes
} from "discord.js";
import { startBackgroundLoops } from "./background.js";
import { config } from "./config.js";
import {
  handleButtonInteraction,
  handleChatInput,
  handleRateModal,
  handleYummyBindModal,
  slashCommands
} from "./commands.js";

async function registerCommands(applicationId: string): Promise<void> {
  const rest = new REST({ version: "10" }).setToken(config.botToken);
  await rest.put(
    Routes.applicationGuildCommands(
      applicationId,
      config.guildId
    ),
    { body: slashCommands }
  );
}

async function main(): Promise<void> {
  const appId = process.env.DISCORD_APPLICATION_ID?.trim();
  if (!appId) {
    throw new Error("Missing required env var: DISCORD_APPLICATION_ID");
  }
  await registerCommands(appId);

  const client = new Client({
    intents: [GatewayIntentBits.Guilds]
  });

  client.once(Events.ClientReady, (readyClient) => {
    console.log(`YTWUTSHYBOT v0.2.1BETA online as ${readyClient.user.tag}`);
    startBackgroundLoops(client);
  });

  client.on(Events.InteractionCreate, async (interaction: Interaction) => {
    try {
      if (interaction.isChatInputCommand()) {
        await handleChatInput(interaction);
        return;
      }
      if (interaction.isButton()) {
        await handleButtonInteraction(interaction as ButtonInteraction);
        return;
      }
      if (interaction.isModalSubmit() && interaction.customId === "yummy_bind_modal") {
        await interaction.deferReply({ ephemeral: true });
        const text = await handleYummyBindModal({
          userId: interaction.user.id,
          login: interaction.fields.getTextInputValue("login"),
          password: interaction.fields.getTextInputValue("password")
        });
        await interaction.editReply(text);
        return;
      }
      if (interaction.isModalSubmit() && interaction.customId.startsWith("rate_modal:")) {
        await interaction.deferReply({ ephemeral: true });
        const threadId = interaction.customId.split(":")[1] || "";
        const text = await handleRateModal({
          userId: interaction.user.id,
          threadId,
          scoreRaw: interaction.fields.getTextInputValue("score")
        });
        await interaction.editReply(text);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : "Unknown error";
      if (interaction.isRepliable()) {
        if (interaction.deferred || interaction.replied) {
          await interaction.followUp({ content: msg, ephemeral: true });
        } else {
          await interaction.reply({ content: msg, ephemeral: true });
        }
      }
    }
  });

  await client.login(config.botToken);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
