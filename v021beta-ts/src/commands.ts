import {
  ActionRowBuilder,
  ChatInputCommandInteraction,
  ModalBuilder,
  SlashCommandBuilder,
  TextInputBuilder,
  TextInputStyle
} from "discord.js";
import { config } from "./config.js";
import { saveYummyBinding } from "./storage.js";
import { getYummyProfile, loginYummyByPassword } from "./yummyApi.js";

export const slashCommands = [
  new SlashCommandBuilder()
    .setName("ping")
    .setDescription("Проверка, что бот онлайн"),
  new SlashCommandBuilder()
    .setName("yummy")
    .setDescription("YummyAnime привязка")
    .addSubcommand((sub) => sub.setName("bind").setDescription("Открыть окно авторизации")),
  new SlashCommandBuilder()
    .setName("adminpanel")
    .setDescription("Базовая админ-панель")
    .addStringOption((opt) =>
      opt
        .setName("action")
        .setDescription("Действие")
        .setRequired(true)
        .addChoices(
          { name: "status", value: "status" },
          { name: "setup_channels", value: "setup_channels" }
        )
    )
].map((c) => c.toJSON());

export async function handleChatInput(interaction: ChatInputCommandInteraction): Promise<void> {
  if (interaction.commandName === "ping") {
    await interaction.reply({ content: "Pong! v0.2.1BETA", ephemeral: true });
    return;
  }

  if (interaction.commandName === "yummy" && interaction.options.getSubcommand() === "bind") {
    const modal = new ModalBuilder().setCustomId("yummy_bind_modal").setTitle("YummyAnime вход");
    const loginInput = new TextInputBuilder()
      .setCustomId("login")
      .setLabel("Логин или почта")
      .setPlaceholder("Введите логин")
      .setRequired(true)
      .setStyle(TextInputStyle.Short);
    const passwordInput = new TextInputBuilder()
      .setCustomId("password")
      .setLabel("Пароль")
      .setPlaceholder("Введите пароль")
      .setRequired(true)
      .setStyle(TextInputStyle.Short);
    modal.addComponents(
      new ActionRowBuilder<TextInputBuilder>().addComponents(loginInput),
      new ActionRowBuilder<TextInputBuilder>().addComponents(passwordInput)
    );
    await interaction.showModal(modal);
    return;
  }

  if (interaction.commandName === "adminpanel") {
    const member = interaction.member;
    const isAdmin =
      member && "permissions" in member && typeof member.permissions !== "string"
        ? member.permissions.has("Administrator")
        : false;
    if (!isAdmin) {
      await interaction.reply({
        content: "Команда доступна только администраторам сервера.",
        ephemeral: true
      });
      return;
    }
    const action = interaction.options.getString("action", true);
    await interaction.reply({
      content: `adminpanel action: ${action} (v0.2.1BETA TypeScript)`,
      ephemeral: true
    });
  }
}

export async function handleYummyBindModal(modal: {
  userId: string;
  login: string;
  password: string;
}): Promise<string> {
  const token = await loginYummyByPassword({
    login: modal.login,
    password: modal.password,
    appToken: config.yummyApplicationToken,
    userAgent: config.userAgent
  });
  const profile = await getYummyProfile({
    accessToken: token,
    appToken: config.yummyApplicationToken,
    userAgent: config.userAgent
  });
  await saveYummyBinding({
    userId: modal.userId,
    yummyUserId: profile.id,
    nickname: profile.nickname,
    accessToken: token,
    updatedAt: new Date().toISOString()
  });
  return `YummyAnime привязан: ${profile.nickname || profile.id}`;
}
