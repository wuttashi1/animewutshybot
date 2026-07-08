import {
  ActionRowBuilder,
  ChannelType,
  ChatInputCommandInteraction,
  EmbedBuilder,
  ForumChannel,
  Guild,
  ModalBuilder,
  SlashCommandBuilder,
  TextInputBuilder,
  TextInputStyle
} from "discord.js";
import { fetchAnimeCard, searchYummySlug, slugFromText } from "./catalogApi.js";
import { config } from "./config.js";
import {
  GuildConfig,
  getAnimeTopic,
  deleteYummyBinding,
  getGuildConfig,
  getPersonalList,
  getYummyBinding,
  saveAnimeTopic,
  saveGuildConfig,
  saveMalBinding,
  saveYummyBinding,
  upsertPersonalListItem
} from "./storage.js";
import {
  filterYummyEntries,
  getYummyProfile,
  getYummyUserLists,
  loginYummyByPassword
} from "./yummyApi.js";

export const slashCommands = [
  new SlashCommandBuilder()
    .setName("ping")
    .setDescription("Проверка, что бот онлайн"),
  new SlashCommandBuilder()
    .setName("anime")
    .setDescription("Каталог аниме")
    .addSubcommand((sub) =>
      sub
        .setName("add")
        .setDescription("Добавить аниме в форум и личный список")
        .addStringOption((opt) =>
          opt.setName("query").setDescription("Ссылка en.yummyani.me или название").setRequired(true)
        )
    )
    .addSubcommand((sub) => sub.setName("rate").setDescription("Оценка в теме (beta placeholder)")),
  new SlashCommandBuilder()
    .setName("list")
    .setDescription("Личный список")
    .addSubcommand((sub) => sub.setName("show").setDescription("Показать ваш список")),
  new SlashCommandBuilder()
    .setName("mal")
    .setDescription("MyAnimeList")
    .addSubcommand((sub) =>
      sub
        .setName("bind")
        .setDescription("Привязать MAL профиль")
        .addStringOption((opt) => opt.setName("url").setDescription("Ссылка MAL").setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName("import")
        .setDescription("Импорт из MAL (beta)")
        .addStringOption((opt) =>
          opt
            .setName("status")
            .setDescription("Фильтр списка")
            .setRequired(true)
            .addChoices(
              { name: "all", value: "all" },
              { name: "watching", value: "watching" },
              { name: "completed", value: "completed" },
              { name: "on_hold", value: "on_hold" },
              { name: "dropped", value: "dropped" },
              { name: "plan_to_watch", value: "plan_to_watch" }
            )
        )
    ),
  new SlashCommandBuilder()
    .setName("yummy")
    .setDescription("YummyAnime привязка")
    .addSubcommand((sub) => sub.setName("bind").setDescription("Открыть окно авторизации"))
    .addSubcommand((sub) => sub.setName("unbind").setDescription("Отвязать Yummy аккаунт"))
    .addSubcommand((sub) =>
      sub
        .setName("sync")
        .setDescription("Синхронизация из Yummy списков")
        .addStringOption((opt) =>
          opt
            .setName("list")
            .setDescription("Какой список синхронизировать")
            .setRequired(true)
            .addChoices(
              { name: "all", value: "all" },
              { name: "watching", value: "watching" },
              { name: "plan_to_watch", value: "plan_to_watch" },
              { name: "completed", value: "completed" },
              { name: "on_hold", value: "on_hold" },
              { name: "dropped", value: "dropped" }
            )
        )
        .addIntegerOption((opt) =>
          opt.setName("max_topics").setDescription("Лимит новых тем").setMinValue(1).setMaxValue(25)
        )
    ),
  new SlashCommandBuilder()
    .setName("adminpanel")
    .setDescription("Единая админ-панель")
    .addStringOption((opt) =>
      opt
        .setName("action")
        .setDescription("Действие")
        .setRequired(true)
        .addChoices(
          { name: "status", value: "status" },
          { name: "setup_channels", value: "setup_channels" },
          { name: "forum_scan", value: "forum_scan" },
          { name: "sync_list", value: "sync_list" }
        )
    )
    .addUserOption((opt) =>
      opt.setName("member").setDescription("Пользователь для sync_list")
    )
].map((c) => c.toJSON());

export async function handleChatInput(interaction: ChatInputCommandInteraction): Promise<void> {
  if (interaction.commandName === "ping") {
    await interaction.reply({ content: "Pong! v0.2.1BETA", ephemeral: true });
    return;
  }

  if (interaction.commandName === "anime") {
    const sub = interaction.options.getSubcommand();
    if (sub === "rate") {
      await interaction.reply({
        content: "Оценка в теме будет перенесена в следующем шаге beta.",
        ephemeral: true
      });
      return;
    }
    if (sub === "add") {
      if (!interaction.guild) {
        await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
        return;
      }
      const query = interaction.options.getString("query", true);
      await interaction.deferReply({ ephemeral: true });
      const cfg = await getGuildConfig(interaction.guildId!);
      const forumId = cfg?.forumChannelId;
      if (!forumId) {
        await interaction.editReply("Форум не настроен. Выполните `/adminpanel action:setup_channels`.");
        return;
      }
      const ch = await interaction.guild.channels.fetch(forumId);
      if (!(ch instanceof ForumChannel)) {
        await interaction.editReply("Канал форума недоступен.");
        return;
      }
      let slug = slugFromText(query);
      if (!slug) {
        slug = await searchYummySlug(query);
      }
      if (!slug) {
        await interaction.editReply("Не найдено аниме по запросу.");
        return;
      }
      const existing = await getAnimeTopic(slug);
      if (existing?.threadId) {
        await upsertPersonalListItem(interaction.user.id, {
          key: slug,
          title: existing.title,
          url: existing.pageUrl,
          addedAt: new Date().toISOString()
        });
        await interaction.editReply(`Тема уже существует: <#${existing.threadId}>. Добавлено в ваш список.`);
        return;
      }
      const card = await fetchAnimeCard(slug);
      if (!card) {
        await interaction.editReply("Карточка аниме не получена.");
        return;
      }
      const thread = await ch.threads.create({
        name: card.title.slice(0, 100),
        message: {
          content: `${card.title}\n${card.pageUrl}\nДобавил: <@${interaction.user.id}>`
        }
      });
      await saveAnimeTopic(card.slug, {
        threadId: thread.id,
        title: card.title,
        pageUrl: card.pageUrl,
        yummySlug: card.slug,
        adders: [interaction.user.id]
      });
      await upsertPersonalListItem(interaction.user.id, {
        key: card.slug,
        title: card.title,
        url: card.pageUrl,
        addedAt: new Date().toISOString()
      });
      await interaction.editReply(`Создана тема: ${thread.url}`);
      return;
    }
  }

  if (interaction.commandName === "list" && interaction.options.getSubcommand() === "show") {
    const items = await getPersonalList(interaction.user.id);
    if (!items.length) {
      await interaction.reply({ content: "Ваш список пока пуст.", ephemeral: true });
      return;
    }
    const lines = items.slice(0, 30).map((x) => `• [${x.title}](${x.url})`);
    const embed = new EmbedBuilder()
      .setTitle(`Личный список — ${interaction.user.displayName}`)
      .setDescription(lines.join("\n"));
    await interaction.reply({ embeds: [embed], ephemeral: true });
    return;
  }

  if (interaction.commandName === "mal") {
    const sub = interaction.options.getSubcommand();
    if (sub === "bind") {
      const url = interaction.options.getString("url", true).trim();
      const m = url.match(/myanimelist\.net\/(?:animelist|profile)\/([^/?#\s]+)/i);
      if (!m?.[1]) {
        await interaction.reply({ content: "Некорректная MAL ссылка.", ephemeral: true });
        return;
      }
      const username = m[1];
      await saveMalBinding(interaction.user.id, {
        username,
        listUrl: `https://myanimelist.net/animelist/${username}`
      });
      await interaction.reply({ content: `MAL привязан: ${username}`, ephemeral: true });
      return;
    }
    if (sub === "import") {
      await interaction.reply({
        content:
          "Импорт MAL в beta включён как следующий шаг миграции. Привязка MAL уже работает, синк будет перенесён модулем.",
        ephemeral: true
      });
      return;
    }
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

  if (interaction.commandName === "yummy" && interaction.options.getSubcommand() === "unbind") {
    const stateBinding = await getYummyBinding(interaction.user.id);
    if (!stateBinding) {
      await interaction.reply({ content: "Yummy аккаунт не был привязан.", ephemeral: true });
      return;
    }
    await deleteYummyBinding(interaction.user.id);
    await interaction.reply({ content: "Yummy привязка очищена.", ephemeral: true });
    return;
  }

  if (interaction.commandName === "yummy" && interaction.options.getSubcommand() === "sync") {
    if (!interaction.guild) {
      await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
      return;
    }
    const binding = await getYummyBinding(interaction.user.id);
    if (!binding || !binding.accessToken) {
      await interaction.reply({ content: "Сначала выполните `/yummy bind`.", ephemeral: true });
      return;
    }
    const listMode = interaction.options.getString("list", true);
    const maxTopics = interaction.options.getInteger("max_topics") ?? 10;
    const cfg = await getGuildConfig(interaction.guildId!);
    if (!cfg?.forumChannelId) {
      await interaction.reply({
        content: "Форум не настроен. Выполните `/adminpanel action:setup_channels`.",
        ephemeral: true
      });
      return;
    }
    const channel = await interaction.guild.channels.fetch(cfg.forumChannelId);
    if (!(channel instanceof ForumChannel)) {
      await interaction.reply({ content: "Форумный канал недоступен.", ephemeral: true });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const allEntries = await getYummyUserLists({
      accessToken: binding.accessToken,
      yummyUserId: binding.yummyUserId,
      appToken: config.yummyApplicationToken,
      userAgent: config.userAgent
    });
    const entries = filterYummyEntries(allEntries, listMode).slice(0, maxTopics * 3);
    let created = 0;
    let merged = 0;
    for (const entry of entries) {
      if (created >= maxTopics) {
        break;
      }
      const slug = (entry.anime_url || "").trim();
      if (!slug) {
        continue;
      }
      const existing = await getAnimeTopic(slug);
      const title = (entry.title || slug).trim();
      const pageUrl = `https://en.yummyani.me/catalog/item/${slug}`;
      if (existing?.threadId) {
        merged += 1;
        await upsertPersonalListItem(interaction.user.id, {
          key: slug,
          title: existing.title || title,
          url: existing.pageUrl || pageUrl,
          addedAt: new Date().toISOString()
        });
        continue;
      }
      const thread = await channel.threads.create({
        name: title.slice(0, 100),
        message: {
          content: `${title}\n${pageUrl}\nИмпорт Yummy: <@${interaction.user.id}>`
        }
      });
      created += 1;
      await saveAnimeTopic(slug, {
        threadId: thread.id,
        title,
        pageUrl,
        yummySlug: slug,
        adders: [interaction.user.id]
      });
      await upsertPersonalListItem(interaction.user.id, {
        key: slug,
        title,
        url: pageUrl,
        addedAt: new Date().toISOString()
      });
    }
    await interaction.editReply(`Yummy sync готов: новых **${created}**, дописано **${merged}**.`);
    return;
  }

  if (interaction.commandName === "adminpanel") {
    const member = interaction.member;
    const isAdmin = Boolean(
      member &&
        "permissions" in member &&
        typeof member.permissions !== "string" &&
        member.permissions.has("Administrator")
    );
    if (!isAdmin) {
      await interaction.reply({
        content: "Команда доступна только администраторам сервера.",
        ephemeral: true
      });
      return;
    }
    const action = interaction.options.getString("action", true);
    if (!interaction.guild) {
      await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
      return;
    }
    if (action === "setup_channels") {
      await interaction.deferReply({ ephemeral: true });
      const out = await setupGuildChannels(interaction.guild);
      await saveGuildConfig(interaction.guild.id, out.cfg);
      await interaction.editReply(out.text);
      return;
    }
    if (action === "status") {
      const cfg = await getGuildConfig(interaction.guild.id);
      await interaction.reply({
        content: cfg
          ? `Каталог: <#${cfg.forumChannelId}>\nЛичные: <#${cfg.listForumChannelId}>`
          : "Сервер не настроен.",
        ephemeral: true
      });
      return;
    }
    if (action === "forum_scan") {
      const cfg = await getGuildConfig(interaction.guild.id);
      const forumId = cfg?.forumChannelId;
      if (!forumId) {
        await interaction.reply({ content: "Сначала setup_channels.", ephemeral: true });
        return;
      }
      const ch = await interaction.guild.channels.fetch(forumId);
      if (!(ch instanceof ForumChannel)) {
        await interaction.reply({ content: "Форум не найден.", ephemeral: true });
        return;
      }
      await interaction.reply({
        content: `forum_scan(beta): активных тем ${ch.threads.cache.size}`,
        ephemeral: true
      });
      return;
    }
    if (action === "sync_list") {
      const target = interaction.options.getUser("member") ?? interaction.user;
      const items = await getPersonalList(target.id);
      await interaction.reply({
        content: `sync_list(beta): у ${target.displayName} записей ${items.length}`,
        ephemeral: true
      });
      return;
    }
    await interaction.reply({ content: "Неизвестное действие.", ephemeral: true });
  }
}

async function setupGuildChannels(guild: Guild): Promise<{ cfg: GuildConfig; text: string }> {
  const category = await guild.channels.create({
    name: "Anime Bot",
    type: ChannelType.GuildCategory
  });
  const forum = await guild.channels.create({
    name: "📺-каталог-аниме",
    type: ChannelType.GuildForum,
    parent: category.id
  });
  const listForum = await guild.channels.create({
    name: "📋-личные-списки",
    type: ChannelType.GuildForum,
    parent: category.id
  });
  const info = await forum.threads.create({
    name: "📌 Справка и команды",
    message: {
      content:
        "YTWUTSHYBOT v0.2.1BETA\n/user: anime add, list show, mal bind/import, yummy bind/sync\n/admin: /adminpanel"
    }
  });
  const cfg = {
    categoryId: category.id,
    forumChannelId: forum.id,
    listForumChannelId: listForum.id,
    infoThreadId: info.id,
    roasterEnabled: false,
    setupAt: new Date().toISOString()
  };
  return {
    cfg,
    text: `Готово.\nКаталог: <#${forum.id}>\nЛичные: <#${listForum.id}>\nСправка: <#${info.id}>`
  };
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
