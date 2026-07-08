import {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonInteraction,
  ButtonStyle,
  ChannelType,
  ChatInputCommandInteraction,
  EmbedBuilder,
  ForumChannel,
  Guild,
  ModalBuilder,
  SlashCommandBuilder,
  StringSelectMenuBuilder,
  TextInputBuilder,
  TextInputStyle,
  ThreadChannel
} from "discord.js";
import { AnimeCard, fetchAnimeCard, searchYummySlug, slugFromText } from "./catalogApi.js";
import { config } from "./config.js";
import { fetchMalList } from "./malApi.js";
import {
  GuildConfig,
  deleteYummyBinding,
  getAnimeTopic,
  getAnimeTopicRatings,
  getGuildConfig,
  getMalBinding,
  getPersonalList,
  getYummyBinding,
  loadState,
  rateAnimeTopic,
  saveAnimeTopic,
  saveGuildConfig,
  saveMalBinding,
  saveYummyBinding,
  syncPersonalListFromTopics,
  upsertPersonalListItem
} from "./storage.js";
import {
  filterYummyEntries,
  getYummyProfile,
  getYummyUserLists,
  loginYummyByPassword
} from "./yummyApi.js";

export const slashCommands = [
  new SlashCommandBuilder().setName("ping").setDescription("Проверка, что бот онлайн"),
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
    .addSubcommand((sub) =>
      sub
        .setName("rate")
        .setDescription("Оценка в теме")
        .addIntegerOption((opt) =>
          opt.setName("score").setDescription("Оценка 1-10").setRequired(true).setMinValue(1).setMaxValue(10)
        )
    ),
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
        .setDescription("Импорт из MAL")
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
        .addIntegerOption((opt) =>
          opt.setName("max_topics").setDescription("Максимум новых тем").setMinValue(1).setMaxValue(25)
        )
        .addStringOption((opt) =>
          opt
            .setName("list_url")
            .setDescription("Необязательно: перепривязать MAL ссылкой перед импортом")
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
          { name: "repair_topics", value: "repair_topics" },
          { name: "sync_list", value: "sync_list" },
          { name: "personal_rebuild", value: "personal_rebuild" },
          { name: "yummy_sync_member", value: "yummy_sync_member" }
        )
    )
    .addUserOption((opt) => opt.setName("member").setDescription("Пользователь для операций"))
].map((c) => c.toJSON());

export async function handleChatInput(interaction: ChatInputCommandInteraction): Promise<void> {
  if (interaction.commandName === "ping") {
    await interaction.reply({ content: "Pong! v0.2.1BETA", ephemeral: true });
    return;
  }

  if (interaction.commandName === "anime") {
    const sub = interaction.options.getSubcommand();
    if (sub === "rate") {
      if (!interaction.channel || interaction.channel.type !== ChannelType.PublicThread) {
        await interaction.reply({ content: "Оценка только внутри темы форума.", ephemeral: true });
        return;
      }
      const score = interaction.options.getInteger("score", true);
      await rateAnimeTopic(interaction.channel.id, interaction.user.id, score);
      const stats = await getAnimeTopicRatings(interaction.channel.id);
      await interaction.reply({
        content:
          `Оценка сохранена: **${score}**\n` +
          `Средняя: **${stats.average?.toFixed(2) ?? "—"}** (${stats.count} голосов)`,
        ephemeral: true
      });
      return;
    }
    if (sub === "add") {
      await handleAnimeAdd(interaction);
      return;
    }
  }

  if (interaction.commandName === "list" && interaction.options.getSubcommand() === "show") {
    const items = await getPersonalList(interaction.user.id);
    if (!items.length) {
      await interaction.reply({ content: "Ваш список пока пуст.", ephemeral: true });
      return;
    }
    const embed = new EmbedBuilder()
      .setTitle(`Личный список — ${interaction.user.displayName}`)
      .setDescription(items.slice(0, 40).map((x) => `• [${x.title}](${x.url})`).join("\n"));
    const controls = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder()
        .setCustomId(`personal_refresh:${interaction.user.id}`)
        .setLabel("Обновить")
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId(`personal_compact:${interaction.user.id}`)
        .setLabel("Компакт")
        .setStyle(ButtonStyle.Secondary)
    );
    await interaction.reply({ embeds: [embed], components: [controls], ephemeral: true });
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
      await handleMalImport(interaction);
      return;
    }
  }

  if (interaction.commandName === "yummy") {
    const sub = interaction.options.getSubcommand();
    if (sub === "bind") {
      const modal = new ModalBuilder().setCustomId("yummy_bind_modal").setTitle("YummyAnime вход");
      modal.addComponents(
        new ActionRowBuilder<TextInputBuilder>().addComponents(
          new TextInputBuilder()
            .setCustomId("login")
            .setLabel("Логин или почта")
            .setPlaceholder("Введите логин")
            .setRequired(true)
            .setStyle(TextInputStyle.Short)
        ),
        new ActionRowBuilder<TextInputBuilder>().addComponents(
          new TextInputBuilder()
            .setCustomId("password")
            .setLabel("Пароль")
            .setPlaceholder("Введите пароль")
            .setRequired(true)
            .setStyle(TextInputStyle.Short)
        )
      );
      await interaction.showModal(modal);
      return;
    }
    if (sub === "unbind") {
      const stateBinding = await getYummyBinding(interaction.user.id);
      if (!stateBinding) {
        await interaction.reply({ content: "Yummy аккаунт не был привязан.", ephemeral: true });
        return;
      }
      await deleteYummyBinding(interaction.user.id);
      await interaction.reply({ content: "Yummy привязка очищена.", ephemeral: true });
      return;
    }
    if (sub === "sync") {
      await handleYummySync(interaction, interaction.user.id);
      return;
    }
  }

  if (interaction.commandName === "adminpanel") {
    await handleAdminPanel(interaction);
  }
}

async function handleAnimeAdd(interaction: ChatInputCommandInteraction): Promise<void> {
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
  if (!slug) slug = await searchYummySlug(query);
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
      content: buildTopicHeader(card.pageUrl, interaction.user.id),
      embeds: [buildAnimeEmbed(card, "YummyAnime · en.yummyani.me")]
    }
  });
  await postTopicPanels(thread);
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
}

async function handleMalImport(interaction: ChatInputCommandInteraction): Promise<void> {
  if (!interaction.guild) {
    await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
    return;
  }
  await interaction.deferReply({ ephemeral: true });
  const listUrl = interaction.options.getString("list_url")?.trim();
  let binding = await getMalBinding(interaction.user.id);
  if (listUrl) {
    const m = listUrl.match(/myanimelist\.net\/(?:animelist|profile)\/([^/?#\s]+)/i);
    if (!m?.[1]) {
      await interaction.editReply("Некорректная MAL ссылка.");
      return;
    }
    const username = m[1];
    await saveMalBinding(interaction.user.id, {
      username,
      listUrl: `https://myanimelist.net/animelist/${username}`
    });
    binding = await getMalBinding(interaction.user.id);
  }
  if (!binding) {
    await interaction.editReply("Сначала выполните `/mal bind`.");
    return;
  }
  const cfg = await getGuildConfig(interaction.guild.id);
  if (!cfg?.forumChannelId) {
    await interaction.editReply("Форум не настроен. `/adminpanel action:setup_channels`.");
    return;
  }
  const forum = await interaction.guild.channels.fetch(cfg.forumChannelId);
  if (!(forum instanceof ForumChannel)) {
    await interaction.editReply("Форумный канал недоступен.");
    return;
  }
  const status = interaction.options.getString("status", true);
  const maxTopics = interaction.options.getInteger("max_topics") ?? 10;
  const malItems = await fetchMalList(binding.username, status);
  let created = 0;
  let merged = 0;
  const errors: string[] = [];
  for (const item of malItems) {
    if (created >= maxTopics) break;
    const slug = await searchYummySlug(item.title);
    const key = slug || `mal:${item.id}`;
    const pageUrl = slug
      ? `https://en.yummyani.me/catalog/item/${slug}`
      : `https://myanimelist.net/anime/${item.id}`;
    const existing = await getAnimeTopic(key);
    if (existing?.threadId) {
      merged += 1;
      await upsertPersonalListItem(interaction.user.id, {
        key,
        title: existing.title,
        url: existing.pageUrl,
        addedAt: new Date().toISOString()
      });
      continue;
    }
    try {
      const thread = await forum.threads.create({
        name: item.title.slice(0, 100),
        message: {
          content: buildTopicHeader(pageUrl, interaction.user.id),
          embeds: [buildMinimalAnimeEmbed(item.title, pageUrl, "Импортировано из MyAnimeList")]
        }
      });
      await postTopicPanels(thread);
      created += 1;
      await saveAnimeTopic(key, {
        threadId: thread.id,
        title: item.title,
        pageUrl,
        yummySlug: slug || "",
        adders: [interaction.user.id]
      });
      await upsertPersonalListItem(interaction.user.id, {
        key,
        title: item.title,
        url: pageUrl,
        addedAt: new Date().toISOString()
      });
    } catch (e) {
      errors.push(`${item.title}: ${(e as Error).message}`);
    }
  }
  await interaction.editReply(
    `MAL import готов.\nНовых: **${created}**\nДописано: **${merged}**` +
      (errors.length ? `\nОшибки: ${errors.slice(0, 3).join("; ")}` : "")
  );
}

async function handleYummySync(interaction: ChatInputCommandInteraction, targetUserId: string): Promise<void> {
  if (!interaction.guild) {
    await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
    return;
  }
  const binding = await getYummyBinding(targetUserId);
  if (!binding?.accessToken) {
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
    if (created >= maxTopics) break;
    const slug = (entry.anime_url || "").trim();
    if (!slug) continue;
    const title = (entry.title || slug).trim();
    const pageUrl = `https://en.yummyani.me/catalog/item/${slug}`;
    const existing = await getAnimeTopic(slug);
    if (existing?.threadId) {
      merged += 1;
      await upsertPersonalListItem(targetUserId, {
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
        content: buildTopicHeader(pageUrl, targetUserId),
        embeds: [buildMinimalAnimeEmbed(title, pageUrl, "Импортировано из Yummy списка")]
      }
    });
    await postTopicPanels(thread);
    created += 1;
    await saveAnimeTopic(slug, {
      threadId: thread.id,
      title,
      pageUrl,
      yummySlug: slug,
      adders: [targetUserId]
    });
    await upsertPersonalListItem(targetUserId, {
      key: slug,
      title,
      url: pageUrl,
      addedAt: new Date().toISOString()
    });
  }
  await interaction.editReply(`Yummy sync готов: новых **${created}**, дописано **${merged}**.`);
}

async function handleAdminPanel(interaction: ChatInputCommandInteraction): Promise<void> {
  const isAdmin = Boolean(interaction.memberPermissions?.has("Administrator"));
  if (!isAdmin) {
    await interaction.reply({ content: "Команда доступна только администраторам сервера.", ephemeral: true });
    return;
  }
  if (!interaction.guild) {
    await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
    return;
  }
  const action = interaction.options.getString("action", true);
  if (action === "setup_channels") {
    await interaction.deferReply({ ephemeral: true });
    const out = await setupGuildChannels(interaction.guild);
    await saveGuildConfig(interaction.guild.id, out.cfg);
    await interaction.editReply(out.text);
    return;
  }
  if (action === "status") {
    const cfg = await getGuildConfig(interaction.guild.id);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId("admin_status").setLabel("Статус").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId("admin_setup").setLabel("Setup").setStyle(ButtonStyle.Primary)
    );
    await interaction.reply({
      content: cfg ? `Каталог: <#${cfg.forumChannelId}>\nЛичные: <#${cfg.listForumChannelId}>` : "Сервер не настроен.",
      components: [row],
      ephemeral: true
    });
    return;
  }
  if (action === "forum_scan" || action === "repair_topics") {
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
    if (action === "repair_topics") {
      for (const th of ch.threads.cache.values()) {
        if (th.locked) await th.setLocked(false).catch(() => undefined);
      }
    }
    await interaction.reply({
      content:
        action === "repair_topics"
          ? `repair_topics: проверено ${ch.threads.cache.size} тем.`
          : `forum_scan: активных тем ${ch.threads.cache.size}`,
      ephemeral: true
    });
    return;
  }
  if (action === "sync_list" || action === "personal_rebuild") {
    const target = interaction.options.getUser("member") ?? interaction.user;
    const n =
      action === "personal_rebuild"
        ? await syncPersonalListFromTopics(target.id)
        : (await getPersonalList(target.id)).length;
    await interaction.reply({
      content: `${action}: у ${target.displayName} записей ${n}`,
      ephemeral: true
    });
    return;
  }
  if (action === "yummy_sync_member") {
    const target = interaction.options.getUser("member");
    if (!target) {
      await interaction.reply({ content: "Укажите member.", ephemeral: true });
      return;
    }
    const binding = await getYummyBinding(target.id);
    if (!binding?.accessToken) {
      await interaction.reply({ content: "У участника нет привязки Yummy.", ephemeral: true });
      return;
    }
    // reuse by constructing synthetic option values is cumbersome; run subset here:
    const cfg = await getGuildConfig(interaction.guild.id);
    const forumId = cfg?.forumChannelId;
    if (!forumId) {
      await interaction.reply({ content: "Сначала setup_channels.", ephemeral: true });
      return;
    }
    const channel = await interaction.guild.channels.fetch(forumId);
    if (!(channel instanceof ForumChannel)) {
      await interaction.reply({ content: "Форумный канал недоступен.", ephemeral: true });
      return;
    }
    const entries = filterYummyEntries(
      await getYummyUserLists({
        accessToken: binding.accessToken,
        yummyUserId: binding.yummyUserId,
        appToken: config.yummyApplicationToken,
        userAgent: config.userAgent
      }),
      "all"
    ).slice(0, 10);
    let created = 0;
    for (const entry of entries) {
      const slug = (entry.anime_url || "").trim();
      if (!slug) continue;
      if (await getAnimeTopic(slug)) continue;
      const title = (entry.title || slug).trim();
      const pageUrl = `https://en.yummyani.me/catalog/item/${slug}`;
      const thread = await channel.threads.create({
        name: title.slice(0, 100),
        message: {
          content: buildTopicHeader(pageUrl, target.id),
          embeds: [buildMinimalAnimeEmbed(title, pageUrl, "Импорт админом")]
        }
      });
      await postTopicPanels(thread);
      created += 1;
      await saveAnimeTopic(slug, {
        threadId: thread.id,
        title,
        pageUrl,
        yummySlug: slug,
        adders: [target.id]
      });
      await upsertPersonalListItem(target.id, {
        key: slug,
        title,
        url: pageUrl,
        addedAt: new Date().toISOString()
      });
    }
    await interaction.reply({
      content: `yummy_sync_member: создано ${created} тем для ${target.displayName}.`,
      ephemeral: true
    });
    return;
  }
  await interaction.reply({ content: "Неизвестное действие.", ephemeral: true });
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

function buildAnimeEmbed(card: AnimeCard, footer: string): EmbedBuilder {
  const embed = new EmbedBuilder()
    .setTitle(card.title)
    .setURL(card.pageUrl)
    .setColor(0x2f6feb)
    .setDescription(card.description ? truncate(card.description, 700) : "Описание отсутствует.");
  const fields = [
    { name: "Тип", value: card.typeName, inline: true },
    { name: "Статус", value: card.statusTitle, inline: true },
    { name: "Год", value: card.year, inline: true },
    { name: "Эпизоды", value: card.episodesLabel, inline: true },
    { name: "Рейтинг", value: card.ratingAvg, inline: true },
    { name: "Жанры", value: card.genres.length ? card.genres.join(", ") : "—", inline: false },
    { name: "Ссылка на YummyAnime", value: card.pageUrl, inline: false }
  ];
  embed.addFields(fields);
  if (card.posterUrl) {
    embed.setImage(card.posterUrl);
  }
  embed.setFooter({ text: footer });
  return embed;
}

function buildMinimalAnimeEmbed(title: string, pageUrl: string, source: string): EmbedBuilder {
  return new EmbedBuilder()
    .setTitle(title)
    .setURL(pageUrl)
    .setDescription(source)
    .setColor(0x5865f2);
}

function buildTopicHeader(pageUrl: string, userId: string): string {
  return [
    "Ссылка на YummyAnime",
    pageUrl,
    "",
    "Статус просмотра — нажмите реакцию под этим сообщением:",
    "👀 смотрю | ✅ просмотрено | 📘 в планах | ⏸️ отложено | ❌ брошено",
    "",
    `Добавил: <@${userId}>`
  ].join("\n");
}

async function postTopicPanels(thread: ThreadChannel): Promise<void> {
  const rateEmbed = new EmbedBuilder()
    .setTitle("⭐ Оценка аниме (1-10)")
    .setDescription(
      "Нажмите кнопку **Оценить** и введите целое число от 1 до 10.\n" +
        "Ниже будет показана средняя оценка участников."
    )
    .setColor(0xf5b041);
  const rateRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder()
      .setCustomId(`rate_open:${thread.id}`)
      .setLabel("Оценить")
      .setStyle(ButtonStyle.Primary)
  );
  await thread.send({ embeds: [rateEmbed], components: [rateRow] }).catch(() => undefined);

  const recEmbed = new EmbedBuilder()
    .setTitle("📣 Порекомендовать аниме")
    .setDescription("Выберите участника сервера в меню ниже — ему придёт уведомление.")
    .setColor(0x5dade2);
  const recSelect = new StringSelectMenuBuilder()
    .setCustomId(`recommend_pick:${thread.id}`)
    .setPlaceholder("Кому порекомендовать?")
    .addOptions([{ label: "Скоро будет доступно", value: "stub" }]);
  await thread
    .send({
      embeds: [recEmbed],
      components: [new ActionRowBuilder<StringSelectMenuBuilder>().addComponents(recSelect)]
    })
    .catch(() => undefined);

  const listEmbed = new EmbedBuilder()
    .setTitle("🧍 Личный список")
    .setDescription("Нажмите кнопку, чтобы добавить это аниме в ваш личный список.")
    .setColor(0x58d68d);
  const listRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder()
      .setCustomId(`list_add:${thread.id}`)
      .setLabel("Добавить в мой список")
      .setStyle(ButtonStyle.Success)
  );
  await thread.send({ embeds: [listEmbed], components: [listRow] }).catch(() => undefined);
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
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

export async function handleButtonInteraction(interaction: ButtonInteraction): Promise<void> {
  if (interaction.customId.startsWith("rate_open:")) {
    const modal = new ModalBuilder()
      .setCustomId(`rate_modal:${interaction.customId.split(":")[1]}`)
      .setTitle("Оценить аниме");
    const scoreInput = new TextInputBuilder()
      .setCustomId("score")
      .setLabel("Оценка (1-10)")
      .setPlaceholder("Введите число от 1 до 10")
      .setRequired(true)
      .setStyle(TextInputStyle.Short)
      .setMaxLength(2);
    modal.addComponents(new ActionRowBuilder<TextInputBuilder>().addComponents(scoreInput));
    await interaction.showModal(modal);
    return;
  }
  if (interaction.customId.startsWith("list_add:")) {
    const threadId = interaction.customId.split(":")[1] || "";
    const topic = (await findTopicByThreadId(threadId)) ?? null;
    if (!topic) {
      await interaction.reply({ content: "Не удалось определить аниме для этой темы.", ephemeral: true });
      return;
    }
    await upsertPersonalListItem(interaction.user.id, {
      key: topic.yummySlug || topic.threadId,
      title: topic.title,
      url: topic.pageUrl,
      addedAt: new Date().toISOString()
    });
    await interaction.reply({ content: "Добавлено в ваш личный список.", ephemeral: true });
    return;
  }
  if (interaction.customId.startsWith("recommend_pick:")) {
    await interaction.reply({
      content: "Рекомендации будут активированы следующим обновлением TS-версии.",
      ephemeral: true
    });
    return;
  }
  if (interaction.customId === "admin_status") {
    if (!interaction.guildId) {
      await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
      return;
    }
    const cfg = await getGuildConfig(interaction.guildId);
    await interaction.reply({
      content: cfg
        ? `Каталог: <#${cfg.forumChannelId}>\nЛичные: <#${cfg.listForumChannelId}>`
        : "Сервер не настроен.",
      ephemeral: true
    });
    return;
  }
  if (interaction.customId === "admin_setup") {
    if (!interaction.guild) {
      await interaction.reply({ content: "Команда только на сервере.", ephemeral: true });
      return;
    }
    if (!interaction.memberPermissions?.has("Administrator")) {
      await interaction.reply({ content: "Нужны права администратора.", ephemeral: true });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const out = await setupGuildChannels(interaction.guild);
    await saveGuildConfig(interaction.guild.id, out.cfg);
    await interaction.editReply(out.text);
    return;
  }
  if (interaction.customId.startsWith("personal_refresh:")) {
    const ownerId = interaction.customId.split(":")[1] || "";
    if (ownerId !== interaction.user.id) {
      await interaction.reply({ content: "Кнопка не для вас.", ephemeral: true });
      return;
    }
    const items = await getPersonalList(ownerId);
    const text = items.length
      ? items
          .slice(0, 20)
          .map((x) => `• [${x.title}](${x.url})`)
          .join("\n")
      : "Список пуст.";
    await interaction.reply({ content: text, ephemeral: true });
    return;
  }
  if (interaction.customId.startsWith("personal_compact:")) {
    const ownerId = interaction.customId.split(":")[1] || "";
    if (ownerId !== interaction.user.id) {
      await interaction.reply({ content: "Кнопка не для вас.", ephemeral: true });
      return;
    }
    const items = await getPersonalList(ownerId);
    const text = items.length
      ? items
          .slice(0, 25)
          .map((x, i) => `${i + 1}. ${x.title}`)
          .join("\n")
      : "Список пуст.";
    await interaction.reply({ content: text, ephemeral: true });
  }
}

export async function handleRateModal(modal: {
  userId: string;
  threadId: string;
  scoreRaw: string;
}): Promise<string> {
  const value = Number.parseInt((modal.scoreRaw || "").trim(), 10);
  if (!Number.isFinite(value) || value < 1 || value > 10) {
    throw new Error("Введите целое число от 1 до 10.");
  }
  await rateAnimeTopic(modal.threadId, modal.userId, value);
  const stats = await getAnimeTopicRatings(modal.threadId);
  return `Оценка сохранена: **${value}**\nСредняя: **${stats.average?.toFixed(2) ?? "—"}** (${stats.count} голосов)`;
}

async function findTopicByThreadId(threadId: string): Promise<{
  threadId: string;
  title: string;
  pageUrl: string;
  yummySlug: string;
} | null> {
  // Small linear scan is acceptable for beta.
  const state = await loadState();
  for (const topic of Object.values(state.animeTopics)) {
    if (topic.threadId === threadId) {
      return topic;
    }
  }
  return null;
}
