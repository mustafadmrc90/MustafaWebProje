#!/usr/bin/env node
/* eslint-disable no-console */

const fs = require("fs");
const path = require("path");

const SLACK_API_BASE = "https://slack.com/api";
const DEFAULT_LIMIT = 200;
const MAX_RATE_LIMIT_RETRY = 8;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseArgs(argv) {
  const args = {
    channels: [],
    includePublic: true,
    includePrivate: false,
    includeArchived: false,
    repliesOnly: false,
    oldest: null,
    latest: null,
    outputJson: "",
    verbose: false
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--channels") {
      const value = argv[i + 1] || "";
      i += 1;
      args.channels = value
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
      continue;
    }

    if (arg === "--all-private") {
      args.includePrivate = true;
      continue;
    }

    if (arg === "--no-public") {
      args.includePublic = false;
      continue;
    }

    if (arg === "--include-archived") {
      args.includeArchived = true;
      continue;
    }

    if (arg === "--replies-only") {
      args.repliesOnly = true;
      continue;
    }

    if (arg === "--from") {
      args.oldest = parseDateToUnix(argv[i + 1]);
      i += 1;
      continue;
    }

    if (arg === "--to") {
      args.latest = parseDateToUnix(argv[i + 1], true);
      i += 1;
      continue;
    }

    if (arg === "--output-json") {
      args.outputJson = String(argv[i + 1] || "").trim();
      i += 1;
      continue;
    }

    if (arg === "--verbose") {
      args.verbose = true;
      continue;
    }
  }

  return args;
}

function parseDateToUnix(input, endOfDay = false) {
  const raw = String(input || "").trim();
  if (!raw) return null;
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;

  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  const hours = endOfDay ? 23 : 0;
  const minutes = endOfDay ? 59 : 0;
  const seconds = endOfDay ? 59 : 0;
  const ms = endOfDay ? 999 : 0;
  const date = new Date(Date.UTC(year, month - 1, day, hours, minutes, seconds, ms));
  return Math.floor(date.getTime() / 1000).toString();
}

async function slackGet(method, token, params = {}, retryCount = 0) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  });

  const url = `${SLACK_API_BASE}/${method}${query.toString() ? `?${query.toString()}` : ""}`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json"
    }
  });

  if (response.status === 429) {
    if (retryCount >= MAX_RATE_LIMIT_RETRY) {
      throw new Error(`${method} HTTP 429 (max retry aşıldı)`);
    }

    const retryAfterRaw = response.headers.get("retry-after");
    const retryAfterSeconds = Number.parseInt(retryAfterRaw || "1", 10);
    const waitMs = (Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : 1) * 1000 + 250;
    await sleep(waitMs);
    return slackGet(method, token, params, retryCount + 1);
  }

  if (!response.ok) {
    throw new Error(`${method} HTTP ${response.status}`);
  }

  const data = await response.json();
  if (!data.ok) {
    const needed = data.needed ? ` needed=${data.needed}` : "";
    const provided = data.provided ? ` provided=${data.provided}` : "";
    throw new Error(`${method} API error: ${data.error || "unknown_error"}${needed}${provided}`);
  }

  return data;
}

async function listChannels({ token, includePublic, includePrivate, includeArchived }) {
  const types = [];
  if (includePublic) types.push("public_channel");
  if (includePrivate) types.push("private_channel");
  if (types.length === 0) return [];

  const channels = [];
  let cursor = "";
  do {
    const data = await slackGet("conversations.list", token, {
      types: types.join(","),
      exclude_archived: includeArchived ? "false" : "true",
      limit: DEFAULT_LIMIT,
      cursor
    });

    (data.channels || []).forEach((channel) => channels.push(channel));
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor);

  return channels;
}

async function listUsers(token) {
  const users = new Map();
  let cursor = "";
  do {
    const data = await slackGet("users.list", token, {
      limit: DEFAULT_LIMIT,
      cursor
    });
    (data.members || []).forEach((member) => {
      const display =
        member?.profile?.display_name ||
        member?.profile?.real_name ||
        member?.real_name ||
        member?.name ||
        member?.id;
      users.set(member.id, display);
    });
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor);

  return users;
}

async function listChannelMessages({ token, channelId, oldest, latest }) {
  const messages = [];
  let cursor = "";

  do {
    const data = await slackGet("conversations.history", token, {
      channel: channelId,
      limit: DEFAULT_LIMIT,
      oldest,
      latest,
      inclusive: "true",
      cursor
    });

    (data.messages || []).forEach((message) => messages.push(message));
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor);

  return messages;
}

async function listThreadReplies({ token, channelId, threadTs, oldest, latest }) {
  const replies = [];
  let cursor = "";

  do {
    const data = await slackGet("conversations.replies", token, {
      channel: channelId,
      ts: threadTs,
      limit: DEFAULT_LIMIT,
      oldest,
      latest,
      inclusive: "true",
      cursor
    });
    (data.messages || []).forEach((message) => replies.push(message));
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor);

  return replies;
}

function shouldCountMessage(message) {
  if (!message || typeof message !== "object") return false;
  if (!message.user) return false;
  if (message.subtype) return false;
  return true;
}

function buildRow(userId, count, usersMap) {
  return {
    user_id: userId,
    name: usersMap.get(userId) || userId,
    count
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const token = String(process.env.SLACK_BOT_TOKEN || "").trim();

  if (!token) {
    console.error("SLACK_BOT_TOKEN gerekli.");
    process.exit(1);
  }

  const channelsFromArgs = args.channels;
  let channels = [];
  if (channelsFromArgs.length > 0) {
    channels = channelsFromArgs.map((id) => ({ id, name: id }));
  } else {
    channels = await listChannels({
      token,
      includePublic: args.includePublic,
      includePrivate: args.includePrivate,
      includeArchived: args.includeArchived
    });
  }

  if (channels.length === 0) {
    console.log("Taranacak kanal bulunamadı.");
    return;
  }

  const usersMap = await listUsers(token);
  const counts = new Map();
  const seen = new Set();

  for (const channel of channels) {
    const channelId = channel.id;
    const channelName = channel.name || channel.id;
    if (args.verbose) {
      console.log(`Taraniyor: ${channelName} (${channelId})`);
    }

    let messages = [];
    try {
      messages = await listChannelMessages({
        token,
        channelId,
        oldest: args.oldest,
        latest: args.latest
      });
    } catch (err) {
      console.error(`Kanal okunamadi ${channelName}: ${err.message}`);
      continue;
    }

    for (const message of messages) {
      if (!args.repliesOnly && shouldCountMessage(message)) {
        const key = `${channelId}:${message.ts}`;
        if (!seen.has(key)) {
          seen.add(key);
          counts.set(message.user, (counts.get(message.user) || 0) + 1);
        }
      }

      const replyCount = Number(message.reply_count || 0);
      if (replyCount <= 0) continue;

      let replies = [];
      try {
        replies = await listThreadReplies({
          token,
          channelId,
          threadTs: message.ts,
          oldest: args.oldest,
          latest: args.latest
        });
      } catch (err) {
        console.error(`Thread okunamadi ${channelName} ${message.ts}: ${err.message}`);
        continue;
      }

      for (const reply of replies) {
        if (reply.ts === message.ts) continue;
        if (!shouldCountMessage(reply)) continue;
        const key = `${channelId}:${reply.ts}`;
        if (seen.has(key)) continue;
        seen.add(key);
        counts.set(reply.user, (counts.get(reply.user) || 0) + 1);
      }
    }
  }

  const rows = Array.from(counts.entries())
    .map(([userId, count]) => buildRow(userId, count, usersMap))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name, "tr"));

  if (rows.length === 0) {
    console.log("Mesaj bulunamadi.");
  } else {
    console.table(rows);
  }

  if (args.outputJson) {
    const outputPath = path.resolve(process.cwd(), args.outputJson);
    fs.writeFileSync(outputPath, JSON.stringify(rows, null, 2), "utf8");
    console.log(`JSON kaydedildi: ${outputPath}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
