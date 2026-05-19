// plugins/pappy-intel.js
'use strict';

const fs   = require('fs');
const path = require('path');
const { ownerTelegramId } = require('../config');
const logger  = require('../core/logger');
const eventBus = require('../core/eventBus');
const { validateGroupLink, validateBatch, startValidator } = require('../core/linkValidator');
const Intel = require('../core/models/Intel');

const dbPath = path.join(__dirname, '../data/intel.json');

const LIMITS = {
    MAX_JOINS_PER_DAY: Math.max(5, Number(process.env.INTEL_MAX_JOINS_PER_DAY || 60)),
    MIN_COOLDOWN_MS:   Math.max(30000, Number(process.env.INTEL_MIN_COOLDOWN_MS || 120000)),
    MAX_COOLDOWN_MS:   Math.max(60000, Number(process.env.INTEL_MAX_COOLDOWN_MS || 300000)),
    REALTIME_MIN_INTERVAL_MS: Math.max(60000, Number(process.env.INTEL_REALTIME_MIN_INTERVAL_MS || 180000)),
    REALTIME_MAX_PER_HOUR: Math.max(1, Number(process.env.INTEL_REALTIME_MAX_PER_HOUR || 8)),
    MAX_CONSECUTIVE_FAILS: Math.max(2, Number(process.env.INTEL_MAX_CONSECUTIVE_FAILS || 6)),
    MAX_RATE_LIMIT_HITS: Math.max(1, Number(process.env.INTEL_MAX_RATE_LIMIT_HITS || 2)),
    EMERGENCY_PAUSE_MS: Math.max(10 * 60 * 1000, Number(process.env.INTEL_EMERGENCY_PAUSE_MS || (6 * 60 * 60 * 1000))),
};

let intelCache = {
    knownLinks:        [],
    pendingQueue:      [],
    groupLinks:        {},        // NEW: groupJid -> [ { code, validatedAt, status } ]
    botJoinState:      {},        // botKey -> { dailyJoins, lastJoinDate, lastJoinTimestamp }
    dailyJoins:        0,
    lastJoinDate:      new Date().toISOString().split('T')[0],
    lastJoinTimestamp: 0,
    autoJoinEnabled:   false,
    autoJoinBotId:     null,
    autoJoinStartedAt: 0,
    realtimeAutoJoin:  false,
    emergencyPauseUntil: 0,
    realtimeWindowStart: 0,
    realtimeWindowCount: 0,
    consecutiveFails: 0,
    rateLimitHits: 0,
};

const _processingByBot = new Map();

function normalizeEntry(entry) {
    if (!entry) return null;
    if (typeof entry === 'string') return { code: entry.trim(), botId: null, queuedAt: 0 };
    const code = String(entry.code || entry.inviteCode || '').trim();
    return code
        ? {
            code,
            botId: entry.botId ? String(entry.botId).trim() : null,
            queuedAt: Number(entry.queuedAt) || 0,
        }
        : null;
}

function hasCode(code) {
    return intelCache.knownLinks.includes(code) ||
           intelCache.pendingQueue.some(e => normalizeEntry(e)?.code === code);
}

function resolveSock(botId) {
    const id = String(botId || '').trim();
    if (id && global.waSockByBotId?.has(id)) return global.waSockByBotId.get(id);
    if (id && global.waSocks) {
        for (const [k, s] of global.waSocks.entries()) {
            if (k.includes(id) && s?.user) return s;
        }
    }
    // fallback — any connected socket
    if (global.waSocks) {
        for (const s of global.waSocks.values()) {
            if (s?.user) return s;
        }
    }
    return null;
}

// Helper: Escape HTML for telegram messages
function escapeHtml(text) {
    return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

async function saveState() {
    try { await fs.promises.writeFile(dbPath, JSON.stringify(intelCache, null, 2)); } catch {}
}

async function upsertSharedIntel(code, patch = {}) {
    const normalizedCode = String(code || '').trim();
    if (!normalizedCode) return;
    const base = {
        code: normalizedCode,
        linkCode: normalizedCode,
        seenAt: new Date(),
    };
    await Intel.findOneAndUpdate(
        { $or: [{ code: normalizedCode }, { linkCode: normalizedCode }] },
        { $set: { ...base, ...patch }, $setOnInsert: { dateAdded: new Date() } },
        { upsert: true, new: true }
    ).catch(() => {});
}

function getBotQueueKey(botId) {
    return String(botId || intelCache.autoJoinBotId || 'global').trim() || 'global';
}

// NEW: Save extracted link to group links mapping with validation
async function addGroupLink(groupJid, code, sock = null) {
    if (!groupJid || !code) return false;
    const jid = String(groupJid).trim();
    if (!intelCache.groupLinks[jid]) intelCache.groupLinks[jid] = [];
    
    // Check if already exists (duplicate prevention)
    const existing = intelCache.groupLinks[jid].find(l => l.code === code);
    if (existing) {
        existing.seenAt = Date.now();
        return false; // already tracked
    }
    
    // Validate link before saving using our validator
    const validation = await validateGroupLink(code, sock).catch(err => ({ valid: false, error: err.message }));
    
    if (validation.valid) {
        intelCache.groupLinks[jid].push({
            code,
            validatedAt: Date.now(),
            status: 'valid',
            seenAt: Date.now(),
        });
        await upsertSharedIntel(code, {
            status: 'valid',
            source: 'group_link_detected',
            groupJid: jid,
            validatedAt: new Date(),
        });
        await saveState();
        return true;
    }
    return false;
}

// NEW: Cleanup invalid links from group
async function cleanupGroupInvalidLinks(groupJid, sock = null) {
    if (!groupJid) return 0;
    const jid = String(groupJid).trim();
    const links = intelCache.groupLinks[jid];
    if (!Array.isArray(links)) return 0;
    
    let removed = 0;
    const codesToCheck = links.map(l => l.code);
    const { valid } = await validateBatch(codesToCheck, sock).catch(() => ({ valid: [] }));
    
    const newLinks = links.filter(link => {
        const keep = valid.includes(link.code);
        if (!keep) removed++;
        return keep;
    });
    
    if (removed > 0) {
        intelCache.groupLinks[jid] = newLinks;
        await saveState();
        logger.info(`[INTEL] Cleaned ${removed} invalid link(s) from ${jid}`);
    }
    return removed;
}

async function initDb() {
    try {
        if (fs.existsSync(dbPath)) {
            const d = JSON.parse(await fs.promises.readFile(dbPath, 'utf8'));
            intelCache = { ...intelCache, ...d };
            intelCache.pendingQueue = (intelCache.pendingQueue || []).map(normalizeEntry).filter(Boolean);
            intelCache.knownLinks   = intelCache.knownLinks || [];
            intelCache.groupLinks   = intelCache.groupLinks || {};
        }
    } catch {}
}
initDb();

async function syncLegacyIntelCacheToMongo() {
    const codes = Array.isArray(intelCache.knownLinks) ? intelCache.knownLinks : [];
    if (!codes.length) return;
    let synced = 0;
    for (const raw of codes) {
        const code = String(raw || '').trim();
        if (!code) continue;
        await upsertSharedIntel(code, {
            status: 'valid',
            source: 'legacy_intel_json',
            seenAt: new Date(),
        });
        synced++;
    }
    if (synced > 0) {
        logger.info(`[INTEL] Synced ${synced} legacy intel.json links to shared Mongo Intel`);
    }
}

setImmediate(() => {
    syncLegacyIntelCacheToMongo().catch(() => {});
});

// Start link validator on initialization
setImmediate(() => {
    try {
        startValidator();
    } catch (err) {
        logger.warn(`[INTEL] Failed to start link validator: ${err.message}`);
    }
});

// Listen for socket open to set up group join handlers
eventBus.on('socket.open', (sock) => {
    if (!sock || !sock.ev) return;
    
    // Listen for group participant updates (joins/leaves)
    sock.ev.on('group-participants.update', async (groupParticipantsUpdate) => {
        try {
            const { id, action, participants } = groupParticipantsUpdate;
            if (!id || !participants || !participants.length) return;
            
            // Only care about 'add' actions (new joins)
            if (action !== 'add') return;
            
            const groupJid = id;
            
            for (const participant of participants) {
                const participantJid = String(participant || '').trim();
                if (!participantJid) continue;
                
                // Check if this is the bot joining
                const isBot = sock.user?.id && participantJid.includes(sock.user.id.split(':')[0]);
                
                if (isBot) {
                    logger.success(`[INTEL] Bot joined group ${groupJid}`);
                    
                    // Send telegram notification about successful group join
                    if (global.tgBot) {
                        try {
                            // Get group metadata
                            const meta = await sock.groupMetadata(groupJid).catch(() => null);
                            const groupName = meta?.subject || groupJid;
                            const memberCount = meta?.participants?.length || '?';
                            
                            global.tgBot.telegram.sendMessage(
                                ownerTelegramId,
                                `🎉 <b>GROUP JOIN SUCCESSFUL</b>\n\n` +
                                `👥 <b>${escapeHtml(groupName)}</b>\n` +
                                `📱 Members: ${memberCount}\n` +
                                `🆔 <code>${groupJid}</code>\n` +
                                `⏰ <i>${new Date().toLocaleString()}</i>`,
                                { parse_mode: 'HTML' }
                            ).catch(() => {});
                        } catch (err) {
                            logger.warn(`[INTEL] Failed to notify telegram of group join: ${err.message}`);
                        }
                    }
                }
            }
        } catch (err) {
            logger.warn(`[INTEL] Error handling group join: ${err.message}`);
        }
    });
});;

function checkDailyReset() {
    const today = new Date().toISOString().split('T')[0];
    if (intelCache.lastJoinDate !== today) {
        intelCache.lastJoinDate = today;
        intelCache.dailyJoins   = 0;
        intelCache.botJoinState = {};
        intelCache.realtimeWindowStart = 0;
        intelCache.realtimeWindowCount = 0;
        intelCache.consecutiveFails = 0;
        intelCache.rateLimitHits = 0;
        saveState();
    }
}

function inEmergencyPause() {
    return Number(intelCache.emergencyPauseUntil || 0) > Date.now();
}

function triggerEmergencyPause(reason) {
    intelCache.emergencyPauseUntil = Date.now() + LIMITS.EMERGENCY_PAUSE_MS;
    intelCache.realtimeAutoJoin = false;
    saveState();
    logger.warn(`[INTEL] Emergency pause activated: ${reason}`);
    if (global.tgBot && ownerTelegramId) {
        const mins = Math.ceil(LIMITS.EMERGENCY_PAUSE_MS / 60000);
        global.tgBot.telegram.sendMessage(
            ownerTelegramId,
            `🛑 <b>INTEL AUTOJOIN SAFETY PAUSE</b>\n\nReason: <code>${escapeHtml(reason)}</code>\nPause: <b>${mins} min</b>\nRealtime mode: <b>OFF</b>`,
            { parse_mode: 'HTML' }
        ).catch(() => {});
    }
}

function canUseRealtimeJoin() {
    checkDailyReset();
    if (!intelCache.autoJoinEnabled || !intelCache.realtimeAutoJoin) return false;
    if (inEmergencyPause()) return false;
    if (intelCache.dailyJoins >= LIMITS.MAX_JOINS_PER_DAY) return false;

    const now = Date.now();
    const lastTs = Number(intelCache.lastJoinTimestamp || 0);
    if (now - lastTs < LIMITS.REALTIME_MIN_INTERVAL_MS) return false;

    const hourMs = 60 * 60 * 1000;
    const wStart = Number(intelCache.realtimeWindowStart || 0);
    if (!wStart || (now - wStart) >= hourMs) {
        intelCache.realtimeWindowStart = now;
        intelCache.realtimeWindowCount = 0;
    }
    if (Number(intelCache.realtimeWindowCount || 0) >= LIMITS.REALTIME_MAX_PER_HOUR) return false;
    return true;
}

function getBotJoinState(botKey) {
    const key = getBotQueueKey(botKey);
    if (!intelCache.botJoinState || typeof intelCache.botJoinState !== 'object') {
        intelCache.botJoinState = {};
    }
    if (!intelCache.botJoinState[key]) {
        intelCache.botJoinState[key] = {
            dailyJoins: 0,
            lastJoinDate: new Date().toISOString().split('T')[0],
            lastJoinTimestamp: 0,
        };
    }

    const state = intelCache.botJoinState[key];
    const today = new Date().toISOString().split('T')[0];
    if (state.lastJoinDate !== today) {
        state.lastJoinDate = today;
        state.dailyJoins = 0;
        state.lastJoinTimestamp = 0;
    }

    return state;
}

// ── Scraper: intercept invite links from any message ─────────────────────────
let _linkQueueNotificationThrottle = {};

eventBus.on('message.upsert', async ({ sock, msg, text, botId }) => {
    if (!text?.includes('chat.whatsapp.com')) return;
    const matches = text.match(/chat\.whatsapp\.com\/([0-9A-Za-z]{20,24})/ig) || [];
    let added = 0;
    const newCodes = [];
    
    // Try to get the group JID where this message was received
    const groupJid = msg?.key?.remoteJid;
    
    for (const m of matches) {
        const code = m.split('chat.whatsapp.com/')[1];
        newCodes.push(code);

        // ENHANCED: Also save link to groupLinks with validation
        if (groupJid) {
            const saved = await addGroupLink(groupJid, code, sock).catch(() => false);
            if (saved) {
                logger.info(`[INTEL] Saved link ${code} to group ${groupJid}`);
            }
        }

        // Real-time autojoin — immediately join without queuing
        if (intelCache.realtimeAutoJoin && sock && canUseRealtimeJoin()) {
            try {
                if (hasCode(code)) continue;
                const joinedJid = await sock.groupAcceptInvite(code);
                logger.info(`[INTEL] Real-time joined: ${code}`);
                if (!intelCache.knownLinks.includes(code)) {
                    intelCache.knownLinks.push(code);
                }
                
                // Send telegram notification immediately on join
                if (global.tgBot && ownerTelegramId) {
                    const meta = await sock.groupMetadata(joinedJid).catch(() => null);
                    const groupName = meta?.subject || 'Unknown Group';
                    global.tgBot.telegram.sendMessage(
                        ownerTelegramId,
                        `✅ <b>AUTO-JOIN SUCCESS</b>\n\n` +
                        `👥 <b>${escapeHtml(groupName)}</b>\n` +
                        `🔗 <code>${code}</code>\n` +
                        `📊 Total joined: <b>${intelCache.dailyJoins}/${LIMITS.MAX_JOINS_PER_DAY}</b>`,
                        { parse_mode: 'HTML' }
                    ).catch(() => {});
                }
                intelCache.dailyJoins++;
                intelCache.realtimeWindowCount = Number(intelCache.realtimeWindowCount || 0) + 1;
                intelCache.lastJoinTimestamp = Date.now();
                intelCache.consecutiveFails = 0;
                await upsertSharedIntel(code, {
                    status: 'joined',
                    source: 'autojoin_realtime',
                    validatedAt: new Date(),
                });
                await saveState();
            } catch (e) {
                logger.warn(`[INTEL] Real-time join failed for ${code}: ${e.message}`);
                intelCache.consecutiveFails = Number(intelCache.consecutiveFails || 0) + 1;
                const lower = String(e.message || '').toLowerCase();
                if (lower.includes('rate') || lower.includes('429') || lower.includes('too many') || lower.includes('spam')) {
                    intelCache.rateLimitHits = Number(intelCache.rateLimitHits || 0) + 1;
                }
                if (
                    Number(intelCache.rateLimitHits || 0) >= LIMITS.MAX_RATE_LIMIT_HITS ||
                    Number(intelCache.consecutiveFails || 0) >= LIMITS.MAX_CONSECUTIVE_FAILS
                ) {
                    triggerEmergencyPause(`realtime failures=${intelCache.consecutiveFails} rateHits=${intelCache.rateLimitHits}`);
                }
                // Fallback: queue this fresh link only if real-time join failed.
                if (!hasCode(code)) {
                    intelCache.pendingQueue.push({
                        code,
                        botId: String(botId || '').trim() || null,
                        queuedAt: Date.now(),
                    });
                    added++;
                }
            }
            continue;
        }

        if (!hasCode(code)) {
            intelCache.pendingQueue.push({
                code,
                botId: String(botId || '').trim() || null,
                queuedAt: Date.now(),
            });
            added++;
        }
    }
    
    if (added > 0) {
        await saveState();
        logger.info(`[INTEL] Queued ${added} new link(s). Queue: ${intelCache.pendingQueue.length}`);
        
        // Send telegram notification for queued links (throttled to once per 30 seconds)
        const throttleKey = `queue_notification`;
        const lastNotif = _linkQueueNotificationThrottle[throttleKey] || 0;
        if (Date.now() - lastNotif > 30000 && global.tgBot && ownerTelegramId) {
            _linkQueueNotificationThrottle[throttleKey] = Date.now();
            global.tgBot.telegram.sendMessage(
                ownerTelegramId,
                `📡 <b>${added} link(s) queued for auto-join</b>\n\n` +
                `⏳ Queue size: <b>${intelCache.pendingQueue.length}</b>\n` +
                `⚙️ Auto-join: ${intelCache.autoJoinEnabled ? '🟢 ACTIVE' : '🔴 INACTIVE'}`,
                { parse_mode: 'HTML' }
            ).catch(() => {});
        }
    }
});

// ── Auto-joiner daemon ────────────────────────────────────────────────────────
async function processQueue(botId = null) {
    if (!intelCache.autoJoinEnabled || !intelCache.pendingQueue.length) return;
    if (inEmergencyPause()) return;
    // When real-time mode is enabled, never drain historical queue items.
    if (intelCache.realtimeAutoJoin) return;
    checkDailyReset();

    const botKey = getBotQueueKey(botId);
    const botJoinState = getBotJoinState(botKey);
    if (botJoinState.dailyJoins >= LIMITS.MAX_JOINS_PER_DAY) return;

    const now = Date.now();
    const cooldown = LIMITS.MIN_COOLDOWN_MS + Math.random() * (LIMITS.MAX_COOLDOWN_MS - LIMITS.MIN_COOLDOWN_MS);
    if (now - Number(botJoinState.lastJoinTimestamp || 0) < cooldown) return;

    const startAt = Number(intelCache.autoJoinStartedAt) || 0;
    if (_processingByBot.get(botKey)) return;

    let queueIndex = -1;
    let entry = null;
    for (let i = 0; i < intelCache.pendingQueue.length; i++) {
        const candidate = normalizeEntry(intelCache.pendingQueue[i]);
        if (!candidate) continue;
        if (getBotQueueKey(candidate.botId) !== botKey) continue;
        if (candidate.queuedAt >= startAt) {
            queueIndex = i;
            entry = candidate;
            break;
        }
    }

    if (!entry) return;

    const sock = resolveSock(entry.botId || intelCache.autoJoinBotId);
    if (!sock) return;

    _processingByBot.set(botKey, true);
    intelCache.pendingQueue.splice(queueIndex, 1);
    intelCache.knownLinks.push(entry.code);

    try {
        await new Promise(r => setTimeout(r, 1500 + Math.random() * 2000));

        let joined = false;
        let groupJid = null;

        try {
            groupJid = await sock.groupAcceptInvite(entry.code);
            joined = true;
        } catch (e) {
            const msg = String(e.message || '').toLowerCase();
            if (msg.includes('approval') || msg.includes('request') || msg.includes('admin') || msg.includes('not-acceptable')) {
                try {
                    const info = await sock.groupGetInviteInfo(entry.code).catch(() => null);
                    if (info?.id) {
                        await sock.groupAcceptInviteV4(sock.user.id, {
                            groupJid: info.id,
                            inviteCode: entry.code,
                            inviteExpiration: info.inviteExpiration || 0
                        });
                        groupJid = info.id;
                        joined = true;
                        logger.info(`[INTEL] Join request sent to ${info.subject || info.id}`);
                    }
                } catch {}
            }
            if (!joined) throw e;
        }

        botJoinState.dailyJoins = Number(botJoinState.dailyJoins || 0) + 1;
        botJoinState.lastJoinTimestamp = Date.now();
        intelCache.botJoinState[botKey] = botJoinState;

        intelCache.dailyJoins++;
        intelCache.lastJoinTimestamp = botJoinState.lastJoinTimestamp;
        intelCache.consecutiveFails = 0;
        await upsertSharedIntel(entry.code, {
            status: 'joined',
            source: 'autojoin_queue',
            groupJid: groupJid || '',
            validatedAt: new Date(),
        });
        saveState();

        const display = groupJid || entry.code;
        logger.success(`[INTEL] Joined: ${display} (${intelCache.dailyJoins}/${LIMITS.MAX_JOINS_PER_DAY})`);

        if (global.tgBot && ownerTelegramId) {
            try {
                const meta = await sock.groupMetadata(groupJid).catch(() => null);
                const groupName = meta?.subject || 'Unknown Group';
                const memberCount = meta?.participants?.length || '?';

                global.tgBot.telegram.sendMessage(
                    ownerTelegramId,
                    `✅ <b>AUTO-JOIN SUCCESSFUL</b>\n\n` +
                    `👥 <b>${escapeHtml(groupName)}</b>\n` +
                    `🔗 <code>${entry.code}</code>\n` +
                    `👤 Members: <b>${memberCount}</b>\n` +
                    `📊 Today: <b>${intelCache.dailyJoins}/${LIMITS.MAX_JOINS_PER_DAY}</b>\n` +
                    `📋 Queue remaining: <b>${intelCache.pendingQueue.length}</b>`,
                    { parse_mode: 'HTML' }
                ).catch(() => {});
            } catch (err) {
                logger.warn(`[INTEL] Failed to notify telegram: ${err.message}`);
            }
        }
    } catch (err) {
        logger.warn(`[INTEL] Failed ${entry.code}: ${err.message}`);
        intelCache.consecutiveFails = Number(intelCache.consecutiveFails || 0) + 1;
        const lower = String(err.message || '').toLowerCase();
        if (lower.includes('rate') || lower.includes('429') || lower.includes('too many') || lower.includes('spam')) {
            intelCache.rateLimitHits = Number(intelCache.rateLimitHits || 0) + 1;
        }
        if (
            Number(intelCache.rateLimitHits || 0) >= LIMITS.MAX_RATE_LIMIT_HITS ||
            Number(intelCache.consecutiveFails || 0) >= LIMITS.MAX_CONSECUTIVE_FAILS
        ) {
            triggerEmergencyPause(`queue failures=${intelCache.consecutiveFails} rateHits=${intelCache.rateLimitHits}`);
        }
        botJoinState.lastJoinTimestamp = Date.now() - (LIMITS.MAX_COOLDOWN_MS - 5000);
        intelCache.botJoinState[botKey] = botJoinState;
        intelCache.lastJoinTimestamp = botJoinState.lastJoinTimestamp;
        saveState();
    } finally {
        _processingByBot.delete(botKey);
    }
}

// Run every 5s — faster than before (was 10s)
setInterval(() => {
    const botKeys = new Set();
    if (intelCache.autoJoinBotId) botKeys.add(getBotQueueKey(intelCache.autoJoinBotId));
    for (const item of intelCache.pendingQueue) {
        const candidate = normalizeEntry(item);
        if (candidate?.botId) botKeys.add(getBotQueueKey(candidate.botId));
    }
    if (!botKeys.size) botKeys.add(getBotQueueKey(null));
    for (const botKey of botKeys) {
        processQueue(botKey).catch(() => {});
    }
}, 5000).unref();

// ── Commands ──────────────────────────────────────────────────────────────────
module.exports = {
    category: 'INTEL',
    commands: [
        { cmd: '.autojoin',  role: 'owner' },
        { cmd: '.joinqueue', role: 'owner' },
        { cmd: '.intelclean', role: 'owner' },
    ],

    execute: async ({ sock, msg, args, text, botId }) => {
        const jid = msg.key.remoteJid;
        const cmd = text.split(' ')[0].toLowerCase();

        if (cmd === '.autojoin') {
            const action = args[0]?.toLowerCase();
            const sub = args[1]?.toLowerCase();

            if (action === 'realtime') {
                if (sub !== 'on' && sub !== 'off') {
                    return sock.sendMessage(jid, {
                        text: '⚙️ Usage: .autojoin realtime on/off'
                    }, { quoted: msg });
                }
                intelCache.realtimeAutoJoin = sub === 'on';
                saveState();
                return sock.sendMessage(jid, {
                    text: `⚙️ Realtime Auto-Join: ${intelCache.realtimeAutoJoin ? 'ON ⚡' : 'OFF 🧯'}\nRecommended: OFF for anti-ban safety.`
                }, { quoted: msg });
            }

            if (action === 'on' || action === 'off') {
                intelCache.autoJoinEnabled = action === 'on';
                intelCache.autoJoinBotId   = intelCache.autoJoinEnabled
                    ? String(botId || sock?.user?.id?.split(':')[0] || '').trim() || null
                    : null;
                // Safety default: queue mode only. Realtime joins are opt-in.
                intelCache.realtimeAutoJoin = false;
                if (intelCache.autoJoinEnabled) {
                    intelCache.autoJoinStartedAt = Date.now();
                    // Drop stale pending links so autojoin acts on newly sent links only.
                    intelCache.pendingQueue = [];
                }
                saveState();
                return sock.sendMessage(jid, {
                    text: `📡 *AUTO-JOIN:* ${intelCache.autoJoinEnabled ? 'ENGAGED 🟢' : 'OFFLINE 🔴'}\n⚡ Real-time join: ${intelCache.realtimeAutoJoin ? 'ON' : 'OFF'}\n📋 Queue: ${intelCache.pendingQueue.length} links`
                }, { quoted: msg });
            }
            return sock.sendMessage(jid, {
                text: `⚙️ Auto-Join: ${intelCache.autoJoinEnabled ? 'ENGAGED 🟢' : 'OFFLINE 🔴'}\nUsage:\n• .autojoin on/off\n• .autojoin realtime on/off`
            }, { quoted: msg });
        }

        if (cmd === '.joinqueue') {
            checkDailyReset();
            // Show first 5 queued codes
            const preview = intelCache.pendingQueue.slice(0, 5)
                .map((e, i) => `${i + 1}. ${normalizeEntry(e)?.code || '?'}`)
                .join('\n') || '_Empty_';
            
            // Count stored links across all groups
            let totalStoredLinks = 0;
            for (const links of Object.values(intelCache.groupLinks || {})) {
                totalStoredLinks += Array.isArray(links) ? links.length : 0;
            }

            return sock.sendMessage(jid, {
                text: `📡 *INTEL QUEUE*\n\n` +
                      `⏳ Pending: *${intelCache.pendingQueue.length}*\n` +
                      `💾 Stored links: *${totalStoredLinks}* across groups\n` +
                      `✅ Joined today: *${intelCache.dailyJoins}/${LIMITS.MAX_JOINS_PER_DAY}*\n` +
                      `⚙️ Status: ${intelCache.autoJoinEnabled ? 'ENGAGED 🟢' : 'OFFLINE 🔴'}\n\n` +
                      `*Next up:*\n${preview}\n\n` +
                      `<code>.intelclean</code> to validate & remove dead links`
            }, { quoted: msg });
        }
        
        if (cmd === '.intelclean') {
            try {
                await sock.sendMessage(jid, {
                    text: `🔍 *Validating & cleaning intel DB...*`
                }, { quoted: msg });
                
                let totalCleaned = 0;
                const groupJids = Object.keys(intelCache.groupLinks || {});
                
                for (const groupJid of groupJids) {
                    const removed = await cleanupGroupInvalidLinks(groupJid, sock).catch(() => 0);
                    totalCleaned += removed;
                }
                
                return sock.sendMessage(jid, {
                    text: `✅ *INTEL CLEANED*\n\n` +
                          `🗑 Removed: *${totalCleaned}* dead link(s)\n` +
                          `💾 Groups scanned: *${groupJids.length}*\n` +
                          `📊 Remaining valid: *${Object.values(intelCache.groupLinks || {}).reduce((s, l) => s + (Array.isArray(l) ? l.length : 0), 0)}*`
                }, { quoted: msg });
            } catch (err) {
                return sock.sendMessage(jid, {
                    text: `❌ Cleanup failed: ${err.message}`
                }, { quoted: msg });
            }
        }
    }
};
