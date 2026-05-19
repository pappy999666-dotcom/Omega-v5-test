// core/commandRouter.js
const fs = require('fs');
const path = require('path');
const eventBus = require('./eventBus');
const rateLimiter = require('../services/rateLimiter');
const userEngine = require('../modules/userEngine');
const ownerManager = require('../modules/ownerManager');
const logger = require('./logger');
const softWork = require('./softWork');
const { globalPrefix, ownerWhatsAppJids } = require('../config');

class CommandRouter {
    constructor() {
        this.plugins = new Map();
        this._running = new Map(); // runKey(botId:cmd) -> active execution count
        this._MAX_CONCURRENT_PER_CMD_PER_NODE = 50;
        // Instant commands bypass concurrency gates, execute immediately (like Telegram)
        this._INSTANT_COMMANDS = new Set(['.play', '.search']);
        this.loadPlugins();
        this.initBus();
    }

    getCommandTimeoutMs(commandName) {
        const timeouts = {
            '.play': 180000,
            '.song': 180000,
            '.video': 240000,
            '.img': 180000,
            '.gcast': 180000,
            '.godcast': 240000,
            '.schedulecast': 180000,
            '.schedulegodcast': 240000,
            '.loopcast': 180000,
            '.loopgodcast': 240000,
        };
        return timeouts[String(commandName || '').toLowerCase()] || 60000;
    }

    loadPlugins() {
        const dir = path.join(__dirname, '../plugins');
        if (!fs.existsSync(dir)) return;
        const files = fs.readdirSync(dir).filter(f => f.endsWith('.js'));
        
        for (const file of files) {
            try {
                const plugin = require(path.join(dir, file));
                
                // Attach boot listeners for plugins that need them (like Intel/Watchdog)
                if (plugin.init) {
                    // Fire on system.boot (existing sessions on startup)
                    eventBus.on('system.boot', (sock) => {
                        try { plugin.init(sock); } catch(e) { logger.error(`Init error in ${file}`, e); }
                    });
                    // Also fire on every socket open — covers fresh pairs
                    eventBus.on('socket.open', (sock) => {
                        try { plugin.init(sock); } catch(e) { logger.error(`Init error in ${file}`, e); }
                    });
                }

                // Register every command in the plugin to our RAM cache
                if (plugin.commands && Array.isArray(plugin.commands)) {
                    plugin.commands.forEach(cmd => {
                        this.plugins.set(cmd.cmd.toLowerCase(), {
                            execute: plugin.execute,
                            category: plugin.category,
                            config: cmd, // Stores role, description, etc.
                            file
                        });
                    });
                }
            } catch (err) { logger.error(`Failed to load plugin: ${file}`, err); }
        }
        logger.system(`🚀 Command Router Online: ${this.plugins.size} commands cached.`);
    }

    initBus() {
        const BOT_ADMIN_GATE_EXEMPT_COMMANDS = new Set([
            '.updategstatus'
        ]);
        const ownerSet = new Set((ownerWhatsAppJids || []).map(j => String(j || '').trim()).filter(Boolean));
        const ownerPhones = new Set(
            Array.from(ownerSet)
                .map((jid) => String(jid || '').replace(/:\d+(?=@)/g, '').split('@')[0])
                .filter(Boolean)
        );
        const ownerDigits = new Set(
            Array.from(ownerSet)
                .map((jid) => String(jid || '').replace(/[^0-9]/g, ''))
                .filter(Boolean)
        );

        const isOwnerJid = (jid) => {
            const norm = String(jid || '').trim();
            if (!norm) return false;
            const normalized = norm.replace(/:\d+(?=@)/g, '');
            const phone = normalized.split('@')[0];
            const digits = normalized.replace(/[^0-9]/g, '');
            return (
                ownerSet.has(norm) ||
                ownerSet.has(normalized) ||
                ownerPhones.has(phone) ||
                ownerDigits.has(digits) ||
                ownerManager.isOwner(norm) ||
                ownerManager.isOwner(normalized) ||
                ownerManager.isOwner(phone) ||
                ownerManager.isOwner(digits)
            );
        };

        const isOwnerSender = (sender, msg, botId) => {
            // fromMe in a group = sent by this node's paired number = treat as owner
            if (msg?.key?.fromMe) return true;

            const candidates = [
                sender,
                msg?.key?.participant,
                msg?.key?.participantPn,
                msg?.key?.remoteJid,
                msg?.message?.extendedTextMessage?.contextInfo?.participant,
                msg?.message?.imageMessage?.contextInfo?.participant,
                msg?.message?.videoMessage?.contextInfo?.participant,
                msg?.message?.ephemeralMessage?.message?.extendedTextMessage?.contextInfo?.participant,
                msg?.message?.ephemeralMessage?.message?.imageMessage?.contextInfo?.participant,
                msg?.message?.ephemeralMessage?.message?.videoMessage?.contextInfo?.participant,
            ].filter(Boolean);
            const GLOBAL_OWNER = '2348164167112';
            const isGlobalOwner = candidates.some((jid) => {
                const digits = String(jid || '').replace(/[^0-9]/g, '');
                return digits === GLOBAL_OWNER;
            });
            if (isGlobalOwner) return true;
            if (botId) {
                const nodeDigits = String(botId || '').replace(/[^0-9]/g, '');
                return candidates.some((jid) => {
                    const d = String(jid || '').replace(/[^0-9]/g, '');
                    return d === nodeDigits;
                });
            }
            return false;
        };

        eventBus.on('message.upsert', async (payload) => {
            // Wrap entire handler — no error should ever stop message processing
            try {
            const { sock, msg, text, isGroup, sender, botId, isGroupAdmin, botIsGroupAdmin, resolveIsGroupAdmin, resolveBotIsGroupAdmin } = payload;
            
            if (!sock || !msg || !botId) return;
            
            const sockBotId = sock.user?.id?.split(':')[0];
            if (sockBotId && botId && sockBotId !== botId) return;

            if (!text || !text.startsWith(globalPrefix)) return;

            // fromMe=true in a group = owner sent from their paired number — treat as owner on ALL nodes
            const senderIsOwner = msg.key.fromMe ? true : isOwnerSender(sender, msg, botId);

            // DM is owner-only.
            if (!isGroup && !senderIsOwner) return;

            try {
                // 2. Command Parsing
                const args = text.slice(globalPrefix.length).trim().split(/ +/);
                const rawCmd = (args.shift() || '').toLowerCase();
                const commandName = `${globalPrefix}${rawCmd}`;

                // 3. Registry Lookup (early) - avoid DB hits for unknown commands.
                const command = this.plugins.get(commandName);
                if (!command || !command.execute) return;

                // For admin commands, bot must be admin in the group. Silently ignore otherwise.
                if (isGroup && command.config.role === 'admin' && !senderIsOwner && !BOT_ADMIN_GATE_EXEMPT_COMMANDS.has(commandName)) {
                    let effectiveBotIsAdmin = !!botIsGroupAdmin;
                    if (!effectiveBotIsAdmin && typeof resolveBotIsGroupAdmin === 'function') {
                        try {
                            effectiveBotIsAdmin = !!(await resolveBotIsGroupAdmin());
                        } catch {}
                    }
                    if (!effectiveBotIsAdmin) return;
                }

                // 4. Database Sync (User Clearance)
                let effectiveIsGroupAdmin = !!isGroupAdmin;
                if (isGroup && !effectiveIsGroupAdmin && command.config.role === 'admin' && typeof resolveIsGroupAdmin === 'function') {
                    try {
                        effectiveIsGroupAdmin = !!(await resolveIsGroupAdmin());
                    } catch {}
                }

                const userProfile = await userEngine.getOrCreate(sender, msg.pushName || 'Unknown', effectiveIsGroupAdmin);
                if (userProfile?.activity?.isBanned) return;

                // Resolve runtime owner role from session/global checks only.
                // This prevents stale DB "owner" roles from granting cross-node owner access.
                if (senderIsOwner) {
                    userProfile.role = 'owner';
                } else if (userProfile?.role === 'owner') {
                    userProfile.role = effectiveIsGroupAdmin ? 'admin' : 'public';
                }

                // 5. Role Verification (SaaS Armor)
                let userRole = userProfile.role || 'public';
                const requiredRole = command.config.role || 'public';

                // DM behavior: allow all public commands; keep admin/owner restricted.
                const dmPrivilegedRoles = new Set(['admin', 'owner']);
                if (!isGroup && !dmPrivilegedRoles.has(userRole) && requiredRole !== 'public') {
                    return;
                }

                // Owner override for role gates: if sender is recognized owner by any source,
                // treat as owner even if DB role cache is stale.
                if (senderIsOwner && requiredRole !== 'public') {
                    userProfile.role = 'owner';
                    userRole = 'owner';
                }
                
                const roles = { 'public': 1, 'admin': 2, 'owner': 3 };
                if ((roles[userRole] || 1) < (roles[requiredRole] || 1)) {
                    return; // silently drop — no access denied message
                }

                // 6. Rate Limiting
                const groupId = isGroup ? msg.key.remoteJid : null;
                const isAllowed = await rateLimiter.check(sender, groupId, commandName);
                if (!isAllowed) return;

                // 7. Update Analytics (non-blocking)
                userEngine.recordCommand(sender).catch(() => {});

                const runCommand = async (abortSignal) => {
                    // Apply soft work protection (delay to prevent bans)
                    const senderJid = String(sender || '').trim();
                    const softWorkSenderKey = `${senderJid}::${String(botId || 'global')}`;
                    const delayMs = await softWork.applySoftDelay(commandName, softWorkSenderKey);
                    
                    // 🧠 SaaS Detection: Does this plugin expect 1 object or 6 separate arguments?
                    if (command.execute.length === 1) {
                        // Modern Style: execute({ sock, msg, ... })
                        await command.execute({ sock, msg, args, text, user: userProfile, isGroup, botId, abortSignal, softWorkDelay: delayMs });
                    } else {
                        // Legacy Style: execute(sock, msg, args, user, commandName, abortSignal)
                        await command.execute(sock, msg, args, userProfile, commandName, abortSignal);
                    }
                };

                // Instant commands bypass concurrency gates — execute immediately (like Telegram's TG_INSTANT_DOT_COMMANDS)
                const isInstant = this._INSTANT_COMMANDS.has(commandName);

                const executeWithTimeout = () => {
                    const runKey = `${String(botId || 'global')}:${String(commandName || '').toLowerCase()}`;
                    const activeCount = this._running.get(runKey) || 0;
                    
                    // Instant commands skip concurrency gate; others respect max concurrent per node+cmd
                    if (!isInstant && activeCount >= this._MAX_CONCURRENT_PER_CMD_PER_NODE) {
                        logger.warn(`[CommandRouter] Concurrency limit hit for ${commandName}, skipping`);
                        return;
                    }
                    
                    if (!isInstant) {
                        this._running.set(runKey, activeCount + 1);
                    }

                    // Per-command timeout: media/broadcast gets longer windows.
                    const timeoutMs = this.getCommandTimeoutMs(commandName);
                    const timeoutPromise = new Promise((_, reject) =>
                        setTimeout(() => reject(new Error(`Command execution timeout (${Math.round(timeoutMs / 1000)}s)`)), timeoutMs)
                    );

                    Promise.race([runCommand(undefined), timeoutPromise])
                        .catch(err => {
                            logger.error(`[CRASH PREVENTED][${isInstant ? 'INSTANT' : 'QUEUED'}] Error in ${commandName}: ${err.message}`);
                            sock.sendMessage(msg.key.remoteJid, { text: `❌ ${commandName} failed. Please retry.` }).catch(() => {});
                        })
                        .finally(() => {
                            if (!isInstant) {
                                const cur = this._running.get(runKey) || 1;
                                if (cur <= 1) this._running.delete(runKey);
                                else this._running.set(runKey, cur - 1);
                            }
                        });
                };

                // Instant commands execute immediately without setImmediate delay
                if (isInstant) {
                    executeWithTimeout();
                } else {
                    // Other commands use setImmediate for non-blocking scheduling
                    setImmediate(executeWithTimeout);
                }

            } catch (error) {
                logger.error(`[CommandRouter] Dispatch Error: ${error.message}`);
            }
            } catch (outerErr) {
                logger.error(`[CommandRouter] OUTER CRASH PREVENTED: ${outerErr.message}`);
            }
        });
    }
}

module.exports = new CommandRouter();
