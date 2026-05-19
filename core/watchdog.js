// core/watchdog.js
'use strict';

const logger = require('./logger');
const taskManager = require('./taskManager');

const PING_THRESHOLD_RATIO = 0.5;
const HEALTH_INTERVAL_MS   = 30000;  // check every 30s instead of 60s
const MEMORY_WARN_MB       = 800;    // warn earlier at 800MB

class SmartWatchdog {
    constructor(timeoutMs = 120000) {
        this.timeoutMs = timeoutMs;
        // Each session gets its own isolated monitor — no shared state
        this.monitors = new Map();
        this.silentCounts = new Map();

        this.healthInterval = setInterval(() => this._runDiagnostics(), HEALTH_INTERVAL_MS);
        this.healthInterval.unref(); // don't block process exit
    }

    detach(botId) {
        const m = this.monitors.get(botId);
        if (!m) return;
        clearInterval(m.interval);
        this.monitors.delete(botId);
    }

    _touch(botId) {
        const m = this.monitors.get(botId);
        if (m) m.lastSeen = Date.now();
    }

    attach(botId, sock, restartCallback) {
        this.detach(botId);

        const monitor = {
            botId,
            lastSeen: Date.now(),
            sock,
            restartCallback,
            // Each session has its own independent interval — no cross-session blocking
            interval: setInterval(() => this._check(botId), 30000),
        };
        monitor.interval.unref();

        this.monitors.set(botId, monitor);

        // Update lastSeen on any WS message — isolated per socket
        try {
            sock.ws.on('message', () => this._touch(botId));
        } catch {}

        logger.info(`[WATCHDOG] Attached to session: ${botId}`);
    }

    _check(botId) {
        const m = this.monitors.get(botId);
        if (!m) return;
        const idle = Date.now() - m.lastSeen;

        try {
            if (idle > this.timeoutMs * PING_THRESHOLD_RATIO) {
                try {
                    const pingFn = m?.sock?.ws?.ping;
                    if (typeof pingFn === 'function') pingFn.call(m.sock.ws);
                } catch (e) {
                    logger.error(`[WATCHDOG] Ping failed: ${e.message}`);
                }
            }

            if (idle > this.timeoutMs) {
                logger.error(`[WATCHDOG] Zombie detected: ${botId} (idle ${Math.round(idle / 1000)}s). Restarting...`);
                this.detach(botId);
                try { m.restartCallback(); } catch (e) { logger.error(`[WATCHDOG] Restart callback failed for ${botId}: ${e.message}`); }
            }
        } catch (err) {
            logger.error(`[WATCHDOG] Uncaught error in _check: ${err.message}`);
        }
    }

    // (removed duplicate code)

    _runDiagnostics() {
        const stats = taskManager.getStats();
        const memMB = Math.round(process.memoryUsage().rss / 1024 / 1024);

        if (stats.queued > 100 && stats.running >= taskManager.concurrency) {
            logger.warn('[WATCHDOG] Queue congestion — flushing low-priority tasks');
            taskManager.queue = taskManager.queue.filter(j => j.priority >= 3);
        }

        if (memMB > MEMORY_WARN_MB) {
            logger.error(`[WATCHDOG] High memory: ${memMB}MB — forcing GC`);
            if (global.gc) global.gc();
        }

        // Self-healing: restart silent sockets that haven't received messages in 10 min
        // but only if they were previously active (have a monitor)
        const now = Date.now();
        const SILENCE_THRESHOLD = 5 * 60 * 1000; // 5 min (was 10)
        const SILENCE_ESCALATE_COUNT = 3;
        if (global._lastMsgActivity && global.waSocks) {
            for (const [sessionKey, sock] of global.waSocks.entries()) {
                if (!sock?.user) continue; // not connected yet
                const lastActivity = global._lastMsgActivity.get(sessionKey) || 0;
                const silent = now - lastActivity;
                if (lastActivity > 0 && silent > SILENCE_THRESHOLD) {
                    const count = (this.silentCounts.get(sessionKey) || 0) + 1;
                    this.silentCounts.set(sessionKey, count);
                    logger.warn(`[WATCHDOG] Session ${sessionKey} silent for ${Math.round(silent/60000)}min — pinging (strike ${count})`);
                    try { sock.ws?.ping?.(); } catch {}
                    if (count >= SILENCE_ESCALATE_COUNT) {
                        logger.error(`[WATCHDOG] Session ${sessionKey} still silent after ${count} checks — forcing reconnect`);
                        this.silentCounts.set(sessionKey, 0);
                        try { sock.ws?.close?.(); } catch {}
                    }
                } else {
                    this.silentCounts.set(sessionKey, 0);
                }
            }
        }

        logger.info(`[WATCHDOG] Health: sessions=${this.monitors.size} mem=${memMB}MB queue=${stats.queued} running=${stats.running}`);
    }
}

module.exports = new SmartWatchdog();
// Bind all methods so 'this' is never lost when called as callbacks
const _wd = module.exports;
['attach', 'detach', '_check', '_touch', '_runDiagnostics'].forEach(m => {
    if (typeof _wd[m] === 'function') _wd[m] = _wd[m].bind(_wd);
});
