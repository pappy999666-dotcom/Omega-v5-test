'use strict';

const os = require('os');
const { performance } = require('perf_hooks');

class CoreHealthMonitor {
    constructor({ logger, lifecycle, metrics, socketManager, cacheManager, reconnectManager }) {
        this.logger = logger;
        this.lifecycle = lifecycle;
        this.metrics = metrics;
        this.socketManager = socketManager;
        this.cacheManager = cacheManager;
        this.reconnectManager = reconnectManager;
        this.lastTick = performance.now();
    }

    start() {
        this.lifecycle.addInterval('core-health', () => this.tick(), 15000);
    }

    tick() {
        const now = performance.now();
        const lagMs = Math.max(0, now - this.lastTick - 15000);
        this.lastTick = now;

        const mem = process.memoryUsage();
        const rssMb = Math.round(mem.rss / (1024 * 1024));
        const cpuCount = os.cpus()?.length || 1;

        this.metrics?.setGauge?.('health.eventLoopLagMs', lagMs);
        this.metrics?.setGauge?.('health.rssMb', rssMb);
        this.metrics?.setGauge?.('health.cpuCount', cpuCount);

        this.cacheManager?.sweep?.();
        this.socketManager?.cleanupZombies?.();

        if (rssMb > 1200) {
            this.logger?.warn?.(`[Health] High RSS: ${rssMb}MB`);
        }
        if (lagMs > 500) {
            this.logger?.warn?.(`[Health] Event loop lag spike: ${Math.round(lagMs)}ms`);
        }
    }
}

module.exports = { CoreHealthMonitor };
