// core/linkValidator.js
// 🔗 COMPREHENSIVE LINK VALIDATION & INTEL DB WATCHER

'use strict';

const fs = require('fs');
const path = require('path');
const { isLinkValid } = require('./linkPreview');
const logger = require('./logger');

const INTEL_DB_PATH = path.join(__dirname, '../data/intel.json');
const VALIDATION_CACHE_PATH = path.join(__dirname, '../data/link-validation-cache.json');
const VALIDATION_TTL = 24 * 60 * 60 * 1000; // 24 hours
const CHECK_INTERVAL = 60 * 60 * 1000; // Check every 1 hour

let validationCache = {};
let _checkRunning = false;

/**
 * Load and initialize validation cache
 */
async function initValidationCache() {
    try {
        if (fs.existsSync(VALIDATION_CACHE_PATH)) {
            const data = JSON.parse(await fs.promises.readFile(VALIDATION_CACHE_PATH, 'utf8'));
            validationCache = data || {};
        }
    } catch (err) {
        logger.warn(`[LinkValidator] Failed to load validation cache: ${err.message}`);
    }
}

/**
 * Save validation cache to disk
 */
async function saveValidationCache() {
    try {
        await fs.promises.writeFile(VALIDATION_CACHE_PATH, JSON.stringify(validationCache, null, 2));
    } catch (err) {
        logger.warn(`[LinkValidator] Failed to save validation cache: ${err.message}`);
    }
}

/**
 * Check if a WhatsApp group link is valid
 * Returns { valid: boolean, expired: boolean, error: string|null }
 */
async function validateGroupLink(code, sock = null) {
    if (!code || typeof code !== 'string') {
        return { valid: false, expired: false, error: 'Invalid code format' };
    }

    // Check cache first
    const cacheKey = `wa_${code}`;
    const cached = validationCache[cacheKey];
    if (cached && Date.now() - cached.checkedAt < VALIDATION_TTL) {
        return { valid: cached.valid, expired: cached.expired, error: cached.error };
    }

    try {
        const url = `https://chat.whatsapp.com/${code}`;
        const isValid = await isLinkValid(url, 'whatsapp', sock);

        // If socket available, try groupGetInviteInfo for detailed status
        let isExpired = false;
        if (sock && typeof sock.groupGetInviteInfo === 'function') {
            try {
                const info = await sock.groupGetInviteInfo(code);
                if (!info?.id) isExpired = true;
            } catch (e) {
                const msg = String(e.message || '').toLowerCase();
                if (msg.includes('expired') || msg.includes('not-found')) {
                    isExpired = true;
                }
            }
        }

        // Cache the result
        validationCache[cacheKey] = {
            code,
            valid: isValid,
            expired: isExpired,
            error: null,
            checkedAt: Date.now(),
        };

        return { valid: isValid && !isExpired, expired: isExpired, error: null };
    } catch (err) {
        const error = err.message;
        validationCache[cacheKey] = {
            code,
            valid: false,
            expired: false,
            error,
            checkedAt: Date.now(),
        };
        return { valid: false, expired: false, error };
    }
}

/**
 * Batch validate a list of WhatsApp group codes
 * Returns { valid: [], invalid: [], expired: [] }
 */
async function validateBatch(codes, sock = null) {
    if (!Array.isArray(codes)) return { valid: [], invalid: [], expired: [] };

    const results = {
        valid: [],
        invalid: [],
        expired: [],
    };

    for (const code of codes) {
        const { valid, expired, error } = await validateGroupLink(code, sock);
        if (expired) {
            results.expired.push(code);
        } else if (valid) {
            results.valid.push(code);
        } else {
            results.invalid.push(code);
        }
    }

    await saveValidationCache();
    return results;
}

/**
 * Watch and validate all links in Intel DB
 * Removes expired/invalid links and transfers valid ones to validated list
 */
async function watchAndCleanupIntelDB(sock = null) {
    if (_checkRunning) return;
    _checkRunning = true;

    try {
        if (!fs.existsSync(INTEL_DB_PATH)) {
            _checkRunning = false;
            return;
        }

        const intelData = JSON.parse(await fs.promises.readFile(INTEL_DB_PATH, 'utf8'));
        let changed = false;

        // Check groupLinks entries
        if (intelData.groupLinks && typeof intelData.groupLinks === 'object') {
            for (const [groupJid, links] of Object.entries(intelData.groupLinks)) {
                if (!Array.isArray(links)) continue;

                const codesToCheck = links.map(l => l.code || l);
                const { valid, expired } = await validateBatch(codesToCheck, sock);

                // Keep only valid links
                intelData.groupLinks[groupJid] = links.filter(l => {
                    const code = l.code || l;
                    return valid.includes(code);
                });

                if (expired.length > 0) {
                    logger.warn(`[LinkValidator] Removed ${expired.length} expired links from group ${groupJid}`);
                    changed = true;
                }

                // Remove empty group entries
                if (intelData.groupLinks[groupJid].length === 0) {
                    delete intelData.groupLinks[groupJid];
                    changed = true;
                }
            }
        }

        // Check pendingQueue entries
        if (Array.isArray(intelData.pendingQueue)) {
            const codesToCheck = intelData.pendingQueue.map(e => e.code || e);
            const { valid, expired } = await validateBatch(codesToCheck, sock);

            intelData.pendingQueue = intelData.pendingQueue.filter(e => {
                const code = e.code || e;
                return valid.includes(code);
            });

            if (expired.length > 0) {
                logger.warn(`[LinkValidator] Removed ${expired.length} expired links from pending queue`);
                changed = true;
            }
        }

        // Check knownLinks array
        if (Array.isArray(intelData.knownLinks)) {
            const { valid, expired } = await validateBatch(intelData.knownLinks, sock);
            intelData.knownLinks = valid;

            if (expired.length > 0) {
                logger.warn(`[LinkValidator] Removed ${expired.length} expired links from knownLinks`);
                changed = true;
            }
        }

        // Save if changed
        if (changed) {
            await fs.promises.writeFile(INTEL_DB_PATH, JSON.stringify(intelData, null, 2));
            logger.success(`[LinkValidator] Intel DB cleaned and saved`);
        }
    } catch (err) {
        logger.error(`[LinkValidator] Error during cleanup: ${err.message}`);
    } finally {
        _checkRunning = false;
    }
}

/**
 * Start periodic validation watcher
 */
function startValidator(sock = null) {
    initValidationCache().catch(() => {});

    // Run initial cleanup
    watchAndCleanupIntelDB(sock).catch(() => {});

    // Run periodic checks
    setInterval(() => {
        watchAndCleanupIntelDB(sock).catch(() => {});
    }, CHECK_INTERVAL).unref();

    logger.success(`[LinkValidator] Validator started (checks every ${CHECK_INTERVAL / 60000} minutes)`);
}

module.exports = {
    validateGroupLink,
    validateBatch,
    watchAndCleanupIntelDB,
    startValidator,
    initValidationCache,
    saveValidationCache,
};
