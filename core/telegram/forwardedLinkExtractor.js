/**
 * 🔄 TELEGRAM FORWARDED MESSAGE LINK EXTRACTOR
 * 
 * Captures WhatsApp group links from Telegram forwarded messages
 * Validates links and stores them in Intel DB for cross-node access
 * 
 * Flow:
 * 1. User forwards a message containing WhatsApp links
 * 2. Extract all links from message text
 * 3. Pass through link validator
 * 4. Store valid links to Intel DB per-node
 * 5. Reply with confirmation + count/status
 */

const logger = require('../logger');

const WHATSAPP_LINK_PATTERNS = [
    // Standard invite formats
    /https?:\/\/chat\.whatsapp\.com\/([A-Za-z0-9_\-]+)/gi,
    // Short links
    /https?:\/\/wa\.me\/invite\/([A-Za-z0-9_\-]+)/gi,
    // Group codes (if shared as plain text)
    /^([A-Za-z0-9]{22,})$/gm,
];

/**
 * Extract WhatsApp invitation codes from text
 * @param {string} text - Message text to search
 * @returns {string[]} - Array of unique invitation codes
 */
function extractWhatsAppCodes(text) {
    const text_str = String(text || '').trim();
    const codes = new Set();

    // Try each pattern
    for (const pattern of WHATSAPP_LINK_PATTERNS) {
        let match;
        while ((match = pattern.exec(text_str)) !== null) {
            if (match[1]) {
                codes.add(String(match[1]));
            }
        }
    }

    return Array.from(codes);
}

/**
 * Validate WhatsApp invite code
 * @param {string} code - Invitation code
 * @param {object} sock - Baileys socket with groupGetInviteInfo
 * @returns {object} - { valid: bool, info: object }
 */
async function validateWhatsAppCode(code, sock) {
    try {
        if (!code || !sock?.groupGetInviteInfo) {
            return { valid: false, info: null };
        }

        const info = await sock.groupGetInviteInfo(code);
        if (!info || !info.id) {
            return { valid: false, info: null };
        }

        return {
            valid: true,
            info: {
                id: info.id,
                subject: info.subject || '',
                size: info.size || 0,
                creator: info.creator || '',
                rejoinable: info.rejoinable !== false,
                restricted: info.restricted || false,
            }
        };
    } catch (err) {
        // Link expired or invalid
        logger.debug('[ForwardedLinkExtractor] Code validation failed', { code, error: err.message });
        return { valid: false, info: null };
    }
}

/**
 * Process forwarded message containing WhatsApp links
 * @param {object} ctx - Telegraf context
 * @param {object} sock - Active WhatsApp socket
 * @param {string} sessionKey - Node session key
 * @param {object} intelCache - Intel cache object (inMemory)
 * @returns {object} - Result with { extracted, valid, invalid, saved }
 */
async function processForwardedLinks(ctx, sock, sessionKey, intelCache) {
    const result = {
        extracted: 0,
        valid: 0,
        invalid: 0,
        saved: 0,
        duplicates: 0,
        errors: [],
    };

    try {
        // Get text from message
        let text = ctx.message?.text || ctx.message?.caption || '';
        text = String(text || '').trim();

        if (!text) {
            result.errors.push('No text or caption in message');
            return result;
        }

        // Extract codes
        const codes = extractWhatsAppCodes(text);
        if (!codes.length) {
            result.errors.push('No WhatsApp links found in message');
            return result;
        }

        result.extracted = codes.length;

        // Validate each code
        for (const code of codes) {
            const { valid, info } = await validateWhatsAppCode(code, sock);

            if (!valid) {
                result.invalid += 1;
                continue;
            }

            result.valid += 1;

            // Check for duplicates in Intel cache
            const groupJid = info.id || code;
            if (intelCache.groupLinks && intelCache.groupLinks[groupJid]) {
                result.duplicates += 1;
                continue;
            }

            // Store to Intel cache
            try {
                if (!intelCache.groupLinks) intelCache.groupLinks = {};
                if (!intelCache.groupLinks[groupJid]) {
                    intelCache.groupLinks[groupJid] = [];
                }

                intelCache.groupLinks[groupJid].push({
                    code,
                    validatedAt: Date.now(),
                    status: 'valid',
                    seenAt: Date.now(),
                    groupName: info.subject || '',
                    members: info.size || 0,
                    source: 'telegram_forward',
                });

                result.saved += 1;
            } catch (err) {
                logger.warn('[ForwardedLinkExtractor] Failed to save link', { code, error: err.message });
                result.errors.push(`Failed to save ${code.slice(0, 8)}...`);
            }
        }

        return result;
    } catch (err) {
        logger.error('[ForwardedLinkExtractor] Processing failed', { error: err.message });
        result.errors.push(err.message);
        return result;
    }
}

/**
 * Format result message for user
 * @param {object} result - Processing result from processForwardedLinks
 * @returns {string} - Formatted HTML message
 */
function formatResultMessage(result) {
    const lines = [
        '📥 <b>FORWARDED LINK EXTRACTION</b>',
        '',
        `📊 <b>Results:</b>`,
        `  Extracted: <b>${result.extracted}</b> link(s)`,
        `  ✅ Valid: <b>${result.valid}</b>`,
        `  ❌ Invalid/Expired: <b>${result.invalid}</b>`,
        `  💾 Saved to Intel DB: <b>${result.saved}</b>`,
        `  ⚠️ Duplicates (already stored): <b>${result.duplicates}</b>`,
    ];

    if (result.errors.length > 0) {
        lines.push('');
        lines.push('<b>⚠️ Issues:</b>');
        result.errors.slice(0, 3).forEach(err => {
            lines.push(`  • ${escapeHtml(err)}`);
        });
    }

    if (result.saved === 0 && result.invalid === 0 && result.duplicates === 0) {
        lines.push('');
        lines.push('💡 <i>No new links were found or saved. Links must be valid WhatsApp group invites.</i>');
    } else if (result.saved > 0) {
        lines.push('');
        lines.push(`✅ <b>Saved ${result.saved} new link(s) to Intel database!</b>`);
        lines.push('<i>Other nodes can now join these groups when ready.</i>');
    }

    return lines.join('\n');
}

function escapeHtml(str) {
    return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

module.exports = {
    extractWhatsAppCodes,
    validateWhatsAppCode,
    processForwardedLinks,
    formatResultMessage,
};
