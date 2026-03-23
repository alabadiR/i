'use strict';

const { chromium } = require('playwright');
const nodemailer   = require('nodemailer');
const { google }   = require('googleapis');
const fs           = require('fs');
const path         = require('path');

// ─────────────────────────────────────────────
//  STEALTH — session fingerprint pool
// ─────────────────────────────────────────────

const UA_POOL = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];

// sec-ch-ua templates keyed by browser type
const SEC_CH_UA_MAP = {
    chrome: (v) => `"Chromium";v="${v}", "Google Chrome";v="${v}", "Not-A.Brand";v="99"`,
    edge:   (v) => `"Chromium";v="${v}", "Microsoft Edge";v="${v}", "Not-A.Brand";v="99"`,
};

// Real-world common resolutions (W3Schools counter 2024)
const SCREEN_POOL = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768  },
    { width: 1440, height: 900  },
    { width: 1536, height: 864  },
    { width: 1280, height: 720  },
];

// Extract sec-ch-ua dynamically from UA string.
// Static version map breaks silently when new Chrome versions are added.
function buildSecChUa(ua) {
    const edgeMatch   = ua.match(/Edg\/(\d+)/);
    if (edgeMatch)   return SEC_CH_UA_MAP.edge(edgeMatch[1]);

    const chromeMatch = ua.match(/Chrome\/(\d+)/);
    if (chromeMatch) return SEC_CH_UA_MAP.chrome(chromeMatch[1]);

    return null; // Firefox — intentionally omits sec-ch-ua
}

// Picks a UA + viewport pair for one browser session
function pickSession() {
    const ua     = UA_POOL[Math.floor(Math.random() * UA_POOL.length)];
    const screen = SCREEN_POOL[Math.floor(Math.random() * SCREEN_POOL.length)];
    return { ua, screen, secChUa: buildSecChUa(ua) };
}

// Common referers a real user might arrive from.
// Real browsers almost always carry a Referer — its absence is an automation signal.
// Values are generic search/social origins, not site-specific.
const REFERER_POOL = [
    'https://www.google.com/',
    'https://www.google.com/search?q=',
    'https://t.co/',
    'https://l.instagram.com/',
    'https://www.facebook.com/',
    '',  // ~15 % of real sessions have no referer (direct navigation)
    '',
];

function pickReferer() {
    return REFERER_POOL[Math.floor(Math.random() * REFERER_POOL.length)];
}

// ─────────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────────

const CONFIG = {
    rounds:           300,
    logEvery:         50,
    minPerCycle:      20,
    cycleDelayMin:    2_000,
    cycleDelayMax:    5_000,
    resetAfterMin:    15,
    resetAfterMax:    20,
    snapshotEvery:    25,
    startDelayMax:    30_000, // random warm-up delay before first cycle (0–30s)

    cookieRetries:    3,
    cookieRetryDelay: 5_000,

    timezone:    '',
    dailyHour:   23,
    reportHours: [0, 6, 12, 18],

    email:   '',
    logsDir: 'logs',

    items: [],

    delays: {
        pageLoad:        [2000, 3000],
        scrollReveal:    [150,  350],
        mouseSettle:     [40,   100],
        afterFirstBtn:   [600,  900],
        beforeSecondBtn: [200,  350],
        afterSecondBtn:  [600,  900],
        beforeSave:      [800,  1200],
        afterSave:       [1200, 1800],
        hoverSave:       [200,  500],
        clickDelay:      [50,   150],
        beforeNext:      [0,    80],
        beforeSkip:      [0,    80],
    },

    scheduler: {
        ok:               [90_000,    180_000],
        error:            [60_000,    120_000],
        failOdd:          [60_000,     75_000],
        failEven:         [75_000,     90_000],
        cooldown:         [600_000,   900_000],
        cooldownExtended: [1_800_000, 3_600_000],
        noReadyItemWait:  [10_000,     20_000],
    },

    extendedCooldownThreshold: 15,

    locale: '',

    selectors: {
        listboxBtn: 'button[aria-haspopup="listbox"]',
        option1:    '',
        option2:    '',
        saveBtn:    '',
    },
};

// ─────────────────────────────────────────────
//  STATE
// ─────────────────────────────────────────────

let BATCH_DATE      = null;
let driveClient     = null;
let mailTransporter = null;

// Per-item scheduler state — persists for the full lifetime of one script run.
// nextCheck  : timestamp after which the item is eligible for processing
// failStreak : consecutive pass count — resets on ok or when cooldown activates
// totalFail  : lifetime pass count — never resets, drives extended cooldown logic
// cooldown   : true while the item is in its rest period
const itemState = {};

const batchStats = {
    totalCycles:   0,
    totalI:        0,
    ok:            0,
    pass:          0,
    errors:        0,
    resets:        0,
    sessionChecks: 0,
    driveUploads:  0,
    cooldowns:     0,
    startTime:     null,
};

// ─────────────────────────────────────────────
//  UTILITIES
// ─────────────────────────────────────────────

const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

const sleepRand = ([min, max]) => new Promise(res => setTimeout(res, rand(min, max)));
const sleepMs   = (ms)         => new Promise(res => setTimeout(res, ms));

const tz        = () => ({ timeZone: CONFIG.timezone || 'UTC' });
const timeStrEN = () => new Date().toLocaleTimeString('en-CA', { ...tz(), hour12: false });
const dateStr   = () => new Date().toLocaleDateString('en-CA', tz());
const hourNow   = () => new Date(new Date().toLocaleString('en', tz())).getHours();

const norm = (v) => String(v ?? '').trim();

// Anchored, case-insensitive regex — prevents partial matches on option text.
// Without anchors a short word can match longer options containing it as a substring.
function buildRegex(text) {
    const escaped = text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`^${escaped}$`, 'i');
}

// Short label for console/log — numeric ID only, no full URL exposed.
function itemLabel(item) {
    const m = item.url.match(/\/(\d{8,})\//);
    return m ? `I#${item.num}[${m[1]}]` : `I#${item.num}`;
}

// ETA estimate based on average cycle duration so far.
function calcETA() {
    if (!batchStats.startTime || batchStats.totalCycles === 0) return '--:--:--';
    const elapsed   = Date.now() - batchStats.startTime;
    const perCycle  = elapsed / batchStats.totalCycles;
    const remaining = (CONFIG.rounds - batchStats.totalCycles) * perCycle;
    return new Date(Date.now() + remaining)
        .toLocaleTimeString('en-CA', { ...tz(), hour12: false });
}

// ─────────────────────────────────────────────
//  STEALTH — human interaction helper
// ─────────────────────────────────────────────

// Merged scroll + mouse-move into one function — one boundingBox call instead of two.
// Pattern: scroll element into view → pause (reading) → move mouse toward it → pause (settling).
// Caller clicks immediately after.
async function humanInteract(page, locator) {
    try {
        await locator.scrollIntoViewIfNeeded();
        await sleepRand(CONFIG.delays.scrollReveal);

        const box = await locator.boundingBox();
        if (!box) return;

        const tx = box.x + rand(Math.floor(box.width  * 0.2), Math.floor(box.width  * 0.8));
        const ty = box.y + rand(Math.floor(box.height * 0.2), Math.floor(box.height * 0.8));

        await page.mouse.move(tx, ty, { steps: rand(5, 12) });
        await sleepRand(CONFIG.delays.mouseSettle);
    } catch {} // non-critical — click proceeds regardless
}

// ─────────────────────────────────────────────
//  SCHEDULER
// ─────────────────────────────────────────────

function initItemState(item) {
    itemState[item.num] = {
        nextCheck:  0,
        failStreak: 0,
        totalFail:  0,
        cooldown:   false,
    };
}

function scheduleNext(item, status) {
    const st  = itemState[item.num];
    const now = Date.now();

    if (status === 'ok') {
        st.failStreak = 0;
        st.cooldown   = false;
        st.nextCheck  = now + rand(...CONFIG.scheduler.ok);
        return;
    }

    if (status === 'error') {
        // Network/timeout errors are not pass failures — keep failStreak unchanged.
        st.nextCheck = now + rand(...CONFIG.scheduler.error);
        return;
    }

    // status === 'pass' — adaptive retry pattern
    st.failStreak++;
    st.totalFail++;

    if (st.failStreak >= 5) {
        st.cooldown   = true;
        st.failStreak = 0;
        batchStats.cooldowns++;

        // totalFail >= threshold = 3+ full fail cycles → item likely broken
        const isExtended = st.totalFail >= CONFIG.extendedCooldownThreshold;

        if (isExtended) {
            st.nextCheck = now + rand(...CONFIG.scheduler.cooldownExtended);
            const mins   = Math.round((st.nextCheck - now) / 60_000);
            const msg    = `🚨 I#${item.num} — extended cooldown ${mins} min (totalFail: ${st.totalFail})`;
            console.error(msg);
            sendEmail(`🚨 Extended Cooldown - I#${item.num} - ${timeStrEN()}`, msg + '\n\n' + formatBatchStats())
                .catch(() => {});
        } else {
            st.nextCheck = now + rand(...CONFIG.scheduler.cooldown);
            const msg    = `🧊 I#${item.num} — cooldown triggered (totalFail: ${st.totalFail})`;
            console.warn(msg);
            sendEmail(`🧊 Cooldown - I#${item.num} - ${timeStrEN()}`, msg + '\n\n' + formatBatchStats())
                .catch(() => {});
        }
        return;
    }

    // Alternating delay: streak 1,3 → fast retry | streak 2,4 → slightly slower
    const delayRange = (st.failStreak % 2 === 1)
        ? CONFIG.scheduler.failOdd
        : CONFIG.scheduler.failEven;

    st.cooldown  = false;
    st.nextCheck = now + rand(...delayRange);
}

// Returns items eligible right now, sorted by urgency (most overdue first).
function getReadyItems() {
    const now = Date.now();
    return CONFIG.items
        .filter(it => itemState[it.num].nextCheck <= now)
        .sort((a, b) => itemState[a.num].nextCheck - itemState[b.num].nextCheck);
}

// ─────────────────────────────────────────────
//  STATS + DISPLAY
// ─────────────────────────────────────────────

function newCycleStats() {
    return { i: 0, ok: 0, pass: 0, errors: 0 };
}

function trackResult(result, cycleStats) {
    cycleStats.i++;
    batchStats.totalI++;
    if (result.status === 'ok')    { cycleStats.ok++;     batchStats.ok++;     }
    if (result.status === 'pass')  { cycleStats.pass++;   batchStats.pass++;   }
    if (result.status === 'error') { cycleStats.errors++; batchStats.errors++; }
}

// item param added so format is consistent between console and log file.
function formatResult(r, item) {
    const tag = itemLabel(item);
    if (r.status === 'ok')   return `  ✅ ${tag} - ${r.message}`;
    if (r.status === 'pass') return `  ⚠️  ${tag} - ${r.message}`;
    return                          `  ❌ ${tag} - ${r.message}`;
}

function formatCycleStats(s) {
    const rate = s.i > 0 ? ((s.ok / s.i) * 100).toFixed(0) : 0;
    return `💾 ${s.ok} | ⚠️  ${s.pass} | ❌ ${s.errors} | ${rate}%`;
}

function formatBatchStats() {
    const elapsed = batchStats.startTime
        ? Math.round((Date.now() - batchStats.startTime) / 60_000)
        : 0;
    const rate = batchStats.totalI > 0
        ? ((batchStats.ok / batchStats.totalI) * 100).toFixed(1)
        : 0;
    return [
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
        '📊 Summary:',
        `  🔄 Cycles    : ${batchStats.totalCycles}/${CONFIG.rounds}`,
        `  📋 Total I   : ${batchStats.totalI}`,
        `  💾 OK        : ${batchStats.ok}`,
        `  ⚠️  Pass      : ${batchStats.pass}`,
        `  ❌ Errors    : ${batchStats.errors}`,
        `  🧊 Cooldowns : ${batchStats.cooldowns}`,
        `  🔁 Resets    : ${batchStats.resets}`,
        `  🔐 Checks    : ${batchStats.sessionChecks}`,
        `  ☁️  Uploads   : ${batchStats.driveUploads}`,
        `  ✅ Rate      : ${rate}%`,
        `  ⏱️  Elapsed   : ${elapsed} min`,
        `  🏁 ETA       : ${calcETA()}`,
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
    ].join('\n');
}

function printStartupSummary() {
    const bar = '═'.repeat(46);
    console.log(`\n${bar}`);
    console.log('  ⚙️  Startup Configuration');
    console.log(bar);
    console.log(`  📋 Items        : ${CONFIG.items.length}`);
    console.log(`  🔄 Rounds       : ${CONFIG.rounds}`);
    console.log(`  📦 Log every    : ${CONFIG.logEvery} cycles`);
    console.log(`  🔁 Reset every  : ${CONFIG.resetAfterMin}–${CONFIG.resetAfterMax} cycles`);
    console.log(`  🕐 Timezone     : ${CONFIG.timezone}`);
    console.log(`  🌐 Locale       : ${CONFIG.locale}`);
    console.log(`  📂 Logs dir     : ${CONFIG.logsDir}`);
    console.log('');
    console.log('  📋 Items (ID only):');
    for (const it of CONFIG.items) {
        const m = it.url.match(/\/(\d{8,})\//);
        console.log(`     ${String(it.num).padStart(3)} → ${m ? m[1] : '—'}`);
    }
    console.log('\n  🕰️  Scheduler:');
    console.log(`     OK      : ${CONFIG.scheduler.ok.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     Pass 1,3: ${CONFIG.scheduler.failOdd.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     Pass 2,4: ${CONFIG.scheduler.failEven.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     Cooldown: ${CONFIG.scheduler.cooldown.map(v => v / 60_000 + 'm').join('–')}`);
    console.log(`     Extended: ${CONFIG.scheduler.cooldownExtended.map(v => v / 60_000 + 'm').join('–')} (after ${CONFIG.extendedCooldownThreshold} total fails)`);
    console.log(bar + '\n');
}

function printStateSnapshot() {
    const now = Date.now();
    const sep = '─'.repeat(48);
    console.log(`\n${sep}`);
    console.log(`  📊 Item State — cycle ${batchStats.totalCycles}/${CONFIG.rounds}  ETA: ${calcETA()}`);
    console.log(sep);
    console.log(`  ${'ID'.padEnd(5)} ${'Next'.padEnd(8)} ${'Streak'.padEnd(8)} ${'Total'.padEnd(7)} State`);
    console.log(sep);
    for (const it of CONFIG.items) {
        const st      = itemState[it.num];
        const secLeft = Math.max(0, Math.round((st.nextCheck - now) / 1000));
        const nextStr = secLeft >= 60 ? `${Math.round(secLeft / 60)}m` : `${secLeft}s`;
        const state   = st.cooldown   ? '🧊 cool'
                      : secLeft === 0  ? '✅ rdy '
                      :                  '⏳ wait';
        console.log(
            `  ${String(it.num).padEnd(5)} ${nextStr.padEnd(8)}` +
            ` ${String(st.failStreak).padStart(2).padEnd(8)}` +
            ` ${String(st.totalFail).padStart(3).padEnd(7)} ${state}`
        );
    }
    console.log(sep + '\n');
}

// ─────────────────────────────────────────────
//  LOGGING
// ─────────────────────────────────────────────

function ensureDirs() {
    if (!fs.existsSync(CONFIG.logsDir)) {
        fs.mkdirSync(CONFIG.logsDir, { recursive: true });
    }
}

function getLogLabel(cycleNum) {
    return cycleNum === CONFIG.rounds
        ? 'final'
        : `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
}

function getLogPath(cycleNum) {
    return path.join(CONFIG.logsDir, `${BATCH_DATE}-${getLogLabel(cycleNum)}.txt`);
}

function appendLog(text, cycleNum) {
    try {
        fs.appendFileSync(getLogPath(cycleNum), text + '\n');
    } catch (err) {
        console.error('❌ Log write error:', err.message);
    }
}

// ─────────────────────────────────────────────
//  GOOGLE DRIVE
// ─────────────────────────────────────────────

function initDriveClient() {
    const credentials = JSON.parse(process.env.GDR_K);
    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/drive.file'],
    });
    driveClient = google.drive({ version: 'v3', auth });
}

async function uploadToDrive(logPath) {
    try {
        const fileName = path.basename(logPath);
        const folderId = process.env.GDR_D;

        // Upsert — update existing file to avoid Drive duplicates on every upload
        const { data } = await driveClient.files.list({
            q:      `name='${fileName}' and '${folderId}' in parents and trashed=false`,
            fields: 'files(id)',
        });

        const media = { mimeType: 'text/plain', body: fs.createReadStream(logPath) };

        if (data.files.length > 0) {
            await driveClient.files.update({ fileId: data.files[0].id, media });
            console.log(`☁️  updated  ${fileName}`);
        } else {
            await driveClient.files.create({
                requestBody: { name: fileName, parents: [folderId] },
                media,
            });
            console.log(`☁️  uploaded ${fileName}`);
        }

        batchStats.driveUploads++;
    } catch (err) {
        console.error('❌ Drive error:', err.message);
    }
}

// ─────────────────────────────────────────────
//  EMAIL
// ─────────────────────────────────────────────

function initMailTransporter() {
    mailTransporter = nodemailer.createTransport({
        service: 'gmail',
        auth: { user: CONFIG.email, pass: process.env.M_P },
    });
}

async function sendEmail(subject, body) {
    try {
        await mailTransporter.sendMail({
            from:    CONFIG.email,
            to:      CONFIG.email,
            subject,
            text:    body,
        });
        console.log(`📧 Sent: ${subject}`);
    } catch (err) {
        console.error(`❌ Email error: ${err.message}`);
    }
}

async function sendPartSummary(cycleNum) {
    const logPath = getLogPath(cycleNum);
    if (!fs.existsSync(logPath)) return;

    const label   = getLogLabel(cycleNum);
    const from    = cycleNum - CONFIG.logEvery + 1;
    const subject = `📋 Log ${label} - cycles ${from}-${cycleNum} - ${BATCH_DATE}`;

    await sendEmail(subject, fs.readFileSync(logPath, 'utf8') + '\n\n' + formatBatchStats());
    await uploadToDrive(logPath);
}

async function sendSummary(batchStartTime) {
    const hour = hourNow();
    const body  = formatBatchStats();

    await sendEmail(`📦 Complete - ${batchStartTime}`, body);

    if (CONFIG.reportHours.includes(hour)) {
        await sendEmail(`🕐 6h report - ${timeStrEN()}`, body);
    } else if (hour === CONFIG.dailyHour) {
        await sendEmail(`📊 Daily report - ${BATCH_DATE}`, body);
    }
}

// ─────────────────────────────────────────────
//  COOKIES
// ─────────────────────────────────────────────

function parseCookies(raw) {
    const trimmed = raw.trim();

    if (trimmed.startsWith('[')) {
        console.log('📝 Cookie format: JSON');
        try { return JSON.parse(trimmed); }
        catch (err) {
            console.error('❌ Cookie JSON parse failed:', err.message);
            return [];
        }
    }

    console.log('📝 Cookie format: string');
    try {
        // Derive domain from items — validate all share the same host.
        // Items on different domains would require separate cookie jars.
        const hostnames = [...new Set(
            CONFIG.items.map(it => new URL(it.url).hostname)
        )];
        if (hostnames.length > 1) {
            console.warn(`⚠️  Items span multiple hosts: ${hostnames.join(', ')} — using first`);
        }
        const domain = hostnames[0].replace(/^[^.]+/, ''); // strip subdomain → .domain.tld
        return trimmed
            .split(';')
            .map(c => {
                const [name, ...rest] = c.trim().split('=');
                if (!name?.trim()) return null;
                return { name: name.trim(), value: rest.join('=').trim(), domain, path: '/' };
            })
            .filter(Boolean);
    } catch (err) {
        console.error('❌ Cookie string parse failed:', err.message);
        return [];
    }
}

// ─────────────────────────────────────────────
//  BROWSER — stealth context
// ─────────────────────────────────────────────

async function createBrowser(cookies) {
    const browser = await chromium.launch({
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-web-security',
            '--disable-infobars',
        ],
    });

    const { ua, screen, secChUa } = pickSession();

    // sec-ch-ua only for Chromium — Firefox omits these headers entirely.
    // Accept-Language is built from the locale secret — no language hardcoded here.
    const localeLang    = CONFIG.locale.split('-')[0];           // base language code
    const acceptLangVal = CONFIG.locale === localeLang
        ? `${localeLang};q=1,en-US;q=0.8,en;q=0.7`
        : `${CONFIG.locale};q=1,${localeLang};q=0.9,en-US;q=0.8,en;q=0.7`;

    const extraHeaders = {
        'Accept-Language': acceptLangVal,
    };
    if (secChUa) {
        extraHeaders['sec-ch-ua']          = secChUa;
        extraHeaders['sec-ch-ua-mobile']   = '?0';
        extraHeaders['sec-ch-ua-platform'] = '"Windows"';
    }
    // Add referer only when non-empty — absence is valid for ~15% of real sessions
    const referer = pickReferer();
    if (referer) extraHeaders['Referer'] = referer;

    const context = await browser.newContext({
        userAgent:        ua,
        locale:           CONFIG.locale,
        timezoneId:       CONFIG.timezone,
        colorScheme:      'dark',          // matches theme=dark stored in cookies
        viewport:         screen,
        extraHTTPHeaders: extraHeaders,
        permissions:      ['notifications'],
    });

    // Single consolidated route handler — one evaluation per request instead of five.
    const BLOCK_IMAGE = /\.(png|jpe?g|gif|webp|ico)(\?|$)/i;
    const BLOCK_TRACK = /analytics|tracking|collect|beacon/i;
    await context.route('**/*', (route) => {
        const url = route.request().url();
        (BLOCK_IMAGE.test(url) || BLOCK_TRACK.test(url))
            ? route.abort()
            : route.continue();
    });

    await context.addCookies(cookies);

    const sessionTag = ua.match(/Chrome\/(\d+)|Firefox\/(\d+)|Edg\/(\d+)/)?.[0] ?? 'UA';
    console.log(`🛡️  Session: ${sessionTag} | ${screen.width}×${screen.height}`);

    return { browser, context };
}

async function newPage(context) {
    const page = await context.newPage();

    // All spoofing runs via addInitScript so it executes before any page script.
    // CONFIG.locale is captured at call time and injected into the script as a literal —
    // addInitScript runs in the page context where CONFIG does not exist.
    await page.addInitScript((localeSnapshot) => {
        // Rename for clarity inside the page-scope IIFE
        const CONFIG_LOCALE_SNAPSHOT = localeSnapshot;


        // webdriver property
        Object.defineProperty(navigator, 'webdriver', { get: () => false });

        // chrome object — real browsers expose a richer structure
        window.chrome = {
            runtime:   {},
            loadTimes: () => ({}),
            csi:       () => ({}),
            app:       {},
        };

        // plugins — headless reports 0; real browsers have several
        Object.defineProperty(navigator, 'plugins', {
            get: () => Object.assign(
                [
                    { name: 'Chrome PDF Plugin',  filename: 'internal-pdf-viewer',              description: 'Portable Document Format' },
                    { name: 'Chrome PDF Viewer',  filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                    { name: 'Native Client',      filename: 'internal-nacl-plugin',             description: '' },
                ],
                { length: 3 },
            ),
        });

        // languages — derived from CONFIG.locale at page creation time.
        // Must match the Accept-Language header; hardcoding breaks consistency
        // if the locale secret is ever changed.
        const _primaryLocale = CONFIG_LOCALE_SNAPSHOT;
        const _lang          = _primaryLocale.split('-')[0]; // base language code
        const _langList      = _primaryLocale === _lang
            ? [_lang, 'en-US', 'en']
            : [_primaryLocale, _lang, 'en-US', 'en'];
        Object.defineProperty(navigator, 'languages', {
            get: () => _langList,
        });

        // platform
        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });

        // hardware — values chosen ONCE per page load and stored in closures.
        // FIX: previous code returned a NEW random value on every read.
        // Bot detectors call hardwareConcurrency twice and flag inconsistency.
        const _cores  = [4, 6, 8, 12, 16];
        const _memory = [4, 8, 16];
        const _hwc    = _cores[Math.floor(Math.random() * _cores.length)];
        const _dm     = _memory[Math.floor(Math.random() * _memory.length)];

        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => _hwc });
        Object.defineProperty(navigator, 'deviceMemory',        { get: () => _dm  });

        // Canvas fingerprint noise — intercept getImageData only.
        // FIX: removed toDataURL canvas modification.
        // Writing a pixel before toDataURL corrupts any ongoing canvas rendering.
        // getImageData noise is sufficient and does not touch canvas content.
        const _origGetCtx = HTMLCanvasElement.prototype.getContext;
        HTMLCanvasElement.prototype.getContext = function (type, ...args) {
            const ctx = _origGetCtx.call(this, type, ...args);
            if (type === '2d' && ctx) {
                const _origGetImageData = ctx.getImageData.bind(ctx);
                ctx.getImageData = (x, y, w, h) => {
                    const data = _origGetImageData(x, y, w, h);
                    for (let i = 0; i < data.data.length; i += 64) {
                        data.data[i] ^= (Math.random() * 3) | 0;
                    }
                    return data;
                };
            }
            return ctx;
        };

        // Permissions API
        const _origQuery = navigator.permissions?.query?.bind(navigator.permissions);
        if (_origQuery) {
            navigator.permissions.query = (params) =>
                (['notifications', 'geolocation'].includes(params?.name))
                    ? Promise.resolve({ state: 'granted', onchange: null })
                    : _origQuery(params);
        }
    }, CONFIG.locale);  // locale injected as argument — not accessible as CONFIG inside page scope

    return page;
}

// ─────────────────────────────────────────────
//  SESSION CHECK
// ─────────────────────────────────────────────

async function checkSession(page, label = 'general') {
    batchStats.sessionChecks++;
    console.log(`🔐 Check (${label})...`);

    // Pick a random item each check — avoids false failure if items[0] has expired.
    // Any item in the list serves as a valid session probe (all share the same auth).
    const probeItem = CONFIG.items[Math.floor(Math.random() * CONFIG.items.length)];

    for (let attempt = 1; attempt <= CONFIG.cookieRetries; attempt++) {
        try {
            await page.goto(probeItem.url, {
                waitUntil: 'domcontentloaded',
                timeout:   30_000,
            });

            const found = await page
                .waitForSelector(CONFIG.selectors.listboxBtn, { timeout: 10_000 })
                .then(() => true)
                .catch(() => false);

            if (found) {
                console.log('✅ Session ok');
                return { valid: true };
            }

            console.warn(`⚠️  Attempt ${attempt}/${CONFIG.cookieRetries} - button not found`);

        } catch (err) {
            console.warn(`⚠️  Attempt ${attempt}/${CONFIG.cookieRetries} - ${err.message}`);
        }

        if (attempt < CONFIG.cookieRetries) {
            console.log(`⏳ Waiting ${CONFIG.cookieRetryDelay / 1000}s before retry...`);
            await sleepMs(CONFIG.cookieRetryDelay);
        }
    }

    console.error(`❌ Session check failed after ${CONFIG.cookieRetries} attempts`);
    return { valid: false };
}

// ─────────────────────────────────────────────
//  HALT
// ─────────────────────────────────────────────

async function haltAndNotify(browser, context, message) {
    await sendEmail(`🚨 Alert - ${timeStrEN()}`, message + '\n\n' + formatBatchStats());
    try { await context.close(); } catch {}
    try { await browser.close(); } catch {}
    process.exit(1);
}

// ─────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────

function sanitizeError(msg) {
    return msg.replace(/https?:\/\/\S+/g, '[URL]').split('\n')[0];
}

// Timer is cleared when the main promise settles — no memory leak.
function withTimeout(promise, ms) {
    let timer;
    return Promise.race([
        promise.finally(() => clearTimeout(timer)),
        new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error('item timeout')), ms);
        }),
    ]);
}

// ─────────────────────────────────────────────
//  ITEM PROCESSING
// ─────────────────────────────────────────────

async function processItem(page, item) {
    try {
        console.log(`   open:  ${timeStrEN()}`);
        await page.goto(item.url, { waitUntil: 'domcontentloaded', timeout: 60_000 });
        await sleepRand(CONFIG.delays.pageLoad);

        await page.waitForSelector(CONFIG.selectors.listboxBtn, { timeout: 20_000 });

        const btnLocator = page.locator(CONFIG.selectors.listboxBtn);

        // FIX: read count once — previous code issued two separate DOM queries.
        const btnCount = await btnLocator.count();
        if (btnCount < 2) {
            await sleepRand(CONFIG.delays.beforeSkip);
            console.log(`⚠️  ${itemLabel(item)} - buttons not found (${btnCount})`);
            return { status: 'pass', num: item.num, message: `buttons not found (${btnCount})` };
        }

        // ── First listbox (index 0) ────────────────────────────────────────
        // FIX: store locator reference — previous code re-evaluated nth(0) three
        // times (humanInteract, hover, click), each triggering a DOM query.
        const btn0 = btnLocator.nth(0);
        await humanInteract(page, btn0);
        await btn0.click({ force: true, delay: rand(...CONFIG.delays.clickDelay) });
        await sleepRand(CONFIG.delays.afterFirstBtn);

        // Scope [role="option"] to the visible listbox only.
        // Without :visible scoping both option lists exist in DOM simultaneously —
        // matching against the full page could pick an option from the wrong dropdown.
        const option1 = page.locator('[role="listbox"]:visible [role="option"]')
            .filter({ hasText: buildRegex(CONFIG.selectors.option1) });

        if (await option1.count() === 0) {
            console.log(`⚠️  ${itemLabel(item)} - option1 not found`);
            return { status: 'pass', num: item.num, message: 'option1 not found' };
        }
        const opt1 = option1.first();
        await humanInteract(page, opt1);
        await opt1.click({ force: true });

        // Wait for DOM to update — second listbox options change after first selection
        await sleepRand(CONFIG.delays.beforeSecondBtn);

        // Re-query buttons after DOM change — previous locator reference is stale
        const freshBtns      = page.locator(CONFIG.selectors.listboxBtn);
        const freshBtnCount  = await freshBtns.count();

        if (freshBtnCount < 2) {
            console.log(`⚠️  ${itemLabel(item)} - second dropdown disappeared`);
            return { status: 'pass', num: item.num, message: 'second dropdown not found' };
        }

        // ── Second listbox (index 1) ───────────────────────────────────────
        const btn1 = freshBtns.nth(1);
        await humanInteract(page, btn1);
        await btn1.click({ force: true, delay: rand(...CONFIG.delays.clickDelay) });
        await sleepRand(CONFIG.delays.afterSecondBtn);

        const option2 = page.locator('[role="listbox"]:visible [role="option"]')
            .filter({ hasText: buildRegex(CONFIG.selectors.option2) });

        if (await option2.count() === 0) {
            console.log(`⚠️  ${itemLabel(item)} - option2 not found`);
            return { status: 'pass', num: item.num, message: 'option2 not found' };
        }
        const opt2 = option2.first();
        await humanInteract(page, opt2);
        await opt2.click({ force: true });

        await sleepRand(CONFIG.delays.beforeSave);

        // isVisible() — a button may exist in DOM but be hidden
        const saveBtn = page.locator('button')
            .filter({ hasText: buildRegex(CONFIG.selectors.saveBtn) })
            .first();

        if (!(await saveBtn.isVisible())) {
            console.log(`⚠️  ${itemLabel(item)} - save button not visible`);
            return { status: 'pass', num: item.num, message: 'save button not visible' };
        }

        await humanInteract(page, saveBtn);
        // humanInteract already positions the mouse over the element.
        // No separate hover() call needed — that would be a double-move.
        await sleepRand(CONFIG.delays.hoverSave);
        await saveBtn.click({ force: true });
        await sleepRand(CONFIG.delays.afterSave);

        console.log(`✅ ${itemLabel(item)} - ok`);
        return { status: 'ok', num: item.num, message: timeStrEN() };

    } catch (err) {
        const shortMsg = sanitizeError(err.message);
        console.log(`❌ ${itemLabel(item)} - ${shortMsg}`);

        // Capture screenshot on unexpected errors — stored locally in logs dir.
        // Lets you see exactly what the page looked like when the failure occurred.
        // Screenshot is never uploaded to Drive or sent via email.
        try {
            const shotPath = path.join(
                CONFIG.logsDir,
                `err-${BATCH_DATE}-I${item.num}-${Date.now()}.png`,
            );
            await page.screenshot({ path: shotPath, fullPage: false });
            console.log(`📸 Screenshot: ${path.basename(shotPath)}`);
        } catch {} // non-critical — original error still returned

        return { status: 'error', num: item.num, message: shortMsg };
    } finally {
        await sleepRand(CONFIG.delays.beforeNext);
    }
}

async function runItemWithTimeout(page, item) {
    try {
        return await withTimeout(processItem(page, item), 90_000);
    } catch (err) {
        try { await page.reload({ waitUntil: 'domcontentloaded', timeout: 30_000 }); } catch {}
        return { status: 'error', num: item.num, message: sanitizeError(err.message) };
    }
}

// ─────────────────────────────────────────────
//  CYCLE
// ─────────────────────────────────────────────

async function runCycle(page, cycleNum) {
    console.log(`\n🔄 Cycle ${cycleNum}/${CONFIG.rounds} - ${timeStrEN()} - ETA: ${calcETA()}`);

    const totalI = CONFIG.items.length;
    const ready  = getReadyItems();

    // Check ready.length BEFORE computing iCount — avoids dead slice(0,0) path.
    if (ready.length === 0) {
        const waitMs = rand(...CONFIG.scheduler.noReadyItemWait);
        console.log(`⏸️  No items ready — waiting ${(waitMs / 1000).toFixed(1)}s`);
        await sleepMs(waitMs);
        batchStats.totalCycles++;
        return { i: 0, ok: 0, pass: 0, errors: 0, noItems: true };
    }

    const iCount     = Math.min(ready.length, rand(CONFIG.minPerCycle, totalI));
    const selected   = ready.slice(0, iCount);
    // FIX: exclude items already in ready from inCooldown count.
    // When a cooldown expires (nextCheck <= now), the item appears in getReadyItems()
    // while st.cooldown is still true — it would be double-counted without this filter.
    const now_snap   = Date.now();
    const inCooldown = CONFIG.items.filter(it =>
        itemState[it.num].cooldown && itemState[it.num].nextCheck > now_snap
    ).length;
    const scheduled  = totalI - ready.length - inCooldown; // waiting, not in cooldown

    console.log(`📌 I: ${selected.length}/${totalI}  ready:${ready.length}  cooldown:${inCooldown}  scheduled:${scheduled}`);

    const cycleStats = newCycleStats();
    const lines      = [];

    for (const item of selected) {
        const result = await runItemWithTimeout(page, item);

        scheduleNext(item, result.status);

        const st    = itemState[item.num];
        const parts = [`next:${Math.round((st.nextCheck - Date.now()) / 1000)}s`];
        if (st.failStreak > 0) parts.push(`streak:${st.failStreak}`);
        if (st.cooldown)       parts.push('🧊');
        console.log(`   → ${parts.join(' ')}`);

        trackResult(result, cycleStats);
        lines.push(formatResult(result, item));
    }

    batchStats.totalCycles++;
    console.log(`📈 ${formatCycleStats(cycleStats)}\n`);

    appendLog([
        '\n==========================================',
        `Cycle ${cycleNum}/${CONFIG.rounds} - ${timeStrEN()}`,
        `I: ${selected.length}/${totalI}`,
        '------------------------------------------',
        ...lines,
        '------------------------------------------',
        `📈 ${formatCycleStats(cycleStats)}`,
        `Cycle ${cycleNum} done: ${timeStrEN()}`,
    ].join('\n'), cycleNum);

    if (cycleNum % CONFIG.logEvery === 0 || cycleNum === CONFIG.rounds) {
        console.log(`📁 Log: ${getLogPath(cycleNum)}`);
        await sendPartSummary(cycleNum);
    }

    if (cycleNum % CONFIG.snapshotEvery === 0) {
        printStateSnapshot();
    }

    return cycleStats;
}

// ─────────────────────────────────────────────
//  RESET ENGINE
// ─────────────────────────────────────────────

async function resetEngine(cookies, cycleNum) {
    console.log(`\n🔁 Reset at cycle ${cycleNum}...`);
    batchStats.resets++;

    const { browser: newBrowser, context: newContext } = await createBrowser(cookies);
    const pg = await newPage(newContext);

    const { valid } = await checkSession(pg, `reset-${cycleNum}`);
    if (!valid) {
        await haltAndNotify(newBrowser, newContext, `Session invalid at cycle ${cycleNum}.`);
    }

    console.log('✅ Reset done');
    return { browser: newBrowser, context: newContext, page: pg };
}

// ─────────────────────────────────────────────
//  MAIN
// ─────────────────────────────────────────────

(async () => {
    console.log('🚀 New run:', timeStrEN());

    // Validate all required env vars — report every missing key at once
    const required = ['I_CO','GDR_K','GDR_D','M_P','I_M','I_U','I_TZ','I_LC','I_O1','I_O2','I_SB'];
    const missing  = required.filter(k => !process.env[k]);
    if (missing.length > 0) {
        console.error('❌ Missing env vars:', missing.join(', '));
        process.exit(1);
    }

    CONFIG.email             = norm(process.env.I_M);
    CONFIG.timezone          = norm(process.env.I_TZ);
    CONFIG.locale            = norm(process.env.I_LC);
    CONFIG.selectors.option1 = norm(process.env.I_O1);
    CONFIG.selectors.option2 = norm(process.env.I_O2);
    CONFIG.selectors.saveBtn = norm(process.env.I_SB);

    CONFIG.items = norm(process.env.I_U)
        .split(',')
        .map(entry => {
            const [num, url] = entry.trim().split('|');
            return (norm(num) && norm(url))
                ? { num: norm(num), url: norm(url) }
                : null;
        })
        .filter(Boolean);

    if (CONFIG.items.length === 0) {
        console.error('❌ I_U is empty or invalid — expected format: num|url,num|url,...');
        process.exit(1);
    }

    // FIX: fail fast before launching a browser if cookies are empty.
    const cookies = parseCookies(process.env.I_CO);
    if (cookies.length === 0) {
        console.error('❌ Cookies empty or failed to parse — cannot authenticate');
        process.exit(1);
    }

    CONFIG.items.forEach(initItemState);

    const timeTag = new Date().toLocaleTimeString('en-CA', {
        timeZone: CONFIG.timezone,
        hour:     '2-digit',
        minute:   '2-digit',
        hour12:   false,
    }).replace(':', '');
    BATCH_DATE = `${dateStr()}-${timeTag}`;

    printStartupSummary();
    ensureDirs();
    batchStats.startTime = Date.now();

    initDriveClient();
    initMailTransporter();

    let { browser, context } = await createBrowser(cookies);
    let page                 = await newPage(context);

    const batchStartTime = timeStrEN();

    const { valid: initialValid } = await checkSession(page, 'startup');
    if (!initialValid) {
        await haltAndNotify(browser, context, 'Session invalid before batch start.');
    }

    // Random warm-up delay — prevents every scheduled run starting at the exact same second.
    // Staggered start times look far more human across multiple executions.
    const warmUpMs = Math.floor(Math.random() * CONFIG.startDelayMax);
    if (warmUpMs > 1000) {
        console.log(`⏳ Warm-up: ${(warmUpMs / 1000).toFixed(1)}s`);
        await sleepMs(warmUpMs);
    }

    let nextReset = rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);

    for (let i = 1; i <= CONFIG.rounds; i++) {

        if (i > 1 && i >= nextReset) {
            try { await context.close(); } catch {}
            try { await browser.close(); } catch {}

            ({ browser, context, page } = await resetEngine(cookies, i));
            nextReset = i + rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
            console.log(`🔁 Next reset at cycle ${nextReset}`);
        }

        const cycleStats = await runCycle(page, i);

        // Only trigger session check when items were processed but none succeeded.
        // noItems=true means all items are scheduled ahead — session is fine.
        if (!cycleStats.noItems && cycleStats.i > 0 && cycleStats.ok === 0) {
            console.warn(`\n⚠️  Cycle ${i}: ${cycleStats.i} items processed, 0 ok → checking session...`);
            const { valid } = await checkSession(page, `check-${i}`);
            if (!valid) {
                await haltAndNotify(browser, context, `Session check failed after cycle ${i}.`);
            }
        }

        if (i < CONFIG.rounds && !cycleStats.noItems) {
            const waitMs = rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax);
            console.log('\n#####################');
            console.log(`⏳ Waiting ${(waitMs / 1000).toFixed(1)}s...`);
            console.log('#####################\n');
            await sleepMs(waitMs);
        }
    }

    // try-catch ensures summary email is always sent even if browser.close() throws
    try { await context.close(); } catch {}
    try { await browser.close(); } catch {}

    console.log(`\n🎉 Done! - ${timeStrEN()}`);
    console.log(formatBatchStats());

    await sendSummary(batchStartTime);
})();
