'use strict';

const { chromium } = require('playwright');
const nodemailer   = require('nodemailer');
const { google }   = require('googleapis');
const fs           = require('fs');
const path         = require('path');

// ─────────────────────────────────────────────
//  STEALTH
// ─────────────────────────────────────────────

const UA_POOL = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];

const SEC_CH_UA_MAP = {
    chrome: (v) => `"Chromium";v="${v}", "Google Chrome";v="${v}", "Not-A.Brand";v="99"`,
    edge:   (v) => `"Chromium";v="${v}", "Microsoft Edge";v="${v}", "Not-A.Brand";v="99"`,
};

const SCREEN_POOL = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768  },
    { width: 1440, height: 900  },
    { width: 1536, height: 864  },
    { width: 1280, height: 720  },
];

function buildSecChUa(ua) {
    const edgeMatch = ua.match(/Edg\/(\d+)/);
    if (edgeMatch) return SEC_CH_UA_MAP.edge(edgeMatch[1]);
    const chromeMatch = ua.match(/Chrome\/(\d+)/);
    if (chromeMatch) return SEC_CH_UA_MAP.chrome(chromeMatch[1]);
    return null;
}

function pickSession() {
    const ua     = UA_POOL[Math.floor(Math.random() * UA_POOL.length)];
    const screen = SCREEN_POOL[Math.floor(Math.random() * SCREEN_POOL.length)];
    return { ua, screen, secChUa: buildSecChUa(ua) };
}

const REFERER_POOL = [
    'https://www.google.com/',
    'https://www.google.com/',
    'https://t.co/',
    'https://l.instagram.com/',
    'https://www.facebook.com/',
    '',
];

function pickReferer() {
    return REFERER_POOL[Math.floor(Math.random() * REFERER_POOL.length)];
}

// ─────────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────────

const CONFIG = {
    logEvery:         50,
    minPerCycle:      20,
    cycleDelayMin:    1_000,
    cycleDelayMax:    2_500,
    resetAfterMin:    15,
    resetAfterMax:    20,
    snapshotEvery:    25,
    startDelayMax:    30_000,

    cookieRetries:    3,
    cookieRetryDelay: 5_000,

    timezone:    '',
    dailyHour:   23,
    reportHours: [0, 6, 12, 18],

    email:   '',
    logsDir: 'logs',

    items: [],

    delays: {
        pageLoad:        [400,  700],
        scrollReveal:    [60,   120],
        mouseSettle:     [20,   50],
        afterFirstBtn:   [150,  280],
        beforeSecondBtn: [60,   120],
        afterSecondBtn:  [150,  280],
        beforeSave:      [100,  200],
        afterSave:       [300,  500],
        hoverSave:       [50,   120],
        clickDelay:      [20,   60],
        beforeNext:      [0,    30],
        beforeSkip:      [0,    30],
    },

    scheduler: {
        ok:               [55_000,     90_000],
        // FIX BUG2: stable = ad already in correct state — same window as ok, no penalty
        stable:           [55_000,     90_000],
        // FIX BUG1: page error (buttons not loaded) — fast retry, no streak
        pageError:        [15_000,     25_000],
        error:            [30_000,     60_000],
        failOdd:          [20_000,     35_000],
        failEven:         [35_000,     50_000],
        cooldown:         [300_000,   600_000],
        cooldownExtended: [900_000, 1_800_000],
    },

    runDurationMin:            330,
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

const rand      = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleepRand = ([min, max]) => new Promise(res => setTimeout(res, rand(min, max)));
const sleepMs   = (ms)         => new Promise(res => setTimeout(res, ms));
const tz        = () => ({ timeZone: CONFIG.timezone || 'UTC' });
const timeStrEN = () => new Date().toLocaleTimeString('en-CA', { ...tz(), hour12: false });
const dateStr   = () => new Date().toLocaleDateString('en-CA', tz());
const hourNow   = () => new Date(new Date().toLocaleString('en', tz())).getHours();
const norm      = (v) => String(v ?? '').trim();

// Compiled once in main() after env vars are loaded.
let REGEX_OPTION1 = null;
let REGEX_OPTION2 = null;
let REGEX_SAVE    = null;

function buildRegex(text) {
    const escaped = text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`^${escaped}$`, 'i');
}

function itemLabel(item) {
    return `I#${item.num}`;
}

let RUN_END = null;
function calcETA() {
    if (RUN_END) {
        const rem = Math.max(0, RUN_END - Date.now());
        return new Date(Date.now() + rem).toLocaleTimeString('en-CA', { ...tz(), hour12: false });
    }
    if (!batchStats.startTime) return '--:--:--';
    const endEst = batchStats.startTime + CONFIG.runDurationMin * 60_000;
    const rem    = Math.max(0, endEst - Date.now());
    return new Date(Date.now() + rem).toLocaleTimeString('en-CA', { ...tz(), hour12: false });
}

// ─────────────────────────────────────────────
//  STEALTH — human interaction helpers
// ─────────────────────────────────────────────

async function humanInteract(page, locator) {
    try {
        await locator.scrollIntoViewIfNeeded();
        await sleepRand(CONFIG.delays.scrollReveal);

        const box = await locator.boundingBox();
        if (!box) return;

        const tx = box.x + rand(Math.floor(box.width  * 0.2), Math.floor(box.width  * 0.8));
        const ty = box.y + rand(Math.floor(box.height * 0.2), Math.floor(box.height * 0.8));

        await page.mouse.move(tx, ty, { steps: rand(3, 7) });
        await sleepRand(CONFIG.delays.mouseSettle);
    } catch {}
}

async function lightInteract(locator) {
    try {
        await locator.scrollIntoViewIfNeeded();
        await sleepRand(CONFIG.delays.mouseSettle);
    } catch {}
}

// ─────────────────────────────────────────────
//  SCHEDULER
//
//  Pass reasons and their meaning:
//    'save button not visible' → ad IS in correct state (stable) — NOT a failure
//    'buttons not found (N)'   → page loaded with only N button(s) — transient page error
//    'option1/2 not found'     → real failure — the option we expect isn't there
//    'second dropdown not found' → real failure
// ─────────────────────────────────────────────

function initItemState(item) {
    itemState[item.num] = {
        nextCheck:  0,
        failStreak: 0,
        totalFail:  0,
        cooldown:   false,
    };
}

// FIX BUG2 + BUG1: scheduleNext now receives the pass message and acts accordingly.
function scheduleNext(item, status, message) {
    const st  = itemState[item.num];
    const now = Date.now();

    if (status === 'ok') {
        st.failStreak = 0;
        st.cooldown   = false;
        st.nextCheck  = now + rand(...CONFIG.scheduler.ok);
        return;
    }

    if (status === 'error') {
        st.nextCheck = now + rand(...CONFIG.scheduler.error);
        return;
    }

    // ── pass ────────────────────────────────────────────────────────────────

    // FIX BUG2: "save button not visible" = ad is already in correct state.
    // Selecting the same values doesn't trigger the save button — this is a
    // TRUE POSITIVE. Do NOT increment failStreak. Schedule at ok-window pace.
    if (message === 'save button not visible') {
        st.cooldown  = false;
        st.nextCheck = now + rand(...CONFIG.scheduler.stable);
        return;
    }

    // FIX BUG1: "buttons not found" = page structure issue — the second listbox
    // button loaded after count() was called. Fast retry, no streak penalty.
    if (message.startsWith('buttons not found')) {
        st.cooldown  = false;
        st.nextCheck = now + rand(...CONFIG.scheduler.pageError);
        return;
    }

    // Real failure: option not found, dropdown disappeared, etc.
    st.failStreak++;
    st.totalFail++;

    if (st.failStreak >= 5) {
        st.cooldown   = true;
        st.failStreak = 0;
        batchStats.cooldowns++;

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

    const delayRange = (st.failStreak % 2 === 1)
        ? CONFIG.scheduler.failOdd
        : CONFIG.scheduler.failEven;

    st.cooldown  = false;
    st.nextCheck = now + rand(...delayRange);
}

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
    // FIX BUG3: track stable separately so session check isn't triggered by
    // items that are simply in the correct state.
    return { i: 0, ok: 0, pass: 0, stable: 0, errors: 0 };
}

function trackResult(result, cycleStats) {
    cycleStats.i++;
    batchStats.totalI++;
    if (result.status === 'ok') {
        cycleStats.ok++;
        batchStats.ok++;
    } else if (result.status === 'pass') {
        cycleStats.pass++;
        batchStats.pass++;
        if (result.message === 'save button not visible') cycleStats.stable++;
    } else if (result.status === 'error') {
        cycleStats.errors++;
        batchStats.errors++;
    }
}

function formatResult(r, item) {
    const tag = itemLabel(item);
    if (r.status === 'ok')   return `  ✅ ${tag} - ${r.message}`;
    if (r.status === 'pass') return `  ⚠️  ${tag} - ${r.message}`;
    return                          `  ❌ ${tag} - ${r.message}`;
}

function formatCycleStats(s) {
    const rate = s.i > 0 ? ((s.ok / s.i) * 100).toFixed(0) : 0;
    return `💾 ${s.ok} | ✔️  ${s.stable} | ⚠️  ${s.pass - s.stable} | ❌ ${s.errors} | ${rate}%`;
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
        `  🔄 Cycles    : ${batchStats.totalCycles}`,
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
    console.log(`  🔄 Run duration : ${CONFIG.runDurationMin} min (time-based)`);
    console.log(`  📦 Log every    : ${CONFIG.logEvery} cycles`);
    console.log(`  🔁 Reset every  : ${CONFIG.resetAfterMin}–${CONFIG.resetAfterMax} cycles`);
    console.log(`  🕐 Timezone     : ${CONFIG.timezone}`);
    console.log(`  🌐 Locale       : ${CONFIG.locale}`);
    console.log(`  📂 Logs dir     : ${CONFIG.logsDir}`);
    console.log('');
    console.log('  📋 Items:');
    for (const it of CONFIG.items) {
        console.log(`     ${String(it.num).padStart(3)}`);
    }
    console.log('\n  🕰️  Scheduler:');
    console.log(`     OK/Stable: ${CONFIG.scheduler.ok.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     PageError: ${CONFIG.scheduler.pageError.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     Pass 1,3 : ${CONFIG.scheduler.failOdd.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     Pass 2,4 : ${CONFIG.scheduler.failEven.map(v => v / 1000 + 's').join('–')}`);
    console.log(`     Cooldown : ${CONFIG.scheduler.cooldown.map(v => v / 60_000 + 'm').join('–')}`);
    console.log(`     Extended : ${CONFIG.scheduler.cooldownExtended.map(v => v / 60_000 + 'm').join('–')} (after ${CONFIG.extendedCooldownThreshold} total fails)`);
    console.log(bar + '\n');
}

function printStateSnapshot() {
    const now = Date.now();
    const sep = '─'.repeat(48);
    console.log(`\n${sep}`);
    console.log(`  📊 Item State — cycle ${batchStats.totalCycles}  ETA: ${calcETA()}`);
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
    return `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
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

        const { data } = await driveClient.files.list({
            q:                         `name='${fileName}' and '${folderId}' in parents and trashed=false`,
            fields:                    'files(id)',
            supportsAllDrives:         true,
            includeItemsFromAllDrives: true,
        });

        const media = { mimeType: 'text/plain', body: fs.createReadStream(logPath) };

        if (data.files.length > 0) {
            await driveClient.files.update({
                fileId:            data.files[0].id,
                media,
                supportsAllDrives: true,
            });
            console.log(`☁️  updated  ${fileName}`);
        } else {
            await driveClient.files.create({
                requestBody: {
                    name:     fileName,
                    parents:  [folderId],
                },
                media,
                supportsAllDrives: true,
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
        const hostnames = [...new Set(
            CONFIG.items.map(it => new URL(it.url).hostname)
        )];
        if (hostnames.length > 1) {
            console.warn(`⚠️  Items span multiple hosts: ${hostnames.join(', ')} — using first`);
        }
        const domain = hostnames[0].replace(/^[^.]+/, '');
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
            '--disable-infobars',
        ],
    });

    const { ua, screen, secChUa } = pickSession();

    const localeLang    = CONFIG.locale.split('-')[0];
    const acceptLangVal = CONFIG.locale === localeLang
        ? `${localeLang};q=1,en-US;q=0.8,en;q=0.7`
        : `${CONFIG.locale};q=1,${localeLang};q=0.9,en-US;q=0.8,en;q=0.7`;

    const extraHeaders = { 'Accept-Language': acceptLangVal };
    if (secChUa) {
        extraHeaders['sec-ch-ua']          = secChUa;
        extraHeaders['sec-ch-ua-mobile']   = '?0';
        extraHeaders['sec-ch-ua-platform'] = '"Windows"';
    }
    const referer = pickReferer();
    if (referer) extraHeaders['Referer'] = referer;

    const context = await browser.newContext({
        userAgent:        ua,
        locale:           CONFIG.locale,
        timezoneId:       CONFIG.timezone,
        colorScheme:      'dark',
        viewport:         screen,
        extraHTTPHeaders: extraHeaders,
        permissions:      ['notifications'],
    });

    const BLOCK_RE = /\.(png|jpe?g|gif|webp|ico|woff2?|ttf|eot|svg)(\?|$)|analytics|tracking|collect|beacon/i;
    await context.route('**/*', (route) => {
        BLOCK_RE.test(route.request().url()) ? route.abort() : route.continue();
    });

    await context.addCookies(cookies);

    const sessionTag = ua.match(/Chrome\/(\d+)|Firefox\/(\d+)|Edg\/(\d+)/)?.[0] ?? 'UA';
    console.log(`🛡️  Session: ${sessionTag} | ${screen.width}×${screen.height}`);

    return { browser, context };
}

async function newPage(context) {
    const page = await context.newPage();

    await page.addInitScript((localeSnapshot) => {
        const CONFIG_LOCALE_SNAPSHOT = localeSnapshot;

        Object.defineProperty(navigator, 'webdriver', { get: () => false });

        window.chrome = {
            runtime:   {},
            loadTimes: () => ({}),
            csi:       () => ({}),
            app:       {},
        };

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

        const _primaryLocale = CONFIG_LOCALE_SNAPSHOT;
        const _lang          = _primaryLocale.split('-')[0];
        const _langList      = _primaryLocale === _lang
            ? [_lang, 'en-US', 'en']
            : [_primaryLocale, _lang, 'en-US', 'en'];
        Object.defineProperty(navigator, 'languages', { get: () => _langList });

        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });

        const _cores  = [4, 6, 8, 12, 16];
        const _memory = [4, 8, 16];
        const _hwc    = _cores[Math.floor(Math.random() * _cores.length)];
        const _dm     = _memory[Math.floor(Math.random() * _memory.length)];

        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => _hwc });
        Object.defineProperty(navigator, 'deviceMemory',        { get: () => _dm  });

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

        const _origQuery = navigator.permissions?.query?.bind(navigator.permissions);
        if (_origQuery) {
            navigator.permissions.query = (params) =>
                (['notifications', 'geolocation'].includes(params?.name))
                    ? Promise.resolve({ state: 'granted', onchange: null })
                    : _origQuery(params);
        }
    }, CONFIG.locale);

    return page;
}

// ─────────────────────────────────────────────
//  SESSION CHECK
// ─────────────────────────────────────────────

async function checkSession(page, label = 'general') {
    batchStats.sessionChecks++;
    console.log(`🔐 Check (${label})...`);

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

        // Wait for the first listbox button to appear.
        await page.waitForSelector(CONFIG.selectors.listboxBtn, { timeout: 15_000 });

        // FIX BUG1: waitForSelector fires on the FIRST button only.
        // The second button is rendered by JS shortly after.
        // waitForFunction polls until BOTH buttons are present (up to 10s).
        await page.waitForFunction(
            (sel) => document.querySelectorAll(sel).length >= 2,
            CONFIG.selectors.listboxBtn,
            { timeout: 10_000 }
        ).catch(() => {});
        // Non-fatal: if still only 1 button after 10s, count check below handles it.

        const btnLocator = page.locator(CONFIG.selectors.listboxBtn);
        const btnCount   = await btnLocator.count();

        if (btnCount < 2) {
            await sleepRand(CONFIG.delays.beforeSkip);
            console.log(`⚠️  ${itemLabel(item)} - buttons not found (${btnCount})`);
            return { status: 'pass', num: item.num, message: `buttons not found (${btnCount})` };
        }

        // ── First listbox ──────────────────────────────────────────────────
        const btn0 = btnLocator.nth(0);
        await humanInteract(page, btn0);
        await btn0.click({ force: true, delay: rand(...CONFIG.delays.clickDelay) });
        await sleepRand(CONFIG.delays.afterFirstBtn);

        const option1 = page.locator('[role="listbox"]:visible [role="option"]')
            .filter({ hasText: REGEX_OPTION1 });

        if (await option1.count() === 0) {
            console.log(`⚠️  ${itemLabel(item)} - option1 not found`);
            return { status: 'pass', num: item.num, message: 'option1 not found' };
        }

        const opt1 = option1.first();
        await humanInteract(page, opt1);
        await opt1.click({ force: true });

        await sleepRand(CONFIG.delays.beforeSecondBtn);

        // ── Second listbox ─────────────────────────────────────────────────
        const freshBtns     = page.locator(CONFIG.selectors.listboxBtn);
        const freshBtnCount = await freshBtns.count();

        if (freshBtnCount < 2) {
            console.log(`⚠️  ${itemLabel(item)} - second dropdown disappeared`);
            return { status: 'pass', num: item.num, message: 'second dropdown not found' };
        }

        const btn1 = freshBtns.nth(1);
        await humanInteract(page, btn1);
        await btn1.click({ force: true, delay: rand(...CONFIG.delays.clickDelay) });
        await sleepRand(CONFIG.delays.afterSecondBtn);

        const option2 = page.locator('[role="listbox"]:visible [role="option"]')
            .filter({ hasText: REGEX_OPTION2 });

        if (await option2.count() === 0) {
            console.log(`⚠️  ${itemLabel(item)} - option2 not found`);
            return { status: 'pass', num: item.num, message: 'option2 not found' };
        }

        const opt2 = option2.first();
        await humanInteract(page, opt2);
        await opt2.click({ force: true });

        await sleepRand(CONFIG.delays.beforeSave);

        const saveBtn = page.locator('button')
            .filter({ hasText: REGEX_SAVE })
            .first();

        if (!(await saveBtn.isVisible())) {
            // FIX BUG2: this is a TRUE POSITIVE — the ad is already in the correct
            // state. The save button only appears when a change is detected.
            // Log as stable (✔️) not as warning.
            console.log(`✔️  ${itemLabel(item)} - stable (already correct)`);
            return { status: 'pass', num: item.num, message: 'save button not visible' };
        }

        await lightInteract(saveBtn);
        await sleepRand(CONFIG.delays.hoverSave);
        await saveBtn.click({ force: true });
        await sleepRand(CONFIG.delays.afterSave);

        console.log(`✅ ${itemLabel(item)} - ok`);
        return { status: 'ok', num: item.num, message: timeStrEN() };

    } catch (err) {
        const shortMsg = sanitizeError(err.message);
        console.log(`❌ ${itemLabel(item)} - ${shortMsg}`);

        try {
            const shotPath = path.join(
                CONFIG.logsDir,
                `err-${BATCH_DATE}-I${item.num}-${Date.now()}.png`,
            );
            await page.screenshot({ path: shotPath, fullPage: false });
            console.log(`📸 Screenshot: ${path.basename(shotPath)}`);
        } catch {}

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
    console.log(`\n🔄 Cycle ${cycleNum} - ${timeStrEN()} - ETA: ${calcETA()}`);

    const totalI = CONFIG.items.length;
    const ready  = getReadyItems();

    if (ready.length === 0) {
        const nearestNext = Math.min(
            ...CONFIG.items.map(it => itemState[it.num].nextCheck)
        );
        const rawWait = nearestNext - Date.now();
        const maxWait = RUN_END ? Math.max(0, RUN_END - Date.now() - 15_000) : rawWait;
        const waitMs  = Math.max(2_000, Math.min(rawWait, maxWait));
        console.log(`⏸️  No items ready — sleeping ${(waitMs / 1000).toFixed(0)}s`);
        await sleepMs(waitMs);
        batchStats.totalCycles++;
        return { i: 0, ok: 0, pass: 0, stable: 0, errors: 0, noItems: true };
    }

    const iCount   = Math.min(ready.length, rand(CONFIG.minPerCycle, totalI));
    const selected = ready.slice(0, iCount);

    const nowMs      = Date.now();
    const inCooldown = CONFIG.items.filter(it =>
        itemState[it.num].cooldown && itemState[it.num].nextCheck > nowMs
    ).length;
    const scheduled  = totalI - ready.length - inCooldown;

    console.log(`📌 I: ${selected.length}/${totalI}  ready:${ready.length}  cooldown:${inCooldown}  scheduled:${scheduled}`);

    const cycleStats = newCycleStats();
    const lines      = [];

    for (const item of selected) {
        const result = await runItemWithTimeout(page, item);

        // FIX BUG2: pass message to scheduleNext so it can distinguish stable
        // items from real failures.
        scheduleNext(item, result.status, result.message || '');

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
        `Cycle ${cycleNum} - ${timeStrEN()}`,
        `I: ${selected.length}/${totalI}`,
        '------------------------------------------',
        ...lines,
        '------------------------------------------',
        `📈 ${formatCycleStats(cycleStats)}`,
        `Cycle ${cycleNum} done: ${timeStrEN()}`,
    ].join('\n'), cycleNum);

    if (cycleNum % CONFIG.logEvery === 0) {
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

    REGEX_OPTION1 = buildRegex(CONFIG.selectors.option1);
    REGEX_OPTION2 = buildRegex(CONFIG.selectors.option2);
    REGEX_SAVE    = buildRegex(CONFIG.selectors.saveBtn);

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

    // parseCookies uses CONFIG.items to derive domain — must be called after items are set.
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

    const warmUpMs = Math.floor(Math.random() * CONFIG.startDelayMax);
    if (warmUpMs > 1000) {
        console.log(`⏳ Warm-up: ${(warmUpMs / 1000).toFixed(1)}s`);
        await sleepMs(warmUpMs);
    }

    RUN_END = Date.now() + CONFIG.runDurationMin * 60_000;
    console.log(`⏱️  Run ends at: ${new Date(RUN_END).toLocaleTimeString('en-CA', { ...tz(), hour12: false })}`);

    let nextReset = rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
    let cycleNum  = 0;

    while (Date.now() < RUN_END) {
        cycleNum++;

        if (cycleNum > 1 && cycleNum >= nextReset) {
            const hasReadyItems = getReadyItems().length > 0;
            if (hasReadyItems) {
                try { await context.close(); } catch {}
                try { await browser.close(); } catch {}

                ({ browser, context, page } = await resetEngine(cookies, cycleNum));
                nextReset = cycleNum + rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
                console.log(`🔁 Next reset at cycle ${nextReset}`);
            } else {
                nextReset++;
            }
        }

        const cycleStats = await runCycle(page, cycleNum);

        // FIX BUG3: only trigger session check when there are REAL failures.
        // "stable" passes (save button not visible) are NOT failures — they mean
        // items are already in the correct state. Checking session for them is wasted time.
        const hasRealFailures = !cycleStats.noItems
            && cycleStats.i > 0
            && cycleStats.ok === 0
            && (cycleStats.pass - cycleStats.stable) > 0;

        if (hasRealFailures) {
            console.warn(`\n⚠️  Cycle ${cycleNum}: ${cycleStats.i} items, 0 ok, ${cycleStats.pass - cycleStats.stable} real fail → checking session...`);
            const { valid } = await checkSession(page, `check-${cycleNum}`);
            if (!valid) {
                await haltAndNotify(browser, context, `Session check failed after cycle ${cycleNum}.`);
            }
        }

        if (!cycleStats.noItems && Date.now() < RUN_END) {
            const waitMs = rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax);
            console.log('\n#####################');
            console.log(`⏳ Waiting ${(waitMs / 1000).toFixed(1)}s...`);
            console.log('#####################\n');
            await sleepMs(waitMs);
        }
    }

    if (cycleNum > 0) {
        const finalLogPath = getLogPath(cycleNum);
        if (fs.existsSync(finalLogPath)) {
            console.log(`📁 Final log: ${finalLogPath}`);
            await sendPartSummary(cycleNum);
        }
    }

    try { await context.close(); } catch {}
    try { await browser.close(); } catch {}

    console.log(`\n🎉 Done! - ${timeStrEN()}`);
    console.log(formatBatchStats());

    await sendSummary(batchStartTime);
})();
