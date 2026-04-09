'use strict';

// ─────────────────────────────────────────────
//  SECRETS REFERENCE
//  I_CO  : cookies — JSON array or "key=val; key=val" string
//  I_UA  : full User-Agent string (from your real browser)
//  I_FP  : JSON { w, h, hwc, mem, platform, colorScheme, canvasSeed }
//  I_REF : referer (optional)
//  M_P   : Gmail app password
//  I_M   : Gmail address
//  I_U   : "num|url,num|url,..."
//  I_TZ  : timezone e.g. "Asia/Riyadh"
//  I_LC  : locale  e.g. "ar-SA"
//  I_O1  : exact text of first dropdown option
//  I_O2  : exact text of second dropdown option
//  I_SB  : exact text of save button
// ─────────────────────────────────────────────

const { chromium } = require('playwright');
const nodemailer   = require('nodemailer');
const fs           = require('fs');
const path         = require('path');

// ─────────────────────────────────────────────
//  CONFIG
//
//  scheduler.ok = [38_000, 45_000]
//  Derived from 3-run analysis:
//    min observed revert = 48s (I#5, I#15)
//    p10 revert          = ~50s across all items
//    optimal re-visit    = p10 - 8s = ~42s
//    range               = [38s, 45s]
//  This ensures we re-visit before the site reverts
//  in the fastest 10% of cases.
// ─────────────────────────────────────────────

const CONFIG = {
    runDurationMin:            330,

    logEvery:                  9999,  // logging to file every cycle, email ONLY at end
    snapshotEvery:             50,

    cycleDelayMin:             500,
    cycleDelayMax:             1_200,

    resetAfterMin:             80,
    resetAfterMax:             120,

    startDelayMax:             30_000,

    cookieRetries:             3,
    cookieRetryDelay:          5_000,

    extendedCooldownThreshold: 15,
    btnErrLoadThreshold:       5,

    timezone:    '',
    email:       '',
    logsDir:     'logs',
    locale:      '',
    items:       [],

    delays: {
        // Values derived from 3-run analysis.
        // Per-item processing time = 3.6s avg.
        // Minimums allow DOM to settle; any lower risks missed interactions.
        pageLoad:        [200,  400],
        scrollReveal:    [40,   80],
        mouseSettle:     [15,   35],
        afterFirstBtn:   [120,  220],
        beforeSecondBtn: [40,   80],
        afterSecondBtn:  [120,  220],
        beforeSave:      [60,   120],
        afterSave:       [250,  400],
        hoverSave:       [30,   70],
        clickDelay:      [15,   40],
        beforeNext:      [0,    20],
        beforeSkip:      [0,    20],
    },

    scheduler: {
        // [38s, 45s] — optimised from 3-run analysis.
        // Site reverts in 48-55s minimum. We re-visit every 38-45s
        // to catch before the revert in ~40% of cases.
        ok:               [38_000,    45_000],
        stable:           [38_000,    45_000],

        pageError:        [12_000,    20_000],
        error:            [30_000,    60_000],
        failOdd:          [20_000,    35_000],
        failEven:         [35_000,    50_000],
        cooldown:         [300_000,  600_000],
        cooldownExtended: [900_000, 1_800_000],
    },

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

let FP              = null;
let BATCH_DATE      = null;
let RUN_END         = null;
let mailTransporter = null;

let REGEX_OPTION1   = null;
let REGEX_OPTION2   = null;
let REGEX_SAVE      = null;

const itemState = {};

const batchStats = {
    totalCycles:   0,
    totalI:        0,
    ok:            0,
    pass:          0,
    errors:        0,
    resets:        0,
    sessionChecks: 0,
    cooldowns:     0,
    startTime:     null,
};

// Per-item runtime stats for end-of-run analysis
const itemRunStats = {};

// ─────────────────────────────────────────────
//  UTILITIES
// ─────────────────────────────────────────────

const rand      = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleepRand = ([min, max]) => new Promise(r => setTimeout(r, rand(min, max)));
const sleepMs   = (ms)         => new Promise(r => setTimeout(r, ms));
const norm      = (v)          => String(v ?? '').trim();

const tzOpts    = () => ({ timeZone: CONFIG.timezone || 'UTC' });
const timeStrEN = () => new Date().toLocaleTimeString('en-CA', { ...tzOpts(), hour12: false });
const dateStr   = () => new Date().toLocaleDateString('en-CA', tzOpts());
const hourNow   = () => new Date(new Date().toLocaleString('en', tzOpts())).getHours();

function buildRegex(text) {
    return new RegExp(`^${text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i');
}

function calcETA() {
    const end = RUN_END ?? (batchStats.startTime && batchStats.startTime + CONFIG.runDurationMin * 60_000);
    if (!end) return '--:--:--';
    return new Date(Math.max(Date.now(), end)).toLocaleTimeString('en-CA', { ...tzOpts(), hour12: false });
}

function buildSecChUa(ua) {
    const em = ua.match(/Edg\/(\d+)/);
    if (em) return `"Chromium";v="${em[1]}", "Microsoft Edge";v="${em[1]}", "Not-A.Brand";v="99"`;
    const cm = ua.match(/Chrome\/(\d+)/);
    if (cm) return `"Chromium";v="${cm[1]}", "Google Chrome";v="${cm[1]}", "Not-A.Brand";v="99"`;
    return null;
}

// ─────────────────────────────────────────────
//  INTERACTION HELPERS
// ─────────────────────────────────────────────

async function humanInteract(page, locator) {
    try {
        await locator.scrollIntoViewIfNeeded();
        await sleepRand(CONFIG.delays.scrollReveal);
        const box = await locator.boundingBox();
        if (!box) return;
        await page.mouse.move(
            box.x + rand(Math.floor(box.width  * 0.2), Math.floor(box.width  * 0.8)),
            box.y + rand(Math.floor(box.height * 0.2), Math.floor(box.height * 0.8)),
            { steps: rand(3, 6) }
        );
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
// ─────────────────────────────────────────────

function initItemState(item) {
    itemState[item.num] = {
        nextCheck:    0,
        failStreak:   0,
        totalFail:    0,
        cooldown:     false,
        btnErrStreak: 0,
    };
    itemRunStats[item.num] = { ok: 0, stable: 0, fail: 0, cooldowns: 0, lastOkTime: null };
}

function scheduleNext(item, status, message) {
    const st  = itemState[item.num];
    const now = Date.now();

    if (status === 'ok') {
        st.failStreak   = 0;
        st.cooldown     = false;
        st.btnErrStreak = 0;
        st.nextCheck    = now + rand(...CONFIG.scheduler.ok);
        itemRunStats[item.num].ok++;
        itemRunStats[item.num].lastOkTime = now;
        return;
    }

    if (status === 'error') {
        st.nextCheck = now + rand(...CONFIG.scheduler.error);
        return;
    }

    // pass — distinguish by message
    if (message === 'save button not visible') {
        st.cooldown     = false;
        st.btnErrStreak = 0;
        st.nextCheck    = now + rand(...CONFIG.scheduler.stable);
        itemRunStats[item.num].stable++;
        return;
    }

    if (message.startsWith('buttons not found')) {
        st.btnErrStreak++;
        st.cooldown  = false;
        st.nextCheck = now + rand(...CONFIG.scheduler.pageError);
        return;
    }

    // real failure
    st.failStreak++;
    st.totalFail++;
    itemRunStats[item.num].fail++;

    if (st.failStreak >= 5) {
        st.cooldown   = true;
        st.failStreak = 0;
        batchStats.cooldowns++;
        const extended = st.totalFail >= CONFIG.extendedCooldownThreshold;
        st.nextCheck   = now + rand(...CONFIG.scheduler[extended ? 'cooldownExtended' : 'cooldown']);
        const mins     = Math.round((st.nextCheck - now) / 60_000);
        const emoji    = extended ? '🚨' : '🧊';
        const label    = extended ? 'extended cooldown' : 'cooldown';
        const msg      = `${emoji} I#${item.num} — ${label} ${mins}min (totalFail:${st.totalFail})`;
        extended ? console.error(msg) : console.warn(msg);
        // No mid-run email — collected in itemRunStats and sent once at end
        itemRunStats[item.num].cooldowns = (itemRunStats[item.num].cooldowns || 0) + 1;
        return;
    }

    st.cooldown  = false;
    st.nextCheck = now + rand(...CONFIG.scheduler[st.failStreak % 2 === 1 ? 'failOdd' : 'failEven']);
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
    return { i: 0, ok: 0, pass: 0, stable: 0, errors: 0 };
}

function trackResult(result, cs) {
    cs.i++;
    batchStats.totalI++;
    if (result.status === 'ok') {
        cs.ok++;
        batchStats.ok++;
    } else if (result.status === 'pass') {
        cs.pass++;
        batchStats.pass++;
        if (result.message === 'save button not visible') cs.stable++;
    } else {
        cs.errors++;
        batchStats.errors++;
    }
}

const fmtResult = (r, item) =>
    r.status === 'ok'   ? `  ✅ I#${item.num} - ${r.message}` :
    r.status === 'pass' ? `  ⚠️  I#${item.num} - ${r.message}` :
                          `  ❌ I#${item.num} - ${r.message}`;

const fmtCycleStats = (s) =>
    `💾 ${s.ok} | ✔️  ${s.stable} | ⚠️  ${s.pass - s.stable} | ❌ ${s.errors} | ${s.i > 0 ? ((s.ok/s.i)*100).toFixed(0) : 0}%`;

function formatBatchStats() {
    const elapsed = batchStats.startTime ? Math.round((Date.now() - batchStats.startTime) / 60_000) : 0;
    const rate    = batchStats.totalI    ? ((batchStats.ok / batchStats.totalI) * 100).toFixed(1) : '0.0';
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
        `  ✅ Rate      : ${rate}%`,
        `  ⏱️  Elapsed   : ${elapsed} min`,
        `  🏁 ETA       : ${calcETA()}`,
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
    ].join('\n');
}

function formatPerItemReport() {
    const sep = '─'.repeat(52);
    const lines = [
        '\n' + sep,
        '  📋 Per-Item Summary',
        sep,
        `  ${'ID'.padEnd(5)} ${'ok'.padEnd(6)} ${'stable'.padEnd(8)} ${'fail'.padEnd(6)} ${'cd'.padEnd(5)} ${'uptime'}`,
        sep,
    ];
    CONFIG.items.forEach(it => {
        const s = itemRunStats[it.num];
        const total  = s.ok + s.stable + s.fail;
        const uptime = total ? ((s.stable / total) * 100).toFixed(0) + '%' : '-';
        const flag   = s.stable/Math.max(1,total) > 0.5 ? '🟢' : s.stable/Math.max(1,total) > 0.2 ? '🟡' : '🔴';
        lines.push(`  ${flag} ${String(it.num).padEnd(4)} ${String(s.ok).padEnd(6)} ${String(s.stable).padEnd(8)} ${String(s.fail).padEnd(6)} ${String(s.cooldowns||0).padEnd(5)} ${uptime}`);
    });
    lines.push(sep);
    return lines.join('\n');
}

function printStartupSummary() {
    const bar = '═'.repeat(46);
    const sch  = CONFIG.scheduler;
    console.log(`\n${bar}\n  ⚙️  Startup Configuration\n${bar}`);
    console.log(`  📋 Items       : ${CONFIG.items.length}`);
    console.log(`  🔄 Duration    : ${CONFIG.runDurationMin} min`);
    console.log(`  🔁 Reset every : ${CONFIG.resetAfterMin}–${CONFIG.resetAfterMax} cycles`);
    console.log(`  🕐 Timezone    : ${CONFIG.timezone}`);
    console.log(`  🌐 Locale      : ${CONFIG.locale}`);
    console.log('\n  📋 Items:');
    CONFIG.items.forEach(it => console.log(`     ${String(it.num).padStart(3)}`));
    console.log('\n  🕰️  Scheduler:');
    console.log(`     OK/Stable  : ${sch.ok.map(v => v/1000+'s').join('–')}`);
    console.log(`     PageError  : ${sch.pageError.map(v => v/1000+'s').join('–')}`);
    console.log(`     Cooldown   : ${sch.cooldown.map(v => v/60_000+'m').join('–')}`);
    console.log(bar + '\n');
}

function printStateSnapshot() {
    const now = Date.now();
    const sep = '─'.repeat(52);
    console.log(`\n${sep}\n  📊 Item State — cycle ${batchStats.totalCycles}  ETA: ${calcETA()}\n${sep}`);
    console.log(`  ${'ID'.padEnd(5)} ${'Next'.padEnd(8)} ${'Streak'.padEnd(8)} ${'Total'.padEnd(7)} State`);
    console.log(sep);
    CONFIG.items.forEach(it => {
        const st      = itemState[it.num];
        const secLeft = Math.max(0, Math.round((st.nextCheck - now) / 1000));
        const nextStr = secLeft >= 60 ? `${Math.round(secLeft/60)}m` : `${secLeft}s`;
        const state   = st.cooldown ? '🧊 cool' : secLeft === 0 ? '✅ rdy ' : '⏳ wait';
        console.log(`  ${String(it.num).padEnd(5)} ${nextStr.padEnd(8)} ${String(st.failStreak).padStart(2).padEnd(8)} ${String(st.totalFail).padStart(3).padEnd(7)} ${state}`);
    });
    console.log(sep + '\n');
}

// ─────────────────────────────────────────────
//  LOGGING  (file only — no email mid-run)
// ─────────────────────────────────────────────

const LOG_PATH = () => path.join(CONFIG.logsDir, `${BATCH_DATE}.txt`);

function ensureDirs() {
    fs.mkdirSync(CONFIG.logsDir, { recursive: true });
}

function appendLog(text) {
    try { fs.appendFileSync(LOG_PATH(), text + '\n'); }
    catch (err) { console.error('❌ Log write error:', err.message); }
}

// ─────────────────────────────────────────────
//  EMAIL  (single send at end of run)
// ─────────────────────────────────────────────

function initMailTransporter() {
    mailTransporter = nodemailer.createTransport({
        service: 'gmail',
        auth: { user: CONFIG.email, pass: process.env.M_P },
    });
}

async function sendEmail(subject, body) {
    try {
        await mailTransporter.sendMail({ from: CONFIG.email, to: CONFIG.email, subject, text: body });
        console.log(`📧 Sent: ${subject}`);
    } catch (err) {
        console.error(`❌ Email error: ${err.message}`);
    }
}

// Called ONCE at the very end — attaches full log file + stats + per-item report
async function sendFinalReport(runStartTime) {
    const logPath = LOG_PATH();
    const stats   = formatBatchStats();
    const items   = formatPerItemReport();
    const subject = `📦 Done - ${runStartTime} → ${timeStrEN()} - ${BATCH_DATE}`;

    let body = stats + items;
    if (fs.existsSync(logPath)) {
        // Append log content (may be large — first 50k chars)
        const logContent = fs.readFileSync(logPath, 'utf8');
        body += '\n\n' + '═'.repeat(40) + '\nFULL LOG\n' + '═'.repeat(40) + '\n';
        body += logContent.length > 50_000
            ? logContent.slice(0, 50_000) + '\n... [truncated]'
            : logContent;
    }

    await sendEmail(subject, body);
}

// ─────────────────────────────────────────────
//  COOKIES
// ─────────────────────────────────────────────

function parseCookies(raw) {
    const trimmed = raw.trim();
    if (trimmed.startsWith('[')) {
        console.log('📝 Cookie format: JSON');
        try { return JSON.parse(trimmed); }
        catch (err) { console.error('❌ Cookie JSON parse failed:', err.message); return []; }
    }
    console.log('📝 Cookie format: string');
    try {
        const hosts  = [...new Set(CONFIG.items.map(it => new URL(it.url).hostname))];
        if (hosts.length > 1) console.warn(`⚠️  Multiple hosts: ${hosts.join(', ')} — using first`);
        const domain = hosts[0].replace(/^[^.]+/, '');
        return trimmed.split(';').map(c => {
            const [name, ...rest] = c.trim().split('=');
            return name?.trim() ? { name: name.trim(), value: rest.join('=').trim(), domain, path: '/' } : null;
        }).filter(Boolean);
    } catch (err) { console.error('❌ Cookie string parse failed:', err.message); return []; }
}

// ─────────────────────────────────────────────
//  BROWSER  (fixed fingerprint from secrets)
// ─────────────────────────────────────────────

async function createBrowser(cookies) {
    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox',
               '--disable-blink-features=AutomationControlled', '--disable-infobars'],
    });

    const ua      = FP.ua;
    const secChUa = buildSecChUa(ua);
    const lang    = CONFIG.locale.split('-')[0];
    const accept  = CONFIG.locale === lang
        ? `${lang};q=1,en-US;q=0.8,en;q=0.7`
        : `${CONFIG.locale};q=1,${lang};q=0.9,en-US;q=0.8,en;q=0.7`;

    const headers = { 'Accept-Language': accept };
    if (secChUa) {
        headers['sec-ch-ua']          = secChUa;
        headers['sec-ch-ua-mobile']   = '?0';
        headers['sec-ch-ua-platform'] = `"${FP.platform || 'Windows'}"`;
    }
    const ref = norm(process.env.I_REF || '');
    if (ref) headers['Referer'] = ref;

    const context = await browser.newContext({
        userAgent:        ua,
        locale:           CONFIG.locale,
        timezoneId:       CONFIG.timezone,
        colorScheme:      FP.colorScheme || 'dark',
        viewport:         { width: FP.w, height: FP.h },
        extraHTTPHeaders: headers,
        permissions:      ['notifications'],
    });

    // Block images, fonts, trackers — no functional value
    const BLOCK_RE = /\.(png|jpe?g|gif|webp|ico|woff2?|ttf|eot|svg)(\?|$)|analytics|tracking|collect|beacon/i;
    await context.route('**/*', route =>
        BLOCK_RE.test(route.request().url()) ? route.abort() : route.continue()
    );

    await context.addCookies(cookies);
    console.log(`🛡️  ${ua.match(/Chrome\/\d+|Firefox\/\d+|Edg\/\d+/)?.[0] ?? 'UA'} | ${FP.w}×${FP.h}`);
    return { browser, context };
}

async function newPage(context) {
    const page = await context.newPage();

    await page.addInitScript((fp) => {
        Object.defineProperty(navigator, 'webdriver',           { get: () => false });
        Object.defineProperty(navigator, 'platform',            { get: () => fp.platform || 'Win32' });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => fp.hwc  || 8 });
        Object.defineProperty(navigator, 'deviceMemory',        { get: () => fp.mem  || 8 });

        const lang = fp.locale.split('-')[0];
        Object.defineProperty(navigator, 'languages', {
            get: () => fp.locale === lang ? [lang,'en-US','en'] : [fp.locale,lang,'en-US','en'],
        });

        Object.defineProperty(navigator, 'plugins', {
            get: () => Object.assign([
                { name:'Chrome PDF Plugin',  filename:'internal-pdf-viewer',              description:'Portable Document Format' },
                { name:'Chrome PDF Viewer',  filename:'mhjfbmdgcfjbbpaeojofohoefgiehjai', description:'' },
                { name:'Native Client',      filename:'internal-nacl-plugin',             description:'' },
            ], { length: 3 }),
        });

        window.chrome = { runtime:{}, loadTimes:()=>({}), csi:()=>({}), app:{} };

        const _origCtx = HTMLCanvasElement.prototype.getContext;
        HTMLCanvasElement.prototype.getContext = function(type, ...args) {
            const ctx = _origCtx.call(this, type, ...args);
            if (type === '2d' && ctx) {
                const _orig = ctx.getImageData.bind(ctx);
                ctx.getImageData = (x, y, w, h) => {
                    const d = _orig(x, y, w, h);
                    const s = fp.canvasSeed || 7;
                    for (let i = 0; i < d.data.length; i += 64) d.data[i] ^= s & 3;
                    return d;
                };
            }
            return ctx;
        };

        const _origQuery = navigator.permissions?.query?.bind(navigator.permissions);
        if (_origQuery) {
            navigator.permissions.query = (p) =>
                ['notifications','geolocation'].includes(p?.name)
                    ? Promise.resolve({ state:'granted', onchange:null })
                    : _origQuery(p);
        }
    }, { ...FP, locale: CONFIG.locale });

    return page;
}

// ─────────────────────────────────────────────
//  SESSION CHECK
// ─────────────────────────────────────────────

async function checkSession(page, label = 'general') {
    batchStats.sessionChecks++;
    console.log(`🔐 Check (${label})...`);
    const probe = CONFIG.items[Math.floor(Math.random() * CONFIG.items.length)];

    for (let attempt = 1; attempt <= CONFIG.cookieRetries; attempt++) {
        try {
            await page.goto(probe.url, { waitUntil: 'domcontentloaded', timeout: 30_000 });
            const found = await page.waitForSelector(CONFIG.selectors.listboxBtn, { timeout: 10_000 })
                .then(() => true).catch(() => false);
            if (found) { console.log('✅ Session ok'); return { valid: true }; }
            console.warn(`⚠️  Attempt ${attempt}/${CONFIG.cookieRetries} - button not found`);
        } catch (err) {
            console.warn(`⚠️  Attempt ${attempt}/${CONFIG.cookieRetries} - ${err.message}`);
        }
        if (attempt < CONFIG.cookieRetries) await sleepMs(CONFIG.cookieRetryDelay);
    }
    console.error('❌ Session check failed');
    return { valid: false };
}

// ─────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────

const sanitizeError = (msg) => msg.replace(/https?:\/\/\S+/g, '[URL]').split('\n')[0];

function withTimeout(promise, ms) {
    let t;
    return Promise.race([
        promise.finally(() => clearTimeout(t)),
        new Promise((_, reject) => { t = setTimeout(() => reject(new Error('item timeout')), ms); }),
    ]);
}

async function haltAndNotify(browser, context, message) {
    await sendEmail(`🚨 Alert - ${timeStrEN()}`, message + '\n\n' + formatBatchStats());
    try { await context.close(); } catch {}
    try { await browser.close(); } catch {}
    process.exit(1);
}

// ─────────────────────────────────────────────
//  ITEM PROCESSING
// ─────────────────────────────────────────────

async function processItem(page, item) {
    try {
        console.log(`   open:  ${timeStrEN()}`);
        const st = itemState[item.num];

        const useFull   = st.btnErrStreak >= 1 && st.btnErrStreak <= CONFIG.btnErrLoadThreshold;
        const waitUntil = useFull ? 'load' : 'domcontentloaded';
        if (useFull) console.log(`   🔄 waitUntil:'load' (btnErrStreak:${st.btnErrStreak})`);

        await page.goto(item.url, { waitUntil, timeout: 60_000 });
        await sleepRand(CONFIG.delays.pageLoad);

        await page.waitForSelector(CONFIG.selectors.listboxBtn, { timeout: 15_000 });

        // Wait up to 3s for both buttons to render — non-fatal
        await page.waitForFunction(
            (sel) => document.querySelectorAll(sel).length >= 2,
            CONFIG.selectors.listboxBtn,
            { timeout: 3_000 }
        ).catch(() => {});

        const btns     = page.locator(CONFIG.selectors.listboxBtn);
        const btnCount = await btns.count();

        if (btnCount < 2) {
            await sleepRand(CONFIG.delays.beforeSkip);
            console.log(`⚠️  I#${item.num} - buttons not found (${btnCount})`);
            return { status: 'pass', num: item.num, message: `buttons not found (${btnCount})` };
        }

        // ── First listbox ──────────────────────────────────────────
        const btn0 = btns.nth(0);
        await humanInteract(page, btn0);
        await btn0.click({ force: true, delay: rand(...CONFIG.delays.clickDelay) });
        await sleepRand(CONFIG.delays.afterFirstBtn);

        const opt1Loc = page.locator('[role="listbox"]:visible [role="option"]').filter({ hasText: REGEX_OPTION1 });
        if (await opt1Loc.count() === 0) {
            console.log(`⚠️  I#${item.num} - option1 not found`);
            return { status: 'pass', num: item.num, message: 'option1 not found' };
        }
        const opt1 = opt1Loc.first();
        await humanInteract(page, opt1);
        await opt1.click({ force: true });

        await sleepRand(CONFIG.delays.beforeSecondBtn);

        // ── Second listbox — re-query after DOM mutation ───────────
        const btns2     = page.locator(CONFIG.selectors.listboxBtn);
        const btnCount2 = await btns2.count();
        if (btnCount2 < 2) {
            console.log(`⚠️  I#${item.num} - second dropdown disappeared`);
            return { status: 'pass', num: item.num, message: 'second dropdown not found' };
        }

        const btn1 = btns2.nth(1);
        await humanInteract(page, btn1);
        await btn1.click({ force: true, delay: rand(...CONFIG.delays.clickDelay) });
        await sleepRand(CONFIG.delays.afterSecondBtn);

        const opt2Loc = page.locator('[role="listbox"]:visible [role="option"]').filter({ hasText: REGEX_OPTION2 });
        if (await opt2Loc.count() === 0) {
            console.log(`⚠️  I#${item.num} - option2 not found`);
            return { status: 'pass', num: item.num, message: 'option2 not found' };
        }
        const opt2 = opt2Loc.first();
        await humanInteract(page, opt2);
        await opt2.click({ force: true });

        await sleepRand(CONFIG.delays.beforeSave);

        // ── Save ───────────────────────────────────────────────────
        const saveBtn = page.locator('button').filter({ hasText: REGEX_SAVE }).first();
        if (!(await saveBtn.isVisible())) {
            console.log(`✔️  I#${item.num} - stable (already correct)`);
            return { status: 'pass', num: item.num, message: 'save button not visible' };
        }

        await lightInteract(saveBtn);
        await sleepRand(CONFIG.delays.hoverSave);
        await saveBtn.click({ force: true });
        await sleepRand(CONFIG.delays.afterSave);

        console.log(`✅ I#${item.num} - ok`);
        return { status: 'ok', num: item.num, message: timeStrEN() };

    } catch (err) {
        console.log(`❌ I#${item.num} - ${sanitizeError(err.message)}`);
        return { status: 'error', num: item.num, message: sanitizeError(err.message) };
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

    const ready = getReadyItems();

    if (ready.length === 0) {
        const nearest = Math.min(...CONFIG.items.map(it => itemState[it.num].nextCheck));
        const waitMs  = Math.max(2_000, Math.min(
            nearest - Date.now(),
            RUN_END ? Math.max(0, RUN_END - Date.now() - 15_000) : Infinity
        ));
        console.log(`⏸️  No items ready — sleeping ${(waitMs/1000).toFixed(0)}s`);
        await sleepMs(waitMs);
        batchStats.totalCycles++;
        return { i: 0, ok: 0, pass: 0, stable: 0, errors: 0, noItems: true };
    }

    const selected   = ready.slice(0, CONFIG.items.length);
    const nowMs      = Date.now();
    const inCooldown = CONFIG.items.filter(it => itemState[it.num].cooldown && itemState[it.num].nextCheck > nowMs).length;

    console.log(`📌 I: ${selected.length}/${CONFIG.items.length}  ready:${ready.length}  cd:${inCooldown}  wait:${CONFIG.items.length - ready.length - inCooldown}`);

    const cs    = newCycleStats();
    const lines = [];

    for (const item of selected) {
        const result = await runItemWithTimeout(page, item);
        scheduleNext(item, result.status, result.message || '');

        const st    = itemState[item.num];
        const parts = [`next:${Math.round((st.nextCheck - Date.now())/1000)}s`];
        if (st.failStreak   > 0) parts.push(`streak:${st.failStreak}`);
        if (st.btnErrStreak > 0) parts.push(`btnErr:${st.btnErrStreak}`);
        if (st.cooldown)         parts.push('🧊');
        console.log(`   → ${parts.join(' ')}`);

        trackResult(result, cs);
        lines.push(fmtResult(result, item));
    }

    batchStats.totalCycles++;
    console.log(`📈 ${fmtCycleStats(cs)}\n`);

    // Always write to the single log file — no email mid-run
    appendLog([
        '\n==========================================',
        `Cycle ${cycleNum} - ${timeStrEN()}`,
        `I: ${selected.length}/${CONFIG.items.length}`,
        '------------------------------------------',
        ...lines,
        '------------------------------------------',
        `📈 ${fmtCycleStats(cs)}`,
        `Cycle ${cycleNum} done: ${timeStrEN()}`,
    ].join('\n'));

    if (cycleNum % CONFIG.snapshotEvery === 0) printStateSnapshot();

    return cs;
}

// ─────────────────────────────────────────────
//  RESET ENGINE
// ─────────────────────────────────────────────

async function resetEngine(cookies, cycleNum) {
    console.log(`\n🔁 Reset at cycle ${cycleNum}...`);
    batchStats.resets++;
    const { browser, context } = await createBrowser(cookies);
    const page                 = await newPage(context);
    const { valid }            = await checkSession(page, `reset-${cycleNum}`);
    if (!valid) await haltAndNotify(browser, context, `Session invalid at cycle ${cycleNum}.`);
    console.log('✅ Reset done');
    return { browser, context, page };
}

// ─────────────────────────────────────────────
//  MAIN
// ─────────────────────────────────────────────

(async () => {
    console.log('🚀 New run:', timeStrEN());

    const missing = ['I_CO','M_P','I_M','I_U','I_TZ','I_LC','I_O1','I_O2','I_SB','I_UA','I_FP']
        .filter(k => !process.env[k]);
    if (missing.length) { console.error('❌ Missing env vars:', missing.join(', ')); process.exit(1); }

    try {
        FP    = JSON.parse(process.env.I_FP);
        FP.ua = norm(process.env.I_UA);
    } catch (err) { console.error('❌ I_FP parse failed:', err.message); process.exit(1); }

    CONFIG.email             = norm(process.env.I_M);
    CONFIG.timezone          = norm(process.env.I_TZ);
    CONFIG.locale            = norm(process.env.I_LC);
    CONFIG.selectors.option1 = norm(process.env.I_O1);
    CONFIG.selectors.option2 = norm(process.env.I_O2);
    CONFIG.selectors.saveBtn = norm(process.env.I_SB);

    REGEX_OPTION1 = buildRegex(CONFIG.selectors.option1);
    REGEX_OPTION2 = buildRegex(CONFIG.selectors.option2);
    REGEX_SAVE    = buildRegex(CONFIG.selectors.saveBtn);

    CONFIG.items = norm(process.env.I_U).split(',').map(entry => {
        const [num, url] = entry.trim().split('|');
        return (norm(num) && norm(url)) ? { num: norm(num), url: norm(url) } : null;
    }).filter(Boolean);
    if (!CONFIG.items.length) { console.error('❌ I_U empty or invalid'); process.exit(1); }

    const cookies = parseCookies(process.env.I_CO);
    if (!cookies.length) { console.error('❌ Cookies empty or invalid'); process.exit(1); }

    CONFIG.items.forEach(initItemState);

    BATCH_DATE = `${dateStr()}-${new Date().toLocaleTimeString('en-CA',
        { timeZone: CONFIG.timezone, hour: '2-digit', minute: '2-digit', hour12: false }
    ).replace(':', '')}`;

    ensureDirs();
    batchStats.startTime = Date.now();
    initMailTransporter();
    printStartupSummary();

    let { browser, context } = await createBrowser(cookies);
    let page                 = await newPage(context);
    const runStartTime       = timeStrEN();

    const { valid } = await checkSession(page, 'startup');
    if (!valid) await haltAndNotify(browser, context, 'Session invalid before batch start.');

    const warmUp = Math.floor(Math.random() * CONFIG.startDelayMax);
    if (warmUp > 1000) { console.log(`⏳ Warm-up: ${(warmUp/1000).toFixed(1)}s`); await sleepMs(warmUp); }

    RUN_END = Date.now() + CONFIG.runDurationMin * 60_000;
    console.log(`⏱️  Run ends at: ${new Date(RUN_END).toLocaleTimeString('en-CA', { ...tzOpts(), hour12: false })}`);

    let nextReset = rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
    let cycleNum  = 0;

    while (Date.now() < RUN_END) {
        cycleNum++;

        if (cycleNum > 1 && cycleNum >= nextReset) {
            if (getReadyItems().length > 0) {
                try { await context.close(); } catch {}
                try { await browser.close(); } catch {}
                ({ browser, context, page } = await resetEngine(cookies, cycleNum));
                nextReset = cycleNum + rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
                console.log(`🔁 Next reset at cycle ${nextReset}`);
            } else {
                nextReset++;
            }
        }

        const cs = await runCycle(page, cycleNum);

        // Session check only on genuine form failures
        if (!cs.noItems && cs.i > 0 && cs.ok === 0 && (cs.pass - cs.stable) > 0) {
            console.warn(`\n⚠️  Cycle ${cycleNum}: 0 ok, ${cs.pass - cs.stable} real fail → checking session...`);
            const { valid: sv } = await checkSession(page, `check-${cycleNum}`);
            if (!sv) await haltAndNotify(browser, context, `Session check failed after cycle ${cycleNum}.`);
        }

        if (!cs.noItems && Date.now() < RUN_END) {
            const w = rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax);
            console.log(`⏳ ${(w/1000).toFixed(1)}s...`);
            await sleepMs(w);
        }
    }

    try { await context.close(); } catch {}
    try { await browser.close(); } catch {}

    console.log(`\n🎉 Done! - ${timeStrEN()}`);
    console.log(formatBatchStats());
    console.log(formatPerItemReport());

    // Single email at end with full log attached
    await sendFinalReport(runStartTime);
})();
