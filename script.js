const { chromium } = require('playwright');
const nodemailer   = require('nodemailer');
const { google }   = require('googleapis');
const fs           = require('fs');
const path         = require('path');

const CONFIG = {
    rounds:           300,
    logEvery:         50,
    minPerCycle:      20,
    cycleDelayMin:    2000,
    cycleDelayMax:    5000,
    resetAfterMin:    15,
    resetAfterMax:    20,
    cookieRetries:    3,
    cookieRetryDelay: 5000,
    timezone:         '',
    dailyHour:        23,
    reportHours:      [0, 6, 12, 18],
    email:            '',
    logsDir:          'logs',
    items: [],
    delays: {
        pageLoad:        [800, 1500],
        beforeFirstBtn:  [150, 250],
        afterFirstBtn:   [300, 700],
        beforeSecondBtn: [150, 250],
        afterSecondBtn:  [300, 700],
        beforeSave:      [400, 900],
        beforeNext:      [50, 150],
        beforeSkip:      [50, 150],
    },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    locale:    '',
    selectors: {
        listboxBtn: 'button[aria-haspopup="listbox"]',
        option1:    '',
        option2:    '',
        saveBtn:    '',
    },
};

let BATCH_DATE = null;
let driveClient = null;
let mailTransporter = null;

const rand    = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleep   = ([min, max]) => new Promise(res => setTimeout(res, rand(min, max)));
const sleepMs = (ms) => new Promise(res => setTimeout(res, ms));
const tz        = () => ({ timeZone: CONFIG.timezone || 'UTC' });
const timeStrEN = () => new Date().toLocaleTimeString('en-CA', { ...tz(), hour12: false });
const dateStr   = () => new Date().toLocaleDateString('en-CA', tz());
const hourNow   = () => new Date(new Date().toLocaleString('en', tz())).getHours();

const batchStats = {
    totalCycles:   0,
    totalI:        0,
    ok:            0,
    pass:          0,
    errors:        0,
    resets:        0,
    sessionChecks: 0,
    driveUploads:  0,
    startTime:     null,
};

function normalize(value) {
    return String(value ?? '').trim();
}

function requireEnv(name, value) {
    const normalized = normalize(value);
    if (!normalized) throw new Error(`${name} missing`);
    return normalized;
}

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

function formatResult(r) {
    const tag = `I#${r.num}`;
    if (r.status === 'ok') return `  ✅ ${tag} - ${r.message}`;
    if (r.status === 'pass') return `  ⚠️ ${tag} - ${r.message}`;
    return `  ❌ ${tag} - ${r.message}`;
}

function formatCycleStats(cycleStats) {
    const rate = cycleStats.i > 0 ? ((cycleStats.ok / cycleStats.i) * 100).toFixed(0) : 0;
    return `💾 ${cycleStats.ok} | ⚠️ ${cycleStats.pass} | ❌ ${cycleStats.errors} | ${rate}%`;
}

function formatBatchStats() {
    const elapsed = Math.round((Date.now() - batchStats.startTime) / 60000);
    const rate    = batchStats.totalI > 0 ? ((batchStats.ok / batchStats.totalI) * 100).toFixed(1) : 0;
    return [
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
        '📊 Summary:',
        `  🔄 Cycles    : ${batchStats.totalCycles}/${CONFIG.rounds}`,
        `  📋 Total I   : ${batchStats.totalI}`,
        `  💾 OK        : ${batchStats.ok}`,
        `  ⚠️  Pass     : ${batchStats.pass}`,
        `  ❌ Errors    : ${batchStats.errors}`,
        `  🔁 Resets    : ${batchStats.resets}`,
        `  🔐 Checks    : ${batchStats.sessionChecks}`,
        `  ☁️  Uploads  : ${batchStats.driveUploads}`,
        `  ✅ Rate      : ${rate}%`,
        `  ⏱️  Elapsed  : ${elapsed} min`,
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
    ].join('\n');
}

function ensureDirs() {
    if (!fs.existsSync(CONFIG.logsDir)) fs.mkdirSync(CONFIG.logsDir, { recursive: true });
}

function getLogLabel(cycleNum) {
    return cycleNum === CONFIG.rounds ? 'final' : `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
}

function getLogPath(cycleNum) {
    return `${CONFIG.logsDir}/${BATCH_DATE}-${getLogLabel(cycleNum)}.txt`;
}

function appendLog(text, cycleNum) {
    fs.appendFileSync(getLogPath(cycleNum), text + '\n');
}

function initDriveClient() {
    const credentials = JSON.parse(process.env.GDR_K);
    const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/drive.file'] });
    driveClient = google.drive({ version: 'v3', auth });
}

async function uploadToDrive(logPath) {
    try {
        const fileName = path.basename(logPath);
        const folderId = process.env.GDR_D;
        const existing = await driveClient.files.list({ q: `name='${fileName}' and '${folderId}' in parents and trashed=false`, fields: 'files(id)' });
        const fileStream   = fs.createReadStream(logPath);
        const fileMetadata = { name: fileName, parents: [folderId] };
        const media        = { mimeType: 'text/plain', body: fileStream };
        if (existing.data.files.length > 0) {
            const fileId = existing.data.files[0].id;
            await driveClient.files.update({ fileId, media });
        } else {
            await driveClient.files.create({ requestBody: fileMetadata, media });
        }
        batchStats.driveUploads++;
    } catch (err) { console.error('❌ Drive error:', err); }
}

function initMailTransporter() {
    mailTransporter = nodemailer.createTransport({ service: 'gmail', auth: { user: CONFIG.email, pass: process.env.M_P } });
}

async function sendEmail(subject, body) {
    try { await mailTransporter.sendMail({ from: CONFIG.email, to: CONFIG.email, subject, text: body }); }
    catch (err) { console.error(`❌ Email error: ${err.message}`); }
}

async function sendPartSummary(cycleNum) {
    const logPath = getLogPath(cycleNum);
    if (!fs.existsSync(logPath)) return;
    const logContent = fs.readFileSync(logPath, 'utf8');
    const label = getLogLabel(cycleNum);
    const subject = `📋 Log ${label} - cycles ${cycleNum - CONFIG.logEvery + 1}-${cycleNum} - ${BATCH_DATE}`;
    await sendEmail(subject, logContent + '\n\n' + formatBatchStats());
    await uploadToDrive(logPath);
}

async function sendSummary(batchStartTime) {
    const hour = hourNow();
    const body = formatBatchStats();
    await sendEmail(`📦 Complete - ${batchStartTime}`, body);
    if (CONFIG.reportHours.includes(hour)) await sendEmail(`🕐 6h report - ${timeStrEN()}`, body);
    if (hour === CONFIG.dailyHour && !CONFIG.reportHours.includes(hour)) await sendEmail(`📊 Daily report - ${BATCH_DATE}`, body);
}

function parseCookies(raw) {
    try {
        const trimmed = normalize(raw);
        if (!trimmed) return [];
        if (trimmed.startsWith('[')) return JSON.parse(trimmed);
        const d = new URL(CONFIG.items[0].url).hostname.replace(/^[^.]+/, '');
        return trimmed.split(';').map(c => { const [name, ...rest] = c.trim().split('='); if (!name) return null; return { name: name.trim(), value: rest.join('=').trim(), domain: d, path: '/' }; }).filter(Boolean);
    } catch (e) { console.error('❌ Parse failed:', e.message); return []; }
}

async function createBrowser(cookies) {
    const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-blink-features=AutomationControlled'] });
    const context = await browser.newContext({
        userAgent: CONFIG.userAgent,
        locale: CONFIG.locale,
        viewport: { width: 1366 + Math.floor(Math.random() * 100), height: 768 + Math.floor(Math.random() * 100) },
    });
    await context.route('**/*.{png,jpg,jpeg,gif,webp,svg,css,woff,woff2,ttf,eot}', r => r.abort());
    await context.route('**/*analytics*', r => r.abort());
    await context.route('**/*tracking*', r => r.abort());
    await context.route('**/*collect*', r => r.abort());
    await context.route('**/*beacon*', r => r.abort());
    await context.addCookies(cookies);
    return { browser, context };
}

async function checkSession(page, label = 'general') {
    batchStats.sessionChecks++;
    for (let attempt = 1; attempt <= CONFIG.cookieRetries; attempt++) {
        try {
            await page.goto(CONFIG.items[0].url, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await sleep(CONFIG.delays.pageLoad);
            const foundCount = await page.locator(CONFIG.selectors.listboxBtn).count();
            if (foundCount > 0) return { valid: true };
        } catch {}
        if (attempt < CONFIG.cookieRetries) await sleepMs(CONFIG.cookieRetryDelay);
    }
    return { valid: false };
}

async function haltAndNotify(browser, message) {
    const body = `${message}\n\n${formatBatchStats()}`;
    await sendEmail(`🚨 Alert - ${timeStrEN()}`, body);
    await browser.close();
    process.exit(1);
}

function sanitizeError(msg) {
    return String(msg || '').replace(/https?:\/\/\S+/g, '[URL]').split('\n')[0];
}

function isCycleEmpty(cycleStats) {
    return cycleStats.ok === 0;
}

async function withTimeout(promise, ms) {
    return Promise.race([
        promise,
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error('item timeout')), ms)
        )
    ]);
}

function escapeRegex(text) {
    return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function optionLocator(page, text) {
    const rx = new RegExp(`^\\s*${escapeRegex(String(text).trim())}\\s*$`, 'i');
    return page.getByRole('option', { name: rx }).first();
}

async function processItem(page, item) {
    try {
        await page.goto(item.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await sleep(CONFIG.delays.pageLoad);

        const btns = await page.$$(CONFIG.selectors.listboxBtn);
        if (btns.length < 2) {
            return { status: 'pass', num: item.num, message: 'buttons not found' };
        }

        await btns[0].click();
        await sleep([600, 1000]);

        const options = await page.getByRole('option').allInnerTexts();
        const cleanOptions = options.map(t => t.trim()).filter(t => t.length > 0);
        console.log(`🔍 [Item ${item.num}] Options:`, cleanOptions);
        appendLog(`[TEST] Found options: ${cleanOptions.join(' | ')}`, batchStats.totalCycles);

        const option1 = optionLocator(page, CONFIG.selectors.option1);
        await option1.waitFor({ state: 'visible', timeout: 5000 });
        await option1.click();
        await sleep([500, 800]);

        const freshBtns = await page.$$(CONFIG.selectors.listboxBtn);
        if (!freshBtns[1]) {
            return { status: 'pass', num: item.num, message: 'second dropdown not found' };
        }

        await freshBtns[1].click();
        await sleep([500, 800]);

        const option2 = optionLocator(page, CONFIG.selectors.option2);
        await option2.waitFor({ state: 'visible', timeout: 5000 });
        await option2.click();
        await sleep(CONFIG.delays.beforeSave);

        const saveBtn = page.getByRole('button', {
            name: new RegExp(`^\\s*${escapeRegex(String(CONFIG.selectors.saveBtn).trim())}\\s*$`, 'i')
        }).first();

        await saveBtn.waitFor({ state: 'visible', timeout: 5000 });
        await saveBtn.click();

        return { status: 'ok', num: item.num, message: timeStrEN() };

    } catch (err) {
        return { status: 'error', num: item.num, message: sanitizeError(err.message) };
    } finally {
        await sleep(CONFIG.delays.beforeNext);
    }
}

async function runItemWithTimeout(page, item) {
    try { return await withTimeout(processItem(page, item), 90000); }
    catch (err) { await page.reload({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {}); return { status: 'error', num: item.num, message: sanitizeError(err.message) }; }
}

async function runCycle(page, cycleNum) {
    const totalI = CONFIG.items.length;
    const minI   = Math.min(CONFIG.minPerCycle, totalI);
    const iCount = rand(minI, totalI);
    const selected = [...CONFIG.items].sort(() => 0.5 - Math.random()).slice(0, iCount);
    const cycleStats = newCycleStats();
    const lines = [];
    for (const item of selected) { const result = await runItemWithTimeout(page, item); trackResult(result, cycleStats); lines.push(formatResult(result)); }
    batchStats.totalCycles++;
    appendLog([
        '\n==========================================',
        `Cycle ${cycleNum}/${CONFIG.rounds} - ${timeStrEN()}`,
        `I: ${iCount}/${totalI}`,
        '------------------------------------------',
        ...lines,
        '------------------------------------------',
        `📈 ${formatCycleStats(cycleStats)}`,
        `Cycle ${cycleNum} done: ${timeStrEN()}`,
    ].join('\n'), cycleNum);
    if (cycleNum % CONFIG.logEvery === 0 || cycleNum === CONFIG.rounds) await sendPartSummary(cycleNum);
    return cycleStats;
}

async function resetEngine(cookies, cycleNum) {
    batchStats.resets++;
    const { browser: newBrowser, context: newContext } = await createBrowser(cookies);
    const newPage = await newContext.newPage();
    await newPage.addInitScript(() => { Object.defineProperty(navigator, 'webdriver', { get: () => false }); window.chrome = { runtime: {} }; });
    const { valid } = await checkSession(newPage, `reset-${cycleNum}`);
    if (!valid) await haltAndNotify(newBrowser, `Session invalid at cycle ${cycleNum}.`);
    return { browser: newBrowser, context: newContext, page: newPage };
}

(async () => {
    const required = ['I_CO','GDR_K','GDR_D','M_P','I_M','I_U','I_TZ','I_LC','I_O1','I_O2','I_SB'];
    for (const key of required) if (!process.env[key]) process.exit(1);

    CONFIG.email             = requireEnv('I_M', process.env.I_M);
    CONFIG.timezone          = requireEnv('I_TZ', process.env.I_TZ);
    CONFIG.locale            = requireEnv('I_LC', process.env.I_LC);
    CONFIG.selectors.option1 = requireEnv('I_O1', process.env.I_O1);
    CONFIG.selectors.option2 = requireEnv('I_O2', process.env.I_O2);
    CONFIG.selectors.saveBtn = requireEnv('I_SB', process.env.I_SB);
    CONFIG.items = requireEnv('I_U', process.env.I_U).split(',').map(entry => {
        const [num, url] = entry.trim().split('|');
        return { num: normalize(num), url: normalize(url) };
    }).filter(e => e.num && e.url);

    const timeTag = new Date().toLocaleTimeString('en-CA', { timeZone: CONFIG.timezone, hour: '2-digit', minute: '2-digit', hour12: false }).replace(':', '');
    BATCH_DATE = `${dateStr()}-${timeTag}`;
    ensureDirs();
    batchStats.startTime = Date.now();
    initDriveClient();
    initMailTransporter();
    const cookies = parseCookies(process.env.I_CO);
    let { browser, context } = await createBrowser(cookies);
    let page = await context.newPage();
    await page.addInitScript(() => { Object.defineProperty(navigator, 'webdriver', { get: () => false }); window.chrome = { runtime: {} }; });
    const { valid: initialValid } = await checkSession(page, 'startup');
    if (!initialValid) await haltAndNotify(browser, 'Session invalid before batch start.');
    let nextReset = rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
    for (let i = 1; i <= CONFIG.rounds; i++) {
        if (i > 1 && i >= nextReset) {
            await context.close(); await browser.close();
            ({ browser, context, page } = await resetEngine(cookies, i));
            nextReset = i + rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
        }
        const cycleStats = await runCycle(page, i);
        if (isCycleEmpty(cycleStats)) {
            const { valid } = await checkSession(page, `check-${i}`);
            if (!valid) await haltAndNotify(browser, `Session check failed after cycle ${i}.`);
        }
        if (i < CONFIG.rounds) await sleepMs(rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax));
    }
    await context.close(); await browser.close();
    await sendSummary(timeStrEN());
})();
