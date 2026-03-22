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
        pageLoad:        [1500, 2500],
        beforeFirstBtn:  [150, 250],
        afterFirstBtn:   [800, 1200],
        beforeSecondBtn: [150, 250],
        afterSecondBtn:  [800, 1200],
        beforeSave:      [1000, 1500],
        beforeNext:      [100, 200],
        beforeSkip:      [50, 150],
    },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
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
    const body = formatBatchStats();
    await sendEmail(`📦 Complete - ${batchStartTime}`, body);
}

function parseCookies(raw) {
    try {
        const trimmed = normalize(raw);
        if (!trimmed) return [];
        if (trimmed.startsWith('[')) return JSON.parse(trimmed);
        const d = new URL(CONFIG.items[0].url).hostname.replace(/^[^.]+/, '');
        return trimmed.split(';').map(c => { const [name, ...rest] = c.trim().split('='); if (!name) return null; return { name: name.trim(), value: rest.join('=').trim(), domain: d, path: '/' }; }).filter(Boolean);
    } catch (e) { return []; }
}

async function createBrowser(cookies) {
    const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-blink-features=AutomationControlled'] });
    const context = await browser.newContext({
        userAgent: CONFIG.userAgent,
        locale: CONFIG.locale,
        viewport: { width: 1280, height: 720 },
    });
    await context.route('**/*.{png,jpg,jpeg,gif,webp,svg,css,woff,woff2}', r => r.abort());
    await context.addCookies(cookies);
    return { browser, context };
}

async function checkSession(page) {
    batchStats.sessionChecks++;
    try {
        await page.goto(CONFIG.items[0].url, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await sleep([1500, 2500]);
        const found = await page.locator(CONFIG.selectors.listboxBtn).first().isVisible();
        return { valid: found };
    } catch { return { valid: false }; }
}

async function haltAndNotify(browser, message) {
    const body = `${message}\n\n${formatBatchStats()}`;
    await sendEmail(`🚨 Alert - ${timeStrEN()}`, body);
    if (browser) await browser.close();
    process.exit(1);
}

function sanitizeError(msg) {
    return String(msg || '').replace(/https?:\/\/\S+/g, '[URL]').split('\n')[0];
}

function optionLocator(page, text) {
    const cleanText = String(text).trim();
    return page.locator('role=option').filter({ hasText: new RegExp(`^\\s*${cleanText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`, 'i') }).first();
}

async function processItem(page, item) {
    try {
        await page.goto(item.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await sleep(CONFIG.delays.pageLoad);

        const btns = page.locator(CONFIG.selectors.listboxBtn);
        const btnCount = await btns.count();
        if (btnCount < 2) return { status: 'pass', num: item.num, message: 'Interface missing' };

        await btns.nth(0).click({ force: true });
        await sleep([1000, 1500]);

        const allOptions = await page.locator('role=option').allInnerTexts();
        const cleanedOptions = allOptions.map(t => t.trim()).filter(Boolean);
        console.log(`🔍 [Item ${item.num}] Full List Content:`, cleanedOptions);
        appendLog(`[TEST] Dropdown Options for Item ${item.num}: ${cleanedOptions.join(' | ')}`, batchStats.totalCycles);

        const opt1 = optionLocator(page, CONFIG.selectors.option1);
        await opt1.waitFor({ state: 'visible', timeout: 7000 });
        await opt1.click({ force: true });
        await sleep([800, 1200]);

        await btns.nth(1).click({ force: true });
        await sleep([800, 1200]);

        const opt2 = optionLocator(page, CONFIG.selectors.option2);
        await opt2.waitFor({ state: 'visible', timeout: 7000 });
        await opt2.click({ force: true });
        await sleep(CONFIG.delays.beforeSave);

        const saveBtn = page.getByRole('button').filter({ hasText: new RegExp(`^\\s*${CONFIG.selectors.saveBtn.trim()}\\s*$`, 'i') }).first();
        await saveBtn.click({ force: true });
        
        return { status: 'ok', num: item.num, message: timeStrEN() };
    } catch (err) {
        return { status: 'error', num: item.num, message: sanitizeError(err.message) };
    } finally {
        await sleep(CONFIG.delays.beforeNext);
    }
}

async function runCycle(page, cycleNum) {
    const totalI = CONFIG.items.length;
    const selected = [...CONFIG.items].sort(() => 0.5 - Math.random()).slice(0, rand(CONFIG.minPerCycle, totalI));
    const cycleStats = newCycleStats();
    const lines = [];
    for (const item of selected) {
        const result = await processItem(page, item);
        trackResult(result, cycleStats);
        lines.push(formatResult(result));
    }
    batchStats.totalCycles++;
    appendLog(`\nCycle ${cycleNum} - ${timeStrEN()}\n` + lines.join('\n') + `\n📈 ${formatCycleStats(cycleStats)}`, cycleNum);
    if (cycleNum % CONFIG.logEvery === 0 || cycleNum === CONFIG.rounds) await sendPartSummary(cycleNum);
    return cycleStats;
}

(async () => {
    CONFIG.email             = requireEnv('I_M', process.env.I_M);
    CONFIG.timezone          = requireEnv('I_TZ', process.env.I_TZ);
    CONFIG.locale            = requireEnv('I_LC', process.env.I_LC);
    CONFIG.selectors.option1 = requireEnv('I_O1', process.env.I_O1);
    CONFIG.selectors.option2 = requireEnv('I_O2', process.env.I_O2);
    CONFIG.selectors.saveBtn = requireEnv('I_SB', process.env.I_SB);
    CONFIG.items = requireEnv('I_U', process.env.I_U).split(',').map(e => {
        const [n, u] = e.trim().split('|');
        return { num: normalize(n), url: normalize(u) };
    }).filter(e => e.num && e.url);

    BATCH_DATE = `${dateStr()}-${timeStrEN().replace(/:/g,'')}`;
    ensureDirs();
    batchStats.startTime = Date.now();
    initDriveClient();
    initMailTransporter();

    const cookies = parseCookies(process.env.I_CO);
    let { browser, context } = await createBrowser(cookies);
    let page = await context.newPage();
    
    const { valid } = await checkSession(page);
    if (!valid) await haltAndNotify(browser, 'Session invalid at start.');

    let nextReset = rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
    for (let i = 1; i <= CONFIG.rounds; i++) {
        if (i >= nextReset) {
            await context.close(); await browser.close();
            ({ browser, context } = await createBrowser(cookies));
            page = await context.newPage();
            nextReset = i + rand(CONFIG.resetAfterMin, CONFIG.resetAfterMax);
        }
        const stats = await runCycle(page, i);
        if (stats.ok === 0 && stats.errors > 0) {
            const { valid: v } = await checkSession(page);
            if (!v) await haltAndNotify(browser, `Session died at cycle ${i}`);
        }
        await sleepMs(rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax));
    }
    await browser.close();
    await sendSummary(timeStrEN());
})();
