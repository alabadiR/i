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
    timezone:         '',
    email:            '',
    logsDir:          'logs',
    items: [],
    delays: {
        pageLoad:        [2500, 3500],
        afterFirstBtn:   [1500, 2500],
        afterSecondBtn:  [1500, 2500],
        beforeSave:      [2000, 3000],
        beforeNext:      [300, 600],
    },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
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

const batchStats = {
    totalCycles: 0,
    totalI: 0,
    ok: 0,
    pass: 0,
    errors: 0,
    startTime: null,
};

function normalize(value) {
    return String(value ?? '').trim();
}

function ensureDirs() {
    if (!fs.existsSync(CONFIG.logsDir)) fs.mkdirSync(CONFIG.logsDir, { recursive: true });
}

function appendLog(text, cycleNum) {
    const label = cycleNum === CONFIG.rounds ? 'final' : `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
    const logPath = `${CONFIG.logsDir}/${BATCH_DATE}-${label}.txt`;
    fs.appendFileSync(logPath, text + '\n');
}

function initDriveClient() {
    const credentials = JSON.parse(process.env.GDR_K);
    const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/drive.file'] });
    driveClient = google.drive({ version: 'v3', auth });
}

async function uploadToDrive(cycleNum) {
    try {
        const label = cycleNum === CONFIG.rounds ? 'final' : `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
        const logPath = `${CONFIG.logsDir}/${BATCH_DATE}-${label}.txt`;
        const fileName = path.basename(logPath);
        const folderId = process.env.GDR_D;
        const fileStream = fs.createReadStream(logPath);
        await driveClient.files.create({ requestBody: { name: fileName, parents: [folderId] }, media: { mimeType: 'text/plain', body: fileStream } });
    } catch (e) {}
}

function initMailTransporter() {
    mailTransporter = nodemailer.createTransport({ service: 'gmail', auth: { user: CONFIG.email, pass: process.env.M_P } });
}

async function sendEmail(subject, body) {
    try { await mailTransporter.sendMail({ from: CONFIG.email, to: CONFIG.email, subject, text: body }); } catch (e) {}
}

async function processItem(page, item) {
    try {
        await page.goto(item.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await sleep(CONFIG.delays.pageLoad);

        const btns = page.locator(CONFIG.selectors.listboxBtn);
        if (await btns.count() < 1) return { status: 'pass', num: item.num, message: 'Dropdown 1 missing' };

        await btns.nth(0).click({ force: true });
        await sleep(CONFIG.delays.afterFirstBtn);

        const opt1 = page.locator('role=option').filter({ hasText: new RegExp(`^${CONFIG.selectors.option1}$`, 'i') }).first();
        await opt1.scrollIntoViewIfNeeded();
        await opt1.click({ force: true });
        await sleep(CONFIG.delays.afterFirstBtn);

        if (await btns.count() < 2) return { status: 'pass', num: item.num, message: 'Dropdown 2 missing' };
        await btns.nth(1).click({ force: true });
        await sleep(CONFIG.delays.afterSecondBtn);

        const opt2 = page.locator('role=option').filter({ hasText: new RegExp(`^${CONFIG.selectors.option2}$`, 'i') }).first();
        await opt2.scrollIntoViewIfNeeded();
        await opt2.click({ force: true });
        await sleep(CONFIG.delays.beforeSave);

        const saveBtn = page.getByRole('button').filter({ hasText: new RegExp(`^${CONFIG.selectors.saveBtn}$`, 'i') }).first();
        await saveBtn.click({ force: true });

        return { status: 'ok', num: item.num, message: timeStrEN() };
    } catch (err) {
        return { status: 'error', num: item.num, message: err.message.split('\n')[0] };
    }
}

async function runCycle(page, cycleNum) {
    const selected = [...CONFIG.items].sort(() => 0.5 - Math.random()).slice(0, rand(CONFIG.minPerCycle, CONFIG.items.length));
    const cycleLines = [];
    
    for (const item of selected) {
        const result = await processItem(page, item);
        batchStats.totalI++;
        if (result.status === 'ok') { batchStats.ok++; cycleLines.push(`✅ I#${item.num} - ${result.message}`); }
        else if (result.status === 'error') { batchStats.errors++; cycleLines.push(`❌ I#${item.num} - ${result.message}`); }
        else { batchStats.pass++; cycleLines.push(`⚠️ I#${item.num} - ${result.message}`); }
    }
    
    batchStats.totalCycles++;
    const summary = `Cycle ${cycleNum} | Total: ${batchStats.totalI} | OK: ${batchStats.ok} | ERR: ${batchStats.errors}`;
    appendLog(`\n${summary}\n${cycleLines.join('\n')}`, cycleNum);
    
    if (cycleNum % CONFIG.logEvery === 0 || cycleNum === CONFIG.rounds) {
        await uploadToDrive(cycleNum);
        await sendEmail(`Status Update - Cycle ${cycleNum}`, summary);
    }
}

(async () => {
    CONFIG.email = normalize(process.env.I_M);
    CONFIG.timezone = normalize(process.env.I_TZ);
    CONFIG.selectors.option1 = normalize(process.env.I_O1);
    CONFIG.selectors.option2 = normalize(process.env.I_O2);
    CONFIG.selectors.saveBtn = normalize(process.env.I_SB);
    CONFIG.items = normalize(process.env.I_U).split(',').map(e => {
        const [n, u] = e.trim().split('|');
        return { num: normalize(n), url: normalize(u) };
    }).filter(e => e.num && e.url);

    BATCH_DATE = `${dateStr()}-${timeStrEN().replace(/:/g,'')}`;
    ensureDirs();
    batchStats.startTime = Date.now();
    initDriveClient();
    initMailTransporter();

    const cookies = JSON.parse(process.env.I_CO);
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ userAgent: CONFIG.userAgent });
    await context.addCookies(cookies);
    const page = await context.newPage();

    for (let i = 1; i <= CONFIG.rounds; i++) {
        await runCycle(page, i);
        await sleepMs(rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax));
    }
    
    await browser.close();
    await sendEmail('Process Completed', `Total Cycles: ${batchStats.totalCycles}\nTotal Operations: ${batchStats.totalI}\nSuccess: ${batchStats.ok}`);
})();
