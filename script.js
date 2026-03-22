const { chromium } = require('playwright');
const nodemailer = require('nodemailer');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

const CONFIG = {
    rounds: 300,
    logEvery: 50,
    minPerCycle: 20,
    cycleDelayMin: 2000,
    cycleDelayMax: 5000,
    logsDir: 'logs',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    delays: {
        pageLoad: [2500, 3500],
        afterFirstBtn: [1500, 2500],
        afterSecondBtn: [1500, 2500],
        beforeSave: [2000, 3000],
    },
    selectors: {
        listboxBtn: 'button[aria-haspopup="listbox"]',
        option1: '',
        option2: '',
        saveBtn: '',
    },
    items: [],
    email: '',
    timezone: 'Asia/Riyadh'
};

let BATCH_DATE = null;
let driveClient = null;
let mailTransporter = null;
let logBuffer = "";

const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleep = ([min, max]) => new Promise(res => setTimeout(res, rand(min, max)));
const sleepMs = (ms) => new Promise(res => setTimeout(res, ms));
const tz = () => ({ timeZone: CONFIG.timezone });
const timeStrEN = () => new Date().toLocaleTimeString('en-CA', { ...tz(), hour12: false });
const dateStr = () => new Date().toLocaleDateString('en-CA', tz());

const batchStats = {
    totalCycles: 0,
    totalI: 0,
    ok: 0,
    pass: 0,
    errors: 0,
};

function out(text) {
    const entry = `[${timeStrEN()}] ${text}\n`;
    process.stdout.write(entry);
    logBuffer += entry;
}

function normalize(value) {
    return String(value ?? '').trim();
}

function ensureDirs() {
    if (!fs.existsSync(CONFIG.logsDir)) fs.mkdirSync(CONFIG.logsDir, { recursive: true });
}

function appendLog(text, cycleNum) {
    const label = cycleNum === CONFIG.rounds ? 'final' : `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
    const logPath = path.join(CONFIG.logsDir, `${BATCH_DATE}-${label}.txt`);
    fs.appendFileSync(logPath, text + '\n');
}

function initDriveClient() {
    try {
        const credentials = JSON.parse(process.env.GDR_K);
        const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/drive.file'] });
        driveClient = google.drive({ version: 'v3', auth });
    } catch (e) {
        out(`Drive Init Error: ${e.message}`);
    }
}

async function uploadToDrive(cycleNum) {
    if (!driveClient) return;
    try {
        const label = cycleNum === CONFIG.rounds ? 'final' : `part${Math.ceil(cycleNum / CONFIG.logEvery)}`;
        const logPath = path.join(CONFIG.logsDir, `${BATCH_DATE}-${label}.txt`);
        if (!fs.existsSync(logPath)) return;
        
        const fileName = path.basename(logPath);
        const folderId = process.env.GDR_D;
        const fileStream = fs.createReadStream(logPath);
        
        await driveClient.files.create({
            requestBody: { name: fileName, parents: [folderId] },
            media: { mimeType: 'text/plain', body: fileStream }
        });
        out(`Uploaded to Drive: ${fileName}`);
    } catch (e) {
        out(`Upload Error: ${e.message}`);
    }
}

function initMailTransporter() {
    if (!process.env.M_P || !CONFIG.email) return;
    mailTransporter = nodemailer.createTransport({
        service: 'gmail',
        auth: { user: CONFIG.email, pass: process.env.M_P }
    });
}

async function sendEmail(subject, body) {
    if (!mailTransporter) return;
    try {
        await mailTransporter.sendMail({ from: CONFIG.email, to: CONFIG.email, subject, text: body });
    } catch (e) {
        out(`Email Error: ${e.message}`);
    }
}

async function processItem(page, item) {
    try {
        await page.goto(item.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await sleep(CONFIG.delays.pageLoad);

        const btns = page.locator(CONFIG.selectors.listboxBtn);
        if (await btns.count() < 1) return { status: 'pass', message: 'Dropdown 1 missing' };

        await btns.nth(0).click({ force: true });
        await sleep(CONFIG.delays.afterFirstBtn);

        const opt1 = page.locator('role=option').filter({ hasText: new RegExp(`^${CONFIG.selectors.option1}$`, 'i') }).first();
        if (await opt1.count() > 0) {
            await opt1.scrollIntoViewIfNeeded();
            await opt1.click({ force: true });
            await sleep(CONFIG.delays.afterFirstBtn);
        }

        if (await btns.count() >= 2) {
            await btns.nth(1).click({ force: true });
            await sleep(CONFIG.delays.afterSecondBtn);
            const opt2 = page.locator('role=option').filter({ hasText: new RegExp(`^${CONFIG.selectors.option2}$`, 'i') }).first();
            if (await opt2.count() > 0) {
                await opt2.scrollIntoViewIfNeeded();
                await opt2.click({ force: true });
                await sleep(CONFIG.delays.beforeSave);
            }
        }

        const saveBtn = page.getByRole('button').filter({ hasText: new RegExp(`^${CONFIG.selectors.saveBtn}$`, 'i') }).first();
        if (await saveBtn.count() > 0) {
            await saveBtn.click({ force: true });
            await sleepMs(1000);
            return { status: 'ok', message: 'Success' };
        }

        return { status: 'pass', message: 'Save button not found' };
    } catch (err) {
        return { status: 'error', message: err.message.split('\n')[0] };
    }
}

async function runCycle(page, cycleNum) {
    const count = rand(CONFIG.minPerCycle, Math.min(CONFIG.minPerCycle + 10, CONFIG.items.length));
    const selected = [...CONFIG.items].sort(() => 0.5 - Math.random()).slice(0, count);
    const cycleLines = [];
    
    out(`Starting Cycle ${cycleNum}/${CONFIG.rounds} with ${selected.length} items`);
    
    for (const item of selected) {
        const result = await processItem(page, item);
        batchStats.totalI++;
        const line = `I#${item.num} - ${result.status.toUpperCase()} - ${result.message}`;
        if (result.status === 'ok') batchStats.ok++;
        else if (result.status === 'error') batchStats.errors++;
        else batchStats.pass++;
        
        out(line);
        cycleLines.push(line);
    }
    
    batchStats.totalCycles++;
    const summary = `Cycle ${cycleNum} Summary: Total=${batchStats.totalI}, OK=${batchStats.ok}, ERR=${batchStats.errors}`;
    appendLog(`\n--- ${summary} ---\n${cycleLines.join('\n')}`, cycleNum);
    
    if (cycleNum % CONFIG.logEvery === 0 || cycleNum === CONFIG.rounds) {
        await uploadToDrive(cycleNum);
        await sendEmail(`Status Update - Cycle ${cycleNum}`, summary);
    }
}

(async () => {
    try {
        CONFIG.email = normalize(process.env.I_M);
        CONFIG.timezone = normalize(process.env.I_TZ) || 'Asia/Riyadh';
        CONFIG.selectors.option1 = normalize(process.env.I_O1);
        CONFIG.selectors.option2 = normalize(process.env.I_O2);
        CONFIG.selectors.saveBtn = normalize(process.env.I_SB);
        
        const rawItems = normalize(process.env.I_U).split(',');
        CONFIG.items = rawItems.map(e => {
            const parts = e.trim().split('|');
            return { num: normalize(parts[0]), url: normalize(parts[1]) };
        }).filter(e => e.num && e.url);

        BATCH_DATE = `${dateStr()}_${timeStrEN().replace(/:/g,'-')}`;
        ensureDirs();
        initDriveClient();
        initMailTransporter();

        const browser = await chromium.launch({ headless: true });
        const context = await browser.newContext({ userAgent: CONFIG.userAgent });
        
        if (process.env.I_CO) {
            const cookies = JSON.parse(process.env.I_CO);
            await context.addCookies(cookies);
        }
        
        const page = await context.newPage();

        for (let i = 1; i <= CONFIG.rounds; i++) {
            await runCycle(page, i);
            const delay = rand(CONFIG.cycleDelayMin, CONFIG.cycleDelayMax);
            out(`Waiting ${delay}ms for next cycle...`);
            await sleepMs(delay);
        }
        
        await browser.close();
        await sendEmail('Process Completed', `Total Cycles: ${batchStats.totalCycles}\nTotal Operations: ${batchStats.totalI}\nSuccess: ${batchStats.ok}\nErrors: ${batchStats.errors}`);
        out("Process Finished Successfully");
    } catch (mainErr) {
        out(`Main Loop Fatal Error: ${mainErr.message}`);
    }
})();
