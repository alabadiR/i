const { chromium } = require('playwright');
const nodemailer = require('nodemailer');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

const CONFIG = {
    r: 300,
    le: 50,
    mC: 20,
    dMin: 1500,
    dMax: 4000,
    lDir: 'logs',
    uA: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    vD: {
        load: [2000, 3000],
        step: [1000, 2000],
        save: [2000, 3500],
    },
    s: {
        b1: 'button[aria-haspopup="listbox"]',
        o1: '',
        o2: '',
        sb: '',
    },
    items: [],
    m: '',
    tz: 'Asia/Riyadh'
};

let B_ID = null;
let dC = null;
let mT = null;
let lB = "";

const rN = (a, b) => Math.floor(Math.random() * (b - a + 1)) + a;
const sP = ([a, b]) => new Promise(r => setTimeout(r, rN(a, b)));
const sM = (ms) => new Promise(r => setTimeout(r, ms));
const gT = () => new Date().toLocaleTimeString('en-CA', { timeZone: CONFIG.tz, hour12: false });
const gD = () => new Date().toLocaleDateString('en-CA', { timeZone: CONFIG.tz });

const bS = { tc: 0, ti: 0, v1: 0, p0: 0, eX: 0 };

function log(t) {
    const e = `[${gT()}] ${t}\n`;
    process.stdout.write(e);
    lB += e;
}

function norm(v) { return String(v ?? '').trim(); }

function setup() { 
    if (!fs.existsSync(CONFIG.lDir)) fs.mkdirSync(CONFIG.lDir, { recursive: true }); 
}

function writeL(t, n) {
    const tag = n === CONFIG.r ? 'f' : `p${Math.ceil(n / CONFIG.le)}`;
    const p = path.join(CONFIG.lDir, `${B_ID}-${tag}.txt`);
    fs.appendFileSync(p, t + '\n');
}

function initD() {
    try {
        const k = JSON.parse(process.env.GDR_K);
        const a = new google.auth.GoogleAuth({ credentials: k, scopes: ['https://www.googleapis.com/auth/drive.file'] });
        dC = google.drive({ version: 'v3', auth: a });
    } catch (e) { log(`D-E`); }
}

async function upD(n) {
    if (!dC) return;
    try {
        const tag = n === CONFIG.r ? 'f' : `p${Math.ceil(n / CONFIG.le)}`;
        const p = path.join(CONFIG.lDir, `${B_ID}-${tag}.txt`);
        if (!fs.existsSync(p)) return;
        await dC.files.create({
            requestBody: { name: path.basename(p), parents: [process.env.GDR_D] },
            media: { mimeType: 'text/plain', body: fs.createReadStream(p) }
        });
    } catch (e) { log(`U-E`); }
}

async function exec(page, item) {
    try {
        await page.goto(item.url, { waitUntil: 'domcontentloaded', timeout: 50000 });
        await sP(CONFIG.vD.load);

        const sel = page.locator(CONFIG.s.b1);
        if (await sel.count() < 1) return { s: 'p', m: 'M-1' };

        await sel.nth(0).click({ force: true, delay: rN(50, 150) });
        await sP(CONFIG.vD.step);

        const t1 = page.locator('role=option').filter({ hasText: new RegExp(`^${CONFIG.s.o1}$`, 'i') }).first();
        if (await t1.count() > 0) {
            await t1.click({ force: true, delay: rN(50, 100) });
            await sP(CONFIG.vD.step);
        }

        if (await sel.count() >= 2) {
            await sel.nth(1).click({ force: true });
            await sP(CONFIG.vD.step);
            const t2 = page.locator('role=option').filter({ hasText: new RegExp(`^${CONFIG.s.o2}$`, 'i') }).first();
            if (await t2.count() > 0) {
                await t2.click({ force: true });
                await sP(CONFIG.vD.save);
            }
        }

        const btn = page.getByRole('button').filter({ hasText: new RegExp(`^${CONFIG.s.sb}$`, 'i') }).first();
        if (await btn.isVisible()) {
            await btn.hover();
            await sM(rN(200, 600));
            await btn.click({ force: true });
            await sM(1500);
            return { s: 'ok', m: 'V-1' };
        }
        return { s: 'p', m: 'X-9' };
    } catch (err) {
        return { s: 'e', m: `X-${err.message.split(' ')[0]}` };
    }
}

async function cycle(page, n) {
    const list = [...CONFIG.items].sort(() => 0.5 - Math.random()).slice(0, rN(CONFIG.mC, CONFIG.items.length));
    const lines = [];
    log(`B-${n}/${CONFIG.r} [${list.length}]`);
    
    for (const it of list) {
        const res = await exec(page, it);
        bS.ti++;
        if (res.s === 'ok') bS.v1++;
        else if (res.s === 'e') bS.eX++;
        else bS.p0++;
        
        const l = `T#${it.num} | ${res.s.toUpperCase()} | ${res.m}`;
        log(l);
        lines.push(l);
    }
    
    bS.tc++;
    writeL(`\n--- B-${n} ---\n${lines.join('\n')}`, n);
    if (n % CONFIG.le === 0 || n === CONFIG.r) await upD(n);
}

(async () => {
    try {
        CONFIG.s.o1 = norm(process.env.I_O1);
        CONFIG.s.o2 = norm(process.env.I_O2);
        CONFIG.s.sb = norm(process.env.I_SB);
        CONFIG.items = norm(process.env.I_U).split(',').map(e => {
            const p = e.trim().split('|');
            return { num: norm(p[0]), url: norm(p[1]) };
        }).filter(e => e.num && e.url);

        B_ID = `${gD()}_${gT().replace(/:/g,'-')}`;
        setup();
        initD();

        const b = await chromium.launch({ headless: true });
        const c = await b.newContext({ 
            userAgent: CONFIG.uA, 
            viewport: { width: rN(1280, 1920), height: rN(720, 1080) } 
        });
        
        if (process.env.I_CO) await c.addCookies(JSON.parse(process.env.I_CO));
        const p = await c.newPage();

        for (let i = 1; i <= CONFIG.r; i++) {
            await cycle(p, i);
            await sM(rN(CONFIG.dMin, CONFIG.dMax));
        }
        await b.close();
    } catch (e) { log(`F-E`); }
})();
