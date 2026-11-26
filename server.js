/**
 * SS Audios Backend Server (Erode, Tamil Nadu)
 * Comprehensive Full-Stack API supporting:
 * - Public Site Data & Catalog Hydration (/api/site-data)
 * - Video Streaming with HTTP 206 Partial Content (/api/media/:slotId/:filename)
 * - Admin Authentication, Google OAuth, Passcode Claim & Verification (/api/admin/auth/*)
 * - Dynamic Catalog & Pricing Management for DJ Packages, Services, Studio & Academy (/api/admin/catalog)
 * - Portfolio Slot Management & Resumable Chunked Video Uploads (/api/admin/uploads/*, /api/admin/portfolio/*)
 * - Google Gemini AI Services (Quick Advisor, Blueprint, Venue Grounding, Image Studio & Veo Animation)
 * - Booking Inquiries & Contact Dispatch with Luxury HTML Email Notifications (Resend & Nodemailer)
 * - AWS S3 Media Vault & DynamoDB Cloud Synchronization with Local JSON Failover
 * - Compatibility with Vercel Serverless Functions and Local Express 5 Development
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const AWS = require('aws-sdk');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const fs = require('fs');
const { promisify } = require('util');
const { execFile } = require('child_process');

const app = express();
const PORT = process.env.PORT || 5000;
const OWNER_EMAIL = process.env.OWNER_EMAIL || 'connectjawa@gmail.com';
const SESSION_SECRET =
    process.env.SESSION_SECRET && process.env.SESSION_SECRET.length >= 16
        ? process.env.SESSION_SECRET
        : 'ss-audios-production-secret-token-key-2026';

// Storage directories (with serverless /tmp fallback when running on Vercel)
const DATA_DIR = process.env.VERCEL ? path.join('/tmp', 'data') : path.join(__dirname, 'data');
const VIDEOS_DIR = path.join(DATA_DIR, 'videos');
const TMP_UPLOADS_DIR = path.join(DATA_DIR, 'tmp_uploads');
const UPLOADS_DIR = path.join(DATA_DIR, 'uploads');

for (const dir of [DATA_DIR, VIDEOS_DIR, TMP_UPLOADS_DIR, UPLOADS_DIR]) {
    try {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
    } catch (e) {
        console.warn(`Notice: Could not create directory ${dir}:`, e.message);
    }
}

const STORE_FILE = path.join(DATA_DIR, 'store.json');
const SERVICES_FILE = path.join(DATA_DIR, 'services.json');
const PLANS_FILE = path.join(DATA_DIR, 'plans.json');
const INQUIRIES_FILE = path.join(DATA_DIR, 'inquiries.json');

// Copy initial seed store from repository root if running on Vercel or freshly cloned
if (!fs.existsSync(STORE_FILE)) {
    const rootSeed = path.join(__dirname, 'data', 'store.json');
    const ssaudiosSeed = path.join(__dirname, '..', 'ssaudios2', '.ss_audios_data', 'store.json');
    try {
        if (fs.existsSync(rootSeed)) {
            fs.copyFileSync(rootSeed, STORE_FILE);
        } else if (fs.existsSync(ssaudiosSeed)) {
            fs.copyFileSync(ssaudiosSeed, STORE_FILE);
        }
    } catch (e) {
        console.warn('Notice: Could not seed store.json:', e.message);
    }
}

// -------------------------------------------------------------
// Constants & Quotas (aligned with ssaudios2 domain specification)
// -------------------------------------------------------------
const STORAGE_QUOTA_BYTES = 5 * 1024 * 1024 * 1024; // 5 GiB total
const MAX_VIDEO_FILE_BYTES = 3 * 1024 * 1024 * 1024; // 3 GiB per video

const BUSINESS_CONTACT = {
    name: 'SS Audios',
    website: 'www.ssaudios.in',
    websiteUrl: 'https://www.ssaudios.in/',
    email: 'ssaudios25@gmail.com',
    ownerEmail: OWNER_EMAIL,
    phoneDisplay: '+91 99653 68899',
    phoneDigits: '919965368899',
    whatsappBase: 'https://wa.me/919965368899',
    instagramHandle: '@ss_audios_and_dj_events_erode',
    cityRegion: 'Erode, Tamil Nadu, India',
    erodeCenter: { lat: 11.329948, lng: 77.704 },
};

// -------------------------------------------------------------
// Nodemailer SMTP Transporter
// -------------------------------------------------------------
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER || 'ssaudios25@gmail.com',
        pass: (process.env.EMAIL_PASS || 'xqkb yzfd tund wlgp').replace(/\s+/g, ''),
    }
});

// -------------------------------------------------------------
// AWS S3 & DynamoDB Configuration
// -------------------------------------------------------------
const REGION = process.env.AWS_REGION || 'ap-south-1';
const BUCKET_NAME = process.env.S3_BUCKET_NAME || 'soundscape-media-vault-' + REGION;
const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME || 'SoundscapeMedia';
const SERVICES_TABLE_NAME = process.env.DYNAMODB_SERVICES_TABLE || 'SoundscapeServices';
const PLANS_TABLE_NAME = process.env.DYNAMODB_PLANS_TABLE || 'SoundscapePlans';
const INQUIRIES_TABLE_NAME = process.env.DYNAMODB_INQUIRIES_TABLE || 'SoundscapeInquiries';
const STORE_TABLE_NAME = process.env.DYNAMODB_STORE_TABLE || 'SoundscapeStore';

AWS.config.update({
    region: REGION,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

let infrastructureReady = false;

async function initAWSInfrastructure() {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
        console.log('ℹ️ AWS Credentials not provided; using high-speed local data persistence.');
        return;
    }
    console.log('🚀 Checking AWS Infrastructure (DynamoDB & S3)...');

    async function ensureTable(tableName, keyName = 'id', keyType = 'S') {
        try {
            const tableDesc = await dynamodb.describeTable({ TableName: tableName }).promise();
            console.log(`✅ [DynamoDB] Table "${tableName}" active (${tableDesc.Table.TableStatus}).`);
        } catch (err) {
            if (err.code === 'ResourceNotFoundException') {
                console.log(`⚠️ [DynamoDB] Creating table "${tableName}"...`);
                await dynamodb.createTable({
                    TableName: tableName,
                    KeySchema: [{ AttributeName: keyName, KeyType: 'HASH' }],
                    AttributeDefinitions: [{ AttributeName: keyName, AttributeType: keyType }],
                    BillingMode: 'PAY_PER_REQUEST'
                }).promise();
                await dynamodb.waitFor('tableExists', { TableName: tableName }).promise();
                console.log(`✅ [DynamoDB] Table "${tableName}" created successfully.`);
            } else {
                console.warn(`[DynamoDB Notice for "${tableName}"]:`, err.message);
            }
        }
    }

    try {
        await ensureTable(TABLE_NAME, 'id', 'S');
        await ensureTable(SERVICES_TABLE_NAME, 'id', 'S');
        await ensureTable(PLANS_TABLE_NAME, 'id', 'S');
        await ensureTable(INQUIRIES_TABLE_NAME, 'id', 'S');
        await ensureTable(STORE_TABLE_NAME, 'id', 'S');

        try {
            await s3.headBucket({ Bucket: BUCKET_NAME }).promise();
            console.log(`✅ [S3] Bucket "${BUCKET_NAME}" accessible.`);
        } catch (s3Err) {
            if (s3Err.statusCode === 404 || s3Err.code === 'NotFound' || s3Err.code === 'NoSuchBucket') {
                console.log(`⚠️ [S3] Creating bucket "${BUCKET_NAME}" in ${REGION}...`);
                await s3.createBucket({
                    Bucket: BUCKET_NAME,
                    ...(REGION !== 'us-east-1' ? { CreateBucketConfiguration: { LocationConstraint: REGION } } : {})
                }).promise();
                console.log(`✅ [S3] Bucket "${BUCKET_NAME}" created.`);
            }
        }
        infrastructureReady = true;

        // 1. Restore store state from DynamoDB if available
        try {
            const ddbRes = await docClient.get({
                TableName: STORE_TABLE_NAME,
                Key: { id: 'ss_audios_store_v1' }
            }).promise();
            if (ddbRes.Item && ddbRes.Item.payload) {
                const persisted = JSON.parse(ddbRes.Item.payload);
                if (persisted && Array.isArray(persisted.portfolioCategories)) {
                    store = { ...store, ...persisted };
                    console.log('✅ [DynamoDB] Loaded store state from DynamoDB.');
                }
            }
        } catch (e) {
            console.warn('⚠️ [DynamoDB Load Notice]:', e.message);
        }

        // 2. Auto-sync with S3 portfolio media objects so no uploaded videos are ever missed
        try {
            console.log('🔍 [S3 Vault] Scanning S3 bucket for portfolio media files...');
            let continuationToken = null;
            const s3MediaMap = new Map();
            do {
                const listParams = { Bucket: BUCKET_NAME, Prefix: 'portfolio/' };
                if (continuationToken) listParams.ContinuationToken = continuationToken;
                const s3Objects = await s3.listObjectsV2(listParams).promise();
                (s3Objects.Contents || []).forEach(item => {
                    const parts = item.Key.split('/');
                    if (parts.length >= 3) {
                        const slotId = parts[1];
                        const filename = parts.slice(2).join('/');
                        if (!s3MediaMap.has(slotId) || item.LastModified > s3MediaMap.get(slotId).lastModified) {
                            s3MediaMap.set(slotId, { filename, size: item.Size, lastModified: item.LastModified });
                        }
                    }
                });
                continuationToken = s3Objects.IsTruncated ? s3Objects.NextContinuationToken : null;
            } while (continuationToken);

            let syncCount = 0;
            if (Array.isArray(store.portfolioCategories)) {
                store.portfolioCategories.forEach(cat => {
                    if (Array.isArray(cat.slots)) {
                        cat.slots.forEach(slot => {
                            const s3Media = s3MediaMap.get(slot.id);
                            if (s3Media) {
                                slot.videoUrl = `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/portfolio/${slot.id}/${s3Media.filename}`;
                                slot.status = 'published';
                                slot.mimeType = 'video/mp4';
                                slot.fileSizeBytes = s3Media.size;
                                slot.updatedAt = s3Media.lastModified && s3Media.lastModified.toISOString ? s3Media.lastModified.toISOString() : new Date().toISOString();
                                syncCount++;
                            }
                        });
                    }
                });
            }
            if (Array.isArray(store.djPackages)) {
                store.djPackages.forEach(pkg => {
                    ensurePackageSlots(pkg);
                    pkg.slots.forEach(slot => {
                        const s3Media = s3MediaMap.get(slot.id);
                        if (s3Media) {
                            slot.videoUrl = `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/portfolio/${slot.id}/${s3Media.filename}`;
                            slot.status = 'published';
                            slot.mimeType = 'video/mp4';
                            slot.fileSizeBytes = s3Media.size;
                            slot.updatedAt = s3Media.lastModified && s3Media.lastModified.toISOString ? s3Media.lastModified.toISOString() : new Date().toISOString();
                            syncCount++;
                        }
                    });
                });
            }
            console.log(`✅ [S3 Vault] Synced ${syncCount} slots directly from S3 media vault.`);
            saveStore(store);
        } catch (s3SyncErr) {
            console.warn('⚠️ [S3 Vault Sync Notice]:', s3SyncErr.message);
        }
    } catch (infraErr) {
        console.warn('⚠️ AWS initialization notice:', infraErr.message);
    }
}

// -------------------------------------------------------------
// Middleware Configuration
// -------------------------------------------------------------
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*';
app.use(cors({
    origin: (origin, callback) => {
        if (!origin || allowedOrigin === '*') {
            return callback(null, true);
        }
        const origins = allowedOrigin.split(',').map(o => o.trim());
        if (origins.includes(origin) || origin.includes('localhost') || origin.includes('127.0.0.1')) {
            return callback(null, true);
        }
        return callback(null, true); // Permissive for preview deployments
    },
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Range', 'x-upload-offset'],
    exposedHeaders: ['Content-Range', 'Accept-Ranges', 'Content-Length', 'x-upload-offset'],
    credentials: true,
}));

// Binary body parser specifically for chunked video uploads (up to 16 MB per chunk)
app.use('/api/admin/uploads/:uploadId/chunk', express.raw({ type: 'application/octet-stream', limit: '16mb' }));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Static file servers for uploads & videos
app.use('/uploads', express.static(UPLOADS_DIR));
app.use('/videos', express.static(VIDEOS_DIR));

// Configure Multer for in-memory file uploads (max 100MB per file)
const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 100 * 1024 * 1024 }
});

// -------------------------------------------------------------
// Helper Functions: Safe Paths & Crypto
// -------------------------------------------------------------
function sanitizeSafeSegment(input) {
    if (!input || typeof input !== 'string') return 'item';
    return input.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 180);
}

function resolveConfinedPath(baseDir, ...segments) {
    const resolvedBase = path.resolve(baseDir);
    const resolvedTarget = path.resolve(resolvedBase, ...segments);
    if (!resolvedTarget.startsWith(resolvedBase + path.sep) && resolvedTarget !== resolvedBase) {
        return null;
    }
    return resolvedTarget;
}

function timingSafeStringEqual(a, b) {
    const bufA = Buffer.from(String(a));
    const bufB = Buffer.from(String(b));
    if (bufA.length !== bufB.length) return false;
    return crypto.timingSafeEqual(bufA, bufB);
}

function hashPasscode(passcode, salt) {
    const finalSalt = salt || crypto.randomBytes(16).toString('hex');
    const derived = crypto.scryptSync(passcode, finalSalt, 64).toString('hex');
    return { salt: finalSalt, hash: derived };
}

function verifyPasscode(passcode, cred) {
    if (!cred || !cred.salt || !cred.hash) return false;
    const { hash } = hashPasscode(passcode, cred.salt);
    const a = Buffer.from(hash, 'hex');
    const b = Buffer.from(cred.hash, 'hex');
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
}

function signAdminToken(subject) {
    const payload = JSON.stringify({
        sub: subject,
        exp: Date.now() + 1000 * 60 * 60 * 24, // 24 hours
    });
    const encoded = Buffer.from(payload).toString('base64url');
    const sig = crypto.createHmac('sha256', SESSION_SECRET).update(encoded).digest('base64url');
    return `${encoded}.${sig}`;
}

function verifyAdminToken(token) {
    if (!token || typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 2) return null;
    const [encoded, sig] = parts;
    const expected = crypto.createHmac('sha256', SESSION_SECRET).update(encoded).digest('base64url');
    if (!timingSafeStringEqual(sig, expected)) return null;
    try {
        const parsed = JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8'));
        if (!parsed.sub || !parsed.exp || Date.now() > parsed.exp) return null;
        return { sub: parsed.sub };
    } catch {
        return null;
    }
}

function requireAdminAuth(req, res, next) {
    const authHeader = req.headers.authorization || '';
    if (!authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Admin authentication required.' });
    }
    const token = authHeader.slice('Bearer '.length).trim();
    const verified = verifyAdminToken(token);
    if (!verified) {
        return res.status(401).json({ error: 'Invalid or expired admin session token.' });
    }
    req.adminSession = verified;
    next();
}

// -------------------------------------------------------------
// Data Persistence: Store, Services, Plans, Inquiries
// -------------------------------------------------------------
function computeStorageStats(categories, packages = []) {
    let usedBytes = 0;
    let publishedCount = 0;
    let totalSlots = 0;

    if (Array.isArray(categories)) {
        for (const cat of categories) {
            if (Array.isArray(cat.slots)) {
                for (const slot of cat.slots) {
                    totalSlots += 1;
                    if (slot.status === 'published' && slot.videoUrl) {
                        publishedCount += 1;
                        usedBytes += Math.max(0, slot.fileSizeBytes || 0);
                    }
                }
            }
        }
    }

    if (Array.isArray(packages)) {
        for (const pkg of packages) {
            if (Array.isArray(pkg.slots)) {
                for (const slot of pkg.slots) {
                    totalSlots += 1;
                    if (slot.status === 'published' && slot.videoUrl) {
                        publishedCount += 1;
                        usedBytes += Math.max(0, slot.fileSizeBytes || 0);
                    }
                }
            }
        }
    }

    const remainingBytes = Math.max(0, STORAGE_QUOTA_BYTES - usedBytes);
    const usedPercent = STORAGE_QUOTA_BYTES > 0 ? Math.min(100, (usedBytes / STORAGE_QUOTA_BYTES) * 100) : 0;

    return {
        usedBytes,
        quotaBytes: STORAGE_QUOTA_BYTES,
        maxFileBytes: MAX_VIDEO_FILE_BYTES,
        remainingBytes,
        usedPercent,
        publishedCount,
        totalSlots,
    };
}

function ensurePackageSlots(pkg) {
    if (!pkg) return [];
    if (!Array.isArray(pkg.slots)) pkg.slots = [];
    const prefix = String(pkg.id || 'package').toLowerCase().replace(/[^a-z0-9]/g, '-');
    const tagPrefix = String(pkg.name || 'DJ').slice(0, 3).toUpperCase();
    for (let i = 1; i <= 10; i++) {
        const pad = String(i).padStart(2, '0');
        const slotId = `dj-pack-${prefix}-slot-${pad}`;
        let existing = pkg.slots.find(s => s.id === slotId || s.slotNumber === i);
        if (!existing) {
            pkg.slots.push({
                id: slotId,
                categoryId: `dj-pack-${prefix}`,
                slotNumber: i,
                title: `${pkg.name} Performance ${pad}`,
                archiveTag: `${tagPrefix}-${pad}`,
                location: 'Erode & Tamil Nadu',
                status: 'awaiting',
                videoUrl: null,
                posterUrl: null,
                mimeType: null,
                fileSizeBytes: 0,
                updatedAt: null,
                hidden: false
            });
        } else {
            if (!existing.id) existing.id = slotId;
            if (!existing.categoryId) existing.categoryId = `dj-pack-${prefix}`;
            if (!existing.slotNumber) existing.slotNumber = i;
            if (!existing.title) existing.title = `${pkg.name} Performance ${pad}`;
            if (!existing.archiveTag) existing.archiveTag = `${tagPrefix}-${pad}`;
            if (typeof existing.hidden !== 'boolean') existing.hidden = false;
        }
    }
    pkg.slots = pkg.slots.slice(0, 10);
    return pkg.slots;
}

function loadStore() {
    try {
        if (fs.existsSync(STORE_FILE)) {
            const raw = fs.readFileSync(STORE_FILE, 'utf8');
            const parsed = JSON.parse(raw);
            if (Array.isArray(parsed.djPackages) && Array.isArray(parsed.portfolioCategories)) {
                parsed.djPackages.forEach(p => ensurePackageSlots(p));
                return parsed;
            }
        }
    } catch (e) {
        console.warn('Notice: Could not read local store.json:', e.message);
    }
    return {
        currency: 'INR',
        djPackages: [],
        signatureServices: [],
        ragaStudioServices: [],
        academyCourses: [],
        sectionVisibility: { services: true, portfolio: true, djPackages: true, ragaStudio: true, sampoornaAcademy: true },
        portfolioCategories: [],
        adminCredential: null,
        updatedAt: new Date().toISOString(),
    };
}

let store = loadStore();

function saveStore(data) {
    data.updatedAt = new Date().toISOString();
    store = data;
    try {
        const tmp = `${STORE_FILE}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
        fs.renameSync(tmp, STORE_FILE);
    } catch (e) {
        console.warn('Notice: Could not write store.json:', e.message);
    }

    if (infrastructureReady) {
        docClient.put({
            TableName: STORE_TABLE_NAME,
            Item: {
                id: 'ss_audios_store_v1',
                updatedAt: data.updatedAt,
                payload: JSON.stringify(data),
            }
        }).promise().catch(err => console.warn('[DynamoDB Store Put]:', err.message));
    }
}

function buildPublicPayload() {
    return {
        currency: store.currency || 'INR',
        djPackages: (store.djPackages || []).map(p => {
            ensurePackageSlots(p);
            return p;
        }),
        signatureServices: store.signatureServices || [],
        ragaStudioServices: store.ragaStudioServices || [],
        academyCourses: store.academyCourses || [],
        sectionVisibility: store.sectionVisibility || { services: true, portfolio: true, djPackages: true, ragaStudio: true, sampoornaAcademy: true },
        portfolioCategories: store.portfolioCategories || [],
        storageStats: computeStorageStats(store.portfolioCategories, store.djPackages),
        ownerEmailHint: OWNER_EMAIL,
        updatedAt: store.updatedAt || new Date().toISOString(),
    };
}

function findSlot(slotId) {
    if (Array.isArray(store.portfolioCategories)) {
        for (const category of store.portfolioCategories) {
            if (Array.isArray(category.slots)) {
                const slot = category.slots.find(s => s.id === slotId);
                if (slot) return { category, slot, type: 'portfolio' };
            }
        }
    }
    if (Array.isArray(store.djPackages)) {
        for (const pkg of store.djPackages) {
            if (Array.isArray(pkg.slots)) {
                const slot = pkg.slots.find(s => s.id === slotId);
                if (slot) {
                    return {
                        category: { id: `dj-pack-${pkg.id}`, name: `${pkg.name} Package`, slots: pkg.slots },
                        package: pkg,
                        slot,
                        type: 'djPackage'
                    };
                }
            }
        }
    }
    return null;
}

function removeLocalVideoFile(videoUrl) {
    if (!videoUrl || typeof videoUrl !== 'string' || !videoUrl.startsWith('/api/media/')) return;
    const parts = videoUrl.replace('/api/media/', '').split('/');
    if (parts.length !== 2) return;
    const safeSlot = sanitizeSafeSegment(parts[0]);
    const safeFile = sanitizeSafeSegment(parts[1]);
    const target = resolveConfinedPath(VIDEOS_DIR, safeSlot, safeFile);
    if (target && fs.existsSync(target)) {
        try {
            fs.unlinkSync(target);
        } catch {
            // ignore
        }
    }
}

function getServices() {
    try {
        if (fs.existsSync(SERVICES_FILE)) {
            return JSON.parse(fs.readFileSync(SERVICES_FILE, 'utf8'));
        }
    } catch (e) {
        console.warn('Notice: Could not read services.json:', e.message);
    }
    return [];
}

function saveServices(services) {
    try {
        fs.writeFileSync(SERVICES_FILE, JSON.stringify(services, null, 2), 'utf8');
    } catch (e) {
        console.warn('Notice: Could not write services.json:', e.message);
    }
}

function getPlans() {
    try {
        if (fs.existsSync(PLANS_FILE)) {
            return JSON.parse(fs.readFileSync(PLANS_FILE, 'utf8'));
        }
    } catch (e) {
        console.warn('Notice: Could not read plans.json:', e.message);
    }
    return [];
}

function savePlans(plans) {
    try {
        fs.writeFileSync(PLANS_FILE, JSON.stringify(plans, null, 2), 'utf8');
    } catch (e) {
        console.warn('Notice: Could not write plans.json:', e.message);
    }
}

function getInquiries() {
    try {
        if (fs.existsSync(INQUIRIES_FILE)) {
            return JSON.parse(fs.readFileSync(INQUIRIES_FILE, 'utf8'));
        }
    } catch (e) {
        console.warn('Notice: Could not read inquiries.json:', e.message);
    }
    return [];
}

function saveInquiries(inquiries) {
    try {
        fs.writeFileSync(INQUIRIES_FILE, JSON.stringify(inquiries, null, 2), 'utf8');
    } catch (e) {
        console.warn('Notice: Could not write inquiries.json:', e.message);
    }
}

// Active chunked upload sessions
const activeUploads = new Map();

// -------------------------------------------------------------
// Google Gemini Client Initialization
// -------------------------------------------------------------
let cachedGoogleGenAI = null;
async function getGeminiClient() {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        throw new Error('GEMINI_API_KEY is not configured in environment variables.');
    }
    if (!cachedGoogleGenAI) {
        const genaiModule = await import('@google/genai');
        cachedGoogleGenAI = genaiModule.GoogleGenAI;
    }
    return new cachedGoogleGenAI({
        apiKey,
        httpOptions: {
            headers: {
                'User-Agent': 'ss-audios-backend',
            },
        },
    });
}

function parseGeminiError(err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes('API_KEY_INVALID') || msg.includes('PERMISSION_DENIED') || msg.includes('403')) {
        return { status: 403, message: 'Please verify your GEMINI_API_KEY in backend .env file.' };
    }
    if (msg.includes('RESOURCE_EXHAUSTED') || msg.includes('429')) {
        return { status: 429, message: 'Gemini API quota reached. Please check your Google Cloud quota.' };
    }
    return { status: 500, message: msg || 'AI service encountered an unexpected error.' };
}

// -------------------------------------------------------------
// REST API ROUTES
// -------------------------------------------------------------

// 1. Health Checks
app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        service: 'SS Audios Backend API',
        location: 'Erode, Tamil Nadu, India',
        infrastructureReady,
        region: REGION,
        s3Bucket: BUCKET_NAME,
        timestamp: new Date().toISOString(),
        storageStats: computeStorageStats(store.portfolioCategories),
    });
});

app.get('/', (req, res) => {
    res.json({
        status: 'ok',
        name: 'SS Audios Backend API',
        website: BUSINESS_CONTACT.websiteUrl,
        phone: BUSINESS_CONTACT.phoneDisplay,
        version: '2.0.0',
    });
});

// 2. Public Site Data & Catalog Hydration (used by ssaudios2 App.tsx)
app.get('/api/site-data', (_req, res) => {
    res.json(buildPublicPayload());
});

// 3. Media & Video Streaming: Local disk stream or Direct 302 Redirect to AWS S3 High-Speed Vault
app.get('/api/media/:slotId/:filename', (req, res) => {
    const safeSlot = sanitizeSafeSegment(req.params.slotId);
    const safeFile = sanitizeSafeSegment(req.params.filename);
    const filePath = resolveConfinedPath(VIDEOS_DIR, safeSlot, safeFile);

    if (filePath && fs.existsSync(filePath)) {
        return streamLocalFile(filePath, req, res);
    }

    // Fallback check in uploads directory
    const uploadFallback = resolveConfinedPath(UPLOADS_DIR, safeFile);
    if (uploadFallback && fs.existsSync(uploadFallback)) {
        return streamLocalFile(uploadFallback, req, res);
    }

    // Direct 302 Redirect to public high-speed AWS S3 CDN
    const s3Url = `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/portfolio/${safeSlot}/${safeFile}`;
    return res.redirect(302, s3Url);
});

// Legacy streaming endpoint (for existing Soundscape S3 assets)
app.get('/api/media/stream/:id', (req, res) => {
    const s3Url = `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/${req.params.id}`;
    return res.redirect(302, s3Url);
});

function streamLocalFile(filePath, req, res) {
    const stat = fs.statSync(filePath);
    const fileSize = stat.size;
    const ext = path.extname(filePath).toLowerCase();
    const contentType =
        ext === '.jpg' || ext === '.jpeg'
            ? 'image/jpeg'
            : ext === '.png'
            ? 'image/png'
            : ext === '.webp'
            ? 'image/webp'
            : ext === '.webm'
            ? 'video/webm'
            : ext === '.mov'
            ? 'video/quicktime'
            : ext === '.mp3'
            ? 'audio/mpeg'
            : ext === '.wav'
            ? 'audio/wav'
            : 'video/mp4';

    const range = req.headers.range;
    if (range) {
        const match = /^bytes=(\d*)-(\d*)$/.exec(range);
        if (!match) {
            return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
        }
        const start = match[1] ? Number.parseInt(match[1], 10) : 0;
        const end = match[2] ? Math.min(Number.parseInt(match[2], 10), fileSize - 1) : fileSize - 1;
        if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= fileSize) {
            return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
        }
        const chunkSize = end - start + 1;
        res.writeHead(206, {
            'Content-Range': `bytes ${start}-${end}/${fileSize}`,
            'Accept-Ranges': 'bytes',
            'Content-Length': chunkSize,
            'Content-Type': contentType,
            'Cache-Control': 'public, max-age=3600',
        });
        fs.createReadStream(filePath, { start, end }).pipe(res);
    } else {
        res.writeHead(200, {
            'Content-Length': fileSize,
            'Content-Type': contentType,
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'public, max-age=3600',
        });
        fs.createReadStream(filePath).pipe(res);
    }
}

// -------------------------------------------------------------
// Admin Authentication Routes
// -------------------------------------------------------------
app.get('/api/admin/auth/status', (_req, res) => {
    const hasEnvPasscode = Boolean(process.env.ADMIN_PASSCODE && process.env.ADMIN_PASSCODE.trim().length >= 4);
    const needsFirstRunClaim = !hasEnvPasscode && !store.adminCredential;
    res.json({
        needsFirstRunClaim,
        hasEnvPasscode,
        ownerEmailHint: OWNER_EMAIL,
    });
});

app.post('/api/admin/auth/claim', (req, res) => {
    const hasEnvPasscode = Boolean(process.env.ADMIN_PASSCODE && process.env.ADMIN_PASSCODE.trim().length >= 4);
    if (hasEnvPasscode || store.adminCredential) {
        return res.status(409).json({ error: 'Admin passcode is already configured.' });
    }
    const passcode = typeof req.body?.passcode === 'string' ? req.body.passcode.trim() : '';
    if (passcode.length < 6) {
        return res.status(400).json({ error: 'Choose an admin passcode of at least 6 characters.' });
    }
    const { salt, hash } = hashPasscode(passcode);
    store.adminCredential = { salt, hash, createdAt: new Date().toISOString() };
    saveStore(store);
    res.json({ token: signAdminToken('admin-passcode'), role: 'admin' });
});

app.post('/api/admin/auth/login', (req, res) => {
    const passcode = typeof req.body?.passcode === 'string' ? req.body.passcode.trim() : '';
    if (!passcode) {
        return res.status(400).json({ error: 'Passcode is required.' });
    }
    const envPasscode = process.env.ADMIN_PASSCODE?.trim();
    if (envPasscode && envPasscode.length >= 4) {
        if (!timingSafeStringEqual(passcode, envPasscode)) {
            return res.status(401).json({ error: 'Incorrect admin passcode.' });
        }
        return res.json({ token: signAdminToken('admin-env'), role: 'admin' });
    }
    if (!store.adminCredential) {
        return res.status(400).json({ error: 'Please complete first-run admin setup first.' });
    }
    if (!verifyPasscode(passcode, store.adminCredential)) {
        return res.status(401).json({ error: 'Incorrect admin passcode.' });
    }
    res.json({ token: signAdminToken('admin-passcode'), role: 'admin' });
});

app.post('/api/admin/auth/google', async (req, res) => {
    const accessToken = typeof req.body?.accessToken === 'string' ? req.body.accessToken.trim() : '';
    if (!accessToken) {
        return res.status(400).json({ error: 'Google access token is required.' });
    }
    try {
        const infoRes = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
            headers: { Authorization: `Bearer ${accessToken}` },
        });
        if (!infoRes.ok) {
            return res.status(401).json({ error: 'Could not verify Google account session.' });
        }
        const profile = await infoRes.json();
        if (!profile.email || !profile.email_verified) {
            return res.status(403).json({ error: 'Verified Google email is required.' });
        }
        if (profile.email.toLowerCase() !== OWNER_EMAIL.toLowerCase()) {
            return res.status(403).json({
                error: `Signed in as ${profile.email}. Only the site owner (${OWNER_EMAIL}) or passcode holder can administer SS Audios.`,
            });
        }
        res.json({
            token: signAdminToken(profile.email),
            role: 'owner',
            email: profile.email,
            name: profile.name || 'SS Audios Admin',
        });
    } catch {
        res.status(502).json({ error: 'Unable to reach Google identity service.' });
    }
});

// -------------------------------------------------------------
// Admin Catalog & Pricing Management
// -------------------------------------------------------------
app.put('/api/admin/dj-packages', requireAdminAuth, (req, res) => {
    const currency = req.body?.currency === 'USD' ? 'USD' : 'INR';
    const incoming = req.body?.djPackages;
    if (!Array.isArray(incoming) || incoming.length !== 5) {
        return res.status(400).json({ error: 'Exactly 5 DJ packages are required.' });
    }

    const sanitized = [];
    for (let i = 0; i < 5; i++) {
        const item = incoming[i];
        const base = (store.djPackages && store.djPackages[i]) || {};
        const priceInr = Number(item?.priceInr);
        const priceUsd = Number(item?.priceUsd);
        if (!Number.isFinite(priceInr) || priceInr < 0 || !Number.isFinite(priceUsd) || priceUsd < 0) {
            return res.status(400).json({ error: `Invalid price for package "${item?.name || i}".` });
        }
        sanitized.push({
            ...base,
            name: typeof item?.name === 'string' && item.name.trim() ? item.name.trim().slice(0, 48) : (base.name || `Package ${i+1}`),
            priceInr: Math.round(priceInr),
            priceUsd: Math.round(priceUsd),
            tagline: typeof item?.tagline === 'string' && item.tagline.trim() ? item.tagline.trim().slice(0, 120) : (base.tagline || ''),
            unitLabel: typeof item?.unitLabel === 'string' && item.unitLabel.trim() ? item.unitLabel.trim().slice(0, 24) : (base.unitLabel || '/ event'),
            features: Array.isArray(item?.features) ? item.features.map(f => String(f).trim()).filter(Boolean).slice(0, 12) : (base.features || []),
            hidden: Boolean(item?.hidden),
            slots: Array.isArray(base.slots) ? base.slots : ensurePackageSlots(base),
        });
    }

    store.currency = currency;
    store.djPackages = sanitized;
    saveStore(store);

    // Also update plans.json for synchronization
    const currentPlans = getPlans();
    for (const pkg of sanitized) {
        const match = currentPlans.find(p => p.id === pkg.id);
        if (match) {
            match.priceInr = pkg.priceInr;
            match.priceUsd = pkg.priceUsd;
            match.monthlyPrice = `₹${pkg.priceInr}`;
            match.yearlyPrice = `$${pkg.priceUsd}`;
            match.tagline = pkg.tagline;
            match.features = pkg.features;
        }
    }
    savePlans(currentPlans);

    res.json(buildPublicPayload());
});

app.put('/api/admin/catalog', requireAdminAuth, (req, res) => {
    const body = req.body || {};

    if (body.currency === 'INR' || body.currency === 'USD') {
        store.currency = body.currency;
    }

    if (Array.isArray(body.djPackages) && body.djPackages.length === 5) {
        const nextDj = [];
        for (let i = 0; i < 5; i++) {
            const item = body.djPackages[i];
            const base = (store.djPackages && store.djPackages[i]) || {};
            const priceInr = Number(item?.priceInr);
            const priceUsd = Number(item?.priceUsd);
            if (!Number.isFinite(priceInr) || priceInr < 0 || !Number.isFinite(priceUsd) || priceUsd < 0) {
                return res.status(400).json({ error: `Invalid price for DJ package "${item?.name || i}".` });
            }
            nextDj.push({
                ...base,
                name: typeof item?.name === 'string' && item.name.trim() ? item.name.trim().slice(0, 48) : base.name,
                priceInr: Math.round(priceInr),
                priceUsd: Math.round(priceUsd),
                tagline: typeof item?.tagline === 'string' && item.tagline.trim() ? item.tagline.trim().slice(0, 120) : base.tagline,
                unitLabel: typeof item?.unitLabel === 'string' && item.unitLabel.trim() ? item.unitLabel.trim().slice(0, 24) : base.unitLabel,
                features: Array.isArray(item?.features) ? item.features.map(f => String(f).trim()).filter(Boolean).slice(0, 12) : base.features,
                hidden: Boolean(item?.hidden),
                slots: Array.isArray(base.slots) ? base.slots : ensurePackageSlots(base),
            });
        }
        store.djPackages = nextDj;
    }

    if (Array.isArray(body.signatureServices)) {
        const nextServices = [];
        for (let i = 0; i < body.signatureServices.length; i++) {
            const item = body.signatureServices[i];
            const base = (store.signatureServices && store.signatureServices[i]) || {};
            const startingPrice = Number(item?.startingPrice);
            if (!Number.isFinite(startingPrice) || startingPrice < 0) {
                return res.status(400).json({ error: `Invalid price for service "${item?.title || i}".` });
            }
            const rounded = Math.round(startingPrice);
            nextServices.push({
                ...base,
                title: typeof item?.title === 'string' && item.title.trim() ? item.title.trim().slice(0, 64) : base.title,
                startingPrice: rounded,
                priceLabel: item?.priceLabel || `₹${rounded.toLocaleString('en-IN')}`,
                summary: typeof item?.summary === 'string' ? item.summary.trim().slice(0, 220) : base.summary,
                hidden: Boolean(item?.hidden),
            });
        }
        store.signatureServices = nextServices;
    }

    if (Array.isArray(body.ragaStudioServices)) {
        const nextStudio = [];
        for (let i = 0; i < body.ragaStudioServices.length; i++) {
            const item = body.ragaStudioServices[i];
            const base = (store.ragaStudioServices && store.ragaStudioServices[i]) || {};
            const priceValue = Number(item?.priceValue);
            if (!Number.isFinite(priceValue) || priceValue < 0) {
                return res.status(400).json({ error: `Invalid price for studio service "${item?.title || i}".` });
            }
            const rounded = Math.round(priceValue);
            const unit = item?.unit || base.unit || 'hour';
            nextStudio.push({
                ...base,
                title: typeof item?.title === 'string' && item.title.trim() ? item.title.trim().slice(0, 64) : base.title,
                priceValue: rounded,
                unit,
                priceLabel: item?.priceLabel || `₹${rounded.toLocaleString('en-IN')}/${unit}`,
                description: typeof item?.description === 'string' ? item.description.trim().slice(0, 260) : base.description,
                hidden: Boolean(item?.hidden),
            });
        }
        store.ragaStudioServices = nextStudio;
    }

    if (Array.isArray(body.academyCourses)) {
        const nextCourses = [];
        for (let i = 0; i < body.academyCourses.length; i++) {
            const item = body.academyCourses[i];
            const base = (store.academyCourses && store.academyCourses[i]) || {};
            const feeMonthly = Number(item?.feeMonthly);
            if (!Number.isFinite(feeMonthly) || feeMonthly < 0) {
                return res.status(400).json({ error: `Invalid fee for academy course "${item?.title || i}".` });
            }
            const rounded = Math.round(feeMonthly);
            nextCourses.push({
                ...base,
                title: typeof item?.title === 'string' && item.title.trim() ? item.title.trim().slice(0, 64) : base.title,
                feeMonthly: rounded,
                feeLabel: item?.feeLabel || `₹${rounded.toLocaleString('en-IN')}/month`,
                duration: typeof item?.duration === 'string' ? item.duration.trim().slice(0, 40) : base.duration,
                description: typeof item?.description === 'string' ? item.description.trim().slice(0, 260) : base.description,
                hidden: Boolean(item?.hidden),
            });
        }
        store.academyCourses = nextCourses;
    }

    if (body.sectionVisibility && typeof body.sectionVisibility === 'object') {
        const v = body.sectionVisibility;
        store.sectionVisibility = {
            services: typeof v.services === 'boolean' ? v.services : store.sectionVisibility.services,
            portfolio: typeof v.portfolio === 'boolean' ? v.portfolio : store.sectionVisibility.portfolio,
            djPackages: typeof v.djPackages === 'boolean' ? v.djPackages : store.sectionVisibility.djPackages,
            ragaStudio: typeof v.ragaStudio === 'boolean' ? v.ragaStudio : store.sectionVisibility.ragaStudio,
            sampoornaAcademy: typeof v.sampoornaAcademy === 'boolean' ? v.sampoornaAcademy : store.sectionVisibility.sampoornaAcademy,
        };
    }

    saveStore(store);
    res.json(buildPublicPayload());
});

// -------------------------------------------------------------
// Admin Portfolio & Resumable Upload Routes
// -------------------------------------------------------------
app.patch('/api/admin/portfolio-categories/:categoryId', requireAdminAuth, (req, res) => {
    let category = store.portfolioCategories.find(c => c.id === req.params.categoryId);
    if (!category && req.params.categoryId.startsWith('dj-pack-')) {
        const pkgId = req.params.categoryId.replace('dj-pack-', '');
        const pkg = store.djPackages.find(p => p.id === pkgId);
        if (pkg) {
            ensurePackageSlots(pkg);
            category = pkg;
        }
    }
    if (!category) {
        return res.status(404).json({ error: 'Portfolio category or DJ package not found.' });
    }

    if (typeof req.body?.hidden === 'boolean') {
        category.hidden = req.body.hidden;
    }

    const slotAction = req.body?.slotAction;
    if (Array.isArray(category.slots)) {
        if (slotAction === 'hide-empty') {
            for (const slot of category.slots) {
                if (slot.status !== 'published' || !slot.videoUrl) {
                    slot.hidden = true;
                }
            }
        } else if (slotAction === 'show-all') {
            category.hidden = false;
            for (const slot of category.slots) {
                slot.hidden = false;
            }
        } else if (slotAction === 'hide-all') {
            for (const slot of category.slots) {
                slot.hidden = true;
            }
        }
    }

    saveStore(store);
    res.json(buildPublicPayload());
});

app.patch('/api/admin/portfolio/:slotId', requireAdminAuth, (req, res) => {
    const found = findSlot(req.params.slotId);
    if (!found) {
        return res.status(404).json({ error: 'Portfolio slot not found.' });
    }
    const { slot } = found;
    if (typeof req.body?.title === 'string' && req.body.title.trim()) {
        slot.title = req.body.title.trim().slice(0, 120);
    }
    if (typeof req.body?.archiveTag === 'string' && req.body.archiveTag.trim()) {
        slot.archiveTag = req.body.archiveTag.trim().slice(0, 60);
    }
    if (typeof req.body?.location === 'string') {
        slot.location = req.body.location.trim().slice(0, 60);
    }
    if (typeof req.body?.hidden === 'boolean') {
        slot.hidden = req.body.hidden;
    }
    if (typeof req.body?.driveFileId === 'string') {
        slot.driveFileId = req.body.driveFileId.trim() || null;
    }
    if (typeof req.body?.driveWebViewLink === 'string') {
        slot.driveWebViewLink = req.body.driveWebViewLink.trim() || null;
    }
    slot.updatedAt = new Date().toISOString();
    saveStore(store);
    res.json(buildPublicPayload());
});

app.delete('/api/admin/portfolio/:slotId/video', requireAdminAuth, (req, res) => {
    const found = findSlot(req.params.slotId);
    if (!found) {
        return res.status(404).json({ error: 'Portfolio slot not found.' });
    }
    const { slot } = found;
    removeLocalVideoFile(slot.videoUrl);
    removeLocalVideoFile(slot.posterUrl);
    slot.videoUrl = null;
    slot.posterUrl = null;
    slot.status = 'awaiting';
    slot.mimeType = null;
    slot.fileSizeBytes = 0;
    slot.updatedAt = new Date().toISOString();
    saveStore(store);
    res.json(buildPublicPayload());
});

// Chunked Resumable Upload: INIT
app.post('/api/admin/uploads/init', requireAdminAuth, (req, res) => {
    const slotId = typeof req.body?.slotId === 'string' ? req.body.slotId : '';
    const fileName = typeof req.body?.fileName === 'string' ? req.body.fileName : 'video.mp4';
    const mimeType = typeof req.body?.mimeType === 'string' ? req.body.mimeType : 'video/mp4';
    const totalSize = Number(req.body?.totalSize);

    const found = findSlot(slotId);
    if (!found) {
        return res.status(404).json({ error: 'Portfolio slot not found.' });
    }
    if (!mimeType.startsWith('video/')) {
        return res.status(400).json({ error: 'Only video files (MP4, WebM, MOV) are supported.' });
    }
    if (!Number.isFinite(totalSize) || totalSize <= 0) {
        return res.status(400).json({ error: 'Invalid file size.' });
    }
    if (totalSize > MAX_VIDEO_FILE_BYTES) {
        return res.status(413).json({ error: 'Single video exceeds the 3 GiB per-video limit.' });
    }

    const stats = computeStorageStats(store.portfolioCategories);
    const availableWithReplace = stats.remainingBytes + (found.slot.fileSizeBytes || 0);
    if (totalSize > availableWithReplace) {
        return res.status(413).json({
            error: 'Upload exceeds the 5 GiB total video storage quota. Remove or replace an older video first.',
        });
    }

    const uploadId = crypto.randomUUID();
    const ext = sanitizeSafeSegment(path.extname(fileName).toLowerCase() || '.mp4').slice(0, 10);
    const stem = sanitizeSafeSegment(path.basename(fileName, ext)).slice(0, 64) || 'video';
    const safeFileName = `${Date.now()}_${stem}${ext}`;
    const tempPath = resolveConfinedPath(TMP_UPLOADS_DIR, `${uploadId}.part`);
    if (!tempPath) {
        return res.status(400).json({ error: 'Invalid upload path.' });
    }

    fs.writeFileSync(tempPath, Buffer.alloc(0));
    activeUploads.set(uploadId, {
        uploadId,
        slotId,
        fileName: safeFileName,
        mimeType,
        totalSize,
        receivedBytes: 0,
        tempPath,
        createdAt: Date.now(),
    });

    res.json({ uploadId, chunkSize: 4 * 1024 * 1024 });
});

// Chunked Resumable Upload: CHUNK
app.put('/api/admin/uploads/:uploadId/chunk', requireAdminAuth, (req, res) => {
    const session = activeUploads.get(req.params.uploadId);
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found or expired.' });
    }
    const offset = Number(req.headers['x-upload-offset']);
    const chunk = req.body;
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) {
        return res.status(400).json({ error: 'Empty chunk payload.' });
    }
    if (!Number.isFinite(offset) || offset < 0 || offset > session.receivedBytes) {
        return res.status(409).json({
            error: 'Chunk offset mismatch.',
            expectedOffset: session.receivedBytes,
        });
    }

    const fd = fs.openSync(session.tempPath, 'r+');
    try {
        fs.writeSync(fd, chunk, 0, chunk.length, offset);
    } finally {
        fs.closeSync(fd);
    }

    session.receivedBytes = Math.max(session.receivedBytes, offset + chunk.length);
    if (session.receivedBytes > MAX_VIDEO_FILE_BYTES) {
        activeUploads.delete(session.uploadId);
        try { fs.unlinkSync(session.tempPath); } catch {}
        return res.status(413).json({ error: 'Upload exceeded 3 GiB limit.' });
    }

    res.json({ receivedBytes: session.receivedBytes, totalSize: session.totalSize });
});

// Chunked Resumable Upload: COMPLETE
app.post('/api/admin/uploads/:uploadId/complete', requireAdminAuth, async (req, res) => {
    const session = activeUploads.get(req.params.uploadId);
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found.' });
    }
    const found = findSlot(session.slotId);
    if (!found) {
        activeUploads.delete(session.uploadId);
        return res.status(404).json({ error: 'Portfolio slot not found.' });
    }

    const safeSlot = sanitizeSafeSegment(session.slotId);
    const slotDir = resolveConfinedPath(VIDEOS_DIR, safeSlot);
    if (!slotDir) {
        return res.status(400).json({ error: 'Invalid slot directory path.' });
    }
    fs.mkdirSync(slotDir, { recursive: true });
    const finalPath = resolveConfinedPath(slotDir, session.fileName);
    if (!finalPath) {
        return res.status(400).json({ error: 'Invalid destination filename.' });
    }

    removeLocalVideoFile(found.slot.videoUrl);
    removeLocalVideoFile(found.slot.posterUrl);
    fs.renameSync(session.tempPath, finalPath);
    activeUploads.delete(session.uploadId);

    const finalStat = fs.existsSync(finalPath) ? fs.statSync(finalPath) : { size: session.totalSize };
    const directS3Url = `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/portfolio/${safeSlot}/${session.fileName}`;
    found.slot.videoUrl = infrastructureReady ? directS3Url : `/api/media/${safeSlot}/${session.fileName}`;
    found.slot.posterUrl = null;
    found.slot.status = 'published';
    found.slot.mimeType = session.mimeType;
    found.slot.fileSizeBytes = finalStat.size;
    if (typeof req.body?.title === 'string' && req.body.title.trim()) {
        found.slot.title = req.body.title.trim().slice(0, 120);
    }
    found.slot.updatedAt = new Date().toISOString();
    saveStore(store);

    // Optional upload to AWS S3 in the background if credentials exist
    if (infrastructureReady) {
        (async () => {
            try {
                const s3Key = `portfolio/${safeSlot}/${session.fileName}`;
                await s3.upload({
                    Bucket: BUCKET_NAME,
                    Key: s3Key,
                    Body: fs.createReadStream(finalPath),
                    ContentType: session.mimeType,
                }).promise();
                console.log(`✅ [S3 Vault] Uploaded ${found.slot.id} video to S3 key: ${s3Key}`);
            } catch (s3Err) {
                console.warn('[S3 Background Upload Notice]:', s3Err.message);
            }
        })();
    }

    res.json({
        slot: found.slot,
        siteData: buildPublicPayload(),
    });
});

app.post('/api/admin/portfolio/:slotId/assign-ai-video', requireAdminAuth, (req, res) => {
    const found = findSlot(req.params.slotId);
    if (!found) {
        return res.status(404).json({ error: 'Portfolio slot not found.' });
    }
    const sourceUrl = typeof req.body?.videoUrl === 'string' ? req.body.videoUrl : '';
    if (!sourceUrl.startsWith('/api/media/ai_generated/')) {
        return res.status(400).json({ error: 'Invalid AI video reference URL.' });
    }
    const fileName = sanitizeSafeSegment(sourceUrl.replace('/api/media/ai_generated/', ''));
    const srcPath = resolveConfinedPath(VIDEOS_DIR, 'ai_generated', fileName);
    if (!srcPath || !fs.existsSync(srcPath)) {
        return res.status(404).json({ error: 'Generated video file not found.' });
    }
    const safeSlot = sanitizeSafeSegment(req.params.slotId);
    const destDir = resolveConfinedPath(VIDEOS_DIR, safeSlot);
    if (!destDir) {
        return res.status(400).json({ error: 'Invalid destination directory.' });
    }
    fs.mkdirSync(destDir, { recursive: true });
    const destPath = resolveConfinedPath(destDir, fileName);
    if (!destPath) {
        return res.status(400).json({ error: 'Invalid destination filename.' });
    }
    removeLocalVideoFile(found.slot.videoUrl);
    fs.copyFileSync(srcPath, destPath);
    const stat = fs.statSync(destPath);
    found.slot.videoUrl = `/api/media/${safeSlot}/${fileName}`;
    found.slot.status = 'published';
    found.slot.mimeType = 'video/mp4';
    found.slot.fileSizeBytes = stat.size;
    found.slot.updatedAt = new Date().toISOString();
    saveStore(store);
    res.json(buildPublicPayload());
});

// -------------------------------------------------------------
// Google Gemini AI Endpoints
// -------------------------------------------------------------

// 1. Quick Advisor (gemini-2.5-flash / gemini-3.1-flash-lite)
app.post('/api/ai/quick-advisor', async (req, res) => {
    const prompt = typeof req.body?.prompt === 'string' ? req.body.prompt.trim() : '';
    if (!prompt) {
        return res.status(400).json({ error: 'Please enter a question.' });
    }
    try {
        const ai = await getGeminiClient();
        const response = await ai.models.generateContent({
            model: 'gemini-2.5-flash',
            contents: prompt,
            config: {
                systemInstruction: `You are the fast booking & audio concierge for SS Audios (www.ssaudios.in, Erode, Tamil Nadu, WhatsApp +91 99653 68899).
Offer concise, accurate answers in under 120 words using Indian Rupees (₹):
- Signature Services: ${store.signatureServices.filter(s => !s.hidden).map(s => `${s.title} ${s.priceLabel}`).join(', ')}.
- DJ Packages: ${store.djPackages.filter(p => !p.hidden).map(p => `${p.name} ₹${p.priceInr} (${p.durationLabel}, ${p.powerRating})`).join(', ')}.
- Raga Studio: ${store.ragaStudioServices.filter(s => !s.hidden).map(s => `${s.title} ${s.priceLabel}`).join(', ')}.
- Sampoorna Music Academy: ${store.academyCourses.filter(c => !c.hidden).map(c => `${c.title} ${c.feeLabel}`).join(', ')}.`,
            },
        });
        res.json({ text: response.text || 'No response generated.' });
    } catch (err) {
        const parsed = parseGeminiError(err);
        res.status(parsed.status).json({ error: parsed.message });
    }
});

// 2. Event Production Blueprint
app.post('/api/ai/event-blueprint', async (req, res) => {
    const eventBrief = typeof req.body?.eventBrief === 'string' ? req.body.eventBrief.trim() : '';
    const guestCount = typeof req.body?.guestCount === 'string' ? req.body.guestCount.trim() : '300';
    const venueType = typeof req.body?.venueType === 'string' ? req.body.venueType.trim() : 'Indoor Marriage Hall';
    if (!eventBrief) {
        return res.status(400).json({ error: 'Please describe your event requirements.' });
    }
    try {
        const ai = await getGeminiClient();
        const response = await ai.models.generateContent({
            model: 'gemini-2.5-flash',
            contents: `Design a detailed, realistic technical audio, lighting, orchestra/DJ cue sheet, and budget breakdown in Indian Rupees (₹) for this SS Audios event brief:
Event Brief: ${eventBrief}
Expected Guests: ${guestCount}
Venue Type: ${venueType}

Include:
1. Recommended SS Audios Services & DJ Package tier (with realistic wattage & speaker placement).
2. Lighting & Visual Rig Blueprint (moving heads, pathway lighting, LED visuals).
3. Hour-by-hour run-of-show audio cue timeline.
4. Estimated investment breakdown in ₹ (INR) referencing SS Audios rates.`,
            config: {
                systemInstruction:
                    'You are the Chief Acoustic & Stage Production Director at SS Audios in Erode, Tamil Nadu. Deliver clear, actionable production plans in clean Markdown.',
            },
        });
        res.json({ text: response.text || 'No blueprint generated.' });
    } catch (err) {
        const parsed = parseGeminiError(err);
        res.status(parsed.status).json({ error: parsed.message });
    }
});

// 3. Venue Grounding Search
app.post('/api/ai/venue-grounding', async (req, res) => {
    const query = typeof req.body?.query === 'string' ? req.body.query.trim() : '';
    if (!query) {
        return res.status(400).json({ error: 'Please enter a venue or location query.' });
    }
    try {
        const ai = await getGeminiClient();
        const response = await ai.models.generateContent({
            model: 'gemini-2.5-flash',
            contents: `${query}. Focus on event venues, marriage halls, auditoriums, or SS Audios location details around Erode, Tamil Nadu unless another city is explicitly named. Note acoustic/event suitability where helpful.`,
        });
        res.json({
            text: response.text || 'No venue details found.',
            places: [],
        });
    } catch (err) {
        const parsed = parseGeminiError(err);
        res.status(parsed.status).json({ error: parsed.message });
    }
});

// 4. Image Creation & Editing
app.post('/api/ai/image-create-edit', async (req, res) => {
    const prompt = typeof req.body?.prompt === 'string' ? req.body.prompt.trim() : '';
    if (!prompt) {
        return res.status(400).json({ error: 'Please provide an image prompt.' });
    }
    try {
        const ai = await getGeminiClient();
        const response = await ai.models.generateContent({
            model: 'imagen-3.0-generate-002',
            contents: prompt,
        });
        let imageUrl = null;
        for (const part of response.candidates?.[0]?.content?.parts ?? []) {
            if (part.inlineData?.data) {
                const outMime = part.inlineData.mimeType || 'image/png';
                imageUrl = `data:${outMime};base64,${part.inlineData.data}`;
            }
        }
        if (!imageUrl) {
            return res.status(500).json({ error: 'Model did not return an image. Try refining your prompt.' });
        }
        res.json({ imageUrl, caption: prompt });
    } catch (err) {
        const parsed = parseGeminiError(err);
        res.status(parsed.status).json({ error: parsed.message });
    }
});

// 5. High-Quality Image Generation
app.post('/api/ai/image-hq', async (req, res) => {
    const prompt = typeof req.body?.prompt === 'string' ? req.body.prompt.trim() : '';
    if (!prompt) {
        return res.status(400).json({ error: 'Please provide a visual prompt.' });
    }
    try {
        const ai = await getGeminiClient();
        const response = await ai.models.generateContent({
            model: 'imagen-3.0-generate-002',
            contents: prompt,
        });
        let imageUrl = null;
        for (const part of response.candidates?.[0]?.content?.parts ?? []) {
            if (part.inlineData?.data) {
                const outMime = part.inlineData.mimeType || 'image/png';
                imageUrl = `data:${outMime};base64,${part.inlineData.data}`;
            }
        }
        if (!imageUrl) {
            return res.status(500).json({ error: 'High-resolution image was not returned.' });
        }
        res.json({ imageUrl, caption: prompt, imageSize: '1K' });
    } catch (err) {
        const parsed = parseGeminiError(err);
        res.status(parsed.status).json({ error: parsed.message });
    }
});

// 6. Photo Animation / Veo Video Generation
app.post('/api/ai/animate-video', async (req, res) => {
    const prompt = typeof req.body?.prompt === 'string' ? req.body.prompt.trim() : '';
    const imageBase64 = typeof req.body?.imageBase64 === 'string' ? req.body.imageBase64 : '';
    if (!imageBase64 || !prompt) {
        return res.status(400).json({ error: 'Both image and motion prompt are required.' });
    }
    res.status(501).json({
        error: 'Veo video generation requires access to Veo video models in your Google Cloud project.',
    });
});

// -------------------------------------------------------------
// Booking Inquiries & Luxury Email Notifications (/api/contact)
// -------------------------------------------------------------
app.post('/api/contact', async (req, res) => {
    try {
        const body = req.body || {};
        const fullName = (body.fullName || '').trim();
        const email = (body.email || '').trim();
        const phone = (body.phone || body.mobile || '').trim();
        const category = (body.category || 'SS Audios – Event & Entertainment').trim();
        const service = (body.service || (Array.isArray(body.services) ? body.services.join(', ') : 'General Enquiry')).trim();
        const venue = (body.venue || '').trim();
        const date = (body.date || '').trim();
        const time = (body.time || '').trim();
        const audience = (body.audience || '').trim();
        const budget = (body.budget || '').trim();
        const sessionDuration = (body.sessionDuration || '').trim();
        const recordingRequirements = (body.recordingRequirements || '').trim();
        const preferredBatch = (body.preferredBatch || '').trim();
        const learningLevel = (body.learningLevel || '').trim();
        const additionalRequirements = (body.additionalRequirements || body.message || '').trim();
        const corporateName = (body.corporateName || body.eventType || category).trim();

        if (!fullName) {
            return res.status(400).json({ success: false, message: 'Your name is required.' });
        }

        const newInquiry = {
            id: `inq_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`,
            fullName,
            email: email || 'Not provided',
            phone: phone || 'Not provided',
            category,
            service,
            venue: venue || 'Not specified',
            date: date || 'Flexible',
            time: time || 'Flexible',
            audience: audience || 'Not specified',
            budget: budget || 'Standard',
            sessionDuration,
            recordingRequirements,
            preferredBatch,
            learningLevel,
            additionalRequirements,
            corporateName,
            createdAt: new Date().toISOString(),
            status: 'new',
        };

        const currentInquiries = getInquiries();
        currentInquiries.unshift(newInquiry);
        saveInquiries(currentInquiries);

        if (infrastructureReady) {
            docClient.put({
                TableName: INQUIRIES_TABLE_NAME,
                Item: {
                    ...newInquiry,
                    id: String(newInquiry.id)
                }
            }).promise().catch(dbErr => console.warn('[DynamoDB Inquiry Error]:', dbErr.message));
        }

        console.log(`📥 [Inquiry Recorded] ${fullName} · ${category} · ${service} (${phone})`);

        // Format timestamp in Indian Standard Time (Asia/Kolkata)
        const istFormattedTime = new Date().toLocaleString('en-IN', {
            timeZone: 'Asia/Kolkata',
            dateStyle: 'full',
            timeStyle: 'medium'
        }) + ' IST';

        const cleanPhone = phone.replace(/[^0-9]/g, '');
        const whatsappReplyUrl = cleanPhone.length >= 10
            ? `https://wa.me/${cleanPhone.startsWith('91') ? cleanPhone : '91' + cleanPhone}?text=${encodeURIComponent(`Hello ${fullName}, thank you for contacting SS Audios regarding "${service}". We would love to confirm your event details.`)}`
            : null;

        // Luxury Dark-Themed Email Notification for SS Audios Admin
        const mailOptions = {
            from: `"SS Audios Inquiries" <${process.env.EMAIL_USER || 'ssaudios25@gmail.com'}>`,
            to: `${process.env.EMAIL_USER || 'ssaudios25@gmail.com'}, ${OWNER_EMAIL}`,
            replyTo: email && email.includes('@') ? email : undefined,
            subject: `🎧 [SS Audios Booking] ${service} • ${fullName} (${date || 'Event Lead'})`,
            html: `
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                </head>
                <body style="margin: 0; padding: 20px; background-color: #000000; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #FDFDFC;">
                    <div style="max-width: 600px; margin: 0 auto; background: linear-gradient(180deg, #0c0c0c 0%, #000000 100%); border: 1px solid #222222; border-radius: 20px; overflow: hidden; box-shadow: 0 10px 40px rgba(0,0,0,0.8);">
                        <div style="background: linear-gradient(90deg, #E04B0A 0%, #FF5A1F 50%, #E04B0A 100%); height: 6px;"></div>
                        <div style="padding: 28px 28px 20px 28px; text-align: center; border-bottom: 1px solid #222222;">
                            <div style="display: inline-block; padding: 4px 14px; background: rgba(255, 90, 31, 0.15); border: 1px solid rgba(255, 90, 31, 0.4); border-radius: 50px; font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 1.5px; color: #FF5A1F; margin-bottom: 12px;">
                                🎧 New Booking Inquiry
                            </div>
                            <h1 style="margin: 0; font-size: 24px; font-weight: 900; letter-spacing: -0.5px; text-transform: uppercase; color: #FDFDFC;">
                                SS <span style="color: #FF5A1F;">Audios</span>
                            </h1>
                            <p style="margin: 6px 0 0 0; font-size: 13px; color: #9E9E9E;">
                                Live Concert Sound, DJ Events, Raga Studio & Sampoorna Academy
                            </p>
                        </div>

                        <div style="padding: 24px 28px;">
                            <div style="background: #0f0f0f; border: 1px solid #222222; border-radius: 14px; padding: 20px; margin-bottom: 24px;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px;">
                                    <div>
                                        <span style="font-size: 11px; font-weight: bold; text-transform: uppercase; color: #9E9E9E; letter-spacing: 1px;">Customer Name</span>
                                        <h2 style="margin: 4px 0 0 0; font-size: 22px; font-weight: 800; color: #FFFFFF;">${fullName}</h2>
                                    </div>
                                    <div style="text-align: right;">
                                        <span style="display: inline-block; padding: 4px 12px; background: #FF5A1F; color: #FFFFFF; font-size: 11px; font-weight: 800; text-transform: uppercase; border-radius: 8px;">
                                            ${category}
                                        </span>
                                    </div>
                                </div>

                                <table style="width: 100%; border-collapse: collapse; font-size: 13px; color: #FDFDFC;">
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; width: 38%; font-weight: 600;">⚡ Requested Service:</td>
                                        <td style="padding: 8px 0; color: #FF5A1F; font-weight: bold;">${service}</td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">📞 Mobile Number:</td>
                                        <td style="padding: 8px 0; font-weight: bold; color: #FFFFFF;">
                                            ${phone ? `<a href="tel:${phone}" style="color: #FFFFFF; text-decoration: none;">${phone}</a>` : 'Not provided'}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">✉️ Email Address:</td>
                                        <td style="padding: 8px 0; font-weight: bold;">
                                            ${email ? `<a href="mailto:${email}" style="color: #FF5A1F; text-decoration: none;">${email}</a>` : 'Not provided'}
                                        </td>
                                    </tr>
                                    ${date ? `
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">📅 Date & Time:</td>
                                        <td style="padding: 8px 0; font-weight: bold; color: #FFFFFF;">${date} ${time ? '· ' + time : ''}</td>
                                    </tr>` : ''}
                                    ${venue ? `
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">📍 Venue / Hall:</td>
                                        <td style="padding: 8px 0; font-weight: bold; color: #FFFFFF;">${venue}</td>
                                    </tr>` : ''}
                                    ${audience ? `
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">👥 Expected Audience:</td>
                                        <td style="padding: 8px 0; font-weight: bold; color: #FFFFFF;">${audience} guests</td>
                                    </tr>` : ''}
                                    ${budget ? `
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">💰 Budget Range:</td>
                                        <td style="padding: 8px 0; font-weight: bold; color: #FFFFFF;">${budget}</td>
                                    </tr>` : ''}
                                    <tr>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-weight: 600;">🕒 Received (IST):</td>
                                        <td style="padding: 8px 0; color: #9E9E9E; font-size: 12px;">${istFormattedTime}</td>
                                    </tr>
                                </table>
                            </div>

                            ${additionalRequirements ? `
                            <div style="background: #000000; border-left: 4px solid #FF5A1F; border-radius: 0 12px 12px 0; padding: 18px; margin-bottom: 24px; border-top: 1px solid #222222; border-right: 1px solid #222222; border-bottom: 1px solid #222222;">
                                <div style="font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 1px; color: #9E9E9E; margin-bottom: 8px;">
                                    📝 Requirements / Notes:
                                </div>
                                <div style="font-size: 14px; line-height: 1.6; color: #EBEBEB; white-space: pre-wrap;">
                                    ${additionalRequirements.replace(/\n/g, '<br/>')}
                                </div>
                            </div>` : ''}

                            <div style="text-align: center; margin-top: 24px; padding-top: 20px; border-top: 1px solid #222222;">
                                ${whatsappReplyUrl ? `
                                <a href="${whatsappReplyUrl}" style="display: inline-block; background-color: #25D366; color: #FFFFFF; padding: 12px 22px; margin: 6px; border-radius: 10px; font-size: 13px; font-weight: 800; text-decoration: none; text-transform: uppercase; letter-spacing: 0.5px;">
                                    💬 Reply on WhatsApp
                                </a>` : ''}
                                ${email && email.includes('@') ? `
                                <a href="mailto:${email}?subject=SS%20Audios%20Booking%20Confirmation" style="display: inline-block; background: linear-gradient(90deg, #E04B0A 0%, #FF5A1F 100%); color: #FFFFFF; padding: 12px 22px; margin: 6px; border-radius: 10px; font-size: 13px; font-weight: 800; text-decoration: none; text-transform: uppercase; letter-spacing: 0.5px;">
                                    ✉️ Reply via Email
                                </a>` : ''}
                            </div>
                        </div>

                        <div style="background: #000000; padding: 16px 28px; text-align: center; border-top: 1px solid #222222; font-size: 11px; color: #666666;">
                            Sent automatically from <strong>SS Audios Web Application</strong> (Erode, Tamil Nadu) • All rights reserved.
                        </div>
                    </div>
                </body>
                </html>
            `
        };

        // Dispatch email via Resend HTTPS (Port 443) or Nodemailer SMTP (Localhost)
        (async () => {
            const resendKey = process.env.RESEND_API_KEY;
            let sentViaResend = false;

            if (resendKey && resendKey.trim().length > 0) {
                try {
                    const resendRes = await fetch('https://api.resend.com/emails', {
                        method: 'POST',
                        headers: {
                            'Authorization': `Bearer ${resendKey.trim()}`,
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            from: 'SS Audios Inquiries <onboarding@resend.dev>',
                            to: [process.env.EMAIL_USER || 'ssaudios25@gmail.com'],
                            reply_to: email && email.includes('@') ? email : undefined,
                            subject: mailOptions.subject,
                            html: mailOptions.html,
                        })
                    });
                    const resendData = await resendRes.json();
                    if (resendRes.ok) {
                        console.log(`✅ [Resend HTTPS] Booking notification delivered (ID: ${resendData.id})`);
                        sentViaResend = true;
                    }
                } catch (resendErr) {
                    console.warn('[Resend Notice]:', resendErr.message);
                }
            }

            if (!sentViaResend) {
                transporter.sendMail(mailOptions)
                    .then(info => console.log(`✅ [Nodemailer] Booking email sent (ID: ${info.messageId})`))
                    .catch(mailErr => console.warn('[Nodemailer Notice]:', mailErr.message));
            }
        })();

        res.json({
            success: true,
            message: 'Thank you! Your booking enquiry has been recorded with SS Audios.',
            inquiryId: newInquiry.id,
        });
    } catch (err) {
        console.error('Error handling contact submission:', err);
        res.status(500).json({ success: false, message: 'Failed to process inquiry.', error: err.message });
    }
});

// Inquiries Admin Endpoints
app.get('/api/inquiries', async (req, res) => {
    try {
        const inquiries = getInquiries();
        inquiries.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));
        res.json({ success: true, count: inquiries.length, data: inquiries });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to retrieve inquiries.', error: err.message });
    }
});

app.delete('/api/inquiries/:id', async (req, res) => {
    try {
        const { id } = req.params;
        let inquiries = getInquiries();
        inquiries = inquiries.filter(i => i.id !== id);
        saveInquiries(inquiries);

        if (infrastructureReady) {
            docClient.delete({ TableName: INQUIRIES_TABLE_NAME, Key: { id } }).promise().catch(() => {});
        }
        res.json({ success: true, message: 'Inquiry deleted successfully.' });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to delete inquiry.', error: err.message });
    }
});

app.post('/api/inquiries/bulk-delete', async (req, res) => {
    try {
        const { ids } = req.body;
        let currentInquiries = getInquiries();
        if (Array.isArray(ids) && ids.length > 0) {
            currentInquiries = currentInquiries.filter(i => !ids.includes(i.id));
        } else {
            currentInquiries = [];
        }
        saveInquiries(currentInquiries);
        res.json({ success: true, message: 'Inquiries deleted successfully.' });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to bulk delete inquiries.', error: err.message });
    }
});

// -------------------------------------------------------------
// Legacy Services & Plans REST API (Full Backward Compatibility)
// -------------------------------------------------------------
app.get('/api/services', async (_req, res) => {
    try {
        const services = getServices();
        res.json({ success: true, count: services.length, data: services });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to retrieve services', error: err.message });
    }
});

app.post('/api/services', async (req, res) => {
    try {
        const { title, price, category, tag, description, image, features } = req.body;
        if (!title) return res.status(400).json({ success: false, message: 'Service title is required.' });

        const currentServices = getServices();
        const numericIds = currentServices.map(s => Number(s.id)).filter(n => !isNaN(n));
        const nextId = numericIds.length > 0 ? Math.max(...numericIds) + 1 : currentServices.length + 1;

        const newService = {
            id: nextId,
            title: title.trim(),
            price: price ? price.trim() : 'Contact for Quote',
            category: category ? category.trim() : 'SS Audios – Event & Entertainment',
            tag: tag ? tag.trim() : 'Live Sound',
            description: description ? description.trim() : '',
            image: image || 'https://images.unsplash.com/photo-1514525253161-7a46d19cd819?auto=format&fit=crop&q=80&w=600',
            features: Array.isArray(features) ? features : [],
        };

        currentServices.push(newService);
        saveServices(currentServices);

        if (infrastructureReady) {
            docClient.put({ TableName: SERVICES_TABLE_NAME, Item: { ...newService, id: String(newService.id) } }).promise().catch(() => {});
        }
        res.status(201).json({ success: true, message: 'Service created successfully', data: newService });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to create service', error: err.message });
    }
});

app.get('/api/plans', async (_req, res) => {
    try {
        const plans = getPlans();
        res.json({ success: true, count: plans.length, data: plans });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to retrieve plans', error: err.message });
    }
});

app.post('/api/plans', async (req, res) => {
    try {
        const { name, theme, monthlyPrice, yearlyPrice, period, desc, badge, buttonText, features } = req.body;
        if (!name) return res.status(400).json({ success: false, message: 'Plan name is required.' });

        const currentPlans = getPlans();
        const planId = name.toLowerCase().replace(/[^a-z0-9]/g, '-');
        const newPlan = {
            id: planId,
            name: name.trim(),
            theme: theme || 'standard',
            monthlyPrice: monthlyPrice || 'Contact for Quote',
            yearlyPrice: yearlyPrice || 'Contact for Quote',
            period: period || '/ event',
            desc: desc || '',
            badge: badge || '',
            buttonText: buttonText || 'Select Plan',
            features: Array.isArray(features) ? features : [],
        };

        currentPlans.push(newPlan);
        savePlans(currentPlans);
        res.status(201).json({ success: true, message: 'Plan created successfully', data: newPlan });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to create plan', error: err.message });
    }
});

// -------------------------------------------------------------
// Media Vault Direct Uploads
// -------------------------------------------------------------
app.get('/api/media', async (req, res) => {
    try {
        const { category, type } = req.query;
        let items = [];
        if (infrastructureReady) {
            const result = await docClient.scan({ TableName: TABLE_NAME }).promise();
            items = result.Items || [];
        } else {
            // Read local uploads
            try {
                const files = fs.readdirSync(UPLOADS_DIR);
                items = files.map(f => ({
                    id: f,
                    fileName: f,
                    url: `/uploads/${f}`,
                    createdAt: new Date().toISOString()
                }));
            } catch {}
        }

        if (category && category !== 'All') {
            items = items.filter(item => item.category?.toLowerCase() === category.toLowerCase());
        }
        if (type && type !== 'All') {
            items = items.filter(item => item.type?.toLowerCase() === type.toLowerCase());
        }
        items.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));

        res.json({ success: true, count: items.length, data: items });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to fetch media records', error: err.message });
    }
});

app.post('/api/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, message: 'No file provided.' });
        }
        const sanitized = req.file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
        const filename = `${Date.now()}-${crypto.randomBytes(3).toString('hex')}-${sanitized}`;
        let fileUrl = '';

        if (infrastructureReady) {
            try {
                const s3Key = `uploads/${filename}`;
                await s3.upload({
                    Bucket: BUCKET_NAME,
                    Key: s3Key,
                    Body: req.file.buffer,
                    ContentType: req.file.mimetype,
                }).promise();
                fileUrl = `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/${s3Key}`;
            } catch (s3Err) {
                console.warn('[S3 Upload Notice]:', s3Err.message);
            }
        }

        if (!fileUrl) {
            const localDest = path.join(UPLOADS_DIR, filename);
            fs.writeFileSync(localDest, req.file.buffer);
            fileUrl = `/uploads/${filename}`;
        }

        res.json({
            success: true,
            fileUrl,
            url: fileUrl,
            fileName: filename,
            mimeType: req.file.mimetype,
            size: req.file.size
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'File upload failed.', error: err.message });
    }
});

// -------------------------------------------------------------
// Server Initialization
// -------------------------------------------------------------
if (process.env.NODE_ENV !== 'test' && !process.env.VERCEL) {
    app.listen(PORT, async () => {
        console.log(`=======================================================`);
        console.log(`🎧 SS Audios Backend running on port ${PORT}`);
        console.log(`🌐 Public Site Data: http://localhost:${PORT}/api/site-data`);
        console.log(`🚀 Health Check:      http://localhost:${PORT}/api/health`);
        console.log(`📍 City & Region:     ${BUSINESS_CONTACT.cityRegion}`);
        console.log(`=======================================================`);
        try {
            await initAWSInfrastructure();
        } catch (e) {
            console.warn('AWS init notice:', e.message);
        }
    });
}

module.exports = app;
