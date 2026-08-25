require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const AWS = require('aws-sdk');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 5000;

// Data directory for local persistence
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

const SERVICES_FILE = path.join(DATA_DIR, 'services.json');
const PLANS_FILE = path.join(DATA_DIR, 'plans.json');
const INQUIRIES_FILE = path.join(DATA_DIR, 'inquiries.json');

// Configure Nodemailer Transporter with Gmail (Forcing IPv4 & Port 587 for Railway compatibility)
const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 587,
    secure: false,
    requireTLS: true,
    family: 4, // Force IPv4 to prevent ENETUNREACH on Railway
    auth: {
        user: process.env.EMAIL_USER || 'manojfa4451e@gmail.com',
        pass: (process.env.EMAIL_PASS || 'jffz nlji ltev ruvr').replace(/\s+/g, ''),
    },
    tls: {
        rejectUnauthorized: false
    },
    connectionTimeout: 10000,
    greetingTimeout: 10000,
    socketTimeout: 15000
});

// Configuration Constants
const REGION = process.env.AWS_REGION || 'ap-south-1';
const BUCKET_NAME = process.env.S3_BUCKET_NAME || 'soundscape-media-vault-' + REGION;
const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME || 'SoundscapeMedia';
const SERVICES_TABLE_NAME = process.env.DYNAMODB_SERVICES_TABLE || 'SoundscapeServices';
const PLANS_TABLE_NAME = process.env.DYNAMODB_PLANS_TABLE || 'SoundscapePlans';
const INQUIRIES_TABLE_NAME = process.env.DYNAMODB_INQUIRIES_TABLE || 'SoundscapeInquiries';

// Middleware
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Configure Multer for memory storage (max 100MB per file)
const storage = multer.memoryStorage();
const upload = multer({
    storage: storage,
    limits: { fileSize: 100 * 1024 * 1024 }
});

// Configure AWS SDK
AWS.config.update({
    region: REGION,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

let infrastructureReady = false;

// -------------------------------------------------------------
// Auto-initialize AWS Infrastructure (DynamoDB Tables & S3 Bucket)
// -------------------------------------------------------------
async function initAWSInfrastructure() {
    console.log('🚀 Checking AWS Infrastructure (DynamoDB & S3)...');

    // Helper to ensure a DynamoDB table exists
    async function ensureTable(tableName, keyName = 'id', keyType = 'S') {
        try {
            console.log(`[DynamoDB] Checking table: "${tableName}"...`);
            const tableDesc = await dynamodb.describeTable({ TableName: tableName }).promise();
            console.log(`✅ [DynamoDB] Table "${tableName}" exists (Status: ${tableDesc.Table.TableStatus}).`);
        } catch (err) {
            if (err.code === 'ResourceNotFoundException') {
                console.log(`⚠️  [DynamoDB] Table "${tableName}" not found. Creating now...`);
                const params = {
                    TableName: tableName,
                    KeySchema: [
                        { AttributeName: keyName, KeyType: 'HASH' }
                    ],
                    AttributeDefinitions: [
                        { AttributeName: keyName, AttributeType: keyType }
                    ],
                    BillingMode: 'PAY_PER_REQUEST'
                };
                await dynamodb.createTable(params).promise();
                console.log(`⏳ [DynamoDB] Waiting for table "${tableName}" to become ACTIVE...`);
                await dynamodb.waitFor('tableExists', { TableName: tableName }).promise();
                console.log(`✅ [DynamoDB] Table "${tableName}" created and ACTIVE!`);
            } else {
                console.error(`❌ [DynamoDB] Error checking/creating table "${tableName}":`, err.message);
            }
        }
    }

    // 1. Check & Create DynamoDB Tables (Media, Services, Plans, Inquiries)
    await ensureTable(TABLE_NAME, 'id', 'S');
    await ensureTable(SERVICES_TABLE_NAME, 'id', 'S');
    await ensureTable(PLANS_TABLE_NAME, 'id', 'S');
    await ensureTable(INQUIRIES_TABLE_NAME, 'id', 'S');

    // 2. Check & Create S3 Bucket
    try {
        console.log(`[S3] Checking bucket: "${BUCKET_NAME}"...`);
        await s3.headBucket({ Bucket: BUCKET_NAME }).promise();
        console.log(`✅ [S3] Bucket "${BUCKET_NAME}" exists and is accessible.`);
    } catch (err) {
        if (err.statusCode === 404 || err.code === 'NotFound' || err.code === 'NoSuchBucket') {
            console.log(`⚠️  [S3] Bucket "${BUCKET_NAME}" not found. Creating bucket in ${REGION}...`);
            const createParams = {
                Bucket: BUCKET_NAME,
                ...(REGION !== 'us-east-1' ? {
                    CreateBucketConfiguration: {
                        LocationConstraint: REGION
                    }
                } : {})
            };
            await s3.createBucket(createParams).promise();
            console.log(`✅ [S3] Bucket "${BUCKET_NAME}" created successfully!`);

            // Apply CORS to the newly created bucket
            try {
                await s3.putBucketCors({
                    Bucket: BUCKET_NAME,
                    CORSConfiguration: {
                        CORSRules: [
                            {
                                AllowedHeaders: ['*'],
                                AllowedMethods: ['GET', 'HEAD'],
                                AllowedOrigins: ['*'],
                                MaxAgeSeconds: 3000
                            }
                        ]
                    }
                }).promise();
                console.log(`✅ [S3] CORS configuration applied to "${BUCKET_NAME}".`);
            } catch (corsErr) {
                console.warn(`⚠️ [S3] Could not apply CORS:`, corsErr.message);
            }
        } else {
            console.error(`❌ [S3] Error accessing bucket "${BUCKET_NAME}":`, err.message);
        }
    }

    infrastructureReady = true;
    console.log('🎉 AWS Infrastructure verification complete.');
}

// -------------------------------------------------------------
// REST API Endpoints
// -------------------------------------------------------------

// Health Check Endpoint
app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        infrastructureReady,
        region: REGION,
        s3Bucket: BUCKET_NAME,
        dynamoTable: TABLE_NAME,
        timestamp: new Date().toISOString()
    });
});

// GET all media items (optionally filter by ?category= or ?type=)
app.get('/api/media', async (req, res) => {
    try {
        const { category, type } = req.query;
        const params = {
            TableName: TABLE_NAME
        };

        const result = await docClient.scan(params).promise();
        let items = result.Items || [];

        // In-memory filter if query params provided
        if (category && category !== 'All') {
            items = items.filter(item => item.category?.toLowerCase() === category.toLowerCase());
        }
        if (type && type !== 'All') {
            items = items.filter(item => item.type?.toLowerCase() === type.toLowerCase());
        }

        // Sort latest first
        items.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));

        res.json({
            success: true,
            count: items.length,
            data: items
        });
    } catch (err) {
        console.error('Error fetching media from DynamoDB:', err);
        res.status(500).json({
            success: false,
            message: 'Failed to fetch media records',
            error: err.message
        });
    }
});

// POST upload new media (Image, Video, Audio)
app.post('/api/media', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, message: 'No media file provided.' });
        }

        const { title, category, type } = req.body;
        if (!title) {
            return res.status(400).json({ success: false, message: 'Title is required.' });
        }

        const fileExt = req.file.originalname.includes('.')
            ? req.file.originalname.split('.').pop()
            : 'bin';
        const sanitizedOriginalName = req.file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
        const s3Key = `media/${Date.now()}-${crypto.randomBytes(4).toString('hex')}-${sanitizedOriginalName}`;

        console.log(`[Upload] Uploading "${sanitizedOriginalName}" (${(req.file.size / 1024 / 1024).toFixed(2)} MB) to S3...`);

        // Upload to S3
        const uploadParams = {
            Bucket: BUCKET_NAME,
            Key: s3Key,
            Body: req.file.buffer,
            ContentType: req.file.mimetype,
        };

        const s3Result = await s3.upload(uploadParams).promise();
        console.log(`✅ [Upload] Uploaded to S3: ${s3Result.Location}`);

        // Determine media type
        let inferredType = type;
        if (!inferredType) {
            if (req.file.mimetype.startsWith('video/')) inferredType = 'video';
            else if (req.file.mimetype.startsWith('audio/')) inferredType = 'audio';
            else inferredType = 'image';
        }

        // Save metadata in DynamoDB
        const mediaItem = {
            id: crypto.randomUUID(),
            title: title.trim(),
            category: category || 'Ambient',
            type: inferredType,
            url: s3Result.Location,
            s3Key: s3Key,
            fileName: req.file.originalname,
            fileSize: req.file.size,
            mimeType: req.file.mimetype,
            createdAt: new Date().toISOString()
        };

        await docClient.put({
            TableName: TABLE_NAME,
            Item: mediaItem
        }).promise();

        console.log(`✅ [DynamoDB] Saved media metadata for "${mediaItem.title}" (ID: ${mediaItem.id})`);

        res.status(201).json({
            success: true,
            message: 'Media uploaded and indexed successfully',
            data: mediaItem
        });
    } catch (err) {
        console.error('Error uploading media:', err);
        res.status(500).json({
            success: false,
            message: 'Failed to upload and save media',
            error: err.message
        });
    }
});

// PUT update media metadata (Title & Category)
app.put('/api/media/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { title, category } = req.body;

        if (!title && !category) {
            return res.status(400).json({ success: false, message: 'Nothing to update.' });
        }

        const updateExpressions = [];
        const expressionAttributeNames = {};
        const expressionAttributeValues = {};

        if (title) {
            updateExpressions.push('#t = :title');
            expressionAttributeNames['#t'] = 'title';
            expressionAttributeValues[':title'] = title.trim();
        }

        if (category) {
            updateExpressions.push('#c = :category');
            expressionAttributeNames['#c'] = 'category';
            expressionAttributeValues[':category'] = category;
        }

        updateExpressions.push('#u = :updatedAt');
        expressionAttributeNames['#u'] = 'updatedAt';
        expressionAttributeValues[':updatedAt'] = new Date().toISOString();

        const params = {
            TableName: TABLE_NAME,
            Key: { id },
            UpdateExpression: 'SET ' + updateExpressions.join(', '),
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
            ReturnValues: 'ALL_NEW'
        };

        const updated = await docClient.update(params).promise();

        res.json({
            success: true,
            message: 'Media item updated successfully',
            data: updated.Attributes
        });
    } catch (err) {
        console.error('Error updating media in DynamoDB:', err);
        res.status(500).json({
            success: false,
            message: 'Failed to update media item',
            error: err.message
        });
    }
});

// DELETE media item from DynamoDB and S3
app.delete('/api/media/:id', async (req, res) => {
    try {
        const { id } = req.params;

        // 1. Fetch item from DynamoDB to get S3 key
        const getResult = await docClient.get({
            TableName: TABLE_NAME,
            Key: { id }
        }).promise();

        if (!getResult.Item) {
            return res.status(404).json({ success: false, message: 'Media item not found.' });
        }

        const s3Key = getResult.Item.s3Key;

        // 2. Delete from S3 if key exists
        if (s3Key) {
            try {
                await s3.deleteObject({
                    Bucket: BUCKET_NAME,
                    Key: s3Key
                }).promise();
                console.log(`✅ [S3] Deleted object: ${s3Key}`);
            } catch (s3Err) {
                console.warn(`⚠️ [S3] Could not delete S3 object:`, s3Err.message);
            }
        }

        // 3. Delete from DynamoDB
        await docClient.delete({
            TableName: TABLE_NAME,
            Key: { id }
        }).promise();

        console.log(`✅ [DynamoDB] Deleted item ID: ${id}`);

        res.json({
            success: true,
            message: 'Media item deleted successfully from S3 and DynamoDB'
        });
    } catch (err) {
        console.error('Error deleting media:', err);
        res.status(500).json({
            success: false,
            message: 'Failed to delete media item',
            error: err.message
        });
    }
});

// Fallback proxy endpoint to stream media directly from S3 by Item ID (supports Range requests for video/audio seek)
app.get('/api/media/stream/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const getResult = await docClient.get({
            TableName: TABLE_NAME,
            Key: { id }
        }).promise();

        if (!getResult.Item || !getResult.Item.s3Key) {
            return res.status(404).send('Media item or S3 key not found.');
        }

        const key = getResult.Item.s3Key;
        const range = req.headers.range;

        const headParams = { Bucket: BUCKET_NAME, Key: key };
        const headData = await s3.headObject(headParams).promise();

        const fileSize = headData.ContentLength;
        const contentType = headData.ContentType || 'application/octet-stream';

        if (range) {
            const parts = range.replace(/bytes=/, '').split('-');
            const start = parseInt(parts[0], 10);
            const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
            const chunksize = (end - start) + 1;

            res.writeHead(206, {
                'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                'Accept-Ranges': 'bytes',
                'Content-Length': chunksize,
                'Content-Type': contentType,
            });

            const streamParams = {
                Bucket: BUCKET_NAME,
                Key: key,
                Range: `bytes=${start}-${end}`
            };
            s3.getObject(streamParams).createReadStream().pipe(res);
        } else {
            res.writeHead(200, {
                'Content-Length': fileSize,
                'Content-Type': contentType,
                'Accept-Ranges': 'bytes'
            });
            s3.getObject(headParams).createReadStream().pipe(res);
        }
    } catch (err) {
        console.error('Stream error:', err.message);
        res.status(404).send('File not found');
    }
});

// -------------------------------------------------------------
// DEFAULT DATA & PERSISTENCE HELPERS FOR SERVICES & PLANS
// -------------------------------------------------------------
const DEFAULT_SERVICES = [
    {
        id: 1,
        title: "Wedding Events",
        price: "₹25,000",
        category: "Grand Celebrations",
        tag: "Tour-Grade Audio",
        description:
            "Unforgettable Wedding Audio & Staging. Tour-grade sound systems, precision acoustic tuning, and ambient staging tailored to make every vow and song crystal clear.",
        image:
            "https://images.unsplash.com/photo-1597157639073-69284dc0fdaf?q=80&w=1174&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D",
        features: ["Precision Acoustic Tuning", "Tour-Grade Wireless Sound", "Ambient Staging & Lighting"],
    },
    {
        id: 2,
        title: "Goldstar Orchestra",
        price: "₹25,000",
        category: "Live Orchestration",
        tag: "Multi-Genre",
        description:
            "Crafted live orchestral arrangements, high-energy beatmatching, and versatile multi-genre music curation designed to keep your celebration vibrant and unforgettable.",
        image:
            "https://images.unsplash.com/photo-1571266028243-3716f02d2d2e?auto=format&fit=crop&q=80&w=600",
        features: ["Live String & Brass Ensemble", "Multi-Genre Song Curation", "Dynamic Live Beatmatching"],
    },
    {
        id: 3,
        title: "Lighting & Audio",
        price: "₹40,000",
        category: "Atmospheric FX",
        tag: "Intelligent Lighting",
        description:
            "Intelligent Lighting & Crystal-Clear Sound. Dynamic moving heads, laser shows, and synchronized strobes paired with high-fidelity audio engineering and low-fog atmospheric effects.",
        image:
            "https://images.unsplash.com/photo-1514525253161-7a46d19cd819?auto=format&fit=crop&q=80&w=600",
        features: ["Moving Head Lasers & Strobes", "Synchronized FX & Low-Fog", "Hi-Fi Audio Engineering"],
    },
    {
        id: 4,
        title: "Welcome Dance",
        price: "₹15,000",
        category: "Stage Choreography",
        tag: "Opening Act",
        description:
            "Vibrant Welcome Dance Performance. Electrifying choreography, custom entrance tracks, and synchronized stage pyrotechnics designed to set an unforgettable opening tone for your guests.",
        image:
            "https://images.unsplash.com/photo-1598488035139-bdbb2231ce04?auto=format&fit=crop&q=80&w=600",
        features: ["Custom Entrance Tracks", "Synchronized Stage Pyros", "Electrifying Choreography"],
    },
    {
        id: 5,
        title: "DJ Events",
        price: "Starting from ₹15,000",
        category: "Club & Festival",
        tag: "Live Stem Remixing",
        description:
            "Electrifying DJ Events & Festival Beats. Festival-grade audio systems, seamless live stem mixing, and real-time visual synchronization designed to keep your dance floor packed all night.",
        image:
            "https://images.unsplash.com/photo-1470225620780-dba8ba36b745?auto=format&fit=crop&q=80&w=600",
        features: ["Real-Time Stem Mixing", "Festival-Grade Sound Array", "Synchronized Visuals"],
    },
    {
        id: 6,
        title: "Instrumentals",
        price: "₹10,000",
        category: "Acoustic Solo & Band",
        tag: "Soulful Live",
        description:
            "Mesmerizing Instrumental Performances. Soulful live solos and ensemble arrangements spanning violin, flute, saxophone, and classical instruments for an elegant, immersive ambiance.",
        image:
            "https://images.unsplash.com/photo-1508700115892-45ecd05ae2ad?auto=format&fit=crop&q=80&w=600",
        features: ["Violin, Sax & Flute Solos", "Ensemble Arrangements", "Immersive Classical Ambiance"],
    },
];

const DEFAULT_PLANS = [
    {
        id: "starter",
        name: "Starter",
        badge: "Solo Creators",
        desc: "Essential DJ set for private home parties and intimate gatherings.",
        monthlyPrice: "$199",
        yearlyPrice: "$159",
        period: "/ event",
        buttonText: "Get Starter",
        theme: "standard",
        videoUrl: "https://assets.mixkit.co/videos/preview/mixkit-dj-playing-music-at-a-party-41338-large.mp4",
        videos: [
            "https://assets.mixkit.co/videos/preview/mixkit-dj-playing-music-at-a-party-41338-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-hands-of-a-dj-mixing-music-41337-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-dj-mixing-music-on-stage-41339-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-stage-lights-and-crowd-at-a-concert-41550-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-laser-lights-in-a-stage-show-41551-large.mp4"
        ],
        features: [
            { text: "3 Hours Live DJ Set", included: true },
            { text: "Basic Sound System (1,000W)", included: true },
            { text: "Standard Playlist Customization", included: true },
            { text: "Dynamic Stage Lighting & FX", included: false },
            { text: "Dedicated Sound Engineer", included: false },
            { text: "Wireless Mic & MC Host", included: false },
            { text: "Custom 3D Visual Projection", included: false },
        ],
    },
    {
        id: "basic",
        name: "Basic",
        badge: "Club Nights",
        desc: "Ideal for medium lounge venues, birthdays, and rooftop parties.",
        monthlyPrice: "$399",
        yearlyPrice: "$319",
        period: "/ event",
        buttonText: "Choose Basic",
        theme: "standard",
        videoUrl: "https://assets.mixkit.co/videos/preview/mixkit-dj-mixing-music-on-stage-41339-large.mp4",
        videos: [
            "https://assets.mixkit.co/videos/preview/mixkit-dj-mixing-music-on-stage-41339-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-stage-lights-and-crowd-at-a-concert-41550-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-hands-of-a-dj-mixing-music-41337-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-laser-lights-in-a-stage-show-41551-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-abstract-laser-lights-background-41552-large.mp4"
        ],
        features: [
            { text: "5 Hours Live DJ Set", included: true },
            { text: "Pro Sound System (3,000W)", included: true },
            { text: "Standard Playlist Customization", included: true },
            { text: "Dynamic Stage Lighting & FX", included: true },
            { text: "Dedicated Sound Engineer", included: false },
            { text: "Wireless Mic & MC Host", included: false },
            { text: "Custom 3D Visual Projection", included: false },
        ],
    },
    {
        id: "standard",
        name: "Standard",
        badge: "Corporate Events",
        desc: "Complete audio-visual setup for corporate events and weddings.",
        monthlyPrice: "$699",
        yearlyPrice: "$559",
        period: "/ event",
        buttonText: "Select Standard",
        theme: "standard",
        videoUrl: "https://assets.mixkit.co/videos/preview/mixkit-stage-lights-and-crowd-at-a-concert-41550-large.mp4",
        videos: [
            "https://assets.mixkit.co/videos/preview/mixkit-stage-lights-and-crowd-at-a-concert-41550-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-laser-lights-in-a-stage-show-41551-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-abstract-laser-lights-background-41552-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-dj-playing-music-at-a-party-41338-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-dj-mixing-music-on-stage-41339-large.mp4"
        ],
        features: [
            { text: "7 Hours Live DJ Performance", included: true },
            { text: "High-Impact Concert Sound (5,000W)", included: true },
            { text: "Custom Playlist & Track Edits", included: true },
            { text: "Dynamic Stage Lighting & FX", included: true },
            { text: "Dedicated Sound Engineer", included: true },
            { text: "Wireless Mic & MC Host", included: true },
            { text: "Custom 3D Visual Projection", included: false },
        ],
    },
    {
        id: "premium",
        name: "Premium",
        badge: "MOST POPULAR",
        desc: "Full-scale concert production with silver-grade audio and staging.",
        monthlyPrice: "$1,299",
        yearlyPrice: "$1,039",
        period: "/ event",
        buttonText: "Upgrade to Premium",
        theme: "silver",
        videoUrl: "https://assets.mixkit.co/videos/preview/mixkit-laser-lights-in-a-stage-show-41551-large.mp4",
        videos: [
            "https://assets.mixkit.co/videos/preview/mixkit-laser-lights-in-a-stage-show-41551-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-abstract-laser-lights-background-41552-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-dj-mixing-music-on-stage-41339-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-hands-of-a-dj-mixing-music-41337-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-stage-lights-and-crowd-at-a-concert-41550-large.mp4"
        ],
        features: [
            { text: "Full Night Live DJ Set (Up to 10h)", included: true },
            { text: "Tour-Grade Array Sound (10,000W)", included: true },
            { text: "Custom Playlist & Track Edits", included: true },
            { text: "Full Moving-Head Light Show & Fog", included: true },
            { text: "2x Sound & Lighting Engineers", included: true },
            { text: "Dual Wireless Mics & Pro MC", included: true },
            { text: "Custom 3D Visual Projection", included: true },
        ],
    },
    {
        id: "elite",
        name: "Elite",
        badge: "VIP / FESTIVAL",
        desc: "Ultimate festival experience with top-tier gold stage production.",
        monthlyPrice: "$2,499",
        yearlyPrice: "$1,999",
        period: "/ event",
        buttonText: "Book Elite VIP",
        theme: "gold",
        videoUrl: "https://assets.mixkit.co/videos/preview/mixkit-hands-of-a-dj-mixing-music-41337-large.mp4",
        videos: [
            "https://assets.mixkit.co/videos/preview/mixkit-hands-of-a-dj-mixing-music-41337-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-laser-lights-in-a-stage-show-41551-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-abstract-laser-lights-background-41552-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-stage-lights-and-crowd-at-a-concert-41550-large.mp4",
            "https://assets.mixkit.co/videos/preview/mixkit-dj-playing-music-at-a-party-41338-large.mp4"
        ],
        features: [
            { text: "Unlimited Performance Duration", included: true },
            { text: "Ultra Concert Sound System (25,000W+)", included: true },
            { text: "Exclusive Original Live Remixes & Stems", included: true },
            { text: "Full Laser Show, CO2 Jets & Pyros", included: true },
            { text: "Full Backstage Audio Crew & Director", included: true },
            { text: "Multi-Wireless System & Celebrity MC", included: true },
            { text: "Custom 3D Video Mapping & LED Wall", included: true },
        ],
    },
];

function getServices() {
    try {
        if (fs.existsSync(SERVICES_FILE)) {
            const raw = fs.readFileSync(SERVICES_FILE, 'utf8');
            return JSON.parse(raw);
        }
    } catch (e) {
        console.error('Error reading services file:', e);
    }
    // Initialize with defaults if file doesn't exist
    saveServices(DEFAULT_SERVICES);
    return DEFAULT_SERVICES;
}

function saveServices(servicesData) {
    try {
        fs.writeFileSync(SERVICES_FILE, JSON.stringify(servicesData, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('Error saving services file:', e);
        return false;
    }
}

function getPlans() {
    try {
        if (fs.existsSync(PLANS_FILE)) {
            const raw = fs.readFileSync(PLANS_FILE, 'utf8');
            return JSON.parse(raw);
        }
    } catch (e) {
        console.error('Error reading plans file:', e);
    }
    // Initialize with defaults if file doesn't exist
    savePlans(DEFAULT_PLANS);
    return DEFAULT_PLANS;
}

function savePlans(plansData) {
    try {
        fs.writeFileSync(PLANS_FILE, JSON.stringify(plansData, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('Error saving plans file:', e);
        return false;
    }
}

// Database / File Sync Helpers for Services
async function getServicesAsync() {
    if (infrastructureReady) {
        try {
            const res = await docClient.scan({ TableName: SERVICES_TABLE_NAME }).promise();
            if (res.Items && res.Items.length > 0) {
                const items = res.Items.map(item => ({
                    ...item,
                    id: isNaN(item.id) ? item.id : Number(item.id)
                })).sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
                saveServices(items);
                return items;
            }
        } catch (dbErr) {
            console.warn('[DynamoDB Services Scan Warning]:', dbErr.message);
        }
    }
    return getServices();
}

async function saveServiceToDb(service) {
    if (infrastructureReady) {
        try {
            await docClient.put({
                TableName: SERVICES_TABLE_NAME,
                Item: {
                    ...service,
                    id: String(service.id)
                }
            }).promise();
        } catch (dbErr) {
            console.warn('[DynamoDB Service Put Error]:', dbErr.message);
        }
    }
}

async function deleteServiceFromDb(id) {
    if (infrastructureReady) {
        try {
            await docClient.delete({
                TableName: SERVICES_TABLE_NAME,
                Key: { id: String(id) }
            }).promise();
        } catch (dbErr) {
            console.warn('[DynamoDB Service Delete Error]:', dbErr.message);
        }
    }
}

// Database / File Sync Helpers for Plans
async function getPlansAsync() {
    if (infrastructureReady) {
        try {
            const res = await docClient.scan({ TableName: PLANS_TABLE_NAME }).promise();
            if (res.Items && res.Items.length > 0) {
                const items = res.Items;
                savePlans(items);
                return items;
            }
        } catch (dbErr) {
            console.warn('[DynamoDB Plans Scan Warning]:', dbErr.message);
        }
    }
    return getPlans();
}

async function savePlanToDb(plan) {
    if (infrastructureReady) {
        try {
            await docClient.put({
                TableName: PLANS_TABLE_NAME,
                Item: {
                    ...plan,
                    id: String(plan.id || plan.name)
                }
            }).promise();
        } catch (dbErr) {
            console.warn('[DynamoDB Plan Put Error]:', dbErr.message);
        }
    }
}

async function deletePlanFromDb(id) {
    if (infrastructureReady) {
        try {
            await docClient.delete({
                TableName: PLANS_TABLE_NAME,
                Key: { id: String(id) }
            }).promise();
        } catch (dbErr) {
            console.warn('[DynamoDB Plan Delete Error]:', dbErr.message);
        }
    }
}

// -------------------------------------------------------------
// SERVICES REST API
// -------------------------------------------------------------

// GET all services
app.get('/api/services', async (req, res) => {
    try {
        const services = await getServicesAsync();
        res.json({
            success: true,
            count: services.length,
            data: services
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to retrieve services', error: err.message });
    }
});

// POST create a new service
app.post('/api/services', async (req, res) => {
    try {
        const { title, price, category, tag, description, image, features } = req.body;

        if (!title) {
            return res.status(400).json({ success: false, message: 'Service title is required.' });
        }

        const currentServices = getServices();
        const numericIds = currentServices.map(s => Number(s.id)).filter(n => !isNaN(n));
        const nextId = numericIds.length > 0 ? Math.max(...numericIds) + 1 : currentServices.length + 1;

        const newService = {
            id: nextId,
            title: title.trim(),
            price: price ? price.trim() : 'Contact for Quote',
            category: category ? category.trim() : 'Live DJ',
            tag: tag ? tag.trim() : 'DJ Experience',
            description: description ? description.trim() : '',
            image: image && image.trim().length > 0
                ? image.trim()
                : 'https://images.unsplash.com/photo-1470225620780-dba8ba36b745?auto=format&fit=crop&q=80&w=600',
            features: Array.isArray(features)
                ? features.filter(f => typeof f === 'string' && f.trim().length > 0)
                : (typeof features === 'string' ? features.split('\n').filter(s => s.trim().length > 0) : [])
        };

        currentServices.push(newService);
        saveServices(currentServices);
        await saveServiceToDb(newService);

        console.log(`✅ [Services] Added new service "${newService.title}" (ID: ${newService.id})`);

        res.status(201).json({
            success: true,
            message: `Service "${newService.title}" created successfully`,
            data: newService
        });
    } catch (err) {
        console.error('Error creating service:', err);
        res.status(500).json({ success: false, message: 'Failed to create service', error: err.message });
    }
});

// PUT update all services
app.put('/api/services', async (req, res) => {
    try {
        const updatedList = req.body;
        if (!Array.isArray(updatedList)) {
            return res.status(400).json({ success: false, message: 'Expected array of services.' });
        }
        saveServices(updatedList);
        for (const item of updatedList) {
            await saveServiceToDb(item);
        }
        res.json({
            success: true,
            message: 'All services updated successfully',
            data: updatedList
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to update services', error: err.message });
    }
});

// PUT update a single service by ID
app.put('/api/services/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        const currentServices = getServices();
        const index = currentServices.findIndex(s => Number(s.id) === id);

        if (index === -1) {
            return res.status(404).json({ success: false, message: `Service with ID ${id} not found.` });
        }

        const updatedService = {
            ...currentServices[index],
            ...req.body,
            id: id // preserve ID
        };

        if (Array.isArray(req.body.features)) {
            updatedService.features = req.body.features;
        }

        currentServices[index] = updatedService;
        saveServices(currentServices);
        await saveServiceToDb(updatedService);

        console.log(`✅ [Services] Updated service "${updatedService.title}" (ID: ${id})`);

        res.json({
            success: true,
            message: `Service "${updatedService.title}" updated successfully`,
            data: updatedService
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to update service', error: err.message });
    }
});

// DELETE a service by ID
app.delete('/api/services/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        const currentServices = getServices();
        const index = currentServices.findIndex(s => Number(s.id) === id);

        if (index === -1) {
            return res.status(404).json({ success: false, message: `Service with ID ${id} not found.` });
        }

        const deletedService = currentServices.splice(index, 1)[0];
        saveServices(currentServices);
        await deleteServiceFromDb(id);

        console.log(`🗑️ [Services] Deleted service "${deletedService.title}" (ID: ${id})`);

        res.json({
            success: true,
            message: `Service "${deletedService.title}" deleted successfully`,
            data: deletedService
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to delete service', error: err.message });
    }
});

// POST reset services to defaults
app.post('/api/services/reset', async (req, res) => {
    try {
        saveServices(DEFAULT_SERVICES);
        for (const item of DEFAULT_SERVICES) {
            await saveServiceToDb(item);
        }
        res.json({
            success: true,
            message: 'Services reset to defaults',
            data: DEFAULT_SERVICES
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to reset services', error: err.message });
    }
});

// -------------------------------------------------------------
// PRICING PLANS REST API
// -------------------------------------------------------------

// GET all pricing plans
app.get('/api/plans', async (req, res) => {
    try {
        const plans = await getPlansAsync();
        res.json({
            success: true,
            count: plans.length,
            data: plans
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to retrieve pricing plans', error: err.message });
    }
});

// POST create a new pricing plan
app.post('/api/plans', async (req, res) => {
    try {
        const { id, name, badge, desc, monthlyPrice, yearlyPrice, period, buttonText, theme, features, videoUrl, videos } = req.body;

        if (!name) {
            return res.status(400).json({ success: false, message: 'Plan name is required.' });
        }

        const planId = (id || name).toLowerCase().replace(/[^a-z0-9]/g, '-');
        const currentPlans = getPlans();

        const newPlan = {
            id: planId,
            name: name.trim(),
            badge: badge ? badge.trim() : 'SPECIAL TIER',
            desc: desc ? desc.trim() : '',
            monthlyPrice: monthlyPrice ? monthlyPrice.trim() : '$499',
            yearlyPrice: yearlyPrice ? yearlyPrice.trim() : '$399',
            period: period ? period.trim() : '/ event',
            buttonText: buttonText ? buttonText.trim() : `Choose ${name.trim()}`,
            theme: theme ? theme.trim() : 'standard',
            videoUrl: videoUrl ? videoUrl.trim() : "https://assets.mixkit.co/videos/preview/mixkit-dj-playing-music-at-a-party-41338-large.mp4",
            videos: Array.isArray(videos) && videos.length > 0 ? videos : [
                videoUrl ? videoUrl.trim() : "https://assets.mixkit.co/videos/preview/mixkit-dj-playing-music-at-a-party-41338-large.mp4"
            ],
            features: Array.isArray(features) ? features : [
                { text: "Live DJ Performance", included: true },
                { text: "Pro Sound Array", included: true },
                { text: "Stage Lighting & FX", included: true }
            ]
        };

        currentPlans.push(newPlan);
        savePlans(currentPlans);
        await savePlanToDb(newPlan);

        console.log(`✅ [Plans] Added new plan "${newPlan.name}" (ID: ${newPlan.id})`);

        res.status(201).json({
            success: true,
            message: `Pricing plan "${newPlan.name}" created successfully`,
            data: newPlan
        });
    } catch (err) {
        console.error('Error creating plan:', err);
        res.status(500).json({ success: false, message: 'Failed to create pricing plan', error: err.message });
    }
});

// PUT update all plans
app.put('/api/plans', async (req, res) => {
    try {
        const updatedPlans = req.body;
        if (!Array.isArray(updatedPlans)) {
            return res.status(400).json({ success: false, message: 'Expected array of plans.' });
        }
        savePlans(updatedPlans);
        for (const item of updatedPlans) {
            await savePlanToDb(item);
        }
        res.json({
            success: true,
            message: 'Pricing plans updated successfully',
            data: updatedPlans
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to update pricing plans', error: err.message });
    }
});

// PUT update a single plan by ID or Name
app.put('/api/plans/:id', async (req, res) => {
    try {
        const idOrName = req.params.id.toLowerCase();
        const currentPlans = getPlans();
        const index = currentPlans.findIndex(p => (p.id && p.id.toLowerCase() === idOrName) || (p.name && p.name.toLowerCase() === idOrName));

        if (index === -1) {
            return res.status(404).json({ success: false, message: `Plan "${req.params.id}" not found.` });
        }

        const updatedPlan = {
            ...currentPlans[index],
            ...req.body
        };

        currentPlans[index] = updatedPlan;
        savePlans(currentPlans);
        await savePlanToDb(updatedPlan);

        console.log(`✅ [Plans] Updated plan "${updatedPlan.name}"`);

        res.json({
            success: true,
            message: `Plan "${updatedPlan.name}" updated successfully`,
            data: updatedPlan
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to update plan', error: err.message });
    }
});

// DELETE a pricing plan by ID or Name
app.delete('/api/plans/:id', async (req, res) => {
    try {
        const idOrName = req.params.id.toLowerCase();
        const currentPlans = getPlans();
        const index = currentPlans.findIndex(p => (p.id && p.id.toLowerCase() === idOrName) || (p.name && p.name.toLowerCase() === idOrName));

        if (index === -1) {
            return res.status(404).json({ success: false, message: `Plan "${req.params.id}" not found.` });
        }

        const deletedPlan = currentPlans.splice(index, 1)[0];
        savePlans(currentPlans);
        await deletePlanFromDb(deletedPlan.id || idOrName);

        console.log(`🗑️ [Plans] Deleted plan "${deletedPlan.name}"`);

        res.json({
            success: true,
            message: `Plan "${deletedPlan.name}" deleted successfully`,
            data: deletedPlan
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to delete plan', error: err.message });
    }
});

// POST reset plans to defaults
app.post('/api/plans/reset', async (req, res) => {
    try {
        savePlans(DEFAULT_PLANS);
        for (const item of DEFAULT_PLANS) {
            await savePlanToDb(item);
        }
        res.json({
            success: true,
            message: 'Pricing plans reset to defaults',
            data: DEFAULT_PLANS
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to reset plans', error: err.message });
    }
});

function getInquiries() {
    try {
        if (fs.existsSync(INQUIRIES_FILE)) {
            const raw = fs.readFileSync(INQUIRIES_FILE, 'utf8');
            return JSON.parse(raw);
        }
    } catch (e) {
        console.error('Error reading inquiries file:', e);
    }
    return [];
}

function saveInquiries(inquiriesData) {
    try {
        fs.writeFileSync(INQUIRIES_FILE, JSON.stringify(inquiriesData, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('Error saving inquiries file:', e);
        return false;
    }
}

async function saveInquiryToDb(inquiry) {
    if (infrastructureReady) {
        try {
            await docClient.put({
                TableName: INQUIRIES_TABLE_NAME,
                Item: inquiry
            }).promise();
        } catch (dbErr) {
            console.warn('[DynamoDB Inquiry Put Error]:', dbErr.message);
        }
    }
}

// -------------------------------------------------------------
// INQUIRIES REST API
// -------------------------------------------------------------

// GET all booking inquiries
app.get('/api/inquiries', async (req, res) => {
    try {
        let items = [];
        if (infrastructureReady) {
            try {
                const dbRes = await docClient.scan({ TableName: INQUIRIES_TABLE_NAME }).promise();
                if (dbRes.Items && dbRes.Items.length > 0) {
                    items = dbRes.Items;
                }
            } catch (err) {
                console.warn('[DynamoDB Inquiries Scan Warning]:', err.message);
            }
        }
        if (items.length === 0) {
            items = getInquiries();
        }
        items.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));
        res.json({
            success: true,
            count: items.length,
            data: items
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to retrieve inquiries', error: err.message });
    }
});

// DELETE an inquiry by ID
app.delete('/api/inquiries/:id', async (req, res) => {
    try {
        const id = req.params.id;
        const currentInquiries = getInquiries();
        const filtered = currentInquiries.filter(inq => inq.id !== id);
        saveInquiries(filtered);

        if (infrastructureReady) {
            try {
                await docClient.delete({
                    TableName: INQUIRIES_TABLE_NAME,
                    Key: { id }
                }).promise();
            } catch (dbErr) {
                console.warn('[DynamoDB Inquiry Delete Error]:', dbErr.message);
            }
        }

        res.json({ success: true, message: 'Inquiry removed successfully' });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to delete inquiry', error: err.message });
    }
});

// Contact Form Email & Booking Endpoint (Non-blocking with local + cloud persistence)
app.post('/api/contact', async (req, res) => {
    try {
        const { fullName, email, phone, eventType, services, message } = req.body;

        if (!fullName || !email) {
            return res.status(400).json({ success: false, message: 'Full name and email are required.' });
        }

        const selectedServices = Array.isArray(services) && services.length > 0
            ? services.join(', ')
            : 'None specified';

        // 1. Create and persist inquiry immediately
        const newInquiry = {
            id: `inq_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`,
            fullName: fullName.trim(),
            email: email.trim(),
            phone: phone ? phone.trim() : '',
            eventType: eventType || 'Private Party',
            services: Array.isArray(services) ? services : [],
            message: message ? message.trim() : '',
            createdAt: new Date().toISOString(),
            status: 'new'
        };

        const currentInquiries = getInquiries();
        currentInquiries.unshift(newInquiry);
        saveInquiries(currentInquiries);
        await saveInquiryToDb(newInquiry);

        console.log(`📥 [Inquiry Saved] ID: ${newInquiry.id} from ${newInquiry.fullName} (${newInquiry.email})`);

        // 2. Dispatch Email asynchronously with a 4.5 second timeout to prevent hanging
        const mailOptions = {
            from: `"Soundscape Inquiries" <${process.env.EMAIL_USER || 'manojfa4451e@gmail.com'}>`,
            to: process.env.EMAIL_USER || 'manojfa4451e@gmail.com',
            replyTo: email,
            subject: `🎧 New Booking Request: ${eventType || 'Event'} from ${fullName}`,
            html: `
                <div style="font-family: Arial, sans-serif; background-color: #141010; color: #FAF6F6; padding: 24px; border-radius: 12px; max-width: 600px; margin: auto; border: 1px solid #C3195D;">
                    <h2 style="color: #F70776; border-bottom: 2px solid #C3195D; padding-bottom: 8px; margin-top: 0;">
                        🎧 Soundscape Booking Request
                    </h2>
                    <p style="font-size: 15px; color: #FAF6F6;">You received a new booking inquiry through the Soundscape website:</p>
                    
                    <table style="width: 100%; border-collapse: collapse; margin-top: 16px; color: #FAF6F6; font-size: 14px;">
                        <tr style="background-color: #1C1717;">
                            <td style="padding: 10px; font-weight: bold; width: 35%; color: #A69B9B;">Client Name:</td>
                            <td style="padding: 10px; color: #FFFFFF; font-weight: bold;">${fullName}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; font-weight: bold; color: #A69B9B;">Email Address:</td>
                            <td style="padding: 10px;"><a href="mailto:${email}" style="color: #F70776; text-decoration: none;">${email}</a></td>
                        </tr>
                        <tr style="background-color: #1C1717;">
                            <td style="padding: 10px; font-weight: bold; color: #A69B9B;">Phone Number:</td>
                            <td style="padding: 10px; color: #FFFFFF;">${phone || 'Not provided'}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; font-weight: bold; color: #A69B9B;">Event Type:</td>
                            <td style="padding: 10px; color: #FFFFFF;">${eventType || 'General Inquiry'}</td>
                        </tr>
                        <tr style="background-color: #1C1717;">
                            <td style="padding: 10px; font-weight: bold; color: #A69B9B;">Required Services:</td>
                            <td style="padding: 10px; color: #F70776; font-weight: bold;">${selectedServices}</td>
                        </tr>
                    </table>

                    <div style="margin-top: 20px; background-color: #1C1717; padding: 16px; border-radius: 8px; border-left: 4px solid #F70776;">
                        <h4 style="margin: 0 0 8px 0; color: #A69B9B; font-size: 13px; text-transform: uppercase;">Message / Event Details:</h4>
                        <p style="margin: 0; color: #FAF6F6; line-height: 1.5; font-size: 14px;">${message ? message.replace(/\n/g, '<br/>') : 'No additional message provided.'}</p>
                    </div>

                    <p style="font-size: 12px; color: #A69B9B; margin-top: 24px; border-top: 1px solid #2B2323; padding-top: 12px;">
                        This email was sent automatically from the Soundscape Web Application. Click reply to respond directly to <strong>${fullName}</strong> (<a href="mailto:${email}" style="color: #F70776;">${email}</a>).
                    </p>
                </div>
            `
        };

        // 2. Dispatch Email in background so response returns in < 200ms
        transporter.sendMail(mailOptions)
            .then(info => console.log(`✅ [Nodemailer] Booking email sent from ${email} (Message ID: ${info.messageId})`))
            .catch(mailErr => console.warn(`⚠️ [Nodemailer Notification]: ${mailErr.message}. (Inquiry saved to database/disk successfully).`));

        // Return immediate success to user
        return res.json({
            success: true,
            message: 'Thank you! Your booking request has been delivered to our team.',
            inquiryId: newInquiry.id
        });
    } catch (err) {
        console.error('❌ Error handling contact request:', err);
        res.status(500).json({
            success: false,
            message: 'Failed to record booking request. Please try again.',
            error: err.message
        });
    }
});

// Start Server and verify infrastructure
app.listen(PORT, async () => {
    console.log(`=============================================`);
    console.log(`🎧 Soundscape Server running on port ${PORT}`);
    console.log(`🌐 Health check: http://localhost:${PORT}/api/health`);
    console.log(`📁 Media API:    http://localhost:${PORT}/api/media`);
    console.log(`=============================================`);
    await initAWSInfrastructure();
});
