require('dotenv').config();
const express = require('express');
const path = require('path');
const http = require('http');
const fs = require('fs');

// Keep original setup logic
const helmet = require('helmet');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

// ==================== SAFE LOADER HELPERS ====================
function safeRequire(label, filePath) {
    try {
        const mod = require(filePath);
        console.log(`✅ Loaded ${label}: ${filePath}`);
        return mod;
    } catch (err) {
        console.error(`❌ Failed to load ${label}: ${filePath}`);
        console.error(err.stack || err);
        return null;
    }
}

function runJob(fileName, jobFn) {
    if (typeof jobFn !== 'function') {
        console.warn(`⚠️ Job file "${fileName}" does not export a function — skipped.`);
        return;
    }
    try {
        jobFn();
        console.log(`✅ Job ran: ${fileName}`);
    } catch (err) {
        console.error(`❌ Job failed: ${fileName}`);
        console.error(err.stack || err);
    }
}

// ==================== ENVIRONMENT VALIDATION ====================
const requiredEnvVars = [
    'OSU_CLIENT_ID', 'OSU_CLIENT_SECRET', 
    'DATABASE_URL', 'REDIS_URL', 'JWT_SECRET'
];
requiredEnvVars.forEach(envVar => {
    if (!process.env[envVar]) {
        console.error(`❌ Missing required environment variable: ${envVar}`);
        process.exit(1);
    }
});

// ==================== APP INITIALIZATION ====================
const app = express();
const PORT = process.env.PORT || 3000;

// Trust proxy & security headers
app.set('trust proxy', 1);
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ==================== RATE LIMITING ====================
const createRateLimit = (windowMs, max, message) => rateLimit({
    windowMs,
    max,
    message: { success: false, error: message },
    standardHeaders: true,
    legacyHeaders: false,
});
app.use('/api/', createRateLimit(15 * 60 * 1000, 100, 'Too many API requests'));
app.use('/api/admin/', createRateLimit(15 * 60 * 1000, 20, 'Too many admin requests'));

// ==================== LOAD MIDDLEWARES ====================
[
    'security',
    'requestLogging',
    'rateLimiting'
].forEach(mw => {
    const mod = safeRequire(`middleware ${mw}`, `./middleware/${mw}`);
    const fn = (typeof mod === 'function') ? mod : 
               (mod && typeof mod.default === 'function' ? mod.default : null);

    if (fn) {
        app.use(fn);
    } else {
        console.warn(`⚠️ Middleware ${mw} is not a function, skipping...`);
    }
});

// ==================== LOAD ROUTES ====================
fs.readdirSync(path.join(__dirname, 'routes')).forEach(file => {
    if (file.endsWith('.js')) {
        const routePath = `/api/${file.replace('.js', '')}`;
        const route = safeRequire(`route ${file}`, `./routes/${file}`);
        const fn = (typeof route === 'function') ? route :
                   (route && typeof route.default === 'function' ? route.default : null);

        if (fn) {
            app.use(routePath, fn);
        } else {
            console.warn(`⚠️ Route ${file} is not a valid middleware/router, skipping...`);
        }
    }
});

// Health check route always present
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Serve public files
app.use(express.static(path.join(__dirname, 'public')));

// Not found + error handler middlewares
const notFound = safeRequire('notFound middleware', './middleware/notFound');
if (notFound) app.use(notFound);
const errorHandler = safeRequire('errorHandler middleware', './middleware/errorHandler');
if (errorHandler) app.use(errorHandler);

// ==================== START JOBS ====================
console.log('\n🔄 Starting jobs...\n');
fs.readdirSync(path.join(__dirname, 'jobs')).forEach(file => {
    if (file.endsWith('.js') && !file.startsWith('schedulingUtils')) {
        const jobModule = safeRequire(`job ${file}`, `./jobs/${file}`);
        if (jobModule) runJob(file, jobModule);
    }
});

// ==================== START SERVER ====================
const server = http.createServer(app);
server.listen(PORT, () => {
    console.log(`\n🚀 Server running on port ${PORT}`);
});

// ==================== WEBSOCKET ====================
const websocket = safeRequire('websocket middleware', './middleware/websocket');
if (websocket) {
    try {
        websocket(server);
    } catch (err) {
        console.error('❌ WebSocket initialization failed:', err.stack || err);
    }
}
