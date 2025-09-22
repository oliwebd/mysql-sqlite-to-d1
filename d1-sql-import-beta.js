#!/usr/bin/env node
/**
 * Full Migration Script: MySQL → SQLite → Cloudflare D1 (Official Import API)
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Database = require('better-sqlite3');
const axios = require('axios');
const { MySQLToSQLiteMigrator, parseMySQLUrl } = require('./mysql-to-sqlite');

const TEMP_DIR = './temp';
const MIGRATION_FILE = path.join(TEMP_DIR, 'migration.sql');
const SQLITE_DB_FILE = 'localsqlite.db';

// Cloudflare D1 env vars
const CF_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CF_DATABASE_ID = process.env.D1_DATABASE_ID;
const CF_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;
const CF_IMPORT_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database/${CF_DATABASE_ID}/import`;

// --- Helpers ---
function ensureTempDirectory() {
    if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });
}

function removeSQLiteDBFile() {
    if (fs.existsSync(SQLITE_DB_FILE)) fs.unlinkSync(SQLITE_DB_FILE);
}

function getSQLiteTableCounts(sqliteFile) {
    const db = new Database(sqliteFile);
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
    const counts = {};
    for (const { name } of tables) {
        try {
            const { count } = db.prepare(`SELECT COUNT(*) as count FROM "${name}"`).get();
            counts[name] = count;
        } catch {
            counts[name] = null;
        }
    }
    db.close();
    return counts;
}


// --- D1 Official Import API ---
async function initImport(etag) {
    const res = await axios.post(CF_IMPORT_URL, { action: 'init', etag }, {
        headers: { Authorization: `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' }
    });
    if (!res.data.success) throw new Error(`Init failed: ${JSON.stringify(res.data)}`);
    return res.data.result;
}

async function ingestImport(etag, filename) {
    const res = await axios.post(CF_IMPORT_URL, { action: 'ingest', etag, filename }, {
        headers: { Authorization: `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' }
    });
    if (!res.data.success) throw new Error(`Ingest failed: ${JSON.stringify(res.data)}`);
    return res.data.result;
}

async function pollImport(current_bookmark) {
    while (true) {
        const res = await axios.post(CF_IMPORT_URL, { action: 'poll', current_bookmark }, {
            headers: { Authorization: `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' }
        });
        if (!res.data.success) throw new Error(`Poll failed: ${JSON.stringify(res.data)}`);

        const status = res.data.result.status || res.data.result.state || 'pending';
        console.log(`⏳ Import status: ${status}`);
        if (status === 'completed') break;
        if (status === 'failed') {
            console.error('❌ Import failed:', res.data.result.error || res.data.errors);
            process.exit(1);
        }
        await new Promise(r => setTimeout(r, 5000));
    }
    console.log('✅ D1 Import completed successfully!');
}

// --- D1 Row Count Verification ---
async function verifyD1RowCounts(tableCounts) {
    const CF_REST_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database/${CF_DATABASE_ID}/query`;
    console.log('\n🔍 Verifying row counts in D1...');
    for (const [table, expectedCount] of Object.entries(tableCounts)) {
        try {
            const res = await axios.post(CF_REST_URL, { query: `SELECT COUNT(*) AS count FROM "${table}";` }, {
                headers: { Authorization: `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' }
            });
            const d1Count = res.data.result[0]?.count ?? null;
            const status = d1Count === expectedCount ? '✅ MATCH' : '❌ MISMATCH';
            console.log(`   • ${table}: MySQL/SQLite=${expectedCount}, D1=${d1Count} → ${status}`);
        } catch (err) {
            console.warn(`   ⚠️ Could not verify table "${table}": ${err.message}`);
        }
    }
}

// --- Main ---
async function main() {
    if (!process.env.MYSQL_URL) throw new Error('Missing MYSQL_URL environment variable');
    if (!CF_ACCOUNT_ID || !CF_DATABASE_ID || !CF_API_TOKEN) throw new Error('Missing D1 environment variables');

    ensureTempDirectory();
    removeSQLiteDBFile();

    console.log('🚀 Starting MySQL → SQLite migration...');
    const mysqlConfig = parseMySQLUrl(process.env.MYSQL_URL);
    const migrator = new MySQLToSQLiteMigrator(mysqlConfig);

    try {
        await migrator.connectToMySQL();
        const statementCount = await migrator.generateSQLFile(MIGRATION_FILE);
        if (statementCount === 0) throw new Error('No SQL statements generated.');
        console.log(`✅ Generated ${statementCount} SQL statements in ${MIGRATION_FILE}`);

        console.log(`🧪 Creating local SQLite database: ${SQLITE_DB_FILE}`);
        const db = new Database(SQLITE_DB_FILE);
        db.exec('PRAGMA foreign_keys = OFF;');
        const sql = fs.readFileSync(MIGRATION_FILE, 'utf8');
        db.exec(sql);
        db.exec('PRAGMA foreign_keys = ON;');
        db.close();

        console.log('📊 Analyzing SQLite tables & row counts...');
        const sqliteCounts = getSQLiteTableCounts(SQLITE_DB_FILE);
        for (const [table, count] of Object.entries(sqliteCounts)) {
            console.log(`   • ${table}: ${count} rows`);
        }


        // --- D1 Official Import ---
        console.log('🚀 Starting D1 official import...');
        const sqlBuffer = fs.readFileSync(MIGRATION_FILE);
        const md5 = crypto.createHash('md5').update(sqlBuffer).digest('hex');

        console.log('📡 Init import...');
        const initResult = await initImport(md5);
        const uploadUrl = initResult.upload_url || initResult.url;
        const currentBookmark = initResult.at_bookmark;

        console.log('📤 Uploading SQL file...');
        const fileSize = sqlBuffer.length;

await axios.put(uploadUrl, sqlBuffer, {
    headers: {
        'Content-Type': 'application/sql',
        'Content-Length': fileSize
    },
    maxBodyLength: Infinity
});

        console.log('📥 Ingest import...');
        const ingestResult = await ingestImport(md5, path.basename(MIGRATION_FILE));
        const ingestBookmark = ingestResult.at_bookmark || currentBookmark;

        console.log('🔍 Polling import status...');
        await pollImport(ingestBookmark);

        // --- Verify D1 counts ---
        await verifyD1RowCounts(mysqlCounts);

    } catch (err) {
        console.error('💥 Migration failed:', err.message);
        if (process.env.DEBUG) console.error(err.stack);
        process.exit(1);
    } finally {
        if (migrator && typeof migrator.disconnect === 'function') {
            await migrator.disconnect();
            console.log('🔌 MySQL connection closed.');
        }
    }
}

// Run
if (require.main === module) main();
