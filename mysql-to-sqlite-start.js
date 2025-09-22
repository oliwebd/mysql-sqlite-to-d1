require('dotenv').config();
const { MySQLToSQLiteMigrator, parseMySQLUrl } = require('./mysql-to-sqlite');
const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const TEMP_DIR = './temp';
const MIGRATION_FILE = path.join(TEMP_DIR, 'migration.sql');
const SQLITE_DB_FILE = 'localsqlite.db';

// Function to remove the SQLite database file if it exists
function removeSQLiteDBFile() {
    if (fs.existsSync(SQLITE_DB_FILE)) {
        fs.unlinkSync(SQLITE_DB_FILE);
        console.log(`🗑️ Removed existing SQLite database file: ${SQLITE_DB_FILE}`);
    }
}

// Function to create temp directory if it doesn't exist
function ensureTempDirectory() {
    if (!fs.existsSync(TEMP_DIR)) {
        fs.mkdirSync(TEMP_DIR, { recursive: true });
        console.log('📁 Created temp directory.');
    }
}

async function main() {
    if (!process.env.MYSQL_URL) {
        throw new Error('Missing environment variable: MYSQL_URL');
    }

    console.log('🚀 Starting MySQL to SQLite migration...');

    const mysqlConfig = parseMySQLUrl(process.env.MYSQL_URL);
    const migrator = new MySQLToSQLiteMigrator(mysqlConfig);

    try {
        // Step 1: Generate migration file
        console.log('📡 Step 1: Connecting to MySQL and generating SQL file...');
        await migrator.connectToMySQL();
        const statementCount = await migrator.generateSQLFile(MIGRATION_FILE);

        if (statementCount === 0) {
            throw new Error('No SQL statements were generated. Check if your MySQL database has tables and data.');
        }

        console.log(`✅ Generated ${statementCount} SQL statements in ${MIGRATION_FILE}`);

        // Step 2: Run migration on SQLite
        console.log(`\n🧪 Step 2: Running migration on ${SQLITE_DB_FILE} for testing...`);
        const db = new Database(SQLITE_DB_FILE);

        db.exec('PRAGMA foreign_keys = OFF;');
        const sql = fs.readFileSync(MIGRATION_FILE, 'utf8');
        db.exec(sql);
        db.exec('PRAGMA foreign_keys = ON;');

        // Optional: validate constraints
        try {
            db.exec('PRAGMA foreign_key_check;');
        } catch (fkErr) {
            console.warn('⚠️ Foreign key issues detected:', fkErr.message);
        }

        db.close();
        console.log(`✅ Migration to ${SQLITE_DB_FILE} was successful. No syntax errors found.`);

        // Step 2.5: Analyze DB structure
        try {
    const analyzeDb = new Database(SQLITE_DB_FILE);
    const tables = analyzeDb.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();

    console.log(`📊 Created ${tables.length} tables in the database`);

    for (const { name } of tables) {
        try {
            const { count } = analyzeDb.prepare(`SELECT COUNT(*) as count FROM "${name}"`).get();
            console.log(`   • ${name}: ${count} rows`);
        } catch (tableErr) {
            console.warn(`   ⚠️ Could not count rows in table "${name}": ${tableErr.message}`);
        }
    }

    analyzeDb.close();
} catch (e) {
    console.log('⚠️ Could not analyze database structure:', e.message);
}


        // Step 3: Summary
        console.log('\n📋 MIGRATION COMPLETE - SUMMARY:');
        console.log('══════════════════════════════════');
        console.log(`1. 📁 File generated: ${MIGRATION_FILE}`);
        console.log(`2. 🧪 Test database created: ${SQLITE_DB_FILE}`);
        console.log('3. 👍 All SQL statements passed without syntax errors on SQLite.');

    } catch (error) {
        console.error('❌ Migration failed:', error.message);
        if (process.env.DEBUG) console.error(error.stack);
        console.log('\n🔧 TROUBLESHOOTING:');
        console.log('════════════════════');
        console.log('1. Check your MYSQL_URL environment variable.');
        console.log('2. Ensure `mysql-to-sqlite` and `better-sqlite3` libraries are installed.');
        console.log('3. Review the error message for specific syntax issues.');
        throw error;
    } finally {
        if (migrator && typeof migrator.disconnect === 'function') {
            await migrator.disconnect();
            console.log('🔌 MySQL connection closed.');
        }
    }
}

// Run the script
if (require.main === module) {
    ensureTempDirectory();
    removeSQLiteDBFile();
    main().catch(error => {
        console.error('💥 Script execution failed:', error.message);
        process.exit(1);
    });
}
