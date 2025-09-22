#!/usr/bin/env node
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { MySQLToSQLiteMigrator, parseMySQLUrl } = require('./mysql-to-sqlite');

const MIGRATION_FILE = './temp/migration.sql';

const CF_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CF_DATABASE_ID = process.env.D1_DATABASE_ID;
const CF_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;
const CF_RAW_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database/${CF_DATABASE_ID}/raw`;

async function executeD1RawSQL(sql) {
    const res = await axios.post(CF_RAW_URL, { sql }, {
        headers: { 
            'Authorization': `Bearer ${CF_API_TOKEN}`,
            'Content-Type': 'application/json'
        },
        maxBodyLength: Infinity
    });
    if (!res.data.success) throw new Error(JSON.stringify(res.data));
    return res.data.result;
}

// Parse SQL statements from the migration file
function parseSQLFile(filePath) {
    if (!fs.existsSync(filePath)) {
        throw new Error(`Migration file not found: ${filePath}`);
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n');
    
    const statements = [];
    let currentStatement = '';
    let inMultiLineStatement = false;
    
    for (const line of lines) {
        const trimmedLine = line.trim();
        
        // Skip empty lines and comments
        if (!trimmedLine || trimmedLine.startsWith('--') || trimmedLine.startsWith('PRAGMA')) {
            continue;
        }
        
        currentStatement += line + '\n';
        
        // Check if this line ends a statement
        if (trimmedLine.endsWith(';')) {
            // Handle multi-line INSERT statements properly
            if (currentStatement.trim()) {
                statements.push(currentStatement.trim());
            }
            currentStatement = '';
            inMultiLineStatement = false;
        } else if (trimmedLine.includes('CREATE TABLE') || trimmedLine.includes('INSERT INTO')) {
            inMultiLineStatement = true;
        }
    }
    
    // Add any remaining statement
    if (currentStatement.trim()) {
        statements.push(currentStatement.trim());
    }
    
    return statements;
}

// Separate CREATE TABLE and INSERT statements
function categorizeStatements(statements) {
    const schemas = [];
    const inserts = [];
    
    for (const stmt of statements) {
        const trimmedStmt = stmt.trim().toUpperCase();
        if (trimmedStmt.startsWith('CREATE TABLE')) {
            schemas.push(stmt);
        } else if (trimmedStmt.startsWith('INSERT INTO')) {
            inserts.push(stmt);
        }
    }
    
    return { schemas, inserts };
}

// Batch statements for D1 API limits
function createBatches(statements, batchSize = 200) {
    const batches = [];
    
    for (let i = 0; i < statements.length; i += batchSize) {
        const batch = statements.slice(i, i + batchSize);
        batches.push(batch.join('\n'));
    }
    
    return batches;
}

// Extract table name from CREATE TABLE or INSERT statement
function extractTableName(statement) {
    const createMatch = statement.match(/CREATE TABLE (?:IF NOT EXISTS )?(?:"([^"]+)"|(\w+))/i);
    if (createMatch) {
        return createMatch[1] || createMatch[2];
    }
    
    const insertMatch = statement.match(/INSERT INTO (?:"([^"]+)"|(\w+))/i);
    if (insertMatch) {
        return insertMatch[1] || insertMatch[2];
    }
    
    return null;
}

// Get unique table names from statements
function getTableNames(statements) {
    const tableNames = new Set();
    
    for (const stmt of statements) {
        const tableName = extractTableName(stmt);
        if (tableName) {
            tableNames.add(tableName);
        }
    }
    
    return Array.from(tableNames);
}

async function generateMigrationFile() {
    console.log('🔧 Generating SQLite migration file from MySQL...');
    
    if (!process.env.MYSQL_URL) {
        throw new Error('MYSQL_URL environment variable is required to generate migration file');
    }
    
    const mysqlConfig = parseMySQLUrl(process.env.MYSQL_URL);
    const migrator = new MySQLToSQLiteMigrator(mysqlConfig);
    
    try {
        await migrator.connectToMySQL();
        const statementCount = await migrator.generateSQLFile();
        console.log(`✅ Generated migration file with ${statementCount} statements`);
        
        // Validate the generated SQL
        const isValid = await migrator.validateGeneratedSQL();
        if (!isValid) {
            console.warn('⚠️ Migration file validation found issues - proceeding with caution');
        }
        
        return statementCount;
    } finally {
        await migrator.disconnect();
    }
}

async function getAllD1Tables() {
    console.log('🔍 Discovering existing D1 tables...');
    try {
        const result = await executeD1RawSQL(`
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%' 
            ORDER BY name;
        `);
        
        const tables = result[0].results.rows.map(row => row[0]);
        console.log(`   • Found ${tables.length} existing tables: ${tables.join(', ') || 'none'}`);
        return tables;
    } catch (error) {
        console.warn('   ⚠️ Could not get table list (database might be empty)');
        return [];
    }
}

async function cleanD1Database() {
    console.log('🧹 Cleaning D1 database...');
    
    // Disable foreign key constraints
    console.log('   • Disabling foreign key constraints...');
    await executeD1RawSQL('PRAGMA foreign_keys = OFF;');
    
    // Get all existing tables
    const existingTables = await getAllD1Tables();
    
    if (existingTables.length === 0) {
        console.log('   • Database is already clean');
        return;
    }
    
    // Drop all existing tables except _cf_KV
    const tablesToDrop = existingTables.filter(table => table !== '_cf_KV');
    console.log(`   • Dropping ${tablesToDrop.length} existing tables (skipping _cf_KV)...`);
    
    for (const table of tablesToDrop) {
        try {
            await executeD1RawSQL(`DROP TABLE IF EXISTS "${table}";`);
            console.log(`     ✓ Dropped table: ${table}`);
        } catch (error) {
            console.warn(`     ⚠️ Could not drop table ${table}: ${error.message}`);
        }
    }
    
    // Verify database is clean (excluding _cf_KV)
    const remainingTables = await getAllD1Tables().then(tables => tables.filter(t => t !== '_cf_KV'));
    if (remainingTables.length > 0) {
        console.warn(`   ⚠️ Warning: ${remainingTables.length} tables still exist: ${remainingTables.join(', ')}`);
    } else {
        console.log('   ✅ Database successfully cleaned (except _cf_KV)');
    }
}

async function migrateSQLiteToD1() {
    console.log('🚀 Starting SQLite file → D1 migration...');
    
    // Check if migration file exists, if not generate it
    if (!fs.existsSync(MIGRATION_FILE)) {
        console.log('📄 Migration file not found, generating it...');
        await generateMigrationFile();
    }
    
    // Parse the migration file
    console.log('📖 Reading migration file...');
    const allStatements = parseSQLFile(MIGRATION_FILE);
    const { schemas, inserts } = categorizeStatements(allStatements);
    
    console.log(`Found ${schemas.length} CREATE TABLE statements`);
    console.log(`Found ${inserts.length} INSERT statements`);
    
    const tableNames = getTableNames([...schemas, ...inserts]);
    console.log(`📋 Tables to migrate: ${tableNames.join(', ')}`);
    
    try {
        // Step 1: Clean D1 database completely
        await cleanD1Database();
        
        // Step 2: Set up D1 for migration
        console.log('\n⚙️ Configuring D1 for migration...');
        await executeD1RawSQL('PRAGMA foreign_keys = OFF;');
        // await executeD1RawSQL('PRAGMA journal_mode = WAL;');
        // await executeD1RawSQL('PRAGMA synchronous = NORMAL;');
        console.log('   ✅ D1 configured for migration');
        
        // Step 3: Create all table schemas
        console.log('\n🏗️ Creating table schemas...');
        for (let i = 0; i < schemas.length; i++) {
            const schema = schemas[i];
            const tableName = extractTableName(schema);
            
            console.log(`   • Creating table: ${tableName || `schema-${i + 1}`}`);
            await executeD1RawSQL(schema);
        }
        
        // Step 4: Insert data in batches
        if (inserts.length > 0) {
            console.log('\n📊 Inserting data...');
            const insertBatches = createBatches(inserts, 200);
            
            console.log(`   • Processing ${inserts.length} INSERT statements in ${insertBatches.length} batches`);
            
            for (let i = 0; i < insertBatches.length; i++) {
                console.log(`   • Executing batch ${i + 1}/${insertBatches.length}...`);
                await executeD1RawSQL(insertBatches[i]);
            }
        } else {
            console.log('\n📊 No data to insert (schema-only migration)');
        }
        
        // Step 5: Re-enable foreign keys after migration
        console.log('\n🔗 Re-enabling foreign key constraints...');
        await executeD1RawSQL('PRAGMA foreign_keys = ON;');
        console.log('   ✅ Foreign keys re-enabled');
        
        console.log('\n✅ Migration completed successfully!');
        
        // Step 6: Verify migration
        if (process.env.MYSQL_URL) {
            console.log('\n🔍 Verifying migration...');
            await verifyMigration(tableNames);
        }
        
    } catch (error) {
        console.error('💥 Migration failed:', error.message);
        if (error.response && error.response.data) {
            console.error('D1 API Error:', JSON.stringify(error.response.data, null, 2));
        }
        throw error;
    }
}

async function verifyMigration(tableNames) {
    console.log('🔍 Verifying migration results...');
    
    // First verify D1 tables were created
    const d1Tables = await getAllD1Tables();
    const expectedTables = tableNames.filter(name => name); // Remove any null/undefined names
    
    console.log(`   • Expected tables: ${expectedTables.length}`);
    console.log(`   • Created tables: ${d1Tables.length}`);
    
    const missingTables = expectedTables.filter(table => !d1Tables.includes(table));
    const extraTables = d1Tables.filter(table => !expectedTables.includes(table));
    
    if (missingTables.length > 0) {
        console.warn(`   ⚠️ Missing tables: ${missingTables.join(', ')}`);
    }
    if (extraTables.length > 0) {
        console.warn(`   ⚠️ Unexpected tables: ${extraTables.join(', ')}`);
    }
    
    if (!process.env.MYSQL_URL) {
        console.log('   ℹ️ Skipping row count verification (no MySQL connection)');
        return;
    }
    
    const mysqlConfig = parseMySQLUrl(process.env.MYSQL_URL);
    const migrator = new MySQLToSQLiteMigrator(mysqlConfig);
    
    try {
        await migrator.connectToMySQL();
        
        console.log('   📊 Comparing row counts...');
        let totalMysqlRows = 0;
        let totalD1Rows = 0;
        let matches = 0;
        let mismatches = 0;
        
        for (const tableName of expectedTables) {
            try {
                // Get MySQL count
                const [mysqlResult] = await migrator.mysqlConnection.execute(
                    `SELECT COUNT(*) as count FROM \`${tableName}\``
                );
                const mysqlCount = mysqlResult[0].count;
                
                // Get D1 count
                const d1Result = await executeD1RawSQL(`SELECT COUNT(*) FROM "${tableName}";`);
                const d1Count = d1Result[0].results.rows[0][0];
                
                totalMysqlRows += mysqlCount;
                totalD1Rows += d1Count;
                
                if (mysqlCount === d1Count) {
                    matches++;
                    console.log(`     ✅ ${tableName}: ${mysqlCount} rows`);
                } else {
                    mismatches++;
                    console.log(`     ❌ ${tableName}: MySQL=${mysqlCount}, D1=${d1Count}`);
                }
                
            } catch (tableError) {
                console.warn(`     ⚠️ Could not verify ${tableName}: ${tableError.message}`);
            }
        }
        
        console.log(`\n   📈 Summary:`);
        console.log(`     • Total MySQL rows: ${totalMysqlRows}`);
        console.log(`     • Total D1 rows: ${totalD1Rows}`);
        console.log(`     • Matching tables: ${matches}`);
        console.log(`     • Mismatched tables: ${mismatches}`);
        
        if (mismatches === 0 && matches > 0) {
            console.log(`     🎉 Perfect migration! All ${matches} tables match.`);
        } else if (mismatches > 0) {
            console.log(`     ⚠️ ${mismatches} tables have row count mismatches.`);
        }
        
    } catch (error) {
        console.warn('⚠️ Could not verify counts:', error.message);
    } finally {
        await migrator.disconnect();
    }
}

async function main() {
    // Validate environment variables
    if (!CF_ACCOUNT_ID || !CF_DATABASE_ID || !CF_API_TOKEN) {
        throw new Error('Missing required D1 environment variables: CLOUDFLARE_ACCOUNT_ID, D1_DATABASE_ID, CLOUDFLARE_API_TOKEN');
    }
    
    try {
        await migrateSQLiteToD1();
    } catch (error) {
        console.error('💥 Migration process failed:', error.message);
        process.exit(1);
    }
}

if (require.main === module) main();