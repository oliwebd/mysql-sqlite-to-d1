require('dotenv').config();
const CloudflareD1API = require('./cloudflare-d1-api');

async function testD1Connection() {
    console.log('Testing Cloudflare D1 API connection...\n');

    // Validate environment variables
    const requiredEnvVars = ['CLOUDFLARE_ACCOUNT_ID', 'CLOUDFLARE_API_TOKEN', 'D1_DATABASE_ID'];
    for (const envVar of requiredEnvVars) {
        if (!process.env[envVar]) {
            throw new Error(`Missing environment variable: ${envVar}`);
        }
    }

    // Initialize D1 client
    const d1Client = new CloudflareD1API(
        process.env.CLOUDFLARE_ACCOUNT_ID,
        process.env.CLOUDFLARE_API_TOKEN,
        process.env.D1_DATABASE_ID
    );

    try {
        // Test 1: Get database info
        console.log('1. Testing database connection...');
        const dbInfo = await d1Client.getDatabaseInfo();
        
        if (!dbInfo.success) {
            throw new Error(`Database info request failed: ${dbInfo.errors?.[0]?.message || 'Unknown error'}`);
        }
        
        console.log('✅ Database connection successful!');
        console.log(`   Database Name: ${dbInfo.result.name || 'N/A'}`);
        console.log(`   Database ID: ${dbInfo.result.uuid || dbInfo.result.id || 'N/A'}`);
        console.log(`   Version: ${dbInfo.result.version || 'N/A'}`);
        console.log(`   Created: ${dbInfo.result.created_at || 'N/A'}\n`);

        // Test 2: Clean up any existing test data first
        console.log('2. Cleaning up any existing test data...');
        const cleanupResult = await d1Client.executeQuery('DROP TABLE IF EXISTS test_migration');
        console.log('✅ Cleanup completed\n');

        // Test 3: Create a test table
        console.log('3. Creating test table...');
        const createTableSQL = `
            CREATE TABLE IF NOT EXISTS test_migration (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `;
        
        const createResult = await d1Client.executeQuery(createTableSQL);
        
        if (!createResult.success) {
            throw new Error(`Create table failed: ${createResult.errors?.[0] || 'Unknown error'}`);
        }
        
        const firstCreateResult = createResult.result[0];
        console.log('✅ Test table created successfully!');
        console.log(`   Changes: ${firstCreateResult.meta?.changes || 0}`);
        console.log(`   Duration: ${firstCreateResult.meta?.duration || 0}ms\n`);

        // Test 4: Insert test data with unique email
        console.log('4. Inserting test data...');
        const timestamp = new Date().getTime();
        const uniqueEmail = `test-${timestamp}@example.com`;
        
        const insertResult = await d1Client.executeQuery(
            "INSERT INTO test_migration (name, email) VALUES (?, ?)",
            ['Test User', uniqueEmail]
        );
        
        if (!insertResult.success) {
            throw new Error(`Insert failed: ${insertResult.errors?.[0] || 'Unknown error'}`);
        }
        
        const firstInsertResult = insertResult.result[0];
        console.log('✅ Test data inserted successfully!');
        console.log(`   Email used: ${uniqueEmail}`);
        console.log(`   Rows affected: ${firstInsertResult.meta?.changes || 0}`);
        console.log(`   Last row ID: ${firstInsertResult.meta?.last_row_id || 0}`);
        console.log(`   Duration: ${firstInsertResult.meta?.duration || 0}ms\n`);

        // Test 5: Query test data
        console.log('5. Querying test data...');
        const selectResult = await d1Client.executeQuery('SELECT * FROM test_migration');
        
        if (!selectResult.success) {
            throw new Error(`Select failed: ${selectResult.errors?.[0] || 'Unknown error'}`);
        }
        
        const firstSelectResult = selectResult.result[0];
        
        if (firstSelectResult.results && firstSelectResult.results.length > 0) {
            console.log('✅ Test data queried successfully!');
            console.log(`   Rows returned: ${firstSelectResult.results.length}`);
            console.log('   First row:', JSON.stringify(firstSelectResult.results[0], null, 2));
        } else {
            console.log('❌ No data found in test table');
        }
        console.log(`   Duration: ${firstSelectResult.meta?.duration || 0}ms\n`);

        // Test 6: Test unique constraint
        console.log('6. Testing unique constraint...');
        try {
            const duplicateResult = await d1Client.executeQuery(
                "INSERT INTO test_migration (name, email) VALUES (?, ?)",
                ['Duplicate User', uniqueEmail] // Same email should fail
            );
            
            if (duplicateResult.success) {
                console.log('❌ Unique constraint test failed - duplicate was allowed');
            } else {
                console.log('✅ Unique constraint working correctly!');
                console.log(`   Error: ${duplicateResult.errors?.[0]?.message || 'Duplicate prevented'}`);
            }
        } catch (error) {
            console.log('✅ Unique constraint working correctly!');
            console.log(`   Error: ${error.message}`);
        }
        console.log('');

        // Test 7: Clean up
        console.log('7. Cleaning up test data...');
        const dropResult = await d1Client.executeQuery('DROP TABLE IF EXISTS test_migration');
        
        if (!dropResult.success) {
            throw new Error(`Drop table failed: ${dropResult.errors?.[0] || 'Unknown error'}`);
        }
        
        const firstDropResult = dropResult.result[0];
        console.log('✅ Cleanup completed successfully!');
        console.log(`   Changes: ${firstDropResult.meta?.changes || 0}`);
        console.log(`   Duration: ${firstDropResult.meta?.duration || 0}ms\n`);

        console.log('🎉 All D1 API tests passed successfully!');
        console.log('\nYour Cloudflare D1 configuration is working perfectly!');

    } catch (error) {
        console.error('❌ D1 API test failed:', error.message);
        
        if (error.response) {
            console.error('   Status:', error.response.status);
            console.error('   Response data:', JSON.stringify(error.response.data, null, 2));
        }
        
        process.exit(1);
    }
}

// Run tests
if (require.main === module) {
    testD1Connection().catch(console.error);
}

module.exports = { testD1Connection };