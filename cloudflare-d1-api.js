const axios = require('axios');
const fs = require('fs');
const path = require('path');

class CloudflareD1API {
    constructor(accountId, apiToken, databaseId) {
        this.accountId = accountId;
        this.apiToken = apiToken;
        this.databaseId = databaseId;
    }

    // Simple axios call that matches curl behavior
    async makeRequest(method, endpoint, data = null) {
        const url = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/d1/database/${this.databaseId}${endpoint}`;
        
        const config = {
            method: method,
            url: url,
            headers: {
                'Authorization': `Bearer ${this.apiToken}`,
                'Content-Type': 'application/json',
            },
            timeout: 30000
        };

        if (data) {
            config.data = data;
        }

        try {
            const response = await axios(config);
            return response.data;
        } catch (error) {
            if (error.response) {
                const { status, data } = error.response;
                let errorMessage = `HTTP Error ${status}`;
                
                if (data && data.errors && data.errors.length > 0) {
                    errorMessage += `: ${JSON.stringify(data.errors[0])}`;
                } else if (data && typeof data === 'object') {
                    errorMessage += `: ${JSON.stringify(data)}`;
                } else {
                    errorMessage += `: ${error.message}`;
                }
                
                throw new Error(errorMessage);
            }
            throw error;
        }
    }

    async executeQuery(sql, params = []) {
        try {
            const response = await this.makeRequest('POST', '/query', {
                sql: sql,
                params: params
            });
            
            return response;
        } catch (error) {
            console.error('D1 Query Error:', error.message);
            throw error;
        }
    }

    // Fixed: Execute multiple statements as individual queries
    async executeBatch(statements) {
        const results = [];
        for (const statement of statements) {
            if (statement.trim()) {
                try {
                    const result = await this.executeQuery(statement);
                    results.push({ success: true, result, statement });
                } catch (error) {
                    results.push({ success: false, error: error.message, statement });
                }
                // Add delay between requests to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        return results;
    }

    // Improved SQL file parsing
    parseSQLFile(sqlContent) {
        const statements = [];
        let currentStatement = '';
        let inString = false;
        let stringChar = '';
        let inComment = false;
        
        const lines = sqlContent.split('\n');
        
        for (let line of lines) {
            line = line.trim();
            
            // Skip empty lines
            if (!line) continue;
            
            // Skip single-line comments
            if (line.startsWith('--') || line.startsWith('#')) continue;
            
            // Handle multi-line comments
            if (line.includes('/*')) {
                inComment = true;
            }
            if (inComment) {
                if (line.includes('*/')) {
                    inComment = false;
                    line = line.substring(line.indexOf('*/') + 2);
                } else {
                    continue;
                }
            }
            
            // Process character by character for proper string handling
            for (let i = 0; i < line.length; i++) {
                const char = line[i];
                
                if (!inString) {
                    if (char === '"' || char === "'" || char === '`') {
                        inString = true;
                        stringChar = char;
                    } else if (char === ';') {
                        // End of statement
                        if (currentStatement.trim()) {
                            statements.push(currentStatement.trim());
                            currentStatement = '';
                        }
                        continue;
                    }
                } else {
                    if (char === stringChar) {
                        // Check if it's escaped
                        if (i > 0 && line[i-1] === '\\') {
                            // It's escaped, continue
                        } else {
                            inString = false;
                            stringChar = '';
                        }
                    }
                }
                
                currentStatement += char;
            }
            
            currentStatement += ' '; // Add space between lines
        }
        
        // Add the last statement if it doesn't end with semicolon
        if (currentStatement.trim()) {
            statements.push(currentStatement.trim());
        }
        
        return statements.filter(stmt => stmt.length > 0);
    }

    async uploadSQLFile(filePath, options = {}) {
        try {
            if (!fs.existsSync(filePath)) {
                throw new Error(`SQL file not found: ${filePath}`);
            }

            const sqlContent = fs.readFileSync(filePath, 'utf8');
            const statements = this.parseSQLFile(sqlContent);
            
            console.log(`Found ${statements.length} SQL statements to execute`);
            
            const batchSize = options.batchSize || 10; // Increased batch size
            const stopOnError = options.stopOnError !== false; // Default to true
            
            let successful = 0;
            let failed = 0;
            const errors = [];

            for (let i = 0; i < statements.length; i += batchSize) {
                const batch = statements.slice(i, i + batchSize);
                const batchNumber = Math.floor(i/batchSize) + 1;
                const totalBatches = Math.ceil(statements.length/batchSize);
                
                console.log(`Processing batch ${batchNumber}/${totalBatches}...`);
                
                try {
                    const results = await this.executeBatch(batch);
                    
                    // Process individual results
                    for (const result of results) {
                        if (result.success) {
                            successful++;
                        } else {
                            failed++;
                            errors.push({
                                batch: batchNumber,
                                statement: result.statement,
                                error: result.error
                            });
                            
                            if (stopOnError) {
                                throw new Error(`Statement failed: ${result.error}`);
                            }
                        }
                    }
                    
                    console.log(`✅ Batch ${batchNumber} completed: ${successful} successful, ${failed} failed`);
                    
                    // Rate limiting delay between batches
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                } catch (error) {
                    if (stopOnError) {
                        console.error(`❌ Migration stopped at batch ${batchNumber}:`, error.message);
                        throw error;
                    } else {
                        failed += batch.length;
                        errors.push({
                            batch: batchNumber,
                            error: error.message
                        });
                        console.error(`❌ Batch ${batchNumber} failed:`, error.message);
                    }
                }
            }

            return { successful, failed, total: statements.length, errors };
        } catch (error) {
            console.error('File upload error:', error.message);
            throw error;
        }
    }

    async getDatabaseInfo() {
        try {
            const response = await this.makeRequest('GET', '');
            return response;
        } catch (error) {
            console.error('Get database info error:', error.message);
            throw error;
        }
    }

    async checkConnection() {
        try {
            const info = await this.getDatabaseInfo();
            
            if (info.success) {
                console.log('✅ Connected to Cloudflare D1 successfully');
                console.log(`   Database: ${info.result?.name || 'N/A'}`);
                return true;
            } else {
                console.error('❌ D1 connection failed:', info.errors?.[0] || 'Unknown error');
                return false;
            }
        } catch (error) {
            console.error('❌ D1 connection failed:', error.message);
            return false;
        }
    }

    // Helper method to test a single query
    async testQuery(sql) {
        try {
            console.log('Testing query:', sql.substring(0, 100) + '...');
            const result = await this.executeQuery(sql);
            console.log('✅ Query successful');
            return result;
        } catch (error) {
            console.error('❌ Query failed:', error.message);
            throw error;
        }
    }
}

module.exports = CloudflareD1API;