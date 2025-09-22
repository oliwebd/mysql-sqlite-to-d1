const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

class MySQLToSQLiteMigrator {
    constructor(mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
        this.sqliteFile = './temp/migration.sql';
    }

    async connectToMySQL() {
        try {
            this.mysqlConnection = await mysql.createConnection({
                host: this.mysqlConfig.host,
                user: this.mysqlConfig.user,
                password: this.mysqlConfig.password,
                database: this.mysqlConfig.database,
                port: this.mysqlConfig.port
            });
            console.log('Connected to MySQL successfully');
        } catch (error) {
            console.error('MySQL connection failed:', error);
            throw error;
        }
    }

    async getTableNames() {
        const [rows] = await this.mysqlConnection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [this.mysqlConfig.database]
        );
        return rows.map(row => row.TABLE_NAME || row.table_name);
    }

    async getTableSchema(tableName) {
        const [rows] = await this.mysqlConnection.execute(
            `DESCRIBE ${this.mysqlConnection.escapeId(tableName)}`
        );
        return rows;
    }

    async getForeignKeys(tableName) {
        const [rows] = await this.mysqlConnection.execute(
            `
            SELECT 
                kcu.CONSTRAINT_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME,
                rc.UPDATE_RULE,
                rc.DELETE_RULE
            FROM information_schema.KEY_COLUMN_USAGE kcu
            JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
            WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ? 
              AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            `,
            [this.mysqlConfig.database, tableName]
        );

        return rows.map(row => ({
            column: row.COLUMN_NAME,
            refTable: row.REFERENCED_TABLE_NAME,
            refColumn: row.REFERENCED_COLUMN_NAME,
            onUpdate: row.UPDATE_RULE || 'NO ACTION',
            onDelete: row.DELETE_RULE || 'NO ACTION'
        }));
    }

    // Enhanced escaping for SQLite compatibility
    escapeForSQLite(value) {
        if (value === null || value === undefined) {
            return 'NULL';
        }

        // Handle numbers and booleans
        if (typeof value === 'number') {
            return value.toString();
        }

        if (typeof value === 'boolean') {
            return value ? '1' : '0';
        }

        // Convert to string if not already
        let stringValue = String(value);

        // Apply comprehensive escaping for SQLite
        stringValue = stringValue
            // Escape single quotes (most important)
            .replace(/'/g, "''")
            
            // Handle newlines and carriage returns
            .replace(/\r\n/g, '\\n')  // Windows line endings
            .replace(/\n/g, '\\n')    // Unix line endings
            .replace(/\r/g, '\\r')    // Mac line endings
            
            // Handle tabs and other whitespace
            .replace(/\t/g, '\\t')
            
            // Handle backslashes (but don't double-escape)
            .replace(/\\/g, '\\\\')
            
            // Handle null bytes (rare but can cause issues)
            .replace(/\x00/g, '\\0')
            
            // Handle vertical tabs and form feeds
            .replace(/\v/g, '\\v')
            .replace(/\f/g, '\\f')
            
            // Handle other control characters that might cause issues
            .replace(/[\x01-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, function(char) {
                return '\\x' + char.charCodeAt(0).toString(16).padStart(2, '0');
            });

        return `'${stringValue}'`;
    }

    // Format datetime -> SQLite-friendly
    formatMySQLDateTime(value) {
        if (value === null || value === undefined) return 'NULL';

        const toSQLiteTS = (date) => {
            return date.getFullYear() + "-" +
                String(date.getMonth() + 1).padStart(2, "0") + "-" +
                String(date.getDate()).padStart(2, "0") + " " +
                String(date.getHours()).padStart(2, "0") + ":" +
                String(date.getMinutes()).padStart(2, "0") + ":" +
                String(date.getSeconds()).padStart(2, "0");
        };

        if (value instanceof Date) {
            return `'${toSQLiteTS(value)}'`;
        }

        if (typeof value === 'string') {
            // Handle MySQL datetime formats
            if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(value)) {
                return this.escapeForSQLite(value);
            }
            
            // Handle MySQL date formats
            if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
                return this.escapeForSQLite(value);
            }
            
            // Handle MySQL time formats
            if (/^\d{2}:\d{2}:\d{2}$/.test(value)) {
                return this.escapeForSQLite(value);
            }
        }

        return this.escapeForSQLite(value);
    }

    mapMySQLTypeToSQLite(mysqlType) {
        if (!mysqlType) return 'TEXT';
        const type = mysqlType.toLowerCase();

        // Integer types
        if (type.includes('tinyint(1)')) return 'INTEGER'; // Boolean in MySQL
        if (type.includes('int')) return 'INTEGER';
        if (type.includes('bigint')) return 'INTEGER';
        if (type.includes('smallint')) return 'INTEGER';
        if (type.includes('mediumint')) return 'INTEGER';
        
        // Text types
        if (type.includes('char') || type.includes('text') || type.includes('enum') || type.includes('set')) return 'TEXT';
        if (type.includes('json')) return 'TEXT';
        
        // Numeric types
        if (type.includes('float') || type.includes('double') || type.includes('decimal') || type.includes('numeric')) return 'REAL';
        
        // Boolean
        if (type.includes('bool')) return 'INTEGER';
        
        // Date/Time types
        if (type.includes('date') || type.includes('time') || type.includes('timestamp') || type.includes('year')) return 'TEXT';
        
        // Binary types
        if (type.includes('blob') || type.includes('binary')) return 'BLOB';

        return 'TEXT';
    }

    // Legacy escapeText method for backward compatibility
    escapeText(value) {
        return this.escapeForSQLite(value).replace(/^'|'$/g, ''); // Remove surrounding quotes
    }

    async generateSQLFile() {
        if (!fs.existsSync('./temp')) {
            fs.mkdirSync('./temp', { recursive: true });
        }

        if (fs.existsSync(this.sqliteFile)) {
            fs.unlinkSync(this.sqliteFile);
        }

        const tableNames = await this.getTableNames();
        const sqlStatements = [];

        // Add SQLite pragmas for better compatibility
        sqlStatements.push(`PRAGMA foreign_keys = OFF;`);
        sqlStatements.push(`-- Generated from MySQL to SQLite migration`);
        sqlStatements.push(`-- Generated on: ${new Date().toISOString()}`);

        console.log(`Found ${tableNames.length} tables to migrate`);

        for (const tableName of tableNames) {
            console.log(`Generating schema for table: ${tableName}`);

            const columns = await this.getTableSchema(tableName);
            const foreignKeys = await this.getForeignKeys(tableName);

            const columnDefinitions = columns.map(col => {
                const type = this.mapMySQLTypeToSQLite(col.Type);
                const nullable = col.Null === 'YES' ? '' : 'NOT NULL';
                const primaryKey = col.Key === 'PRI' ? 'PRIMARY KEY' : '';
                const autoIncrement = col.Extra && col.Extra.includes('auto_increment') ? 'AUTOINCREMENT' : '';

                let defaultValue = '';
                if (col.Default !== null && col.Default !== undefined) {
                    if (String(col.Default).includes('CURRENT_TIMESTAMP')) {
                        if (col.Field.toLowerCase().includes('update')) {
                            defaultValue = '';
                        } else {
                            defaultValue = "DEFAULT (datetime('now'))";
                        }
                    } else if (type === 'TEXT') {
                        // Use the enhanced escaping for default values
                        defaultValue = `DEFAULT ${this.escapeForSQLite(col.Default)}`;
                    } else {
                        defaultValue = `DEFAULT ${col.Default}`;
                    }
                }

                return `"${col.Field}" ${type} ${nullable} ${primaryKey} ${autoIncrement} ${defaultValue}`.trim().replace(/\s+/g, ' ');
            });

            // Append foreign keys
            for (const fk of foreignKeys) {
                columnDefinitions.push(
                    `FOREIGN KEY("${fk.column}") REFERENCES "${fk.refTable}"("${fk.refColumn}") ON DELETE ${fk.onDelete} ON UPDATE ${fk.onUpdate}`
                );
            }

            const createTableSQL = `CREATE TABLE IF NOT EXISTS "${tableName}" (\n  ${columnDefinitions.join(',\n  ')}\n);`;
            sqlStatements.push(createTableSQL);
        }

        // Generate data inserts
        let totalRows = 0;
        for (const tableName of tableNames) {
            console.log(`Generating data for table: ${tableName}`);

            try {
                const [rows] = await this.mysqlConnection.execute(
                    `SELECT * FROM ${this.mysqlConnection.escapeId(tableName)}`
                );

                if (rows.length === 0) {
                    console.log(`  No data found in table: ${tableName}`);
                    continue;
                }

                const columnsMeta = await this.getTableSchema(tableName);
                let insertCount = 0;

                for (const row of rows) {
                    const columns = Object.keys(row);
                    const values = columns.map(col => {
                        const value = row[col];
                        
                        // Handle NULL values
                        if (value === null || value === undefined) {
                            return 'NULL';
                        }

                        // Check column metadata for type-specific handling
                        const columnMeta = columnsMeta.find(c => c.Field === col);
                        if (columnMeta) {
                            const colType = columnMeta.Type.toLowerCase();
                            
                            // Handle datetime/timestamp columns
                            if (colType.includes('datetime') || colType.includes('timestamp')) {
                                return this.formatMySQLDateTime(value);
                            }
                            
                            // Handle date columns
                            if (colType.includes('date') && !colType.includes('datetime')) {
                                return this.escapeForSQLite(value);
                            }
                            
                            // Handle time columns
                            if (colType.includes('time') && !colType.includes('datetime') && !colType.includes('timestamp')) {
                                return this.escapeForSQLite(value);
                            }
                        }

                        // Handle different value types with proper escaping
                        return this.escapeForSQLite(value);
                    });

                    const insertSQL = `INSERT INTO "${tableName}" (${columns.map(col => `"${col}"`).join(', ')}) VALUES (${values.join(', ')});`;
                    sqlStatements.push(insertSQL);
                    insertCount++;
                }

                totalRows += insertCount;
                console.log(`  Generated ${insertCount} insert statements for table: ${tableName}`);
            } catch (error) {
                console.error(`Error generating data for table ${tableName}:`, error.message);
                // Continue with other tables instead of failing completely
                continue;
            }
        }

        // Add summary comment
        sqlStatements.push(`-- Migration completed: ${tableNames.length} tables, ${totalRows} rows`);

        // Write to file with proper line endings
        const sqlContent = sqlStatements.join('\n') + '\n';
        fs.writeFileSync(this.sqliteFile, sqlContent, 'utf8');
        
        console.log(`SQL file generated: ${this.sqliteFile}`);
        console.log(`Total statements: ${sqlStatements.length}`);
        console.log(`Total data rows: ${totalRows}`);
        
        return sqlStatements.length;
    }

    // Method to validate the generated SQL
    async validateGeneratedSQL() {
        if (!fs.existsSync(this.sqliteFile)) {
            console.log('No SQL file found to validate');
            return false;
        }

        const content = fs.readFileSync(this.sqliteFile, 'utf8');
        
        // Basic validation checks
        const issues = [];
        
        // Check for unmatched quotes
        const singleQuotes = (content.match(/'/g) || []).length;
        if (singleQuotes % 2 !== 0) {
            issues.push('Unmatched single quotes detected');
        }
        
        // Check for unescaped newlines (should be \\n not literal newlines in VALUES)
        const valuesRegex = /VALUES\s*\([^)]*\n[^)]*\)/gi;
        const badNewlines = content.match(valuesRegex);
        if (badNewlines && badNewlines.length > 0) {
            issues.push(`${badNewlines.length} statements may have unescaped newlines`);
        }
        
        if (issues.length > 0) {
            console.log('SQL Validation Issues:');
            issues.forEach(issue => console.log(`  - ${issue}`));
            return false;
        }
        
        console.log('SQL validation passed - no obvious issues found');
        return true;
    }

    async closeConnection() {
        if (this.mysqlConnection) {
            await this.mysqlConnection.end();
            console.log('MySQL connection closed');
        }
    }

    // Method to disconnect (alias for closeConnection)
    async disconnect() {
        await this.closeConnection();
    }

    // Clean up temporary files
    async cleanup() {
        try {
            if (fs.existsSync(this.sqliteFile)) {
                fs.unlinkSync(this.sqliteFile);
                console.log('Temporary SQL file cleaned up');
            }
        } catch (error) {
            console.warn('Could not clean up temporary file:', error.message);
        }
    }
}

function parseMySQLUrl(url) {
    const regex = /mysql:\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/(.+)/;
    const match = url.match(regex);

    if (!match) {
        throw new Error('Invalid MySQL URL format. Expected: mysql://user:password@host:port/database');
    }

    return {
        user: match[1],
        password: match[2],
        host: match[3],
        port: parseInt(match[4]),
        database: match[5]
    };
}

module.exports = { MySQLToSQLiteMigrator, parseMySQLUrl };