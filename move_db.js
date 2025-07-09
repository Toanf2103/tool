const sql = require('mssql');
const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

// Cấu hình kết nối SQL Server
const sqlServerConfig = {
    user: 'sa',
    password: 'kutoan1346',
    server: 'MSI',
    database: 'PO3',
    options: {
        trustServerCertificate: true,
    }
};

// Cấu hình kết nối PostgreSQL
const postgresConfig = {
    host: 'localhost',
    port: 5432,
    database: 'POS1',
    user: 'postgres',
    password: '125621'
};

class DataMigrationTool {
    constructor(sourceConfig = null, targetConfig = null) {
        this.sqlPool = null;
        this.pgClient = null;
        this.sourcePgClient = null;
        this.targetPgClient = null;
        this.logFile = path.join(__dirname, 'migration_log.txt');
        
        // Store PostgreSQL configs if provided
        this.sourcePgConfig = sourceConfig || postgresConfig;
        this.targetPgConfig = targetConfig || postgresConfig;
    }

    // Ghi log
    log(message) {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ${message}\n`;
        console.log(logMessage.trim());
        fs.appendFileSync(this.logFile, logMessage);
    }

    // Kết nối đến cả 2 database
    async connect() {
        try {
            // Kết nối SQL Server nếu cần
            if (this.sqlPool === null) {
                this.sqlPool = await sql.connect(sqlServerConfig);
                this.log('✅ Kết nối SQL Server thành công');
            }

            // Kết nối PostgreSQL source
            if (this.sourcePgClient === null) {
                this.sourcePgClient = new Client(this.sourcePgConfig);
                await this.sourcePgClient.connect();
                this.log('✅ Kết nối PostgreSQL source thành công');
            }

            // Kết nối PostgreSQL target
            if (this.targetPgClient === null) {
                this.targetPgClient = new Client(this.targetPgConfig);
                await this.targetPgClient.connect();
                this.log('✅ Kết nối PostgreSQL target thành công');
            }

        } catch (error) {
            this.log(`❌ Lỗi kết nối: ${error.message}`);
            throw error;
        }
    }

    // Đóng kết nối
    async disconnect() {
        try {
            if (this.sqlPool) {
                await this.sqlPool.close();
                this.log('🔌 Đã đóng kết nối SQL Server');
            }
            if (this.sourcePgClient) {
                await this.sourcePgClient.end();
                this.log('🔌 Đã đóng kết nối PostgreSQL source');
            }
            if (this.targetPgClient) {
                await this.targetPgClient.end();
                this.log('🔌 Đã đóng kết nối PostgreSQL target');
            }
        } catch (error) {
            this.log(`❌ Lỗi đóng kết nối: ${error.message}`);
        }
    }

    // Lấy danh sách bảng từ SQL Server
    async getTableList() {
        try {
            const result = await this.sqlPool.request().query(`
                SELECT TABLE_NAME, TABLE_SCHEMA
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
                ORDER BY TABLE_NAME
            `);
            return result.recordset;
        } catch (error) {
            this.log(`❌ Lỗi lấy danh sách bảng: ${error.message}`);
            throw error;
        }
    }

    // Lấy cấu trúc bảng từ SQL Server
    async getTableStructure(tableName, schemaName = 'dbo') {
        try {
            const result = await this.sqlPool.request()
                .input('tableName', sql.NVarChar, tableName)
                .input('schemaName', sql.NVarChar, schemaName)
                .query(`
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = @schemaName
                    ORDER BY ORDINAL_POSITION
                `);
            return result.recordset;
        } catch (error) {
            this.log(`❌ Lỗi lấy cấu trúc bảng ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Chuyển đổi kiểu dữ liệu từ SQL Server sang PostgreSQL
    convertDataType(sqlServerType, maxLength, precision, scale) {
        const typeMap = {
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'bit': 'BOOLEAN',
            'decimal': `DECIMAL(${precision},${scale})`,
            'numeric': `NUMERIC(${precision},${scale})`,
            'money': 'DECIMAL(19,4)',
            'smallmoney': 'DECIMAL(10,4)',
            'float': 'DOUBLE PRECISION',
            'real': 'REAL',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'smalldatetime': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'char': maxLength ? `CHAR(${maxLength})` : 'CHAR(1)',
            'varchar': maxLength && maxLength !== -1 ? `VARCHAR(${maxLength})` : 'TEXT',
            'nchar': maxLength ? `CHAR(${maxLength})` : 'CHAR(1)',
            'nvarchar': maxLength && maxLength !== -1 ? `VARCHAR(${maxLength})` : 'TEXT',
            'text': 'TEXT',
            'ntext': 'TEXT',
            'uniqueidentifier': 'UUID',
            'varbinary': 'BYTEA',
            'binary': 'BYTEA',
            'image': 'BYTEA'
        };

        return typeMap[sqlServerType.toLowerCase()] || 'TEXT';
    }

    // Tạo bảng trong PostgreSQL
    async createTable(tableName, columns) {
        try {
            const columnDefinitions = columns.map(col => {
                const pgType = this.convertDataType(
                    col.DATA_TYPE,
                    col.CHARACTER_MAXIMUM_LENGTH,
                    col.NUMERIC_PRECISION,
                    col.NUMERIC_SCALE
                );
                const nullable = col.IS_NULLABLE === 'YES' ? '' : 'NOT NULL';
                return `    "${col.COLUMN_NAME}" ${pgType} ${nullable}`;
            }).join(',\n');

            const createTableSQL = `
                CREATE TABLE IF NOT EXISTS "${tableName}" (
                ${columnDefinitions}
                );
            `;

            await this.pgClient.query(createTableSQL);
            this.log(`✅ Đã tạo bảng ${tableName} trong PostgreSQL`);
        } catch (error) {
            this.log(`❌ Lỗi tạo bảng ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Lấy dữ liệu từ SQL Server
    async getTableData(tableName, schemaName = 'dbo', batchSize = 1000, offset = 0) {
        try {
            const result = await this.sqlPool.request().query(`
                SELECT * FROM [${schemaName}].[${tableName}]
                ORDER BY (SELECT NULL)
                OFFSET ${offset} ROWS
                FETCH NEXT ${batchSize} ROWS ONLY
            `);
            return result.recordset;
        } catch (error) {
            this.log(`❌ Lỗi lấy dữ liệu từ bảng ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Lấy tổng số bản ghi
    async getTableCount(tableName, schemaName = 'dbo') {
        try {
            const result = await this.sqlPool.request().query(`
                SELECT COUNT(*) as total FROM [${schemaName}].[${tableName}]
            `);
            return result.recordset[0].total;
        } catch (error) {
            this.log(`❌ Lỗi đếm bản ghi bảng ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Chèn dữ liệu vào PostgreSQL
    async insertData(tableName, columns, data) {
        if (!data || data.length === 0) return;

        try {
            const columnNames = columns.map(col => `"${col.COLUMN_NAME}"`).join(', ');
            const placeholders = data.map((_, rowIndex) => {
                const rowPlaceholders = columns.map((_, colIndex) => 
                    `$${rowIndex * columns.length + colIndex + 1}`
                ).join(', ');
                return `(${rowPlaceholders})`;
            }).join(', ');

            const insertSQL = `
                INSERT INTO "${tableName}" (${columnNames})
                VALUES ${placeholders}
                ON CONFLICT DO NOTHING
            `;

            // Flatten dữ liệu cho parameterized query
            const values = [];
            data.forEach(row => {
                columns.forEach(col => {
                    let value = row[col.COLUMN_NAME];
                    
                    // Xử lý các kiểu dữ liệu đặc biệt
                    if (value === null || value === undefined) {
                        values.push(null);
                    } else if (col.DATA_TYPE === 'bit') {
                        values.push(value === 1 || value === true);
                    } else if (col.DATA_TYPE === 'uniqueidentifier') {
                        values.push(value.toString());
                    } else if (col.DATA_TYPE.includes('datetime')) {
                        values.push(new Date(value));
                    } else {
                        values.push(value);
                    }
                });
            });

            await this.pgClient.query(insertSQL, values);
            this.log(`✅ Đã chèn ${data.length} bản ghi vào bảng ${tableName}`);
        } catch (error) {
            this.log(`❌ Lỗi chèn dữ liệu vào bảng ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Migration một bảng cụ thể
    async migrateTable(tableName, schemaName = 'dbo', batchSize = 1000) {
        try {
            this.log(`🚀 Bắt đầu migration bảng: ${tableName}`);

            // Lấy cấu trúc bảng
            const columns = await this.getTableStructure(tableName, schemaName);
            
            // Tạo bảng trong PostgreSQL
            // await this.createTable(tableName, columns);

            // Lấy tổng số bản ghi
            const totalRecords = await this.getTableCount(tableName, schemaName);
            this.log(`📊 Tổng số bản ghi trong bảng ${tableName}: ${totalRecords}`);

            if (totalRecords === 0) {
                this.log(`⚠️ Bảng ${tableName} không có dữ liệu`);
                return;
            }

            // Migration dữ liệu theo batch
            let offset = 0;
            let processedRecords = 0;

            while (offset < totalRecords) {
                const data = await this.getTableData(tableName, schemaName, batchSize, offset);
                
                if (data.length > 0) {
                    await this.insertData(tableName, columns, data);
                    processedRecords += data.length;
                    
                    const progress = ((processedRecords / totalRecords) * 100).toFixed(2);
                    this.log(`📈 Tiến độ bảng ${tableName}: ${processedRecords}/${totalRecords} (${progress}%)`);
                }

                offset += batchSize;
            }

            this.log(`✅ Hoàn thành migration bảng ${tableName}`);
        } catch (error) {
            this.log(`❌ Lỗi migration bảng ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Migration toàn bộ database
    async migrateDatabase(excludeTables = []) {
        try {
            this.log('🎯 Bắt đầu migration toàn bộ database');
            
            const tables = await this.getTableList();
            const filteredTables = tables.filter(table => 
                !excludeTables.includes(table.TABLE_NAME)
            );

            this.log(`📋 Tìm thấy ${filteredTables.length} bảng cần migration`);

            for (const table of filteredTables) {
                await this.migrateTable(table.TABLE_NAME, table.TABLE_SCHEMA);
            }

            this.log('🎉 Hoàn thành migration toàn bộ database');
        } catch (error) {
            this.log(`❌ Lỗi migration database: ${error.message}`);
            throw error;
        }
    }

    // Kiểm tra và so sánh số lượng bản ghi
    async verifyMigration() {
        try {
            this.log('🔍 Bắt đầu kiểm tra migration');
            
            const tables = await this.getTableList();
            
            for (const table of tables) {
                const sqlServerCount = await this.getTableCount(table.TABLE_NAME, table.TABLE_SCHEMA);
                
                const pgResult = await this.pgClient.query(`
                    SELECT COUNT(*) as total FROM "${table.TABLE_NAME}"
                `);
                const postgresCount = parseInt(pgResult.rows[0].total);

                if (sqlServerCount === postgresCount) {
                    this.log(`✅ ${table.TABLE_NAME}: ${sqlServerCount} bản ghi (khớp)`);
                } else {
                    this.log(`⚠️ ${table.TABLE_NAME}: SQL Server(${sqlServerCount}) vs PostgreSQL(${postgresCount}) (không khớp)`);
                }
            }
        } catch (error) {
            this.log(`❌ Lỗi kiểm tra migration: ${error.message}`);
        }
    }

    // Di chuyển dữ liệu giữa các PostgreSQL database
    async movePgToPg(excludeTables = [], batchSize = 1000) {
        try {
            this.log('🚀 Bắt đầu di chuyển dữ liệu giữa PostgreSQL databases');

            // Lấy danh sách bảng từ source database
            const result = await this.sourcePgClient.query(`
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            `);

            const tables = result.rows.map(row => row.table_name);
            const filteredTables = tables.filter(table => !excludeTables.includes(table));

            this.log(`📋 Tìm thấy ${filteredTables.length} bảng cần di chuyển`);

            for (const tableName of filteredTables) {
                this.log(`🔄 Đang di chuyển bảng: ${tableName}`);

                // Lấy tổng số bản ghi
                const countResult = await this.sourcePgClient.query(`
                    SELECT COUNT(*) as total FROM "${tableName}"
                `);
                const totalRecords = parseInt(countResult.rows[0].total);

                if (totalRecords === 0) {
                    this.log(`⚠️ Bảng ${tableName} không có dữ liệu`);
                    continue;
                }

                // Di chuyển dữ liệu theo batch
                let offset = 0;
                let processedRecords = 0;

                while (offset < totalRecords) {
                    // Lấy dữ liệu từ source
                    const data = await this.sourcePgClient.query(`
                        SELECT * FROM "${tableName}"
                        ORDER BY (SELECT NULL)
                        LIMIT ${batchSize}
                        OFFSET ${offset}
                    `);

                    if (data.rows.length > 0) {
                        // Tạo câu lệnh INSERT
                        const columns = Object.keys(data.rows[0]);
                        const columnNames = columns.map(col => `"${col}"`).join(', ');
                        const placeholders = data.rows.map((_, rowIndex) => {
                            const rowPlaceholders = columns.map((_, colIndex) => 
                                `$${rowIndex * columns.length + colIndex + 1}`
                            ).join(', ');
                            return `(${rowPlaceholders})`;
                        }).join(', ');

                        const insertSQL = `
                            INSERT INTO "${tableName}" (${columnNames})
                            VALUES ${placeholders}
                            ON CONFLICT DO NOTHING
                        `;

                        // Flatten dữ liệu cho parameterized query
                        const values = [];
                        data.rows.forEach(row => {
                            columns.forEach(col => {
                                values.push(row[col]);
                            });
                        });

                        // Chèn dữ liệu vào target
                        await this.targetPgClient.query(insertSQL, values);
                        processedRecords += data.rows.length;

                        const progress = ((processedRecords / totalRecords) * 100).toFixed(2);
                        this.log(`📈 Tiến độ bảng ${tableName}: ${processedRecords}/${totalRecords} (${progress}%)`);
                    }

                    offset += batchSize;
                }

                this.log(`✅ Hoàn thành di chuyển bảng ${tableName}`);
            }

            this.log('🎉 Hoàn thành di chuyển dữ liệu giữa PostgreSQL databases');
        } catch (error) {
            this.log(`❌ Lỗi di chuyển dữ liệu: ${error.message}`);
            throw error;
        }
    }
}

// Sử dụng
async function runMigration() {
    const migrationTool = new DataMigrationTool();
    try {
        await migrationTool.connect();

        // Tắt kiểm tra khóa ngoại
        await migrationTool.pgClient.query('SET session_replication_role = replica;');

        // Migration toàn bộ database
        await migrationTool.migrateDatabase(['sysdiagrams', '__MigrationHistory']);

        // Bật lại kiểm tra khóa ngoại
        await migrationTool.pgClient.query('SET session_replication_role = DEFAULT;');

        // Kiểm tra kết quả migration
        await migrationTool.verifyMigration();

    } catch (error) {
        console.error('❌ Migration thất bại:', error.message);
    } finally {
        await migrationTool.disconnect();
    }
}

// Ví dụ sử dụng movePgToPg
async function runPgToPgMigration() {
    const sourceConfig = {
        host: '127.0.0.1',
        port: 5433,
        database: 'POS1',
        user: 'myuser',
        password: 'mypassword'
    };

    const targetConfig = {
        host: '127.0.0.1',
        port: 5432,
        database: 'POS_test',
        user: 'myuser',
        password: 'mypassword'
    };

    const migrationTool = new DataMigrationTool(sourceConfig, targetConfig);
    try {
        await migrationTool.connect();

        // Tắt kiểm tra khóa ngoại
        await migrationTool.targetPgClient.query('SET session_replication_role = replica;');

        // Di chuyển dữ liệu, loại trừ các bảng không cần thiết
        await migrationTool.movePgToPg(['migrations', 'schema_migrations']);

        // Bật lại kiểm tra khóa ngoại
        await migrationTool.targetPgClient.query('SET session_replication_role = DEFAULT;');

    } catch (error) {
        console.error('❌ Di chuyển dữ liệu thất bại:', error.message);
    } finally {
        await migrationTool.disconnect();
    }
}

// Chạy migration
if (require.main === module) {
    runPgToPgMigration();
}

module.exports = DataMigrationTool;