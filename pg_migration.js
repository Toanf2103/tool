const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

class PgMigrationTool {
    constructor(sourceConfig, targetConfig) {
        this.sourcePgClient = null;
        this.targetPgClient = null;
        this.sourcePgConfig = sourceConfig;
        this.targetPgConfig = targetConfig;
        this.logFile = path.join(__dirname, 'migration_log.txt');
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
            // Kết nối PostgreSQL source
            this.sourcePgClient = new Client(this.sourcePgConfig);
            await this.sourcePgClient.connect();
            this.log('✅ Kết nối PostgreSQL source thành công');

            // Kết nối PostgreSQL target
            this.targetPgClient = new Client(this.targetPgConfig);
            await this.targetPgClient.connect();
            this.log('✅ Kết nối PostgreSQL target thành công');

        } catch (error) {
            this.log(`❌ Lỗi kết nối: ${error.message}`);
            throw error;
        }
    }

    // Đóng kết nối
    async disconnect() {
        try {
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

    // Di chuyển dữ liệu giữa các PostgreSQL database
    async moveData(excludeTables = [], skipDataTables = [], batchSize = 1000) {
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

                // Kiểm tra xem bảng có cần di chuyển dữ liệu không
                if (skipDataTables.includes(tableName)) {
                    this.log(`⚠️ Bỏ qua dữ liệu của bảng ${tableName}`);
                    continue;
                }

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
            // throw error;
        }
    }
}

// Cấu hình kết nối
const sourceConfig = {
    host: '127.0.0.1',
    port: 5433,
    database: 'POS1',
    user: 'myuser',
    password: 'mypassword'
};

const targetConfig = {
    host: '127.0.0.1',
    port: 5433,
    database: 'POS23',
    user: 'myuser',
    password: 'mypassword'
};

// Chạy migration
async function runMigration() {
    const migrationTool = new PgMigrationTool(sourceConfig, targetConfig);
    try {
        await migrationTool.connect();

        // Tắt kiểm tra khóa ngoại
        await migrationTool.targetPgClient.query('SET session_replication_role = replica;');

        // Di chuyển dữ liệu, loại trừ các bảng không cần thiết
        await migrationTool.moveData(
            ['__EFMigrationsHistory','AttributeCategories'] // Bảng không di chuyển dữ liệu
        );

        // Bật lại kiểm tra khóa ngoại
        await migrationTool.targetPgClient.query('SET session_replication_role = DEFAULT;');

    } catch (error) {
        console.error('❌ Di chuyển dữ liệu thất bại:', error.message);
    } finally {
        await migrationTool.disconnect();
    }
}

// Chạy migration nếu file được thực thi trực tiếp
if (require.main === module) {
    runMigration();
}

module.exports = PgMigrationTool; 