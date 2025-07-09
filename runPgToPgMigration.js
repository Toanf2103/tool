const DataMigrationTool = require('./move_db');

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
        port: 5433,
        database: 'POS23',
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
runPgToPgMigration(); 