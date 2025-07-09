const { Pool } = require('pg');

/**
 * Di chuyển dữ liệu từ database nguồn sang database đích
 * @param {string} sourceConnectionString - Connection string của DB nguồn
 * @param {string} targetConnectionString - Connection string của DB đích
 * @param {string[]} excludeTables - Mảng tên các bảng không di chuyển
 * @param {string} schema - Schema name (mặc định 'public')
 */
async function migratePostgreSQLData(
  sourceConnectionString,
  targetConnectionString,
  excludeTables = [],
  schema = 'public'
) {
  // Validate connection strings
  if (!sourceConnectionString || !targetConnectionString) {
    throw new Error('Connection strings không được để trống');
  }

  console.log('🔍 Kiểm tra connection strings...');
  console.log('Source:', sourceConnectionString.replace(/\/\/[^:]+:[^@]+@/, '//***:***@'));
  console.log('Target:', targetConnectionString.replace(/\/\/[^:]+:[^@]+@/, '//***:***@'));

  const sourcePool = new Pool({ 
    connectionString: sourceConnectionString,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000
  });
  const targetPool = new Pool({ 
    connectionString: targetConnectionString,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000
  });

  try {
    console.log('🚀 Bắt đầu quá trình di chuyển dữ liệu...');

    // Test connections
    console.log('🔗 Kiểm tra kết nối database...');
    try {
      const sourceTest = await sourcePool.query('SELECT 1');
      console.log('✅ Kết nối source database thành công');
    } catch (error) {
      throw new Error(`Không thể kết nối tới source database: ${error.message}`);
    }

    try {
      const targetTest = await targetPool.query('SELECT 1');
      console.log('✅ Kết nối target database thành công');
    } catch (error) {
      throw new Error(`Không thể kết nối tới target database: ${error.message}`);
    }

    // 1. Tắt foreign key constraints ở DB đích
    console.log('🔒 Tắt foreign key constraints...');
    await disableForeignKeyConstraints(targetPool, schema);

    // 2. Lấy danh sách tất cả bảng từ DB nguồn
    console.log('📋 Lấy danh sách bảng...');
    const tables = await getTables(sourcePool, schema, excludeTables);
    console.log(`Tìm thấy ${tables.length} bảng cần di chuyển:`, tables);

    // 3. Di chuyển dữ liệu từng bảng
    for (const table of tables) {
      console.log(`📦 Đang di chuyển bảng: ${table}...`);
      await migrateTable(sourcePool, targetPool, table, schema);
      console.log(`✅ Hoàn thành bảng: ${table}`);
    }

    // 4. Bật lại foreign key constraints
    console.log('🔓 Bật lại foreign key constraints...');
    await enableForeignKeyConstraints(targetPool, schema);

    console.log('🎉 Di chuyển dữ liệu hoàn tất!');

  } catch (error) {
    console.error('❌ Lỗi trong quá trình di chuyển:', error.message);
    
    // Chỉ cố gắng bật lại foreign key constraints nếu target pool đã kết nối thành công
    if (targetPool && !error.message.includes('target database')) {
      try {
        console.log('🔄 Đang cố gắng bật lại foreign key constraints...');
        await enableForeignKeyConstraints(targetPool, schema);
        console.log('✅ Đã bật lại foreign key constraints');
      } catch (constraintError) {
        console.error('❌ Không thể bật lại foreign key constraints:', constraintError.message);
      }
    }
    
    throw error;
  } finally {
    // Đóng connections an toàn
    try {
      await sourcePool.end();
      console.log('🔌 Đã đóng source connection');
    } catch (e) {
      console.warn('⚠️ Lỗi khi đóng source connection:', e.message);
    }
    
    try {
      await targetPool.end();
      console.log('🔌 Đã đóng target connection');
    } catch (e) {
      console.warn('⚠️ Lỗi khi đóng target connection:', e.message);
    }
  }
}

/**
 * Tắt tất cả foreign key constraints
 */
async function disableForeignKeyConstraints(pool, schema) {
  const query = `
    SELECT 'ALTER TABLE ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || 
           ' DISABLE TRIGGER ALL;' as sql
    FROM pg_tables 
    WHERE schemaname = $1;
  `;
  
  const result = await pool.query(query, [schema]);
  
  for (const row of result.rows) {
    await pool.query(row.sql);
  }
}

/**
 * Bật lại tất cả foreign key constraints
 */
async function enableForeignKeyConstraints(pool, schema) {
  const query = `
    SELECT 'ALTER TABLE ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || 
           ' ENABLE TRIGGER ALL;' as sql
    FROM pg_tables 
    WHERE schemaname = $1;
  `;
  
  const result = await pool.query(query, [schema]);
  
  for (const row of result.rows) {
    await pool.query(row.sql);
  }
}

/**
 * Lấy danh sách tất cả bảng (trừ những bảng trong excludeTables)
 */
async function getTables(pool, schema, excludeTables) {
  const query = `
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = $1 
    AND tablename NOT IN (${excludeTables.map((_, i) => `$${i + 2}`).join(', ')})
    ORDER BY tablename;
  `;
  
  const params = [schema, ...excludeTables];
  const result = await pool.query(query, params);
  
  return result.rows.map(row => row.tablename);
}

/**
 * Di chuyển dữ liệu của một bảng
 */
async function migrateTable(sourcePool, targetPool, tableName, schema) {
  const fullTableName = `${schema}.${tableName}`;
  
  try {
    // Xóa dữ liệu cũ trong bảng đích (nếu có)
    await targetPool.query(`TRUNCATE TABLE ${fullTableName} CASCADE;`);
    
    // Lấy tất cả dữ liệu từ bảng nguồn
    const sourceData = await sourcePool.query(`SELECT * FROM ${fullTableName};`);
    
    if (sourceData.rows.length === 0) {
      console.log(`  ⚠️  Bảng ${tableName} không có dữ liệu`);
      return;
    }

    // Lấy thông tin cột để tạo câu INSERT
    const columns = Object.keys(sourceData.rows[0]);
    const columnsList = columns.map(col => `"${col}"`).join(', ');
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
    
    const insertQuery = `
      INSERT INTO ${fullTableName} (${columnsList}) 
      VALUES (${placeholders});
    `;

    // Insert từng dòng dữ liệu
    let insertedCount = 0;
    for (const row of sourceData.rows) {
      const values = columns.map(col => row[col]);
      await targetPool.query(insertQuery, values);
      insertedCount++;
      
      // Hiển thị tiến trình mỗi 1000 dòng
      if (insertedCount % 1000 === 0) {
        console.log(`  📊 Đã chèn ${insertedCount}/${sourceData.rows.length} dòng...`);
      }
    }
    
    console.log(`  ✨ Đã chèn ${insertedCount} dòng vào bảng ${tableName}`);
    
    // Reset sequence nếu có
    await resetSequences(targetPool, tableName, schema);
    
  } catch (error) {
    console.error(`❌ Lỗi khi di chuyển bảng ${tableName}:`, error);
    throw error;
  }
}

/**
 * Reset auto-increment sequences
 */
async function resetSequences(pool, tableName, schema) {
  const query = `
    SELECT column_name, column_default
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    AND column_default LIKE 'nextval%';
  `;
  
  const result = await pool.query(query, [schema, tableName]);
  
  for (const row of result.rows) {
    const sequenceName = row.column_default.match(/nextval\('([^']+)'/)?.[1];
    if (sequenceName) {
      const resetQuery = `
        SELECT setval('${sequenceName}', 
          COALESCE((SELECT MAX(${row.column_name}) FROM ${schema}.${tableName}), 1)
        );
      `;
      await pool.query(resetQuery);
    }
  }
}

// Hàm helper để validate và parse connection string
function validateConnectionString(connectionString, name) {
  try {
    const url = new URL(connectionString);
    if (url.protocol !== 'postgresql:' && url.protocol !== 'postgres:') {
      throw new Error(`${name}: Protocol phải là postgresql:// hoặc postgres://`);
    }
    if (!url.hostname) {
      throw new Error(`${name}: Thiếu hostname trong connection string`);
    }
    if (!url.pathname || url.pathname === '/') {
      throw new Error(`${name}: Thiếu database name trong connection string`);
    }
    console.log(`✅ ${name} connection string hợp lệ`);
    console.log(`   Host: ${url.hostname}:${url.port || 5432}`);
    console.log(`   Database: ${url.pathname.substring(1)}`);
    console.log(`   User: ${url.username}`);
  } catch (error) {
    throw new Error(`${name} connection string không hợp lệ: ${error.message}`);
  }
}

// Hàm helper để chạy migration với error handling tốt hơn
async function runMigration(config) {
  const {
    sourceConnectionString,
    targetConnectionString,
    excludeTables = [],
    schema = 'public'
  } = config;

  try {
    // Validate connection strings trước
    validateConnectionString(sourceConnectionString, 'Source');
    validateConnectionString(targetConnectionString, 'Target');
    
    await migratePostgreSQLData(
      sourceConnectionString,
      targetConnectionString,
      excludeTables,
      schema
    );
  } catch (error) {
    console.error('💥 Migration thất bại:', error.message);
    
    // Gợi ý sửa lỗi phổ biến
    if (error.message.includes('ENOTFOUND')) {
      console.log('\n🔧 Gợi ý sửa lỗi:');
      console.log('1. Kiểm tra hostname/IP address có đúng không');
      console.log('2. Kiểm tra database server có đang chạy không');
      console.log('3. Kiểm tra network connectivity');
      console.log('4. Thử dùng IP thay vì hostname');
    } else if (error.message.includes('authentication')) {
      console.log('\n🔧 Gợi ý sửa lỗi:');
      console.log('1. Kiểm tra username/password');
      console.log('2. Kiểm tra user có quyền truy cập database không');
    } else if (error.message.includes('database') && error.message.includes('does not exist')) {
      console.log('\n🔧 Gợi ý sửa lỗi:');
      console.log('1. Kiểm tra tên database có đúng không');
      console.log('2. Tạo database nếu chưa có');
    }
    
    process.exit(1);
  }
}

// Export functions
module.exports = {
  migratePostgreSQLData,
  runMigration,
  validateConnectionString
};

// // Ví dụ sử dụng
// if (require.main === module) {
//   // ❌ Connection string lỗi (hostname 'base' không tồn tại)
//   // const config = {
//   //   sourceConnectionString: 'postgresql://user:password@base:5432/source_db',
//   //   targetConnectionString: 'postgresql://user:password@base:5432/target_db',
//   //   excludeTables: ['logs', 'temp_table', 'cache_data'],
//   //   schema: 'public'
//   // };

  // ✅ Ví dụ connection string đúng
  const config = {
    sourceConnectionString: 'Host=127.0.0.1;Port=5433;Database=POS1;Username=myuser;Password=mypassword;SSL Mode=Disable;',
    targetConnectionString: 'Host=127.0.0.1;Port=5433;Database=POS23;Username=myuser;Password=mypassword;SSL Mode=Disable;',
    excludeTables: ['__EFMigrationsHistory'], // Những bảng không di chuyển
    schema: 'public'
  };

  console.log('📝 Lưu ý: Hãy thay đổi connection strings cho phù hợp với môi trường của bạn');
  console.log('Ví dụ format:');
  console.log('- Local: postgresql://user:pass@localhost:5432/dbname');
  console.log('- Remote: postgresql://user:pass@192.168.1.100:5432/dbname');
  console.log('- Cloud: postgresql://user:pass@host.amazonaws.com:5432/dbname');
  
  // Uncomment dòng dưới để chạy (sau khi sửa connection strings)
// }
runMigration(config);