var sql = require('mssql');

 // config for your database

const config = {
    user: 'sa',
    password: 'kutoan1346',
    server: 'MSI',
    database: 'PO1',
    options: {
        trustServerCertificate: true,
    }
};

(async () => {

    try {

        // connect to your database

        let pool = await sql.connect(config);

            // create Request object

            const request = pool.request();

 

            // query to the database and get the records

            request.query('select * from Category where ID = 29', (err, result) => {

                  console.dir(11111)

            })

    } catch (err) {

        // ... error checks

            console.log('This is Error');

            console.log(err);

            console.dir(err);

    }

})()

sql.on('error', err => {

    // ... error handler

      console.log('This is Error handler');


})