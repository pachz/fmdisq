require('dotenv').config();
const sql = require('mssql');

// Config from .env
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

async function runQuery() {
  try {
    await sql.connect(config);

    const result = await sql.query`SELECT TOP 1 Msisdn
FROM dbo.UsedDiscounts
WHERE DiscountPolicyId IN ('3BD25C81-B7DF-428C-83AE-3CA92F722DC7', 'C8D7B75F-D6BB-443F-ACEA-CB588F55150B', 'B3D8C596-67DC-4533-8748-731871EB262C') AND IsDeleted = 0
GROUP BY Msisdn
HAVING COUNT(DISTINCT DiscountPolicyId) < 3`;

    console.log('Result:', result.recordset);
  } catch (err) {
    console.error('Error running query:', err);
  } finally {
    await sql.close();
  }
}

// setInterval(runQuery, 60 * 1000);
runQuery();