require("dotenv").config();
const sql = require("mssql");

const sleep_successful = 1;
const sleep_fail = 10 * 1000;

const policies = {
    paq1: "FEF26F4F-8CA4-49EB-AA7D-33E9F56E8453",
    paq2: "E4229EB5-F12F-44F9-BC1F-2512CC6B2B80",
    // paaq3: "B3D8C596-67DC-4533-8748-731871EB262C",
}

const policyIds = Object.values(policies);

// Config from .env
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

const sleep_time = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function insertRecord(code, Msisdn) {
  try {
    // await sql.connect(config);
    const request = new sql.Request();

    request.input("VoucherCode", sql.VarChar, code);
    request.input("Msisdn", sql.BigInt, Msisdn);
    request.input("VoucherPolicyId", sql.UniqueIdentifier, policies[code]);
    request.input("IsDeleted", sql.Bit, 0);
    request.input(
      "CreatedAt",
      sql.DateTimeOffset,
      "2020-01-01 12:00:00 +00:00"
    );

    const insertQuery = `
        INSERT INTO dbo.UsedVouchers (
            Id, VoucherCode, Msisdn, VoucherPolicyId,
            IsDeleted, CreatedAt
        ) VALUES (
            NEWID(), @VoucherCode, @Msisdn, @VoucherPolicyId,
            @IsDeleted, @CreatedAt
        )
    `;

    await request.query(insertQuery);
    console.log("Record insert successful.", code, Msisdn);
  } catch (err) {
    console.error("Insert failed:", err);
  } finally {
    // await sql.close();
  }
}

async function runQuery() {
  try {

    // Build query using parameterized values
    const request = new sql.Request();
    policyIds.forEach((id, index) => {
      request.input(`id${index}`, sql.UniqueIdentifier, id);
    });

    // Inject the policy count
    request.input("policyCount", sql.Int, policyIds.length);

    const whereClause = policyIds.map((_, index) => `@id${index}`).join(", ");

    const query = `
      SELECT TOP 1 Msisdn
      FROM dbo.UsedVouchers
      WHERE VoucherPolicyId IN (${whereClause}) AND IsDeleted = 0
      GROUP BY Msisdn
      HAVING COUNT(DISTINCT VoucherPolicyId) < @policyCount
    `;

    const result = await request.query(query);

    if (result?.recordset?.length > 0) {
        const Msisdn = result?.recordset[0].Msisdn;
        console.log("Fixing:", Msisdn);
        for (const code in policies) {
            await insertRecord(code, Msisdn);
        }
    }

    return result?.recordset.length ?? 0;

  } catch (err) {
    console.error("Error running query:", err);
  } finally {
    return 0;
  }
}


(async () => {
    await sql.connect(config);
    console.log('Connected to SQL Server...');

    while(true) {
        const count = await runQuery();
        if(count > 0) {
            await sleep_time(sleep_successful);
        } else {
            console.log('No matching Msisdn found. Sleeping...', sleep_fail);
            await sleep_time(sleep_fail);
        }
    }
})();