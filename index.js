require("dotenv").config();
const sql = require("mssql");

const sleep_successful = 1;
const sleep_fail = 10 * 1000;

const policies = {
    paaq1: "3BD25C81-B7DF-428C-83AE-3CA92F722DC7",
    paaq2: "C8D7B75F-D6BB-443F-ACEA-CB588F55150B",
    paaq3: "B3D8C596-67DC-4533-8748-731871EB262C",
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

    request.input("DiscountCode", sql.VarChar, code);
    request.input("Msisdn", sql.BigInt, Msisdn);
    request.input("DiscountPolicyId", sql.UniqueIdentifier, policies[code]);
    request.input("IsDeleted", sql.Bit, 0);
    request.input(
      "CreatedAt",
      sql.DateTimeOffset,
      "2020-01-01 12:00:00 +00:00"
    );

    const insertQuery = `
        INSERT INTO dbo.UsedDiscounts (
            Id, DiscountCode, Msisdn, DiscountPolicyId,
            IsDeleted, CreatedAt
        ) VALUES (
            NEWID(), @DiscountCode, @Msisdn, @DiscountPolicyId,
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
      FROM dbo.UsedDiscounts
      WHERE DiscountPolicyId IN (${whereClause}) AND IsDeleted = 0
      GROUP BY Msisdn
      HAVING COUNT(DISTINCT DiscountPolicyId) < @policyCount
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