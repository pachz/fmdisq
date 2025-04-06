require("dotenv").config();
const sql = require("mssql");

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
    await sql.connect(config);

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
    console.log("Result:", result.recordset);

    if (result?.recordset?.length > 0) {
        const Msisdn = result?.recordset[0].Msisdn;
        for (const code in policies) {
            await insertRecord(code, Msisdn);
        }
    }

  } catch (err) {
    console.error("Error running query:", err);
  } finally {
    await sql.close();
  }
}

// setInterval(runQuery, 60 * 1000);
runQuery();
