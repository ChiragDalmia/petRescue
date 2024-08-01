require("dotenv").config();
const sql = require("mssql");
const oracledb = require("oracledb");

// SQL Server connection configuration
const sqlConfig = {
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASSWORD,
  server: process.env.MSSQL_SERVER,
  port: parseInt(process.env.MSSQL_PORT),
  database: process.env.MSSQL_DATABASE,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

// Oracle connection configuration
const oracleConfig = {
  user: process.env.ORACLE_USER,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_CONNECTSTRING,
};

// Function to extract addresses from SQL Server
async function extractAddressesFromSQLServer() {
  try {
    await sql.connect(sqlConfig);
    const result = await sql.query`
      SELECT 
        STREET_NUM as StreetNumber, 
        UNIT as UnitNumber, 
        STREET_NAME as StreetName, 
        STREET_TYPE as StreetType,
        STREET_DIR as StreetDirection,
        POSTAL_CODE as PostalCode,
        CITY as City, 
        PROVINCE as Province
      FROM dbo.Address
    `;
    return result.recordset;
  } catch (err) {
    console.error("Error extracting from SQL Server:", err);
    throw err;
  } finally {
    await sql.close();
  }
}

// Function to transform address data
function transformAddress(sqlAddress) {
  return {
    streetNumber: parseInt(sqlAddress.StreetNumber, 10) || null,
    unitNum: sqlAddress.UnitNumber || null,
    streetName: sqlAddress.StreetName,
    streetType: sqlAddress.StreetType || "UNKNOWN",
    streetDirection: sqlAddress.StreetDirection || null,
    postalCode: sqlAddress.PostalCode || "UNKNOWN",
    city: sqlAddress.City,
    province: sqlAddress.Province,
  };
}

// Function to get existing Oracle addresses
async function getExistingOracleAddresses(connection) {
  const result = await connection.execute("SELECT * FROM Address", [], {
    outFormat: oracledb.OUT_FORMAT_OBJECT,
  });
  return result.rows;
}

// Function to update existing Oracle address
async function updateOracleAddress(connection, address, oracleId) {
  console.log(`Updating address with ID: ${oracleId}`);
  await connection.execute(
    `UPDATE Address SET 
     UNIT_NUM = :unitNum, 
     STREET_NUMBER = :streetNumber,
     STREET_NAME = :streetName,
     STREET_TYPE = :streetType,
     STREET_DIRECTION = :streetDirection, 
     POSTAL_CODE = :postalCode, 
     CITY = :city, 
     PROVINCE = :province
     WHERE ADDRESS_ID = :id`,
    { ...address, id: oracleId }
  );
}

// Function to get next available Address ID in Oracle
async function getNextAddressId(connection) {
  const result = await connection.execute(
    "SELECT MAX(ADDRESS_ID) AS MAX_ID FROM Address"
  );
  return (result.rows[0][0] || 0) + 1;
}

// Function to insert new Oracle address
async function insertOracleAddress(connection, address) {
  const nextId = await getNextAddressId(connection);
  console.log(`Inserting new address with ID: ${nextId}`);
  await connection.execute(
    `INSERT INTO Address (
      ADDRESS_ID, UNIT_NUM, STREET_NUMBER, STREET_NAME, STREET_TYPE, 
      STREET_DIRECTION, POSTAL_CODE, CITY, PROVINCE
    ) VALUES (
      :id, :unitNum, :streetNumber, :streetName, :streetType, 
      :streetDirection, :postalCode, :city, :province
    )`,
    { ...address, id: nextId }
  );
  return nextId;
}

async function refreshAddressTable() {
  let oracleConnection;

  try {
    console.log("Starting address refresh process...");

    const sqlAddresses = await extractAddressesFromSQLServer();
    console.log(`Extracted ${sqlAddresses.length} addresses from SQL Server`);

    oracleConnection = await oracledb.getConnection(oracleConfig);
    console.log("Connected to Oracle database");

    const existingOracleAddresses = await getExistingOracleAddresses(
      oracleConnection
    );
    console.log(
      `Found ${existingOracleAddresses.length} existing addresses in Oracle`
    );

    const existingAddressMap = new Map(
      existingOracleAddresses.map((a) => [
        `${a.STREET_NUMBER}${a.STREET_NAME}${a.POSTAL_CODE}`.toLowerCase(),
        a,
      ])
    );

    let updateCount = 0;
    let insertCount = 0;
    let errorCount = 0;

    const batchSize = 1000;
    for (let i = 0; i < sqlAddresses.length; i += batchSize) {
      const batch = sqlAddresses.slice(i, i + batchSize);
      console.log(
        `Processing batch ${i / batchSize + 1} of ${Math.ceil(
          sqlAddresses.length / batchSize
        )}`
      );

      for (const sqlAddress of batch) {
        try {
          const transformedAddress = transformAddress(sqlAddress);
          const key =
            `${sqlAddress.StreetNumber}${sqlAddress.StreetName}${sqlAddress.PostalCode}`.toLowerCase();

          if (existingAddressMap.has(key)) {
            console.log(`Updating existing address: ${key}`);
             const existingAddress = existingAddressMap.get(key);
             await updateOracleAddress(
               oracleConnection,
               transformedAddress,
               existingAddress.ADDRESS_ID
             );
             updateCount++;
          } else {
             console.log(`Inserting new address: ${key}`);
            await insertOracleAddress(oracleConnection, transformedAddress);
            insertCount++;
          }
        } catch (err) {
          console.error("Error processing address:", err);
          errorCount++;
        }
      }

      console.log(
        `Batch processed. Total progress: Updated: ${updateCount}, Inserted: ${insertCount}, Errors: ${errorCount}`
      );
       if (i % (batchSize * 10) === 0) {
         await oracleConnection.commit();
         console.log(`Committed batch ${i / batchSize}`);
       }
    }

    await oracleConnection.commit();
    console.log(
      `Address refresh completed. Updated: ${updateCount}, Inserted: ${insertCount}, Errors: ${errorCount}`
    );
  } catch (error) {
    console.error("Error refreshing address table:", error);
    if (oracleConnection) {
      await oracleConnection.rollback();
    }
  } finally {
    if (oracleConnection) {
      try {
        await oracleConnection.close();
        console.log("Oracle connection closed");
      } catch (err) {
        console.error("Error closing Oracle connection:", err);
      }
    }
  }
}

// Run the refresh process
refreshAddressTable()
  .then(() => {
    console.log("Address refresh process finished");
  })
  .catch((err) => {
    console.error("An error occurred during the address refresh process:", err);
  });
