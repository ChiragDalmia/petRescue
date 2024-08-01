require("dotenv").config();
const sql = require("mssql");
const oracledb = require("oracledb");

async function getAddressesFromSQLServer() {
  try {
    await sql.connect({
      server: process.env.MSSQL_SERVER,
      port: parseInt(process.env.MSSQL_PORT),
      database: process.env.MSSQL_DATABASE,
      user: process.env.MSSQL_USER,
      password: process.env.MSSQL_PASSWORD,
      options: {
        encrypt: true,
        trustServerCertificate: true,
      },
    });

    const result = await sql.query`SELECT * FROM dbo.Address`;
    return result.recordset;
  } catch (err) {
    console.error("Error fetching from SQL Server:", err);
    throw err;
  } finally {
    await sql.close();
  }
}

async function refreshOracleAddresses(sqlServerAddresses) {
  let connection;
  try {
    connection = await oracledb.getConnection({
      user: process.env.ORACLE_USER,
      password: process.env.ORACLE_PASSWORD,
      connectString: process.env.ORACLE_CONNECTSTRING,
    });

    // Get existing addresses from Oracle
    const existingAddresses = await connection.execute("SELECT * FROM Address");

    // Create a map of existing addresses for easy lookup
    const existingAddressMap = new Map(
      existingAddresses.rows.map((row) => [row[1], row])
    );

    // Process each address from SQL Server
    for (const address of sqlServerAddresses) {
      const existingAddress = existingAddressMap.get(address.AddressLine1);

      if (!existingAddress) {
        // Insert new address
        const nextAddressId = await getNextAddressId(connection);
        await connection.execute(
          `INSERT INTO Address (AddressID, AddressLine1, City, Province, PostalCode)
           VALUES (:1, :2, :3, :4, :5)`,
          [
            nextAddressId,
            address.AddressLine1,
            address.City,
            address.Province,
            address.PostalCode,
          ]
        );
      } else if (
        existingAddress[2] !== address.City ||
        existingAddress[3] !== address.Province ||
        existingAddress[4] !== address.PostalCode
      ) {
        // Update changed address
        await connection.execute(
          `UPDATE Address SET City = :1, Province = :2, PostalCode = :3
           WHERE AddressID = :4`,
          [
            address.City,
            address.Province,
            address.PostalCode,
            existingAddress[0],
          ]
        );
      }
    }

    await connection.commit();
    console.log("Address refresh completed successfully");
  } catch (err) {
    console.error("Error refreshing Oracle addresses:", err);
    if (connection) {
      await connection.rollback();
    }
    throw err;
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

async function getNextAddressId(connection) {
  const result = await connection.execute("SELECT MAX(AddressID) FROM Address");
  return (result.rows[0][0] || 0) + 1;
}

async function main() {
  try {
    const sqlServerAddresses = await getAddressesFromSQLServer();
    await refreshOracleAddresses(sqlServerAddresses);
  } catch (err) {
    console.error("An error occurred:", err);
  }
}

main();
