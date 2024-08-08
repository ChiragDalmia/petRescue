require("dotenv").config({
  path: "C:\\Users\\chira\\OneDrive\\Desktop\\db_groupProject\\.env",
});
const fs = require("fs");
const csv = require("csv-parser");
const oracledb = require("oracledb");
const path = require("path");

const config = {
  user: process.env.ORACLE_USER,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_CONNECTSTRING,
};

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

async function validateAddress(connection, address_id) {
  const result = await connection.execute(
    "SELECT COUNT(*) as count FROM Address WHERE address_id = :id",
    { id: address_id }
  );
  const isValid = result.rows[0].COUNT > 0;
  if (!isValid) {
    console.log(`Invalid address_id: ${address_id}`);
  }
  return isValid;
}

async function validateVolunteer(connection, volunteer_id) {
  const result = await connection.execute(
    "SELECT VOLUNTEER_ID, GROUP_LEADER FROM Volunteer WHERE volunteer_id = :id",
    { id: volunteer_id }
  );
   console.log(volunteer_id);
  if (result.rows.length > 0) {
    return {
      isValid: true,
      groupLeader: result.rows[0].GROUP_LEADER || result.rows[0].VOLUNTEER_ID,
    };
  }
  console.log(`Invalid volunteer_id: ${volunteer_id}`);
  return { isValid: false, groupLeader: null };
}

async function insertDonation(connection, donation) {
  const sql = `INSERT INTO Donation (
donation_id, donor_first_name, donor_last_name, 
address_id, donation_date, donation_amount, volunteer_id
) VALUES (
:1, :2, :3, 
:4, TO_DATE(:5, 'YYYY-MM-DD'), :6, :7
)`;

  const binds = [
    donation.id,
    donation.firstName,
    donation.lastName,
    donation.address,
    donation.date,
    donation.amount,
    donation.volunteer,
  ];

  console.log("SQL:", sql);
  console.log("Binds:", JSON.stringify(binds, null, 2));

  try {
    await connection.execute(sql, binds);
    console.log(`Successfully inserted donation ${donation.id}`);
  } catch (error) {
    console.error(`Error inserting donation ${donation.id}:`, error.message);
    throw error;
  }
}
async function getGroupLeader(connection, volunteer_id) {
  const result = await connection.execute(
    `SELECT COALESCE(group_leader, volunteer_id) as group_leader 
FROM Volunteer 
WHERE volunteer_id = :id`,
    { id: volunteer_id }
  );
  if (result.rows.length > 0) {
    return result.rows[0].GROUP_LEADER;
  } else {
    return volunteer_id;
  }
}

async function processDonations(connection, filePath) {
  const rejectedDonations = {};
  const rows = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => {
        rows.push(row);
      })
      .on("end", async () => {
        for (const row of rows) {
          const [firstName, lastName] = row.donor_name.split(" ");
          const donation = {
            id: parseInt(row.donation_id),
            firstName: firstName,
            lastName: lastName,
            address: row.address_id ? parseInt(row.address_id) : null,
            date: row.donation_date,
            amount: parseFloat(row.donation_amount),
            volunteer: parseInt(row.volunteer_id),
          };

          try {
            if (!donation.address) {
              throw new Error("Empty address_id");
            }

            const isAddressValid = await validateAddress(
              connection,
              donation.address
            );
            if (!isAddressValid) {
              throw new Error("Invalid address");
            }
            if (!donation.volunteer) {
              throw new Error("Empty volunteer_id");
            }
            const volunteerValidation = await validateVolunteer(
              connection,
              donation.volunteer
            );
            if (!volunteerValidation.isValid) {
              throw new Error("Invalid volunteer");
            }

            const hasNullValues = Object.values(donation).some(
              (val) => val === null || val === undefined
            );
            if (hasNullValues) {
              throw new Error("Contains null values");
            }

            if (donation.amount <= 0) {
              throw new Error("Invalid donation amount");
            }

            await insertDonation(connection, donation);
          } catch (error) {
            console.error(
              `Error processing donation ${donation.id}: ${error.message}`
            );
            console.error("Donation data:", JSON.stringify(donation, null, 2));
            const groupLeader = await getGroupLeader(
              connection,
              donation.volunteer
            );
            if (!rejectedDonations[groupLeader]) {
              rejectedDonations[groupLeader] = [];
            }
            rejectedDonations[groupLeader].push(row);
          }
        }

        // Generate rejection CSV files
        for (const [groupLeaderId, rejections] of Object.entries(
          rejectedDonations
        )) {
          const csvContent = rejections
            .map((obj) => Object.values(obj).join(","))
            .join("\n");
          const rejectionFilePath = path.join(
            __dirname,
            `rejected_${groupLeaderId}.csv`
          );
          fs.writeFileSync(rejectionFilePath, csvContent);
        }

        resolve();
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

async function main() {
  let connection;
  try {
    connection = await oracledb.getConnection(config);
    console.log("Successfully connected to Oracle Database");

    const csvFiles = ["donation1.csv", "donation2.csv", "donation3.csv"];
    for (const file of csvFiles) {
      const filePath = path.join(__dirname, file);
      await processDonations(connection, filePath);
    }

    console.log("All donation processing complete.");
  } catch (error) {
    console.error("Error in main process:", error);
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log("Oracle Database connection closed");
      } catch (error) {
        console.error("Error closing connection:", error);
      }
    }
  }
}

main().catch(console.error);
