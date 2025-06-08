const mysql = require("mysql");
const util = require("util");

// Create a connection to the database

const connection = mysql.createConnection({
          host: "localhost",
          user: "prodapps",
          password: "q-3hw28B(W4+d{g6UR",
          database: "car_trucking",
});

// open the MySQL connection
connection.connect((error) => {
          if (error) throw error;
          console.log("Successfully connected to the database: ");
});
const query = util.promisify(connection.query).bind(connection);

module.exports = {
          connection,
          query,
};
