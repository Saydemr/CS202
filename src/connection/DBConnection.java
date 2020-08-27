package connection;

import java.sql.*;

public class DBConnection {

	private Connection conn = null;


	public DBConnection(String url) throws SQLException {
		conn = DriverManager.getConnection(url);

	}

	public void close() {
		if(conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}


	public ResultSet send_query(String query_statement) {
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query_statement);

		}
		catch (SQLException ex){
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
//		finally {
//			// it is a good idea to release
//			// resources in a finally{} block
//			// in reverse-order of their creation
//			// if they are no-longer needed
//
//			if (rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException sqlEx) { } // ignore
//
//				rs = null;
//			}
//
//			if (stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException sqlEx) { } // ignore
//
//				stmt = null;
//			}
//		}
		return rs;
	}




	public boolean check_connection() throws SQLException {
		return conn.isValid(1);		// Checks connection every 10 seconds
	}


	public boolean insert_data(String table, String data) throws SQLException {
		try {

			createTablesIfNotExists();

			if(table.equalsIgnoreCase("product") ) {


				String[] productData = data.split(",");
				int id = Integer.valueOf(productData[0]);
				Statement statement = conn.createStatement();

				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES(" + id + ",' " + productData[1] + "' ,'" +
						productData[2] + "','" + productData[3]+ "');"));

			} else if (table.equalsIgnoreCase("productkeyword")) {

				String[] pkData = data.split(",");
				int id = Integer.valueOf(pkData[0]);
				Statement statement = conn.createStatement();

				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES(" + id + ",' " + pkData[1] + "');"));

			}
			else if(table.equalsIgnoreCase("ExternalSupplier")){

				String[] extData = data.split(",");
				Statement statement = conn.createStatement();
				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( '" + extData[0] + "' ,'" +
						extData[1] + "','" + extData[2]+ "', ' " + extData[3] + "');"));
			}
			else if(table.equalsIgnoreCase("Website")){

				String[] webData = data.split(",");
				Statement statement = conn.createStatement();
				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( '" + webData[0] + "' ,'" +
						webData[1] + "','" + webData[2]+ "', " + Integer.valueOf(webData[3]) + ",'" + webData[4] + "','"
						+ webData[5] + "');"));


//				int zipp;
//				String wnull = "null";
//				try {
//					 zipp =Integer.valueOf(webData[3]);
//					statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( '" + webData[0] + "' ," + Integer.valueOf(webData[3]) +
//							",'" + webData[4] + "','" + webData[5] + "');"));
//					statement.executeUpdate(String.format(" INSERT INTO " + " zipCity " + " VALUES( " + Integer.valueOf(webData[3]) + " ,'" +
//							webData[2] + "');"));
//					statement.executeUpdate(String.format(" INSERT INTO " + " cityCountry " + " VALUES( '"+ webData[2] + "' , '" +
//							webData[1] + "');"));
//
//				} catch (NumberFormatException e) {
//					statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( '" + webData[0] + "' ," + wnull +
//							",'" + webData[4] + "','" + webData[5] + "');"));
//				}

//				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( '" + webData[0] + "' ," + Integer.valueOf(webData[3]) +
//						",'" + webData[4] + "','" + webData[5] + "');"));


// THERE IS A MIND BUG HERE. IN MYSQL YOU HAVE TO MAKE FOREIGN KEY UNIQUE OR PRIMARY KEY IN REFERENCED TABLE. HOWEVER, IF YOU MAKE CITY UNIQUE,
// THEN YOU CANNOT INSERT VALUES SUCH AS 34794, ISTANBUL AND 34795,ISTANBUL. BOTH IN ISTANBUL BUT BECAUSE I HAVE TO DECLARE CITY UNIQUE BECAUSE
// IT IS REFERENCED FROM CITYCOUNTRY TABLE, I CANNOT INSERT THESE TOGETHER. AAAAAAHHHHHHHHHHHHHHHHH. THEN I CANNOT GET RID OF FD'S. I JUST INSERT
// THE VALUES TO NEW TABLES, ANYWAY.


				try {
					statement.executeUpdate(String.format(" INSERT INTO " + "zipCity" + " VALUES( " + Integer.valueOf(webData[3]) + " ,'" +
							webData[2] + "');"));
					statement.executeUpdate(String.format(" INSERT INTO " + "cityCountry" + " VALUES( '"+ webData[2] + "' , '" +
							webData[1] + "');"));
				} catch (Exception e) {
					// dont do anything because no need to. these tables could be deleted anyways.
				}


			}
			else if(table.equalsIgnoreCase("WebsitePhone")){

				String[] wpData = data.split(",");
				Statement statement = conn.createStatement();
				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( '" + wpData[0] + "' ,'" +
						wpData[1] + "');"));

			}
			else if(table.equalsIgnoreCase("Sell")){
				String[] sData = data.split(",");
				Statement statement = conn.createStatement();
				statement.executeUpdate(String.format(" INSERT INTO " + table + " VALUES( " + Integer.valueOf(sData[0]) + " ,'" +
						sData[1] + "','" + sData[2]+ "', " + Double.valueOf(sData[3]) + "," + Double.valueOf(sData[4]) + ");"));
			}

			return true;

		} catch (SQLException e) {
			return false;
		}
	}

	public void delete_data(String table,String data) throws SQLException {
		String split_data = "";
		String query ="DELETE FROM " + table + " WHERE ";

		if(table.equalsIgnoreCase("Product")){
			query += "Id=" + data + ";";
		} else if(table.equalsIgnoreCase("productKeyword")) {
			int x = 0;

			while (true) {
				if (data.charAt(x) == ',') {
					break;
				}
				split_data += data.charAt(x);
				x++;
			}

			query += "productId=" + split_data + " ";
			x++;
			split_data = "";

			split_data = data.substring(x,data.length());

			query += "AND typeKeyword=\"" + split_data + "\";";

		}
		else if (table.equalsIgnoreCase("Website")) {
			int x = 0;
			split_data = data;
			query += "URL= " + "\"" + split_data + "\"";



		}
		else if (table.equalsIgnoreCase("WebsitePhone")) {
			int x = 0;

			for (; x < data.length(); x++) {
				if (data.charAt(x) == ',') {
					break;
				}
				split_data += data.charAt(x);
			}
			x++;
			query += "URL= " + "\"" + split_data + "\"";

			split_data = "";

			for (; x < data.length(); x++) {
				if (data.charAt(x) == ',') {
					break;
				}
				split_data += data.charAt(x);
			}

			query += " AND " + "Phone_Number= " + "\"" + split_data + "\"";

		}
		else if (table.equalsIgnoreCase("ExternalSupplier")) {
			int x = 0;

			for (; x < data.length(); x++) {
				if (data.charAt(x) == ',') {
					break;
				}
				split_data += data.charAt(x);
			}
			x++;
			query += "URL= " + "\"" + split_data + "\"";

			split_data = "";

			for (; x < data.length(); x++) {
				if (data.charAt(x) == ',') {
					break;
				}
				split_data += data.charAt(x);
			}
			x++;
			query += " AND " + "name= " + "\"" + split_data + "\"";

		}
		else if (table.equalsIgnoreCase("sell")) {
			int x = 0;

			for (; x < data.length(); x++) {
				if (data.charAt(x) == ',') {
					break;
				}
				split_data += data.charAt(x);

			}
			query += "PID=" + split_data;
			x += 1;
			split_data = "";
			split_data = "\"" + data.substring(x, data.length()) + "\"";


			query += " AND " + "WURL= " + split_data;
		}



		Statement statement = conn.createStatement();
		statement.executeUpdate(query);

	}

	public  void createTablesIfNotExists() throws SQLException {
		try {
			Statement statement = conn.createStatement();

			statement.executeUpdate(String.format("Create table IF NOT EXISTS Product" +
					" ( \n\tId INT PRIMARY KEY, \n" +
					"name VARCHAR(20) NOT NULL, \n" +
					"description VARCHAR(200),\n" +
					"brandname VARCHAR(200)\n);"));
			statement.executeUpdate(String.format("Create table IF NOT EXISTS ProductKeyword (\n" +
					"Id INT, \n" +
					"type_keyword VARCHAR(20) NOT NULL, \n" +
					"PRIMARY KEY (Id, type_keyword), \n" +
					"FOREIGN KEY (Id) REFERENCES Product(Id)\n" +
					");"));
			statement.executeUpdate(String.format("Create table IF NOT EXISTS Website(\n" +
					"URL VARCHAR(200) PRIMARY KEY, \n" +
					"Country VARCHAR(30), \n" +
					"City VARCHAR(30), \n" +
					"ZipCode INT, \n" +
					"Street VARCHAR(50), \n" +
					"Email VARCHAR(30) NOT NULL\n" +
					");\n"));




// CANNOT DO THAT. CANNOT DIVIDE THE TABLE
//			statement.executeUpdate(String.format("Create table if not exists Website (\n" +
//					"\n" +
//					"URL varchar(200),\n" +
//					"ZipCode INTEGER ,\n" +
//					"Street varchar(200),\n" +
//					"Email varchar(80) NOT NULL,\n" +
//					"PRIMARY KEY (URL) \n" +
//					");"));
//
//
			statement.executeUpdate(String.format("Create table IF NOT EXISTS ExternalSupplier(\n" +
					"URL VARCHAR(200),\n" +
					"name VARCHAR(30), \n" +
					"phone_number VARCHAR(20), \n" +
					"Email VARCHAR(50), \n" +
					"PRIMARY KEY (URL, name),\n" +
					"FOREIGN KEY (URL) REFERENCES Website (URL)\n" +
					");"));


			statement.executeUpdate(String.format("Create table IF NOT EXISTS zipCity (\n" +
					"ZipCode INTEGER PRIMARY KEY,\n" +
					"City varchar(50)   \n" +
					"#CONSTRAINT UC_zC UNIQUE (ZipCode,City), \n" +
					"#FOREIGN KEY (ZipCode) REFERENCES Website (ZipCode)\n" +
					");"));
			statement.executeUpdate(String.format("Create table if not exists cityCountry (\n" +
					"City varchar(50) PRIMARY KEY,\n" +
					"Country varchar(50)\n" +
					"#foreign key (City) references zipCity (City)\n" +
					");\n"));

			statement.executeUpdate(String.format("Create table IF NOT EXISTS WebsitePhone (\n" +
					"URL VARCHAR(200), \n" +
					"Phone_Number VARCHAR(20),\n" +
					"PRIMARY KEY (URL, Phone_Number), \n" +
					"FOREIGN KEY (URL) REFERENCES Website (URL) \n" +
					");"));

			statement.executeUpdate(String.format("Create table IF NOT EXISTS Sell( \n" +
					"\t\t\t\t\tPID INT, \n" +
					"\t\t\t\t\tWURL VARCHAR(200), \n" +
					"\t\t\t\t\tDate Datetime, \n" +
					"\t\t\t\t\tInitial_price FLOAT, \n" +
					"\t\t\t\t\tDiscounted_price FLOAT,\n" +
					"\t\t\t\t\tPRIMARY KEY (PID, WURL), \n" +
					"\t\t\t\t\tFOREIGN KEY (PID) REFERENCES Product(Id), \n" +
					"\t\t\t\t\tFOREIGN KEY (WURL) REFERENCES Website(URL),\n" +
					"                    check (Initial_price > Discounted_price)\n" +
					"                    \n" +
					"\t\t\t\t\t);"));



		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}