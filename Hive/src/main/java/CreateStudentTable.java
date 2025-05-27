import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateStudentTable {
    public static void main(String[] args) {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String connectionURL = "jdbc:hive2://master:10000/default"; // Adjust hostname and database
        String username = "hadoop";
        String password = "20041031";

        Connection conn = null;
        Statement stmt = null;

        try {
            // Load Hive JDBC driver
            Class.forName(driverName);

            // Establish connection
            conn = DriverManager.getConnection(connectionURL, username, password);
            stmt = conn.createStatement();

            // Drop table if it exists
            String dropTable = "DROP TABLE IF EXISTS student2";
            stmt.execute(dropTable);

            // Create table
            String createTable = "CREATE TABLE student2 ("
                    + "sno STRING, "
                    + "sname STRING, "
                    + "age INT, "
                    + "classid STRING, "
                    + "dept STRING) "
                    + "ROW FORMAT DELIMITED "
                    + "FIELDS TERMINATED BY ',' "
                    + "STORED AS TEXTFILE";
            stmt.execute(createTable);
            System.out.println("Table student2 created successfully.");

        } catch (ClassNotFoundException e) {
            System.out.println("Hive JDBC Driver not found: " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.out.println("Error closing resources: " + e.getMessage());
            }
        }
    }
}