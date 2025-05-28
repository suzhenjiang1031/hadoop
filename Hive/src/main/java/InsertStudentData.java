import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertStudentData {
    public static void main(String[] args) {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String connectionURL = "jdbc:hive2://localhost:10000/default";
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

            // Insert data
            String insertData = "INSERT INTO student2 VALUES "
                    + "('001', 'Alice', 20, 'CS101', 'Computer Science'), "
                    + "('002', 'Bob', 21, 'CS102', 'Computer Science'), "
                    + "('003', 'Charlie', 19, 'CS101', 'Mathematics')";
            stmt.execute(insertData);
            System.out.println("Data inserted into student2 successfully.");

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