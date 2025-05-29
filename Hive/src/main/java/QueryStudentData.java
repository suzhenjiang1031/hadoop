import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryStudentData {
    public static void main(String[] args) {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String connectionURL = "jdbc:hive2://localhost:10000/default";
        String username = "hadoop";
        String password = "20041031";

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Load Hive JDBC driver
            Class.forName(driverName);

            // Establish connection
            conn = DriverManager.getConnection(connectionURL, username, password);
            stmt = conn.createStatement();

            // Query data
            String query = "SELECT * FROM student2";
            rs = stmt.executeQuery(query);

            // Display results
            System.out.println("Query Results:");
            System.out.println("sno\tsname\tage\tclassid\tdept");
            System.out.println("---------------------------------------------");
            while (rs.next()) {
                String sno = rs.getString("sno");
                String sname = rs.getString("sname");
                int age = rs.getInt("age");
                String classid = rs.getString("classid");
                String dept = rs.getString("dept");
                System.out.printf("%s\t%s\t%d\t%s\t%s%n", sno, sname, age, classid, dept);
            }

        } catch (ClassNotFoundException e) {
            System.out.println("Hive JDBC Driver not found: " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.out.println("Error closing resources: " + e.getMessage());
            }
        }
    }
}