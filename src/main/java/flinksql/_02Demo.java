package flinksql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class _02Demo {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/";
        String dbName = "hive";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "root";
        String password = "123456";
        Connection conn = null;
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(url + dbName, userName, password);
            java.sql.Statement stmt = conn.createStatement();
            java.sql.ResultSet rs = stmt.executeQuery("show tables");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            rs.close();
            stmt.close();
            System.out.println("MySQL connected!");
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
