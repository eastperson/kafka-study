package org.example.kafka.consumer;

import java.sql.*;

public class JDBCTester {
    private static final String POSTGRESQL_ADDRESS_LOCAL = "ubuntu.orb.local:5432";
    public static void main(String[] args) {

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;


        String url = "jdbc:postgresql://" + POSTGRESQL_ADDRESS_LOCAL + "/postgres";
        String user = "postgres";
        String password = "postgres";
        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.createStatement();
            rs = st.executeQuery("SELECT 'postgresql is connected' ");

            if (rs.next())
                System.out.println(rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
