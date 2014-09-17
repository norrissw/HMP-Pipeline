/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hmp;

/**
 *
 * @author snorris
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnector {
    private Connection conn;

    public MySQLConnector() {
    }

    public MySQLConnector(String host, String db, String user, String password) throws ClassNotFoundException{
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:mysql://" + host + "/" + db, user, password);
            System.out.println("Connected to " + db + " database on " + host);
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (InstantiationException ex) {
            ex.printStackTrace();
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
    }

    public Connection getConnection() {
        return conn;
    }

}