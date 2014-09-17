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
import db.MySQLConnector;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class HMPBarcodeAdder {

    CommandLine cli;
    Connection conn;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ParseException, ClassNotFoundException, SQLException {
        HMPBarcodeAdder h = new HMPBarcodeAdder();
        h.run(args);
    }

    private void run(String[] args) throws ParseException, ClassNotFoundException, SQLException {
        cli = getOptions(args);
        MySQLConnector mysql = new MySQLConnector("localhost", cli.getOptionValue("db"), cli.getOptionValue("dbUser"), cli.getOptionValue("dbPassword"));
        conn = mysql.getConnection();
        if (!checkIfBarcodeExists()) {
            addBarcode();
        } else {
            System.out.println("Barcode " + cli.getOptionValue("barcode") + " already in database, exiting");
        }
    }

    private CommandLine getOptions(String[] args) throws ParseException {
        Options o = new Options();
        o.addOption("db", true, "name of the database");
        o.addOption("dbUser", true, "db user name");
        o.addOption("dbPassword", true, "db user password");
        o.addOption("barcode", true, "barcode sequence");

        if (args.length == 0) {
            printHelp(o);
        }
        GnuParser parser = new GnuParser();
        CommandLine c = parser.parse(o, args);
        return c;
    }

    private void printHelp(Options o) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("java", o);
        System.exit(0);
    }

    private void addBarcode() throws SQLException {
        System.out.println("Barcode " + cli.getOptionValue("barcode") + " does not exist in database, adding");
        int val = conn.createStatement().executeUpdate("insert into barcodes (barcode_sequence) values ('" + cli.getOptionValue("barcode") + "')");
        System.out.println("added " + val + " rows");
    }

    private boolean checkIfBarcodeExists() throws SQLException {
        int id = -1;
        ResultSet rs = conn.createStatement().executeQuery("select * from barcodes where barcode_sequence = '" + cli.getOptionValue("barcode") + "'");
        while (rs.next()) {
            id = rs.getInt("barcode_id");
        }
        if (id == -1) {
            return false;
        }
        return true;
    }
}