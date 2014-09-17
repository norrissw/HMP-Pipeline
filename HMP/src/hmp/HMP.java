package main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Chris Friedline <cfriedline@vcu.edu>
 */
public class HMPRunRemover {

    CommandLine cli;
    Connection conn;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ParseException, ClassNotFoundException, SQLException {
        HMPRunRemover h = new HMPRunRemover();
        h.run(args);
    }

    private void run(String[] args) throws ParseException, ClassNotFoundException, SQLException {
        cli = getOptions(args);
        conn = new MySQLConnector("localhost", cli.getOptionValue("db"), cli.getOptionValue("dbUser"), cli.getOptionValue("dbPassword")).getConnection();
        int runId = getRunIdFromDate();
        System.out.println("Deleting data for " + cli.getOptionValue("date") + ", id = " + runId);
        ArrayList<Integer> samples = getSamplesForRun(runId);
        System.out.println("Samples " + Arrays.toString(samples.toArray()));
        Collections.sort(samples);
        deleteRpdResultData(samples);
        deleteRdpSummaryData(samples);
        deleteSampleData(samples);
        deleteRunData(runId);
        deleteSampleCounts(samples);
        deleteSample(runId);
        deleteRunCounts(runId);
        deleteRun(runId);
    }

    private CommandLine getOptions(String[] args) throws ParseException {
        Options o = new Options();
        o.addOption("date", true, "date in YYYY_MM_DD");
        o.addOption("db", true, "database");
        o.addOption("dbUser", true, "db user");
        o.addOption("dbPassword", true, "db user password");
        if (args.length == 0) {
            printHelp(o);
        }
        return new GnuParser().parse(o, args);

    }

    private void printHelp(Options o) {
        HelpFormatter h = new HelpFormatter();
        h.printHelp("java", o);
        System.exit(0);
    }

    private int getRunIdFromDate() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("select run_id from run where date = '" + cli.getOptionValue("date") + "'");
        int id = -1;
        while (rs.next()) {
            id = rs.getInt("run_id");
        }
        if (id == -1) {
            System.out.println("Run not found");
            System.exit(0);
        }
        return id;
    }

    private ArrayList<Integer> getSamplesForRun(int runId) throws SQLException {
        ArrayList<Integer> list = new ArrayList<Integer>();
        ResultSet rs = conn.createStatement().executeQuery("select sample_id from sample where run_id = " + runId);
        while (rs.next()) {
            list.add(rs.getInt("sample_id"));
        }
        Collections.sort(list);
        return list;
    }

    private void deleteRpdResultData(ArrayList<Integer> samples) {
        for (int id : samples) {
            String sql = "delete from rdp_result_data where sample_id = " + id;
            int rows = 0;
            try {
                rows = conn.createStatement().executeUpdate(sql);
            } catch (SQLException e) {
                System.out.println("ERROR deleteRdpResultData:" + sql);
                e.printStackTrace();
                System.exit(0);
            }
            System.out.println("deleted " + rows + " from rdp_result_data for sample " + id);
        }
    }

    private void deleteRdpSummaryData(ArrayList<Integer> samples) {
        for (int id : samples) {
            String sql = "delete from rdp_summary_data where sample_id = " + id;
            int rows = 0;
            try {
                rows = conn.createStatement().executeUpdate(sql);
            } catch (SQLException e) {
                System.out.println("ERROR deleteRdpSummaryData:" + sql);
                e.printStackTrace();
                System.exit(0);
            }
            System.out.println("deleted " + rows + " from rdp_summary_data for sample " + id);
        }
    }

    private void deleteSampleData(ArrayList<Integer> samples) throws SQLException {
        for (int id : samples) {
            String sql = "delete from sample_data where sample_id = " + id;
            int rows = 0;
            try {
                rows = conn.createStatement().executeUpdate(sql);
            } catch (SQLException sQLException) {
                System.out.println("ERROR deleteSampleData:" + sql);
                sQLException.printStackTrace();
                System.exit(0);
            }
            System.out.println("deleted " + rows + " from sample_data for sample " + id);
        }
    }

    private void deleteSample(int runId) throws SQLException {
        int rows = conn.createStatement().executeUpdate("delete from sample where run_id = " + runId);
        System.out.println("deleted " + rows + " samples for run " + runId);
    }

    private void deleteRunData(int runId) throws SQLException {
        int rows = conn.createStatement().executeUpdate("delete from run_data where run_id = " + runId);
        System.out.println("deleted " + rows + " reads for run " + runId);
    }

    private void deleteRun(int runId) throws SQLException {
        int rows = conn.createStatement().executeUpdate("delete from run where run_id = " + runId);
        System.out.println("deleted run " + runId);
    }

    private void deleteSampleCounts(ArrayList<Integer> samples) {
        for (int id : samples) {
            String sql = "delete from sample_count where sample_id = " + id;
            int rows = 0;
            try {
                rows = conn.createStatement().executeUpdate(sql);
            } catch (SQLException sQLException) {
                System.out.println("ERROR deleteSampleCounts:" + sql);
                sQLException.printStackTrace();
                System.exit(0);
            }
            System.out.println("deleted " + rows + " from sample_count for sample " + id);
        }
    }

    private void deleteRunCounts(int runId) throws SQLException {
        int rows = conn.createStatement().executeUpdate("delete from run_count where run_id = " + runId);
        System.out.println("deleted " + rows + " from run_count for run " + runId);
    }
}