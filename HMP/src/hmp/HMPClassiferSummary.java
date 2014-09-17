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
import com.mysql.jdbc.PreparedStatement;
import data.PrimerContainer;
import data.ReadContainer;
import data.Read;
import data.Result;
import data.ResultContainer;
import data.ResultEntry;
import data.Summary;
import data.SummaryEntry;
import db.MySQLConnector;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class HMPClassiferSummary {

    private Connection conn;
    private Connection microbiome;
    private PrimerContainer primerContainer;
    private ReadContainer readContainer;
    private String tab = "\t";
    private ArrayList<String> rdpLevelCols;
    private CommandLine cli;
    private PreparedStatement levelStatement;
    /**
     *
     * default cli values
     *
     */
    private double cutoff = 0.8;
    private String readDelimiter = "\\|";
    private int readIdIndex = 1;
    private String tagDelimiter = "_";
    private int tagIndex = 5;
    private String fileSuffix = "_rdp.out";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, SQLException, IOException, ParseException {
        HMPClassiferSummary summary = new HMPClassiferSummary();
        summary.run(args);
    }

    private void run(String[] args) throws ClassNotFoundException, FileNotFoundException, SQLException, IOException, ParseException {

        /*
         * Get command line options
         */

        cli = getOptions(args);

        /*
         * Determine if rdp results is file or directory
         */

        File rdpFile = new File(cli.getOptionValue("rdpDir"));
        boolean isDirectory = false;
        if (rdpFile.isDirectory()) {
            isDirectory = true;
        }

        /*
         * Setup database connections
         *
         */
        MySQLConnector connector = new MySQLConnector("localhost", "biosql", "biosql", "biosql");
        conn = connector.getConnection();
        connector = new MySQLConnector("localhost", cli.getOptionValue("db"), cli.getOptionValue("dbUser"), cli.getOptionValue("dbPassword"));
        microbiome = connector.getConnection();

        /*
         * create output directory
         */
        createOutputDir("summary");

        /*
         * Determine which columns are available for rdp levels
         */

        rdpLevelCols = new ArrayList<String>();
        ResultSet rsCol = microbiome.createStatement().executeQuery("select * from rdp_result_data limit 10");
        ResultSetMetaData rsMeta = rsCol.getMetaData();
        int colNumber = rsMeta.getColumnCount();
        for (int i = 1; i < colNumber + 1; i++) {
            rdpLevelCols.add(rsMeta.getColumnName(i));
        }

        /*
         * process file/files
         */

        PrintWriter writer = new PrintWriter(new FileWriter("summary_all.txt"));
        writer.println("sampleid|level|taxa|count|len|sd|gc|sd|skew");
        if (isDirectory) {
            File[] files = getFiles(rdpFile);
            int count = 0;
            for (int i = 0; i < files.length; i++) {
                if (count >= 0) {
                    System.out.println("===PROCESSING FILE " + i + " ===");
                    processReads(files[i], writer);
                    count++;
                }
            }
        } else {
            processReads(rdpFile, writer);
        }
        writer.close();
        System.out.println("Done!");
    }

    private void processReads(File file, PrintWriter writer) throws FileNotFoundException, SQLException, IOException {
        System.out.println("Processing reads");
        /*
         * Import and store the raw reads
         * Set tag from the file name
         */
        readContainer = getReads(file);
        readContainer.setTag(file.getName().split(tagDelimiter)[tagIndex]);

        /*
         * Set the sampleID
         */
        int sampleId = getSampleIDFromFileName(file.getName());

        /*
         * Get the RDP results
         */
        ResultContainer rdpResults = getRawRdpResults(file.getAbsolutePath(), readContainer);
        importRdpResultDataIntoDB(rdpResults);
        Summary summaryData = summarizeRdpResults(rdpResults);
        importRdpSummaryDataIntoDB(summaryData, sampleId, writer);

//        HashMap<String, Integer> levelHash = processReadsForLevel(countData);
//        printTaxaHash(taxaCounter, file.getName());
//        printLevelhash(levelHash, file.getName());
    }

    private ResultContainer getRawRdpResults(String fileName, ReadContainer reads) throws FileNotFoundException, SQLException {
        System.out.println("Getting RDP Results");
        ResultContainer results = new ResultContainer((float) cutoff);
        Scanner scan = new Scanner(new File(fileName));
        while (scan.hasNext()) {
            results.addResult(new Result(scan.nextLine(), (float) cutoff, readDelimiter, readIdIndex));
        }

        /*
         * Add read object to each rdp result
         * Used for stats calculation later on
         */
        for (Result r : results.getResults()) {
            r.addRead(reads.getRead(r.getId()));
        }
//
//        /*
//         * Add rank to reach result entry
//         */
//        System.out.println("Adding ranks for each taxa entry");
//        for (Result r : results.getResults()) {
//            for (ResultEntry e : r.getEntries()) {
//                e.setLevel(getTaxaLevelFromDB(e.getTaxa(), e.getIndex()));
//            }
//
//            /*
//             * get level for processed data, using taxa information from
//             * pre processed array.  this ensures that unclassified entries
//             * are at the proper level
//             */
//
//            for (ResultEntry e : r.getProcessedEntries()) {
//                e.setLevel(getTaxaLevelFromDB(r.getEntries()[e.getIndex()].getTaxa(), e.getIndex()));
//            }
//        }

        return results;
    }

    private int getSampleIDFromFileName(String fileName) throws SQLException {
        String splitString = "_2010";
        if (fileName.contains("_2009")) {
            splitString = "_2009";
        }
        /*
         * for CEFos
         */
        String[] data = null;
        if (fileName.contains("_2009") || fileName.contains("_2010")) {
            data = fileName.split(splitString);
        } else {
            System.out.println("Data is CEFos data!");
            data = fileName.split("\\.");
        }
        String sampleName = data[0];
        ResultSet rs = microbiome.createStatement().executeQuery("select sample_id from sample where sample_name=\"" + sampleName + "\"");
        while (rs.next()) {
            return rs.getInt("sample_id");
        }
        return 0;
    }

    private File[] getFiles(File directory) {
        return directory.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                if (name.endsWith(fileSuffix)) {
                    return true;
                }
                return false;
            }
        });
    }

    private void createOutputDir(String path) {
        File file = new File(cli.getOptionValue("rdpDir") + "/" + path);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    private ReadContainer getReads(File file) throws FileNotFoundException {
        String readFileName = cli.getOptionValue("sourceDir") + File.separator + file.getName().replace(fileSuffix, "");
        File readFile = new File(readFileName);
        System.out.println("Getting reads from " + readFile.getName() + " size=" + readFile.length() + " bytes");
        ReadContainer container = new ReadContainer();

        Scanner scan = new Scanner(readFile);
        String line = null;
        Read read = null;
        while (scan.hasNext()) {
            line = scan.nextLine();
            if (!line.isEmpty()) {
                if (line.startsWith(">")) {
                    read = new Read(line, readDelimiter, String.valueOf(readIdIndex));
                    container.addRead(read);
                } else {
                    read.addSequence(line);
                }
            }
        }
        System.out.println("Read " + readFile.getName() + " reads=" + container.getNumberOfReads());
        return container;
    }

    private void importRdpResultDataIntoDB(ResultContainer rdpResults) throws SQLException, IOException {
        if (rdpResults.getResults().size() > 0) {
            String readHeader = rdpResults.getResults().get(0).getRead().getHeader();
            String sampleName = readHeader.split("\\|")[0].replaceFirst(">", "");
            int sampleId = getSampleIdFromDB(sampleName);
            deleteRdpResultDataForSample(sampleId);

            System.out.println("Writing db import file");
            File rdpFile = new File("rdp_data.txt");
            PrintWriter writer = new PrintWriter(new FileWriter(rdpFile));
            PrintWriter error = new PrintWriter(new FileWriter("rdp_data_errors.txt"));
            writer.println(arrayListToString(rdpLevelCols, "|"));
            for (Result result : rdpResults.getResults()) {
                String[] taxaArray = createBlankStringArray(rdpLevelCols.size());
                for (ResultEntry entry : result.getResultEntries()) {
                    int index = rdpLevelCols.indexOf(entry.getLevel());
                    taxaArray[index] = entry.getTaxa();
                    taxaArray[index + 1] = String.valueOf(entry.getConfidence());
                    taxaArray[index + 2] = String.valueOf(0d);
                }
                writer.println(sampleId + "|" + result.getId() + arrayToString(taxaArray, "|"));
            }
            writer.close();
            error.close();
            bulkLoadRdpResultData(rdpFile);
        }
    }

    private void importRdpSummaryDataIntoDB(Summary summaryData, int sampleId, PrintWriter allWriter) throws IOException, SQLException {
        File file = new File("rdp_summary.txt");
        PrintWriter writer = new PrintWriter(new FileWriter(file));
        writer.println("sampleid|level|taxa|count|len|sd|gc|sd|skew");
        for (String level : summaryData.getLevelCounts().keySet()) {
            for (String taxa : summaryData.getLevelCounts().get(level).getTaxaCounts().keySet()) {
                SummaryEntry se = summaryData.getLevelCounts().get(level);
                writer.println(sampleId + "|" + level + "|" + taxa + "|" + se.getCountOf(taxa) + "|" + se.getLengthStats(taxa)[0] + "|" + se.getLengthStats(taxa)[1] + "|" + se.getGcStats(taxa)[0] + "|" + se.getGcStats(taxa)[1] + "|" + se.getGcSkew(taxa));
                allWriter.println(sampleId + "|" + level + "|" + taxa + "|" + se.getCountOf(taxa) + "|" + se.getLengthStats(taxa)[0] + "|" + se.getLengthStats(taxa)[1] + "|" + se.getGcStats(taxa)[0] + "|" + se.getGcStats(taxa)[1] + "|" + se.getGcSkew(taxa));
            }
        }
        writer.close();
        deleteRdpSummaryDataForSample(sampleId);
        bulkLoadRdpSummaryData(file);

    }

    private String getTaxaLevelFromDB(String taxa, int index) throws SQLException {
        taxa = taxa.replaceAll("\"", "");
        Statement s = conn.createStatement();
        String level = "Rank not found";
        ResultSet rs = s.executeQuery("select distinct node_rank from v_taxonomy where name='" + taxa + "'");
        while (rs.next()) {
            level = rs.getString("node_rank");
        }
        rs.close();
        s.close();

        if (level.equalsIgnoreCase("phylum") && index != 2) {
            level = "class";
        }
        return level;
    }

    private int getSampleIdFromDB(String sampleName) throws SQLException {
        int id = 0;
        ResultSet rs;
        Statement s = microbiome.createStatement();
        String sql = "select sample_id from sample where sample_name=\'" + sampleName + "\'";
        System.out.println(sql);
        rs = s.executeQuery(sql);
        while (rs.next()) {
            id = rs.getInt("sample_id");
        }
        return id;
    }

    private void deleteRdpResultDataForSample(int sampleId) throws SQLException {
        Statement s = microbiome.createStatement();
        String sql = "delete from rdp_result_data where sample_id=" + sampleId;
        int rows = 0;
        if (!cli.hasOption("test")) {
            rows = s.executeUpdate(sql);
        }
        System.out.println("Deleted " + rows + " from rdp_result_data for sample " + sampleId);
    }

    private void deleteRdpSummaryDataForSample(int sampleId) throws SQLException {
        int rows = 0;
        if (!cli.hasOption("test")) {
            rows = microbiome.createStatement().executeUpdate("delete from rdp_summary_data where sample_id=" + sampleId);
        }
        System.out.println("Deleted " + rows + " from rdp_summary_data for sample " + sampleId);
    }

    public String arrayListToString(ArrayList<String> list, String delim) {
        String s = "";
        for (String str : list) {
            s += delim + str;
        }
        return s.replaceFirst("\\" + delim, "");
    }

    public String arrayToString(String[] list, String delim) {
        String s = "";
        for (int i = 2; i < list.length; i++) {
            s += delim + list[i];
        }
        return s;
    }

    private String[] createBlankStringArray(int size) {
        String[] data = new String[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = "\\N";
        }
        return data;
    }

    private CommandLine getOptions(String[] args) throws ParseException {
        Options opt = new Options();
        opt.addOption("db", true, "database name");
        opt.addOption("dbUser", true, "database user name");
        opt.addOption("dbPassword", true, "password for database usage");
        opt.addOption("cutoff", true, "value for rdp meaningful cutoff (0.8)");
        opt.addOption("fileSuffix", true, "file suffix for rdp files to process");
        opt.addOption("sourceDir", true, "source directory of fasta files");
        opt.addOption("rdpDir", true, "directory containing rdp result files");
        opt.addOption("readDelimiter", true, "delimiter in read header ( '|' )");
        opt.addOption("readIdIndex", true, "index of read id in header using readDelimiter (1)");
        opt.addOption("tagDelimiter", true, "delimiter of file name ( '_' )");
        opt.addOption("tagIndex", true, "index of tag in file name using tagDelimiter (5)");
        opt.addOption("outputDir", true, "name of output directory (summary)");
        opt.addOption("test", false, "if running in test mode (no db upload)");

        if (args.length == 0) {
            printHelp(opt);
        }

        CommandLine c = new GnuParser().parse(opt, args);

        if (c.hasOption("cutoff")) {
            cutoff = Double.valueOf(c.getOptionValue("cutoff"));
        }

        if (c.hasOption("readDelimiter")) {
            readDelimiter = c.getOptionValue("readDelimiter");
        }

        if (c.hasOption("readIdIndex")) {
            readIdIndex = Integer.valueOf(c.getOptionValue("readIdIndex"));
        }

        if (c.hasOption("tagDelimiter")) {
            tagDelimiter = c.getOptionValue("tagDelimiter");
        }

        if (c.hasOption("tagIndex")) {
            tagIndex = Integer.valueOf(c.getOptionValue("tagIndex"));
        }

        if (c.hasOption("fileSuffix")) {
            fileSuffix = c.getOptionValue("fileSuffix");
        }
        return c;
    }

    private void printHelp(Options opt) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar <pathToJar>", opt);
        System.exit(0);
    }

    private void bulkLoadRdpResultData(File file) {
        String sql = "load data infile \'" + file.getAbsolutePath() + "\' into table rdp_result_data fields terminated by \"|\"  lines terminated by \"\\n\" ignore 1 lines";
        try {
            System.out.println("loading individual rdp result data into database");
            if (!cli.hasOption("test")) {
                microbiome.createStatement().executeUpdate(sql);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private Summary summarizeRdpResults(ResultContainer rdpResults) {
        System.out.println("Summarizing results");
        Summary summary = new Summary();
        for (Result r : rdpResults.getResults()) {
            for (ResultEntry e : r.getProcessedResultEntries()) {
                summary.addResultEntry(e, r);
            }
        }
        return summary;
    }

    private void bulkLoadRdpSummaryData(File file) {
        String sql = "load data infile \'" + file.getAbsolutePath() + "\' into table rdp_summary_data fields terminated by \"|\"  lines terminated by \"\\n\" ignore 1 lines";
        try {
            System.out.println("loading sample summary data into database");
            if (!cli.hasOption("test")) {
                microbiome.createStatement().executeUpdate(sql);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}