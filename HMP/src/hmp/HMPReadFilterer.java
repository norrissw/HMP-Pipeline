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
import data.CountData;
import data.TagCollection;
import data.PrimerCollection;
import data.SequencingRead;
import data.SequencingReadCollection;
import db.MySQLConnector;
import io.FileLister;
import java.sql.Connection;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import opt.OptionHolder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import util.Version;

public class HMPReadFilterer {

    private String outputDirectory = "filtered";
    private Connection conn;
    private boolean runInDB = false;
    private boolean sampleInDB = false;
    private boolean rawRunDataInDB = false;
    private boolean runDataCleared;
    private OptionHolder optionHolder;

    /*
     * default options
     */
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException, ParseException {
        HMPReadFilterer f = new HMPReadFilterer();
        f.run(args);
    }

    private OptionHolder createOptions(String[] args) throws ParseException {
        Options opt = new Options();
        OptionHolder holder = new OptionHolder();
        opt.addOption("sourceDir", true, "source directory");
        opt.addOption("sourceExtension", true, "extension of files in source directory (default=fna");
        opt.addOption("tagMappingFile", true, "file containing tag-sequence-sample mappling (e.g., sequencing_sample.txt");
        opt.addOption("primerFile", true, "file containing degenerate primers");
        opt.addOption("minReadLength", true, "minimum read length (default=200)");
        opt.addOption("maxReadLength", true, "maximum read length (default=540)");
        opt.addOption("filterByQuality", true, "whether or not to filter by quality (T/F; default=T)");
        opt.addOption("minQualityAverage", true, "if filtering by quality, then what should the average be (default=20)");
        opt.addOption("removeTag", true, "whether or not to remove tag from sequence (T/F; default=T)");
        opt.addOption("removePrimer", true, "whether or not to remove primer from sequence (T/F; default=T)");
        opt.addOption("minQualityScore", true, "minimum quality score to keep (default=10)");
        opt.addOption("maxMinQualityProportion", true, "max proportion (not percent) of min quality scores (default=0.1)");
        opt.addOption("db", true, "database name");
        opt.addOption("dbUser", true, "database user name");
        opt.addOption("dbPassword", true, "database user password");

        if (args.length == 0) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("ReadFilterer " + Version.getVersion(), opt);
            System.exit(0);
        } else {
            CommandLine c = new GnuParser().parse(opt, args);

            /*
             * Non default options
             */
            holder.setSourceDir(c.getOptionValue("sourceDir"));
            holder.setTagMappingFile(c.getOptionValue("tagMappingFile"));
            holder.setDbUser(c.getOptionValue("dbUser"));
            holder.setDbPassword(c.getOptionValue("dbPassword"));
            holder.setDb(c.getOptionValue("db"));

            /*
             * Default options
             */

            if (c.hasOption("primerFile")) {
                holder.setPrimerFile(c.getOptionValue("primerFile"));
            }

            if (c.hasOption("minReadLength")) {
                holder.setMinReadLength(Integer.valueOf(c.getOptionValue("minReadLength")));
            }

            if (c.hasOption("maxReadLength")) {
                holder.setMaxReadLength(Integer.valueOf(c.getOptionValue("maxReadLength")));
            }

            if (c.hasOption("removePrimer")) {
                holder.setRemovePrimer(c.getOptionValue("removePrimer"));
            }

            if (c.hasOption("removeTag")) {
                holder.setRemoveTag(c.getOptionValue("removeTag"));
            }

            if (c.hasOption("sourceExtension")) {
                holder.setSourceExtension(c.getOptionValue("sourceExtension"));
            }

            if (c.hasOption("filterByQuality")) {
                holder.setFilterByQuality(c.getOptionValue("filterByQuality"));
            }

            if (c.hasOption("minQualityScore")) {
                holder.setMinQualityScore(Double.valueOf(c.getOptionValue("minQualityScore")));
            }

            if (c.hasOption("maxMinQualityProportion")) {
                holder.setMaxMinQualityProportion(Double.valueOf(c.getOptionValue("maxMinQualityProportion")));
            }

            if (c.hasOption("minQualityAverage")) {
                holder.setMinQualityAverage(Double.valueOf(c.getOptionValue("minQualityAverage")));
            }
            return holder;
        }
        return null;
    }

    private void run(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException, ParseException {
        optionHolder = createOptions(args);
        CountData runCountData = new CountData();
        MySQLConnector mysql = new MySQLConnector("localhost", optionHolder.getDb(), optionHolder.getDbUser(), optionHolder.getDbPassword());
        conn = mysql.getConnection();
        TagCollection tags = new TagCollection(optionHolder.getTagMappingFile());
        PrimerCollection primers = new PrimerCollection(optionHolder.getPrimerFile());
        FileLister lister = new FileLister(optionHolder.getSourceDir(), new String[]{optionHolder.getSourceExtension().replace("\\.", "")}, false);
        Collection<File> readCol = lister.getFiles();
        ArrayList<File> readFiles = new ArrayList<File>(readCol);
        createDirectory(readFiles.get(0).getParent() + "/" + outputDirectory);
        PrintWriter logger = new PrintWriter(new FileWriter(readFiles.get(0).getParent() + "/" + outputDirectory + "/filter.log"));
        PrintWriter debug = new PrintWriter(new FileWriter("debug.log"));
        for (File readFile : readFiles) {
            SequencingReadCollection readCollection = new SequencingReadCollection(readFile, tags, primers, optionHolder, runCountData);
            System.out.println(readFile.getName());
            importRawReads(readFile.getParentFile().getName().replaceAll("_", "-"), readCollection);
            outputGoodReadsByTag(readFile, readCollection, tags, debug);
            printBadReads(readFile, readCollection);

            /*
             * write logging information for files
             */
            logger.println(readFile.getName());
            logger.println("Total reads: " + readCollection.getOriginalReadNum());
            logger.println("length: good=" + readCollection.getGoodLengthNum() + " bad=" + readCollection.getBadLengthNum());
            logger.println("tag: good=" + readCollection.getGoodTagNum() + " bad=" + readCollection.getBadTagNum());
            logger.println("primer: good=" + readCollection.getGoodPrimerNum() + " bad=" + readCollection.getBadPrimerNum());
            logger.println("quality: good=" + readCollection.getGoodQualityNum() + " bad=" + readCollection.getBadQualityNum());
            logger.println();
            logger.flush();
        }
        logger.close();
        debug.close();
        loadRunCounts(readFiles, runCountData);
        System.out.println("Done!");
    }

    private boolean getBoolean(String s) {
        if (s.toLowerCase().startsWith("t")) {
            return true;
        }
        return false;
    }

    private void outputGoodReadsByTag(File readFile, SequencingReadCollection readCollection, TagCollection tags, PrintWriter debug) throws IOException, SQLException {
        String runDate = readFile.getParentFile().getName().replaceAll("_", "-");
        HashMap<String, PrintWriter> writerMap = new HashMap<String, PrintWriter>();
        HashMap<Integer, CountData> countMap = new HashMap<Integer, CountData>();
        String x;
        String y;
        String sampleName = readFile.getName().split("_")[0];
        File dbFile = new File("sample_data.txt");
        PrintWriter dbtemp = new PrintWriter(new FileWriter(dbFile));
        for (int region : tags.getRegions()) {
            if (tags.regionHasTags(region) && region == readCollection.getRegion()) {
                System.out.println("Processing region " + region);
                for (String tag : tags.getTagsForRegion(region)) {
                    createDBStructure(runDate, tag, region, tags, debug);
                    System.out.println("\tCreated writer for tag " + tag + " in region " + region + " " + runDate);
                    writerMap.put(tag, new PrintWriter(new FileWriter(readFile.getParent() + "/" + outputDirectory + "/" + tags.getSampleForTagFromRegion(region, tag) + "_" + readFile.getParentFile().getName() + "_" + region + "_" + tag + "_reads_cjf.fa")));
                }
                System.out.println("\tImporting/printing reads");
                for (SequencingRead read : readCollection.getProcessedReads()) {
                    String readTag = null;
                    for (String tag : tags.getTagsForRegion(region)) {
                        if (read.getSequence().startsWith(tag)) {
                            readTag = tag;
                            break;
                        }
                    }
                    if (readTag != null) {
                        String seq = read.getSequence();
                        String primerSeq = seq.replaceFirst(readTag, "").substring(0, 20);
                        String subseq = null;
                        boolean removeTag = true;
                        boolean removePrimer = true;

                        removeTag = getBoolean(optionHolder.getRemoveTag());
                        removePrimer = getBoolean(optionHolder.getRemovePrimer());

                        if (removeTag && removePrimer) {
                            subseq = read.getSequence().replaceFirst(readTag, "").substring(20);
                        } else if (removeTag && !removePrimer) {
                            subseq = read.getSequence().replaceFirst(readTag, "");
                        } else if (!removeTag && removePrimer) {
                            System.out.println("Invalid option: Cannot remove primer without removing tag");
                            System.exit(0);
                        } else {
                            subseq = seq;
                        }
                        writerMap.get(readTag).println(">" + tags.getSampleForTagFromRegion(region, readTag) + "|" + read.getID() + "|" + seq.length() + "|" + subseq.length() + "|" + read.getQualityMean() + "|" + readTag + "|" + primerSeq);
                        writerMap.get(readTag).println(subseq);
                        writeDbRecord(dbtemp, countMap, tags.getSampleForTagFromRegion(region, readTag), read.getID(), subseq, subseq.length(), seq.length(), read.getQualityMean());
                    }
                }

                for (String key : writerMap.keySet()) {
                    writerMap.get(key).close();
                }
            }
        }
        dbtemp.close();
        loadSampleData(dbFile);
        loadSampleCounts(countMap);
    }

    private void createDirectory(String string) {
        File dir = new File(string);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

    private void printBadReads(File readFile, SequencingReadCollection readCollection) throws IOException {
        String filePrefix = readFile.getName().split("\\.")[0];
        filePrefix = readFile.getParent() + "/" + filePrefix;
        PrintWriter shortWriter = new PrintWriter(new FileWriter(filePrefix + "_shortReads.txt"));
        PrintWriter badTagWriter = new PrintWriter(new FileWriter(filePrefix + "_badTag.txt"));
        PrintWriter badPrimerWriter = new PrintWriter(new FileWriter(filePrefix + "_badPrimer.txt"));
        PrintWriter badQualityWriter = new PrintWriter(new FileWriter(filePrefix + "_badQuality.txt"));

        for (SequencingRead read : readCollection.getBadLengthReads()) {
            printBadRead(shortWriter, read);
        }

        for (SequencingRead read : readCollection.getBadTagReads()) {
            printBadRead(badTagWriter, read);
        }

        for (SequencingRead read : readCollection.getBadPrimerReads()) {
            printBadRead(badPrimerWriter, read);
        }

        for (SequencingRead read : readCollection.getBadQualityReads()) {
            printBadRead(badQualityWriter, read, read.getQualityMean());
        }


        shortWriter.close();
        badTagWriter.close();
        badPrimerWriter.close();
        badQualityWriter.close();
    }

    private void printBadRead(PrintWriter pw, SequencingRead read) {
        pw.println(">" + read.getID() + "|" + read.getSequence().length());
        pw.println(read.getSequence());
    }

    private void printBadRead(PrintWriter pw, SequencingRead read, double qualityMean) {
        pw.println(">" + read.getID() + "|" + read.getSequence().length() + "|quality=" + read.getQualityMean());
        pw.println(read.getSequence());
    }

    private void createDBStructure(String runDate, String tag, int region, TagCollection tags, PrintWriter debug) throws SQLException {
        System.out.println("\tChecking DB Structure");
        Statement s = conn.createStatement();
        ResultSet rs;
        if (!runInDB) {
            rs = s.executeQuery("select * from run where date ='" + runDate + "'");
            rs.last();
            int rowCount = rs.getRow();
            rs.beforeFirst();
            if (rowCount == 0) {
                System.out.println("rowCount == 0");
                createRunInDatabase(s, runDate);
            } else {
                runInDB = true;
            }
        }

        if (!sampleInDB && runInDB) {
            String sample = tags.getSampleForTagFromRegion(region, tag);
            rs = s.executeQuery("select * from sample where sample_name ='" + sample + "'");
            rs.last();
            int rowCount = rs.getRow();
            rs.beforeFirst();
            if (rowCount == 0) {
                createSampleForRunInDatabase(s, runDate, tag, sample, region, debug);
            } else {
                deleteSampleDataFromDatabase(sample);
            }
        } else {
            sampleInDB = true;
        }
    }

    private int createRunInDatabase(Statement s, String runDate) throws SQLException {
        System.out.println(runDate + " not found, creating");
        s.execute("insert into run (date) values (\"" + runDate + "\")");
        ResultSet rs = s.executeQuery("select run_id from run where date = '" + runDate + "'");
        while (rs.next()) {
            return rs.getInt("run_id");
        }
        return 0;
    }

    private void createSampleForRunInDatabase(Statement s, String runDate, String tag, String sample, int region, PrintWriter debug) throws SQLException {
        System.out.println(sample + " not found, creating ");
        ResultSet rs = s.executeQuery("select run_id from run where date='" + runDate + "'");
        int runID = -1;
        int barcodeID = -1;
        while (rs.next()) {
            runID = rs.getInt("run_id");
        }
        rs = s.executeQuery("select barcode_id from barcodes where barcode_sequence=\"" + tag + "\"");
        while (rs.next()) {
            barcodeID = rs.getInt("barcode_id");
        }
        String sql = "insert into sample (run_id, sample_name, sample_barcode_id, plate_region, sample_description) values (" + runID + ",\"" + sample + "\"," + barcodeID + "," + region + "," + "\"test\")";
        debug.println(sql);
        debug.flush();
        try {
            System.out.println(sql);
            s.execute(sql);
        } catch (SQLException e) {
            System.out.println("barcode=" + tag);
            System.out.println(sql);
            e.printStackTrace();
        }
    }

    private void writeDbRecord(PrintWriter writer, HashMap<Integer, CountData> countMap, String sampleName, String readID, String subseq, int subseqLength, int originalLength, double qualityMean) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs;
        String sql = "select sample_id from sample where sample_name=\"" + sampleName + "\"";
//        System.out.println(sql);
        rs = s.executeQuery(sql);
        int sampleID = -1;
        while (rs.next()) {
            sampleID = rs.getInt("sample_id");
        }
        writer.println(sampleID + "," + readID + "," + subseq + "," + subseqLength + "," + originalLength + "," + qualityMean);

        CountData count;
        if (countMap.containsKey(sampleID)) {
            count = countMap.get(sampleID);
        } else {
            count = new CountData();
            countMap.put(sampleID, count);
        }
        count.addRead(subseq);
    }

    private void deleteSampleDataFromDatabase(String sampleName) throws SQLException {
        System.out.println("\tRemoving data from database for sample " + sampleName);
        ResultSet rs;
        Statement s = conn.createStatement();
        rs = s.executeQuery("select sample_id from sample where sample_name=\"" + sampleName + "\"");
        int sampleID = -1;
        while (rs.next()) {
            sampleID = rs.getInt("sample_id");
        }

        int returnVal = s.executeUpdate("delete from rdp_summary_data where sample_id=\"" + sampleID + "\"");
        System.out.println("\tDeleted " + returnVal + " rows from rdp_summary_data");

        returnVal = s.executeUpdate("delete from rdp_result_data where sample_id=\"" + sampleID + "\"");
        System.out.println("\tDeleted " + returnVal + " rows from rdp_result_data");

        returnVal = s.executeUpdate("delete from sample_data where sample_id = \"" + sampleID + "\"");
        System.out.println("\tDeleted " + returnVal + " rows from sample_data");
    }

    private void importRawReads(String runDate, SequencingReadCollection readCollection) throws SQLException, IOException {
        System.out.println("importing raw reads for " + runDate);
        ResultSet rs = conn.createStatement().executeQuery("select * from run where date=\"" + runDate + "\"");
        int runId = 0;
        while (rs.next()) {
            runId = rs.getInt("run_id");
        }

        if (runId == 0) {
            System.out.println("importRawReads says runID == 0");
            runId = createRunInDatabase(conn.createStatement(), runDate);
            runInDB = true;
        }

        if (!runDataCleared) {
            System.out.println("Run data needs to be cleared");
            try {
                int rows;
                // clear sample + sample_data
                rs = conn.createStatement().executeQuery("select * from sample where run_id = " + runId);
                while (rs.next()) {
                    int sampleId = rs.getInt("sample_id");
                    rows = conn.createStatement().executeUpdate("delete from sample_data where sample_id = " + sampleId);
                    System.out.println("Deleted " + rows + " from sample_data for sample " + sampleId);
                }

                // clear run_data
                rows = conn.createStatement().executeUpdate("delete from run_data where run_id=" + runId);
                System.out.println("Deleted " + rows + " rows from run_data");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            runDataCleared = true;
        }

        File runDataFile = new File("run_data.txt");
        PrintWriter writer = new PrintWriter(new FileWriter(runDataFile));

        for (SequencingRead read : readCollection.getAllReads()) {
            writer.println(runId + "," + read.getID() + "," + read.getSequence().length());
        }
        writer.close();
        String sql = "load data infile \'" + runDataFile.getAbsolutePath() + "\' into table run_data fields terminated by \",\" lines terminated by \"\\n\" (run_id, read_id, read_length)";


        try {
            System.out.println("Loading raw run data");
            conn.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(sql);
            System.out.println(e.getMessage());
        }

    }

    private void loadSampleData(File file) throws SQLException {
        String sql = "load data infile \'" + file.getAbsolutePath() + "\' into table sample_data fields terminated by \",\" lines terminated by \"\\n\" (sample_id, read_id, sequence, sequence_length, sequence_orig_length, quality_avg)";
        System.out.println(sql);
        try {
            conn.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadSampleCounts(HashMap<Integer, CountData> countMap) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("insert into sample_count (sample_id, read_count, base_count) values(?,?,?)");
            for (int id : countMap.keySet()) {
                CountData cd = countMap.get(id);
                ps.setInt(1, id);
                ps.setInt(2, cd.getReadCount());
                ps.setInt(3, cd.getBaseCount());
                ps.executeUpdate();
            }
        } catch (SQLException ex) {

        }

    }

    private void loadRunCounts(ArrayList<File> files, CountData runCountData) {
        String runDate = files.get(0).getParentFile().getName().replaceAll("_", "-");
        int runId = -1;
        try {
            ResultSet rs = conn.createStatement().executeQuery("select run_id from run where date = '" + runDate + "'");
            while (rs.next()) {
                runId = rs.getInt("run_id");
            }
            if (runId == -1) {
                throw new RuntimeException("can't find run in db, so can't insert counts");
            }
            int rows = conn.createStatement().executeUpdate("insert into run_count (run_id, read_count, base_count) values (" + runId + "," + runCountData.getReadCount() + "," + runCountData.getBaseCount() + ")");

        } catch (SQLException sQLException) {
        }
    }
}
