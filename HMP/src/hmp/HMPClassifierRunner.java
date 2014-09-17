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
import io.FileLister;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;
import java.util.Scanner;

public class HMPClassifierRunner {

    private Properties props;
    File inputDir;
    File outputDir;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {
        HMPClassifierRunner rdp = new HMPClassifierRunner();
        rdp.run(args);
    }

    private void run(String[] args) throws FileNotFoundException, IOException {
        props = new Properties();
        props.load(new FileInputStream("properties"));
        inputDir = new File(props.getProperty("inputDir"));
        outputDir = new File(inputDir, props.getProperty("outputDir"));
        FileLister lister = new FileLister(inputDir, new String[]{props.getProperty("fileExtension")}, false);
        File[] files = sortFiles(lister.getFiles());
        doRDPClassification(files);
        System.out.println("Done!");

    }

    private void doRDPClassification(File[] files) throws FileNotFoundException, IOException {
        int fileCount = 0;
        for (File file : files) {
            System.out.println(file.getName());
            Scanner scan = new Scanner(file);
            String line = null;
            Sample sample = new Sample(file, props);
            Read read = null;
            while (scan.hasNext()) {
                line = scan.nextLine();
                if (line.startsWith(">")) {
                    read = new Read(line, props);
                    sample.addRead(read);
                } else {
                    read.appendSequence(line);
                }
            }
            runRDPOnSample(sample, fileCount);
//            printRdpResult(sample);
            fileCount++;
        }
    }

    private void runRDPOnSample(Sample sample, int count) throws IOException {
        String rdpCommand = "java -Xmx" + props.getProperty("rdpMemory") + " -jar " + props.getProperty("rdpClassifierDir") + File.separator + props.getProperty("rdpClassifierBin");
        createOutputDir(outputDir);
        File outputFile = new File(outputDir, sample.getFileName() + "_rdp.out");
        String propsFile = props.getProperty("rdpClassifierDir") + File.separator + props.getProperty("rdpTrainingDataDir") + File.separator + props.getProperty("rdpPropsFile");
        String command = rdpCommand + " -q" + sample.getFileAbsolutePath() + " -o" + outputFile.getAbsolutePath() + " -t" + propsFile;
        int iterations = Integer.valueOf(props.getProperty("iterations"));
        String line;
        if (!outputFile.exists()) {
            for (int i = 0; i < iterations; i++) {
                System.out.print(count + ": sample " + sample.getSampleNumber() + "\r");
                Process proc = Runtime.getRuntime().exec(command);
                BufferedReader Resultset = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                while ((line = Resultset.readLine()) != null) {
                    //System.out.println(line);
                }
//                processRDPResult(sample, outputFile);
            }
        }
    }

//    private void processRDPResult(Sample sample, File outputFile) throws FileNotFoundException {
//        ArrayList<RDPResult> results = new ArrayList<RDPResult>();
//        Scanner scan = new Scanner(outputFile);
//        String line = null;
//        RDPResult result = null;
//        while (scan.hasNext()) {
//            line = scan.nextLine();
//            if (line.startsWith(">")) {
//                result = new RDPResult(line, props);
//                results.add(result);
//            } else {
//                result.addResult(line);
//            }
//        }
//
//        for (Iterator<RDPResult> i = results.iterator(); i.hasNext();) {
//            result = i.next();
//            Read read = sample.getRead(result.getId());
//            if (read != null) {
//                for (Iterator<String> j = result.getResult().keySet().iterator(); j.hasNext();) {
//                    String taxa = j.next();
//                    double value = result.getResult().get(taxa);
//                    read.addRDPResult(taxa, value);
//                }
//            }
//        }
//    }

    private File[] sortFiles(Collection<File> files) {
        return sortFiles(files.toArray(new File[0]));
    }

    private File[] sortFiles(File[] files) {
        ArrayList<File> list = new ArrayList<File>();
        for (int i = 0; i < files.length; i++) {
            list.add(files[i]);
        }
        Collections.sort(list, new Comparator<File>() {

            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        return list.toArray(files);
    }

//    private void printRdpResult(Sample sample) throws IOException {
//        String outDir = props.getProperty("outputDir");
//        String outputName = outDir + "/" + sample.getFileName() + "_rdpresults.out";
//        if (!new File(outputName).exists()) {
//            PrintWriter writer = new PrintWriter(new FileWriter(outputName));
//            writer.println("iterations=" + props.getProperty("iterations"));
//            for (Iterator<Read> i = sample.getReadList().iterator(); i.hasNext();) {
//                Read read = i.next();
//                writer.println(read.getId() + "; " + read.getRDPResultString());
//            }
//            writer.close();
//        }
//    }

    private void createOutputDir(File outputDir) {
        if (!outputDir.exists()) {
            System.out.println("Creating " + outputDir.getAbsolutePath());
            outputDir.mkdir();
        }
    }

}