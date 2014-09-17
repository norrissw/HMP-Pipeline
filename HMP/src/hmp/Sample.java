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
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Sample {
    private String fileName;
    private String sampleNumber;
    private ArrayList<Read> readList;
    private String fileAbsolutePath;
    private HashMap<String, Read> readLookupHash;

    public Sample() {
    }

    public Sample(File file, Properties props) {
        this.fileName = file.getName();
        this.fileAbsolutePath = file.getAbsolutePath();
        String delimiter = props.getProperty("sampleDelimiter");
        int index = Integer.valueOf(props.getProperty("sampleNameIndex"));
        this.sampleNumber = fileName.split(delimiter)[index];
        readList = new ArrayList<Read>();
        readLookupHash = new HashMap<String, Read>();
    }

    public void addRead(Read read) {
        readList.add(read);
        readLookupHash.put(read.getId(), read);
    }

    public String getFileAbsolutePath() {
        return fileAbsolutePath;
    }

    public String getFileName() {
        return fileName;
    }

    public ArrayList<Read> getReadList() {
        return readList;
    }

    public String getSampleNumber() {
        return sampleNumber;
    }

    public Read getRead(String readID) {
        return readLookupHash.get(readID);
    }

}