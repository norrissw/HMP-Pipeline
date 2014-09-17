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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.util.MathUtils;

public class Read {

    private StringBuilder sequenceBuilder;
    private String header;
    private LinkedHashMap<String, SummaryStatistics> rdpResultHash;
    private String id;

    public Read() {
    }

    public Read(String header, Properties props) {
        sequenceBuilder = new StringBuilder();
        rdpResultHash = new LinkedHashMap<String, SummaryStatistics>();
        this.header = header;
        String delimiter = props.getProperty("readDelimiter");
        if (delimiter.equalsIgnoreCase("space")) {
            delimiter = "\\s+";
        }
        String[] headerData = header.split(delimiter);
        this.id = headerData[Integer.valueOf(props.getProperty("readNameIndex"))].replace(">", "");
    }

    public String getHeader() {
        return header;
    }

    public void setID(String id) {
        this.id = id;
    }

    public String getSequence() {
        return sequenceBuilder.toString();
    }

    public void appendSequence(String sequence) {
        sequenceBuilder.append(sequence);
    }

    public void addRDPResult(String taxa, double value) {
        addToHash(rdpResultHash, taxa, value);
    }

    private void addToHash(HashMap<String, SummaryStatistics> hash, String key, double value) {
        if (hash.containsKey(key)) {
            SummaryStatistics stat = hash.get(key);
            stat.addValue(value);
        } else {
            SummaryStatistics stat = new SummaryStatistics();
            stat.addValue(value);
            hash.put(key, stat);
        }
    }

    public double getBootstrapMean(String taxa) {
        return rdpResultHash.get(taxa).getMean();
    }

    public double getBootstrapSD(String taxa) {
        return rdpResultHash.get(taxa).getStandardDeviation();
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Read other = (Read) obj;
        if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }

    public String getRDPResultString() {
        String s = "";
        for (Iterator<String> i = rdpResultHash.keySet().iterator(); i.hasNext();) {
            String key = i.next();
            double mean = MathUtils.round(rdpResultHash.get(key).getMean(), 3);
            double sd = MathUtils.round(rdpResultHash.get(key).getStandardDeviation(), 3);
            s += " " + key + "; " + mean + "; " + sd + ";";

        }
        return s.replaceFirst(" ", "");
    }
}
