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
import java.util.LinkedHashMap;
import java.util.Properties;

public class RDPResult {

    private String header;
    private String id;
    private LinkedHashMap<String, Double> result;
    private String[] keyOrder;

    public RDPResult() {
    }

    public RDPResult(String header, Properties props) {
        this.header = header;
        String delimiter = props.getProperty("resultDelimiter");
        if (delimiter.equalsIgnoreCase("space")) {
            delimiter = "\\s+";
        }
        String[] headerData = header.split(delimiter);
        this.id = headerData[Integer.valueOf(props.getProperty("resultNameIndex"))].replace(">", "");
        result = new LinkedHashMap<String, Double>();
    }

    public String getHeader() {
        return header;
    }

    public String getId() {
        return id;
    }

    public void addResult(String resultLine) {
        String[] data = resultLine.split(";");
        for (int i = 0; i < data.length - 1; i += 2) {
            String key = data[i].trim();
            result.put(key, Double.valueOf(data[i + 1].trim()));
        }
    }

    public HashMap<String, Double> getResult() {
        return result;
    }
}