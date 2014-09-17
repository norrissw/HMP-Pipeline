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
public class TrimmedRead {

    private StringBuilder builder;
    private String header;

    public TrimmedRead(String header) {
        this.header = header;
        builder = new StringBuilder();
    }

    public void addSequence(String seq) {
        builder.append(seq);
    }

    public String getHeader() {
        return header;
    }

    public String getSequence(int length) {
        String seq = builder.toString();
        if (seq.length() < length) {
            return seq;
        } 
        return builder.toString().substring(0, length);
    }

}
