package edu.uci.ics.textdb.common.field;

public class Span {
    private int start;
    private int end;
    
    public Span(int start, int end){
        this.start = start;
        this.end = end;
    }
    
    public int getStart() {
        return start;
    }
    
    public int getEnd() {
        return end;
    }
}
