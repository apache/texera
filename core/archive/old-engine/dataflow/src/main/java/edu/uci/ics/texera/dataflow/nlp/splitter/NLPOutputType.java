package edu.uci.ics.texera.dataflow.nlp.splitter;

import com.fasterxml.jackson.annotation.JsonValue;

public enum NLPOutputType {
    ONE_TO_ONE("oneToOne"),
    ONE_TO_MANY("oneToMany");
    
    private final String name;
    
    private NLPOutputType(String name) {
        this.name = name;
    }
    
    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }
    
    @Override
    public String toString() {
        return this.name;
    }
}
