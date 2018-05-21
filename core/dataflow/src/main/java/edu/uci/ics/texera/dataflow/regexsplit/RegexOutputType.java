package edu.uci.ics.texera.dataflow.regexsplit;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RegexOutputType {
    ONE_TO_ONE(RegexOutputTypeName.ONE_TO_ONE),
    ONE_TO_MANY(RegexOutputTypeName.ONE_TO_MANY);
    
    private final String name;
    
    private RegexOutputType(String name) {
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
    
    public class RegexOutputTypeName {
        public static final String ONE_TO_ONE = "one to one";
        public static final String ONE_TO_MANY = "one to many";
    }

}
