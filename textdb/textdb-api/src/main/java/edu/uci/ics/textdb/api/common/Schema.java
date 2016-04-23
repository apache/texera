package edu.uci.ics.textdb.api.common;

import java.util.List;

public class Schema {
    private List<Attribute> attributes;
    
    public Schema(List<Attribute> attributes){
        this.attributes = attributes;
    }
    
    public List<Attribute> getAttributes() {
        return attributes;
    }
}
