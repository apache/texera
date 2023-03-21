package edu.uci.ics.texera.workflow.operators.source.fetcher;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DecodingMethod {
    UTF_8("UTF-8"),

    RAW_BYTES("RAW BYTES");

    private final String name;

    private DecodingMethod(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
