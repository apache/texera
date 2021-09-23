package edu.uci.ics.texera.workflow.operators.dictionary;

import com.fasterxml.jackson.annotation.JsonValue;

public enum MatchingType {
    SUBSTRING_SCANBASED("Scan"),

    CONJUNCTION_INDEXBASED("Conjunction"),

    PHRASE_INDEXBASED("Phrase");

    private final String name;

    private MatchingType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
