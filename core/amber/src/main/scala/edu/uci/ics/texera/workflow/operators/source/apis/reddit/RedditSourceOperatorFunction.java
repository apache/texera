package edu.uci.ics.texera.workflow.operators.source.apis.reddit;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RedditSourceOperatorFunction {
    None("none"),

    Controversial("controversial"),

    Gilded("gilded"),

    Hot("hot"),

    New("new"),

    Rising("rising"),

    Top("top");

    private final String name;

    RedditSourceOperatorFunction(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
