package edu.uci.ics.textdb.web.response;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a response bean class, that is required for
 * Jackson to serialize an object of this class to JSON
 * Created by kishore on 10/4/16.
 */
public class SampleResponse {
    private int id;
    private String text;

    public SampleResponse() {
        // Default constructor is required for Jackson JSON serialization
    }

    public SampleResponse(int id, String text) {
        this.id = id;
        this.text = text;
    }

    @JsonProperty
    public int getId() {
        return id;
    }

    @JsonProperty
    public String getText() {
        return text;
    }
}
