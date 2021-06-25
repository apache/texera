package edu.uci.ics.texera.web.response;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a response bean class, that is required for
 * Jackson to serialize an object of this class to JSON
 * Created by kishore on 10/4/16.
 */
public class GenericWebResponse {
    private int code;
    private String message;
    
    public static GenericWebResponse generateSuccessResponse() {
        return new GenericWebResponse(0, "success");
    }

    public GenericWebResponse() {
        // Default constructor is required for Jackson JSON serialization
    }

    public GenericWebResponse(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @JsonProperty
    public int getCode() {
        return code;
    }

    @JsonProperty
    public String getMessage() {
        return message;
    }
}
