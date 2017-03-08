package edu.uci.ics.textdb.web;

import edu.uci.ics.textdb.web.response.TextdbWebResponse;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * A custom exception class for the textdb-web module, that gracefully handles any exceptions and
 * propagates a meaningful message as an API response.
 * Created by kishorenarendran on 2/24/17.
 */
public class TextdbWebException extends WebApplicationException {
    public TextdbWebException() {
        super(Response.status(400).build());
    }

    public TextdbWebException(String message) {
        super(Response.status(400).entity(new TextdbWebResponse(1, message))
                .type(MediaType.APPLICATION_JSON_TYPE).build());
    }
}