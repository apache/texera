package edu.uci.ics.texera.web;

import edu.uci.ics.texera.web.response.GenericWebResponse;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * A custom exception class for the web module, that gracefully handles any exceptions and
 * propagates a meaningful message as an API response.
 * Created by kishorenarendran on 2/24/17.
 */
public class TexeraWebException extends WebApplicationException {
    
    private static final long serialVersionUID = 4321691337540833526L;

    public TexeraWebException() {
        super(Response.status(400).build());
    }
    
    public TexeraWebException(Exception e) {
        super(e, Response.status(400).entity(new GenericWebResponse(1, e.getMessage()))
                .type(MediaType.APPLICATION_JSON_TYPE).build());
    }

    public TexeraWebException(String message) {
        super(message, Response.status(400).entity(new GenericWebResponse(1, message))
                .type(MediaType.APPLICATION_JSON_TYPE).build());
    }
}