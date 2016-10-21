package edu.uci.ics.textdb.web.resource;

import edu.uci.ics.textdb.web.response.SampleResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * This class is a Resource class. Classes like these are used to handle requests
 * on a specific URL
 * Created by kishore on 10/4/16.
 */
@Path("/sample")
@Produces(MediaType.APPLICATION_JSON)
public class SampleResource {
    /**
     * Handles GET requests on the URL aforementioned above the SampleResource class
     * @return - An object of SampleResource which is serialized to JSON by Jackson
     */
    @GET
    public SampleResponse sampleRequestHandler() {
        SampleResponse sampleResponse = new SampleResponse(1, "Hello, World!");
        return sampleResponse;
    }
}
