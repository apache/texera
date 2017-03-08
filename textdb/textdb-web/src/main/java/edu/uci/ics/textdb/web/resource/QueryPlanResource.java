package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.web.request.QueryPlanRequest;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class will be the resource class for accepting a query plan edu.uci.ics.textdb.web.request and executing the
 * query plan to get the query response
 * Created by kishorenarendran on 10/17/16.
 */
@Path("/queryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryPlanResource {

    /**
     * This is the edu.uci.ics.textdb.web.request handler for the execution of a Query Plan.
     * @param queryPlanRequest - An object that models the query plan edu.uci.ics.textdb.web.request that will be POSTed
     * @return - Generic response object for now, which wjust verifies the creation of operator properties' hashmap
     * @throws Exception
     */
    @POST
    @Path("/execute")
    public Response executeQueryPlan(QueryPlanRequest queryPlanRequest) throws Exception {
        // Aggregating all the operator properties, and creating a logical plan object
        boolean aggregatePropertiesFlag = queryPlanRequest.aggregateOperatorProperties();
        boolean createLogicalPlanFlag = queryPlanRequest.createLogicalPlan();

        ObjectMapper objectMapper = new ObjectMapper();
        if(aggregatePropertiesFlag && createLogicalPlanFlag) {
            // generate the physical plan
            Plan plan = queryPlanRequest.getLogicalPlan().buildQueryPlan();
            
            // if the sink is TupleStreamSink, send the response back to front-end
            if (plan.getRoot() instanceof TupleStreamSink) {
                TupleStreamSink sink = (TupleStreamSink) plan.getRoot();
                
                // get all the results and send them to front-end
                // returning all the results at once is a **temporary** solution
                // TODO: in the future, request some number of results at a time
                sink.open();
                List<Tuple> results = sink.collectAllTuples();
                sink.close();
                
                TextdbWebResponse textdbWebResponse = new TextdbWebResponse(0, Utils.getTupleListJSON(results).toString());
                return Response.status(200)
                        .entity(objectMapper.writeValueAsString(textdbWebResponse))
                        .build();
                
            } else {
                // if the sink is not TupleStreamSink, execute the plan directly
                Engine.getEngine().evaluate(plan);    
                TextdbWebResponse textdbWebResponse = new TextdbWebResponse(0, "Plan Successfully Executed");
                return Response.status(200)
                        .entity(objectMapper.writeValueAsString(textdbWebResponse))
                        .build();
            }
        }
        else {
            // Temporary sample response when the operator properties aggregation does not function
            TextdbWebResponse textdbWebResponse = new TextdbWebResponse(1, "Unsuccessful");
            return Response.status(400)
                    .entity(objectMapper.writeValueAsString(textdbWebResponse))
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "OPTIONS,GET,PUT,POST,DELETE,HEAD")
                    .header("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
                    .header("Access-Control-Max-Age", "1728000")
                    .build();
        }
    }
}