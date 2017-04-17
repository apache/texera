package edu.uci.ics.textdb.web.resource;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONArray;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.engine.Engine;
import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.exp.plangen.LogicalPlan;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.web.TextdbWebException;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;

@Path("/newqueryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NewQueryPlanResource {
    
    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public TextdbWebResponse executeQueryPlan(String logicalPlanJson) {
        System.out.println("enter new execute");
        try {
            LogicalPlan logicalPlan = new ObjectMapper().readValue(logicalPlanJson, LogicalPlan.class);
            Plan plan = logicalPlan.buildQueryPlan();
            ISink sink = plan.getRoot();
            
            // send response back to frontend
            if (sink instanceof TupleSink) {
                TupleSink tupleSink = (TupleSink) sink;
                tupleSink.open();
                List<Tuple> results = tupleSink.collectAllTuples();
                tupleSink.close();
                
                JSONArray resultJson = DataflowUtils.getTupleListJSON(results);
                
                return new TextdbWebResponse(0, resultJson.toString());
            } else {
                // execute the plan and return success message
                Engine.getEngine().evaluate(plan);
                return new TextdbWebResponse(0, "plan sucessfully executed");
            }
            
        } catch (TextDBException | IOException | RuntimeException e) {
            // TODO remove RuntimeException after the exception refactor
            System.out.println(e.getMessage());
            throw new TextdbWebException(e.getMessage());
        }   
    }

}
