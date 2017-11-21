package edu.uci.ics.texera.web.resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.engine.Engine;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.web.TexeraWebException;

/**
 * This class will be the resource class for accepting a query plan edu.uci.ics.texera.web.request and executing the
 * query plan to get the query response
 * Created by kishorenarendran on 10/17/16.
 * 
 * @author Kishore
 * @author Zuozhi
 */
@Path("/queryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryPlanResource {
    
    public static java.nio.file.Path resultDirectory = Utils.getTexeraHomePath().resolve("query-results");
    
    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic TexeraWebResponse object
     */
    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public JsonNode executeQueryPlan(String logicalPlanJson) {
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
                
                // make sure result directory is created
                if (Files.notExists(resultDirectory)) {
                    Files.createDirectories(resultDirectory);
                }
                
                // clean up old result files
                cleanupOldResults();
                
                // generate new UUID as the result id
                String resultID = UUID.randomUUID().toString();
                
                // write original json of the result into a file                
                java.nio.file.Path resultFile = resultDirectory.resolve(resultID + ".json");

                Files.createFile(resultFile);
                Files.write(resultFile, new ObjectMapper().writeValueAsBytes(results));
                
                // put readable json of the result into response
                ArrayNode resultNode = new ObjectMapper().createArrayNode();
                for (Tuple tuple : results) {
                    resultNode.add(tuple.getReadableJson());
                }
                
                ObjectNode response = new ObjectMapper().createObjectNode();
                response.put("code", 0);
                response.set("result",resultNode);
                response.put("resultID", resultID);
                return response;
            } else {
                // execute the plan and return success message
                Engine.getEngine().evaluate(plan);
                ObjectNode response = new ObjectMapper().createObjectNode();
                response.put("code", 1);
                response.put("message", "plan sucessfully executed");
                return response;
            }
            
        } catch (IOException | TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }   
    }

    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic TexeraWebResponse object
     */
    @POST
    @Path("/autoExecute")
    /* EG of using /autoExecute end point (how this inline update method works):

    1. At the beginning of creating a graph, (for example) when a scan source and a keyword search
        operators are initailized (dragged in the flow-chart) but unlinked, the graph looks like this:

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: N/A   |                           |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    2. Then, you can feel free to link these two operators together, or go ahead and select a
        table as the source first. Let's link them together first.

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: N/A   | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    3. At this moment, the Keyword Search operator still does NOT have any available options for
        its Attributes field because of the lack of the source. Therefore, we can select a table
        name as the source next (let's use table "plan" as an example here)

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: plan  | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    4. After select table "plan" as the source, now you can see the options list of Attributes in
        the Keyword Search operator becomes available. you should see 4 options in the list: name,
        description, logicPlan, payload. Feel free to choose whichever you need for your desired result.

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: name |
        |  TableName: plan  | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    5. Basically, the method supports that whenever you connect a source (with a valid table name)
        to a regular search operator, the later operator is able to recognize the metadata of its
        input operator (which is the source), and then updates its attribute options in the drop-down
        list. To illustrate how powerful this functionality is, you can add a new (Scan) Source and
        pick another table which is different than table "plan" we have already created. The graph
        now should be looked like the following:

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: name |
        |  TableName: plan  | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|


         _______________________
        |                       |
        |                       |
        | Source: Scan          |
        | TableName: dictionary |
        |                       |
        |                       |
        |                       |
        |_______________________|

    6. Then, connect "dictionary" to the Keyword Search operator. The original link between "plan"
        and Keyword Search will automatically disappear.


         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: plan  |                           |  (Other inputs...)|
        |                   |            /============> |                   |
        |                   |           //              |                   |
        |                   |          //               |                   |
        |___________________|         //                |___________________|
                                     //
                                    //
         _______________________   //
        |                       | //
        |                       |//
        | Source: Scan          |/
        | TableName: dictionary |
        |                       |
        |                       |
        |                       |
        |_______________________|

    7. After the new link generated, the Attributes field of the Keyword Search will be empty again. When
        you try to check its drop-down list, the options are all updated to dictionary's attributes, which
        are name and payload. The options from "plan" are all gone.

    */
    public JsonNode executeAutoQueryPlan(String logicalPlanJson) {
        try {
            LogicalPlan logicalPlan = new ObjectMapper().readValue(logicalPlanJson, LogicalPlan.class);
            String resultID = UUID.randomUUID().toString();

            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", 0);
            response.set("result", logicalPlan.retrieveAllOperatorInputSchema());
            response.put("resultID", resultID);
            return response;

        } catch (IOException | TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
    }
    
    
    /**
     * Cleans up the old result json files stored in the file system.
     * The current cleanup policy is to keep the latest 5 files.
     * 
     * TODO: In the case where there are multiple users, they need their own spaces for storing files.
     * 
     * @throws IOException
     */
    public static void cleanupOldResults() throws IOException {
    		// list all the files in the reuslt directory
    		List<java.nio.file.Path> resultFiles = Files.list(resultDirectory).collect(Collectors.toList());
    		
    		// clean up if there are more than 5 files
    		if (resultFiles.size() <= 5) {
    			return;
    		}
    		
    		// sort the files by their creation time
    		Collections.sort(resultFiles, (java.nio.file.Path f1, java.nio.file.Path f2) -> {
				try {
					return (
							Files.readAttributes(f1, BasicFileAttributes.class).creationTime().compareTo(
									Files.readAttributes(f2, BasicFileAttributes.class).creationTime()));
				} catch (IOException e) {
					return 0;
				}
			});
    		
    		// remove the oldest file
    		java.nio.file.Path oldestFile = resultFiles.get(0);
    		Files.delete(oldestFile);
    }    
    

}
