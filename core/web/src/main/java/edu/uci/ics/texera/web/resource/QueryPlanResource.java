package edu.uci.ics.texera.web.resource;


import edu.uci.ics.texera.dataflow.sink.ITupleSink;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.plangen.QueryContext;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.dataflow.sink.VisualizationOperator;

import io.dropwizard.jersey.sessions.Session;

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
     * @param plan Logical plan to be executed
     * @return Generic GenericWebResponse object
     */
    private JsonNode executeMutipleSinkPlan(Plan plan)  {
        HashMap<String, ISink> sinkMap = plan.getSinkMap();
        ObjectNode response = new ObjectMapper().createObjectNode();
        HashMap<String, List<Tuple>> executionResult = new HashMap<>();
        // execute the query plan from all sink operators and collect result
        for (HashMap.Entry<String, ISink> sinkEntry: sinkMap.entrySet()) {

            ISink sinkOperator = sinkEntry.getValue();

            if (sinkOperator instanceof ITupleSink) {
                ITupleSink tupleSink = (ITupleSink) sinkOperator;
                tupleSink.open();
                List<Tuple> result = tupleSink.collectAllTuples();
                tupleSink.close();
                executionResult.put(sinkEntry.getKey(), result);

            } else {
                sinkOperator.open();
                sinkOperator.processTuples();
                sinkOperator.close();
            }
        }

        // put all results in the array node.
        // each result has three fields : table, operatorID, chartType (if the sink operator is a visualization operator).
        ArrayNode arrayNode = new ObjectMapper().createArrayNode();

        for (HashMap.Entry<String, List<Tuple>> result: executionResult.entrySet()) {
            ObjectNode operatorMap = new ObjectMapper().createObjectNode();
            operatorMap.put("operatorID", result.getKey());

            ArrayNode resultNode = new ObjectMapper().createArrayNode();
            for (Tuple tuple : result.getValue()) {
                resultNode.add(tuple.getReadableJson());
            }
            operatorMap.set("table", resultNode);

            String operatorID = result.getKey();
            ISink operator = sinkMap.get(operatorID);
            if (operator instanceof VisualizationOperator) {
                operatorMap.put("chartType", ((VisualizationOperator) operator).getChartType());
            }
            arrayNode.add(operatorMap);
        }

        String resultID = UUID.randomUUID().toString();
        response.put("code", executionResult.isEmpty() ? 1 : 0);
        response.put("resultID", resultID);
        response.set("result", arrayNode);
        return response;
    }

    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic GenericWebResponse object
     */
    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public JsonNode executeQueryPlan(@Session HttpSession session, String logicalPlanJson) {
        try {
            UserResource.User user = UserResource.getUser(session);
            QueryContext ctx = new QueryContext();
            if (user != null) {
                ctx.setProjectOwnerID(user.userID.toString());
            }

            LogicalPlan logicalPlan = new ObjectMapper().readValue(logicalPlanJson, LogicalPlan.class);
            logicalPlan.setContext(ctx);
            Plan plan = logicalPlan.buildQueryPlan();

            return executeMutipleSinkPlan(plan);
        } catch (IOException | TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
    }

    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic GenericWebResponse object
     */
    /* EG of using /autocomplete end point (how this inline update method works):

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
    @POST
    @Path("/autocomplete")
    public JsonNode suggestAutocompleteSchema(@Session HttpSession session, String logicalPlanJson) {
        try {
            UserResource.User user = UserResource.getUser(session);
            QueryContext ctx = new QueryContext();
            if (user != null) {
                ctx.setProjectOwnerID(user.userID.toString());
            }

            JsonNode logicalPlanNode = new ObjectMapper().readTree(logicalPlanJson);
            ArrayNode operators = (ArrayNode) logicalPlanNode.get(PropertyNameConstants.OPERATOR_LIST);
            ArrayNode links = (ArrayNode) logicalPlanNode.get(PropertyNameConstants.OPERATOR_LINK_LIST);

            ArrayNode validOperators = new ObjectMapper().createArrayNode();
            ArrayNode validLinks = new ObjectMapper().createArrayNode();
            ArrayNode linksEndWithInvalidDest = new ObjectMapper().createArrayNode();

            Set<String> validOperatorsId = new HashSet<>();
            getValidOperatorsAndLinks(operators, links, validOperators, validLinks,
                                      linksEndWithInvalidDest, validOperatorsId);

            ObjectNode validLogicalPlanNode = new ObjectMapper().createObjectNode();
            (validLogicalPlanNode).putArray(PropertyNameConstants.OPERATOR_LIST).addAll(validOperators);
            (validLogicalPlanNode).putArray(PropertyNameConstants.OPERATOR_LINK_LIST).addAll(validLinks);

            LogicalPlan logicalPlan = new ObjectMapper().treeToValue(validLogicalPlanNode, LogicalPlan.class);
            logicalPlan.setContext(ctx);

            // Get all input schema for valid operator with valid links
            Map<String, List<Schema>> inputSchema = logicalPlan.retrieveAllOperatorInputSchema();
            // Get all input schema for invalid operator with valid input operator
            for (JsonNode linkNode: linksEndWithInvalidDest) {
                String origin = linkNode.get(PropertyNameConstants.ORIGIN_OPERATOR_ID).textValue();
                String dest = linkNode.get(PropertyNameConstants.DESTINATION_OPERATOR_ID).textValue();

                Optional<Schema> schema = logicalPlan.getOperatorOutputSchema(origin, inputSchema);
                if(schema.isPresent()) {
                    if (inputSchema.containsKey(dest)) {
                        inputSchema.get(dest).add(schema.get());
                    } else {
                        inputSchema.put(dest, new ArrayList<>(Arrays.asList(schema.get())));
                    }
                }
            }

            ObjectNode result = new ObjectMapper().createObjectNode();
            for (Map.Entry<String, List<Schema>> entry: inputSchema.entrySet()) {
                Set<String> attributes = new HashSet<>();
                for (Schema schema: entry.getValue()) {
                    attributes.addAll(schema.getAttributeNames());
                }

                ArrayNode currentSchemaNode = result.putArray(entry.getKey());
                for (String attrName: attributes) {
                    currentSchemaNode.add(attrName);
                }

            }

            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", 0);
            response.set("result", result);
            return response;

        } catch (JsonMappingException je) {
            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", -1);
            response.put("message", "Json Mapping Exception would not be handled for auto plan. " + je.getMessage());
            return response;

        } catch (IOException | TexeraException e) {
            if (e.getMessage().contains("does not exist in the schema:")) {
                ObjectNode response = new ObjectMapper().createObjectNode();
                response.put("code", -1);
                response.put("message", "Attribute Not Exist Exception would not be handled for auto plan. " + e.getMessage());
                return response;
            }
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


    /**
     * Used for automatic schema propagation as user is building the graph.
     *
     * Retrieve all the valid operator and links and store into node validOperators and validLinks
     *
     * A operator is valid if the json representation of the operator can pass all the test in the specific
     * operator predicate constructor and can successfully be casted into a PredicateBase object (without throwing any error).
     * The standard of valid is different from operator to operator and it can be seen in the definition of the
     * constructor of a specific operator predicate.
     * An example of invalid operator is a KeywordPredicate has empty query
     *
     * A valid link would be a link that connects two valid operators
     * @param operators
     * @param links
     * @param validOperators
     * @param validLinks
     * @param linksEndWithInvalidDest
     * @param validOperatorsId
     */
    private void getValidOperatorsAndLinks(ArrayNode operators, ArrayNode links,
                                           ArrayNode validOperators, ArrayNode validLinks,
                                           ArrayNode linksEndWithInvalidDest, Set<String> validOperatorsId) {
        // Try to convert to valid operator
        for (JsonNode operatorNode: operators) {
            try {
                new ObjectMapper().treeToValue(operatorNode, PredicateBase.class);
                validOperators.add(operatorNode);
                validOperatorsId.add(operatorNode.get(PropertyNameConstants.OPERATOR_ID).textValue());
                // Json Parsing Exception will mean that the user hasn't provided all input parameters for the operator till now.
                // As this function is just used for input suggestion to the user it is fine to skip this exception here.
            } catch (JsonProcessingException e) {
                System.out.println(e);
            }
        }

        // Only include edges that connect valid operators
        for (JsonNode linkNode: links) {
            String origin = linkNode.get(PropertyNameConstants.ORIGIN_OPERATOR_ID).textValue();
            String dest = linkNode.get(PropertyNameConstants.DESTINATION_OPERATOR_ID).textValue();

            if (validOperatorsId.contains(origin) && validOperatorsId.contains(dest)) {
                validLinks.add(linkNode);
            } else if (validOperatorsId.contains(origin) && !validOperatorsId.contains(dest)) {
                linksEndWithInvalidDest.add(linkNode);
            }
        }
    }

}
