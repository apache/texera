package edu.uci.ics.textdb.exp.plangen;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Assert;

public class LogicalPlanJsonSerializationTest {
    
    @Test
    public void testLogicalPlan() throws Exception {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();
        ObjectMapper objectMapper = new ObjectMapper();
        String logicalPlanJson = objectMapper.writeValueAsString(logicalPlan);
        LogicalPlan resultLogicalPlan = objectMapper.readValue(logicalPlanJson, LogicalPlan.class);
        String rseultLogicalPlanJson = objectMapper.writeValueAsString(resultLogicalPlan);
        
        JsonNode logicalPlanJsonNode = objectMapper.readValue(logicalPlanJson, JsonNode.class);
        JsonNode resultLogicalPlanJsonNode = objectMapper.readValue(rseultLogicalPlanJson, JsonNode.class);
        
        Assert.assertEquals(logicalPlanJsonNode, resultLogicalPlanJsonNode);
    }
    
    @Test
    public void testOperatorLink() throws Exception {
        OperatorLink operatorLink = new OperatorLink("origin", "destination");
        ObjectMapper objectMapper = new ObjectMapper();
        String operatorLinkJson = objectMapper.writeValueAsString(operatorLink);
        OperatorLink resultOperatorLink = objectMapper.readValue(operatorLinkJson, OperatorLink.class);
        String rseultOperatorLinkJson = objectMapper.writeValueAsString(resultOperatorLink);
        
        JsonNode operatorLinkJsonNode = objectMapper.readValue(operatorLinkJson, JsonNode.class);
        JsonNode resultOperatorLinkJsonNode = objectMapper.readValue(rseultOperatorLinkJson, JsonNode.class);
        
        Assert.assertEquals(operatorLinkJsonNode, resultOperatorLinkJsonNode);
    }

}
