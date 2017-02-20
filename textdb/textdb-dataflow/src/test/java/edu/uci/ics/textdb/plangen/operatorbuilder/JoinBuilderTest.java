package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.join.SimilarityJoinPredicate;

public class JoinBuilderTest {
    
    /*
     * Test the Join builder for CharacterDistance predicate
     */
    @Test
    public void testJoinBuilder1() throws Exception {
        
        String joinPredicateType = JoinBuilder.JOIN_CHARACTER_DISTANCE;
        String attributeName = "content";
        String distanceStr = "10";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        
        operatorProperties.put(JoinBuilder.JOIN_PREDICATE, joinPredicateType);
        operatorProperties.put(JoinBuilder.JOIN_INNER_ATTR_NAME, attributeName);
        operatorProperties.put(JoinBuilder.JOIN_OUTER_ATTR_NAME, attributeName);
        operatorProperties.put(JoinBuilder.JOIN_THRESHOLD, distanceStr);
        
        Join join = JoinBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(join.getPredicate().getInnerAttributeName(), attributeName);
        Assert.assertEquals(join.getPredicate().getOuterAttributeName(), attributeName);
        
        Assert.assertTrue(join.getPredicate() instanceof JoinDistancePredicate);
        JoinDistancePredicate joinDistancePredicate = (JoinDistancePredicate) join.getPredicate();
        Assert.assertEquals(joinDistancePredicate.getThreshold().toString(), distanceStr);
        
    }
    
    /*
     * Test the Join builder for SimilarityJoin predicate
     */
    @Test
    public void testJoinBuilder2() throws Exception {
        
        String joinPredicateType = JoinBuilder.JOIN_SIMILARITY;
        String attributeName = "content";
        String distanceStr = "0.8";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        
        operatorProperties.put(JoinBuilder.JOIN_PREDICATE, joinPredicateType);
        operatorProperties.put(JoinBuilder.JOIN_INNER_ATTR_NAME, attributeName);
        operatorProperties.put(JoinBuilder.JOIN_OUTER_ATTR_NAME, attributeName);
        operatorProperties.put(JoinBuilder.JOIN_THRESHOLD, distanceStr);
        
        Join join = JoinBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(join.getPredicate().getInnerAttributeName(), attributeName);
        Assert.assertEquals(join.getPredicate().getOuterAttributeName(), attributeName);
        
        Assert.assertTrue(join.getPredicate() instanceof SimilarityJoinPredicate);
        SimilarityJoinPredicate similarityJoinPredicate = (SimilarityJoinPredicate) join.getPredicate();
        Assert.assertEquals(similarityJoinPredicate.getThreshold().toString(), distanceStr);
        
    }

}
