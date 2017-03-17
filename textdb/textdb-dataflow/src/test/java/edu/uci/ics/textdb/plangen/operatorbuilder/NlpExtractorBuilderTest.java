package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import junit.framework.Assert;

public class NlpExtractorBuilderTest {
    
    /*
     * test RegexMatcherBuilder with the following properties:
     *   nlpType: Location
     *   attribute list: {content, TEXT}
     *   limit: default (Integer.MAX_VALUE)
     *   offset: default (0)
     * 
     */
    @Test
    public void testNlpExtractorBuilder1() throws PlanGenException, DataFlowException, IOException {
        NlpPredicate.NlpTokenType nlpType = NlpPredicate.NlpTokenType.Location;
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(NlpExtractorBuilder.NLP_TYPE, "location");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        
        NlpExtractor operator = NlpExtractorBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(nlpType, operator.getPredicate().getNlpTokenType());
        List<String> attrNames = Arrays.asList("content");
        Assert.assertEquals(attrNames, operator.getPredicate().getAttributeNames());
        Assert.assertEquals(Integer.MAX_VALUE, operator.getLimit());
        Assert.assertEquals(0, operator.getOffset());    
    }
    
    /*
     * test RegexMatcherBuilder with the following properties:
     *   nlpType: NE_ALL
     *   attribute list: {content, TEXT}
     *   limit: default (Integer.MAX_VALUE)
     *   offset: default (0)
     * 
     */
    @Test
    public void testNlpExtractorBuilder2() throws PlanGenException, DataFlowException, IOException {
        NlpPredicate.NlpTokenType nlpType = NlpPredicate.NlpTokenType.NE_ALL;
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(NlpExtractorBuilder.NLP_TYPE, "nE_AlL");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        
        NlpExtractor operator = NlpExtractorBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(nlpType, operator.getPredicate().getNlpTokenType());
        List<String> attrList = Arrays.asList("content");
        Assert.assertEquals(attrList, operator.getPredicate().getAttributeNames());
        Assert.assertEquals(Integer.MAX_VALUE, operator.getLimit());
        Assert.assertEquals(0, operator.getOffset());    
    }

}
