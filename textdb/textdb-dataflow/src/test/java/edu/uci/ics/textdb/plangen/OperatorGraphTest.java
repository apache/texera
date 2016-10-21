package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector.ConnectorOutputOperator;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.FuzzyTokenMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;
import junit.framework.Assert;

public class OperatorGraphTest {

    public static HashMap<String, String> keywordSourceProperties = new HashMap<String, String>() {
        {
            JSONObject schemaJsonJSONObject = new JSONObject();
            schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
            schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

            put(KeywordMatcherBuilder.KEYWORD, "irvine");
            put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
            put(OperatorBuilderUtils.DATA_DIRECTORY, "./index");
            put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        }
    };

    public static HashMap<String, String> regexMatcherProperties = new HashMap<String, String>() {
        {
            put(RegexMatcherBuilder.REGEX, "ca(lifornia)?");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "location, content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "string, text");
        }
    };
    
    public static HashMap<String, String> fuzzyTokenMatcherProperties = new HashMap<String, String>() {
        {
            put(FuzzyTokenMatcherBuilder.FUZZY_STRING, "university college school");
            put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "0.5");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> nlpExtractorProperties = new HashMap<String, String>() {
        {
            put(NlpExtractorBuilder.NLP_TYPE, "Location");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> joinProperties = new HashMap<String, String>() {
        {
            put(JoinBuilder.JOIN_PREDICATE, "CharacterDistance");
            put(JoinBuilder.JOIN_DISTANCE, "100");
            put(JoinBuilder.JOIN_ID_ATTRIBUTE_NAME, "id");
            put(JoinBuilder.JOIN_ID_ATTRIBUTE_TYPE, "integer");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> fileSinkProperties = new HashMap<String, String>() {
        {
            put(FileSinkBuilder.FILE_PATH, "./result.txt");
        }
    };

    /*
     * Test a valid operator graph.
     * 
     * KeywordSource --> RegexMatcher --> FileSink
     * 
     */
    @Test
    public void testOperatorGraph1() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties);
        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "sink");

        Plan queryPlan = operatorGraph.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator regexMatcher = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(regexMatcher instanceof RegexMatcher);

        IOperator keywordSource = ((RegexMatcher) regexMatcher).getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);

    }

    /*
     * Test a valid operator graph.
     *                  -> RegexMatcher -->
     * KeywordSource --<                     >-- Join --> FileSink
     *                  -> NlpExtractor -->
     * 
     */
    @Test
    public void testOperatorGraph2() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        operatorGraph.addOperator("join", "Join", joinProperties);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source", "nlp");
        operatorGraph.addLink("regex", "join");
        operatorGraph.addLink("nlp", "join");
        operatorGraph.addLink("join", "sink");

        Plan queryPlan = operatorGraph.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator join = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(join instanceof Join);

        IOperator joinInput1 = ((Join) join).getInnerOperator();
        Assert.assertTrue(joinInput1 instanceof RegexMatcher);

        IOperator joinInput2 = ((Join) join).getOuterOperator();
        Assert.assertTrue(joinInput2 instanceof NlpExtractor);

        IOperator connectorOut1 = ((RegexMatcher) joinInput1).getInputOperator();
        Assert.assertTrue(connectorOut1 instanceof ConnectorOutputOperator);

        IOperator connectorOut2 = ((NlpExtractor) joinInput2).getInputOperator();
        Assert.assertTrue(connectorOut2 instanceof ConnectorOutputOperator);       
        
        HashSet<Integer> connectorIndices = new HashSet<>();
        connectorIndices.add(((ConnectorOutputOperator) connectorOut1).getOutputIndex());
        connectorIndices.add(((ConnectorOutputOperator) connectorOut2).getOutputIndex());
        Assert.assertEquals(connectorIndices.size(), 2);

        OneToNBroadcastConnector connector1 = ((ConnectorOutputOperator) connectorOut1).getOwnerConnector();
        OneToNBroadcastConnector connector2 = ((ConnectorOutputOperator) connectorOut2).getOwnerConnector();

        Assert.assertSame(connector1, connector2);

        IOperator keywordSource = connector1.getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);
    }
    
    /*
     * Test a valid operator graph.
     * 
     *                  --> RegexMatcher -->
     *                  |                    >-- Join1
     * KeywordSource --< -> NlpExtractor -->          >-- Join2 --> FileSink
     *                  |                           /
     *                  --> FuzzyTokenMatcher ----->
     * 
     */
    @Test
    public void testOperatorGraph3() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        operatorGraph.addOperator("fuzzytoken", "FuzzyTokenMatcher", fuzzyTokenMatcherProperties);
        operatorGraph.addOperator("join", "Join", joinProperties);
        operatorGraph.addOperator("join2", "Join", joinProperties);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source", "nlp");
        operatorGraph.addLink("source", "fuzzytoken");
        operatorGraph.addLink("regex", "join");
        operatorGraph.addLink("nlp", "join");
        operatorGraph.addLink("join", "join2");
        operatorGraph.addLink("fuzzytoken", "join2");
        operatorGraph.addLink("join2", "sink");

        Plan queryPlan = operatorGraph.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator join2 = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(join2 instanceof Join);
        
        IOperator join2Input1 = ((Join) join2).getInnerOperator();
        Assert.assertTrue(join2Input1 instanceof Join);
        
        IOperator join2Input2 = ((Join) join2).getOuterOperator();
        Assert.assertTrue(join2Input2 instanceof FuzzyTokenMatcher);

        IOperator join1Input1 = ((Join) join2Input1).getInnerOperator();
        Assert.assertTrue(join1Input1 instanceof RegexMatcher);

        IOperator join1Input2 = ((Join) join2Input1).getOuterOperator();
        Assert.assertTrue(join1Input2 instanceof NlpExtractor);

        IOperator connectorOut1 = ((RegexMatcher) join1Input1).getInputOperator();
        Assert.assertTrue(connectorOut1 instanceof ConnectorOutputOperator);

        IOperator connectorOut2 = ((NlpExtractor) join1Input2).getInputOperator();
        Assert.assertTrue(connectorOut2 instanceof ConnectorOutputOperator);       
        
        IOperator connectorOut3 = ((FuzzyTokenMatcher) join2Input2).getInputOperator();
        Assert.assertTrue(connectorOut3 instanceof ConnectorOutputOperator);
        
        HashSet<Integer> connectorIndices = new HashSet<>();
        connectorIndices.add(((ConnectorOutputOperator) connectorOut1).getOutputIndex());
        connectorIndices.add(((ConnectorOutputOperator) connectorOut2).getOutputIndex());
        connectorIndices.add(((ConnectorOutputOperator) connectorOut3).getOutputIndex());
        Assert.assertEquals(connectorIndices.size(), 3);

        OneToNBroadcastConnector connector1 = ((ConnectorOutputOperator) connectorOut1).getOwnerConnector();
        OneToNBroadcastConnector connector2 = ((ConnectorOutputOperator) connectorOut2).getOwnerConnector();
        OneToNBroadcastConnector connector3 = ((ConnectorOutputOperator) connectorOut3).getOwnerConnector();

        Assert.assertSame(connector1, connector2);
        Assert.assertSame(connector1, connector3);

        IOperator keywordSource = connector1.getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);
    }


    
    /*
     * Test a operator graph without a source operator
     * 
     * RegexMatcher --> FileSink
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph1() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties);
        operatorGraph.addLink("regex", "sink");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph without a sink operator
     * 
     * KeywordSource --> RegexMatcher
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph2() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addLink("source", "regex");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph with more than one sink operators
     *                                   -> FileSink1
     * KeywordSource --> RegexMatcher --<
     *                                   -> FileSink2
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph3() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties);
        operatorGraph.addOperator("sink2", "FileSink", fileSinkProperties);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "sink1");
        operatorGraph.addLink("regex", "sink2");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph with a disconnected component
     * 
     * KeywordSource --> RegexMatcher --> FileSink
     * RegexMatcher --> NlpExtractor
     * (a disconnected graph)
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph4() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties);

        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "sink1");
        operatorGraph.addLink("regex2", "nlp");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph with a cycle
     * 
     * KeywordSource --> RegexMatcher1 -->   
     *                                     >- Join --> FileSink                              
     *                                 -->
     * RegexMathcer2 -> NlpExtractor -< 
     *                                 --> (back to the same) RegexMatcher2
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph5() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("regex1", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties);

        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        operatorGraph.addOperator("join", "Join", joinProperties);


        operatorGraph.addLink("source", "regex1");
        operatorGraph.addLink("regex1", "join");
        operatorGraph.addLink("regex2", "nlp");
        operatorGraph.addLink("nlp", "regex2");
        operatorGraph.addLink("nlp", "join");
        operatorGraph.addLink("join", "sink1");

        operatorGraph.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with an invalid input arity
     * 
     * KeywordSource1 --> RegexMatcher1 ->
     *                                    >-- FileSink
     * KeywordSource2 --> RegexMatcher2 ->
     * 
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph6() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("source2", "KeywordSource", keywordSourceProperties);

        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties);

        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source2", "regex2");
        operatorGraph.addLink("regex", "sink1");
        operatorGraph.addLink("regex2", "sink1");

        operatorGraph.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with an invalid output arity
     *                 -> RegexMatcher1 --> FileSink
     * KeywordSource -<
     *                 -> RegexMatcher2
     *                 
     * It's okay for KeywordSource to have 2 outputs,
     * the problem is RegexMatcher2 doesn't have any outputs.
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph7() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);

        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties);

        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source", "regex2");
        operatorGraph.addLink("regex", "sink1");

        operatorGraph.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with a cycle
     * 
     * KeywordSource --> FileSik --> (back to the same) KeywordSource
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph8() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties);

        operatorGraph.addLink("source", "sink1");
        operatorGraph.addLink("sink1", "source");

        operatorGraph.buildQueryPlan();
    }

}
