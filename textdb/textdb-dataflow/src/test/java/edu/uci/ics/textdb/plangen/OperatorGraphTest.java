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
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;
import junit.framework.Assert;

public class OperatorGraphTest {

    public static HashMap<String, String> keywordSourceProperties1 = new HashMap<String, String>() {
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

    public static HashMap<String, String> regexMatcherProperties1 = new HashMap<String, String>() {
        {
            put(RegexMatcherBuilder.REGEX, "ca(lifornia)?");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "location, content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "string, text");
        }
    };

    public static HashMap<String, String> nlpExtractorProperties1 = new HashMap<String, String>() {
        {
            put(NlpExtractorBuilder.NLP_TYPE, "date");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> joinProperties1 = new HashMap<String, String>() {
        {
            put(JoinBuilder.JOIN_PREDICATE, "CharacterDistance");
            put(JoinBuilder.JOIN_DISTANCE, "100");
            put(JoinBuilder.JOIN_ID_ATTRIBUTE_NAME, "id");
            put(JoinBuilder.JOIN_ID_ATTRIBUTE_TYPE, "integer");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> fileSinkProperties1 = new HashMap<String, String>() {
        {
            put(FileSinkBuilder.FILE_PATH, "./result.txt");
        }
    };

    @Test
    public void testOperatorGraph1() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties1);
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

    @Test
    public void testOperatorGraph2() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties1);
        operatorGraph.addOperator("join", "Join", joinProperties1);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties1);

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
     * Test a operator graph without a source operator
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph1() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties1);
        operatorGraph.addLink("regex", "sink");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph without a sink operator
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph2() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addLink("source", "regex");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph with more than one sink operators
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph3() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties1);
        operatorGraph.addOperator("sink2", "FileSink", fileSinkProperties1);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "sink1");
        operatorGraph.addLink("regex", "sink2");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph with a disconnected component
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph4() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties1);

        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties1);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "sink1");
        operatorGraph.addLink("regex2", "nlp");

        operatorGraph.buildQueryPlan();
    }

    /*
     * Test a operator graph with a cycle
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph5() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties1);

        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties1);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "regex2");
        operatorGraph.addLink("regex2", "nlp");
        operatorGraph.addLink("nlp", "regex");
        operatorGraph.addLink("nlp", "sink1");

        operatorGraph.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with an invalid input arity
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph6() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("source2", "KeywordSource", keywordSourceProperties1);

        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties1);

        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties1);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source2", "regex2");
        operatorGraph.addLink("regex", "sink1");
        operatorGraph.addLink("regex2", "sink1");

        operatorGraph.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with an invalid output arity
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph7() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);

        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);
        operatorGraph.addOperator("regex2", "RegexMatcher", regexMatcherProperties1);

        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties1);

        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source", "regex2");
        operatorGraph.addLink("regex", "sink1");

        operatorGraph.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with a cycle
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidOperatorGraph8() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();

        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);
        operatorGraph.addOperator("sink1", "FileSink", fileSinkProperties1);

        operatorGraph.addLink("source", "sink1");
        operatorGraph.addLink("sink1", "source");

        operatorGraph.buildQueryPlan();
    }

}
