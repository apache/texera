package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.HashSet;

import edu.uci.ics.textdb.api.common.AttributeType;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector.ConnectorOutputOperator;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.FuzzyTokenMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;
import edu.uci.ics.textdb.storage.RelationManager;
import junit.framework.Assert;

public class LogicalPlanTest {
    
    public static final String TEST_TABLE = "logical_plan_test_table";
    
    public static final Schema TEST_SCHEMA = new Schema(
            new Attribute("city", AttributeType.STRING), new Attribute("location", AttributeType.STRING),
            new Attribute("content", AttributeType.TEXT));
    
    @BeforeClass
    public static void setUp() throws StorageException {
        RelationManager.getRelationManager().createTable(
                TEST_TABLE, "../index/test_tables/"+TEST_TABLE,
                TEST_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
    }
    
    @AfterClass
    public static void cleanUp() throws StorageException {
        RelationManager.getRelationManager().deleteTable(TEST_TABLE);
    }
    
    public static HashMap<String, String> keywordSourceProperties = new HashMap<String, String>() {
        {
            JSONObject schemaJsonJSONObject = new JSONObject();
            schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
            schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

            put(KeywordMatcherBuilder.KEYWORD, "irvine");
            put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
            put(OperatorBuilderUtils.DATA_SOURCE, TEST_TABLE);
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
            put(JoinBuilder.JOIN_THRESHOLD, "100");
            put(JoinBuilder.JOIN_INNER_ATTR_NAME, "content");
            put(JoinBuilder.JOIN_OUTER_ATTR_NAME, "content");
        }
    };

    public static HashMap<String, String> fileSinkProperties = new HashMap<String, String>() {
        {
            put(FileSinkBuilder.FILE_PATH, "./result.txt");
        }
    };

    /*
     * It generates a valid logical plan as follows.
     *
     * KeywordSource --> RegexMatcher --> FileSink
     *
     */
    public static LogicalPlan getLogicalPlan1() throws PlanGenException {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("sink", "FileSink", fileSinkProperties);
        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("regex", "sink");
        return logicalPlan;
    }

    /*
     * It generates a valid logical plan as follows.
     *
     *                  -> RegexMatcher -->
     * KeywordSource --<                     >-- Join --> FileSink
     *                  -> NlpExtractor -->
     *
     */
    public static LogicalPlan getLogicalPlan2() throws PlanGenException {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        logicalPlan.addOperator("join", "Join", joinProperties);
        logicalPlan.addOperator("sink", "FileSink", fileSinkProperties);

        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("source", "nlp");
        logicalPlan.addLink("regex", "join");
        logicalPlan.addLink("nlp", "join");
        logicalPlan.addLink("join", "sink");
        return logicalPlan;
    }

    /*
     * It generates a valid logical plan as follows.
     *
     *                  --> RegexMatcher -->
     *                  |                    >-- Join1
     * KeywordSource --< -> NlpExtractor -->          >-- Join2 --> TupleStreamSink
     *                  |                           /
     *                  --> FuzzyTokenMatcher ----->
     *
     */
    public static LogicalPlan getLogicalPlan3() throws PlanGenException {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        logicalPlan.addOperator("fuzzytoken", "FuzzyTokenMatcher", fuzzyTokenMatcherProperties);
        logicalPlan.addOperator("join", "Join", joinProperties);
        logicalPlan.addOperator("join2", "Join", joinProperties);
        logicalPlan.addOperator("sink", "TupleStreamSink", new HashMap<String, String>());

        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("source", "nlp");
        logicalPlan.addLink("source", "fuzzytoken");
        logicalPlan.addLink("regex", "join");
        logicalPlan.addLink("nlp", "join");
        logicalPlan.addLink("join", "join2");
        logicalPlan.addLink("fuzzytoken", "join2");
        logicalPlan.addLink("join2", "sink");
        return logicalPlan;
    }

    /*
     * Test a valid operator graph.
     * 
     * KeywordSource --> RegexMatcher --> FileSink
     * 
     */
    @Test
    public void testLogicalPlan1() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan1();

        Plan queryPlan = logicalPlan.buildQueryPlan();

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
    public void testLogicalPlan2() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan2();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        Plan queryPlan = logicalPlan.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator join = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(join instanceof Join);

        IOperator joinInput1 = ((Join) join).getInnerInputOperator();
        Assert.assertTrue(joinInput1 instanceof RegexMatcher);

        IOperator joinInput2 = ((Join) join).getOuterInputOperator();
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
     * KeywordSource --< -> NlpExtractor -->          >-- Join2 --> TupleStreamSink
     *                  |                           /
     *                  --> FuzzyTokenMatcher ----->
     * 
     */
    @Test
    public void testLogicalPlan3() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan3();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        Plan queryPlan = logicalPlan.buildQueryPlan();

        ISink tupleStreamSink = queryPlan.getRoot();
        Assert.assertTrue(tupleStreamSink instanceof TupleStreamSink);

        IOperator join2 = ((TupleStreamSink) tupleStreamSink).getInputOperator();
        Assert.assertTrue(join2 instanceof Join);

        IOperator join2Input1 = ((Join) join2).getInnerInputOperator();
        Assert.assertTrue(join2Input1 instanceof Join);

        IOperator join2Input2 = ((Join) join2).getOuterInputOperator();
        Assert.assertTrue(join2Input2 instanceof FuzzyTokenMatcher);

        IOperator join1Input1 = ((Join) join2Input1).getInnerInputOperator();
        Assert.assertTrue(join1Input1 instanceof RegexMatcher);

        IOperator join1Input2 = ((Join) join2Input1).getOuterInputOperator();
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
    public void testInvalidLogicalPlan1() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("sink", "FileSink", fileSinkProperties);
        logicalPlan.addLink("regex", "sink");

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph without a sink operator
     * 
     * KeywordSource --> RegexMatcher
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan2() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addLink("source", "regex");

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with more than one sink operators
     *                                   -> FileSink1
     * KeywordSource --> RegexMatcher --<
     *                                   -> FileSink2
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan3() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);
        logicalPlan.addOperator("sink2", "FileSink", fileSinkProperties);

        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("regex", "sink1");
        logicalPlan.addLink("regex", "sink2");

        logicalPlan.buildQueryPlan();
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
    public void testInvalidLogicalPlan4() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        logicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);

        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("regex", "sink1");
        logicalPlan.addLink("regex2", "nlp");

        logicalPlan.buildQueryPlan();
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
    public void testInvalidLogicalPlan5() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("regex1", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        logicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        logicalPlan.addOperator("join", "Join", joinProperties);


        logicalPlan.addLink("source", "regex1");
        logicalPlan.addLink("regex1", "join");
        logicalPlan.addLink("regex2", "nlp");
        logicalPlan.addLink("nlp", "regex2");
        logicalPlan.addLink("nlp", "join");
        logicalPlan.addLink("join", "sink1");

        logicalPlan.buildQueryPlan();
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
    public void testInvalidLogicalPlan6() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("source2", "KeywordSource", keywordSourceProperties);

        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);

        logicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("source2", "regex2");
        logicalPlan.addLink("regex", "sink1");
        logicalPlan.addLink("regex2", "sink1");

        logicalPlan.buildQueryPlan();
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
    public void testInvalidLogicalPlan7() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);

        logicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        logicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);

        logicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        logicalPlan.addLink("source", "regex");
        logicalPlan.addLink("source", "regex2");
        logicalPlan.addLink("regex", "sink1");

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with a cycle
     * 
     * KeywordSource --> FileSik --> (back to the same) KeywordSource
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan8() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        logicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        logicalPlan.addLink("source", "sink1");
        logicalPlan.addLink("sink1", "source");

        logicalPlan.buildQueryPlan();
    }

}
