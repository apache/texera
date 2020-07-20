package edu.uci.ics.texera.dataflow.plangen;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.texera.dataflow.connector.OneToNBroadcastConnector.ConnectorOutputOperator;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenPredicate;
import edu.uci.ics.texera.dataflow.join.Join;
import edu.uci.ics.texera.dataflow.join.JoinDistancePredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityOperator;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityType;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcher;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import junit.framework.Assert;

public class LogicalPlanTest {

    public static final String TEST_TABLE = "logical_plan_test_table";

    public static final Schema TEST_SCHEMA = new Schema(
        new Attribute("city", AttributeType.STRING), new Attribute("location", AttributeType.STRING),
        new Attribute("content", AttributeType.TEXT), new Attribute(SchemaConstants.SPAN_LIST, AttributeType.LIST));

    @BeforeClass
    public static void setUp() throws StorageException {
        cleanUp();
        RelationManager.getInstance().createTable(
            TEST_TABLE, Utils.getDefaultIndexDirectory().resolve("test_tables").resolve(TEST_TABLE),
            TEST_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
    }

    @AfterClass
    public static void cleanUp() throws StorageException {
        RelationManager.getInstance().deleteTable(TEST_TABLE);
    }

    public static KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(
        "irvine",
        Arrays.asList("city", "location", "content"),
        LuceneAnalyzerConstants.standardAnalyzerString(),
        KeywordMatchingType.PHRASE_INDEXBASED,
        TEST_TABLE,
        "keywordSourceResults");
    public static String KEYWORD_SOURCE_ID = "keyword source";

    public static RegexPredicate regexPredicate = new RegexPredicate(
        "ca(lifornia)?",
        Arrays.asList("location", "content"),
        "regexResults");
    public static String REGEX_ID = "regex";

    public static FuzzyTokenPredicate fuzzyTokenPredicate = new FuzzyTokenPredicate(
        "university college school",
        Arrays.asList("content"),
        LuceneAnalyzerConstants.standardAnalyzerString(),
        0.5,
        "fuzzyTokenResults");
    public static String FUZZY_TOKEN_ID = "fuzzy token";

    public static NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(
        NlpEntityType.LOCATION,
        Arrays.asList("content"),
        "nlpEntityResults");
    public static String NLP_ENTITY_ID = "nlp entity";

    public static JoinDistancePredicate joinDistancePredicate = new JoinDistancePredicate(
        "content",
        "content",
        100);
    public static String JOIN_DISTANCE_ID = "join distance";

    public static TupleSinkPredicate tupleSinkPredicate = new TupleSinkPredicate();
    public static String TUPLE_SINK_ID = "tuple sink";

    public static void setDefaultID() {
        keywordSourcePredicate.setID(KEYWORD_SOURCE_ID);
        regexPredicate.setID(REGEX_ID);
        fuzzyTokenPredicate.setID(FUZZY_TOKEN_ID);
        nlpEntityPredicate.setID(NLP_ENTITY_ID);
        joinDistancePredicate.setID(JOIN_DISTANCE_ID);
        tupleSinkPredicate.setID(TUPLE_SINK_ID);
    }

    static {
        setDefaultID();
    }

    /*
     * It generates a valid logical plan as follows.
     *
     * KeywordSource --> RegexMatcher --> TupleSink
     *
     */
    public static LogicalPlan getLogicalPlan1() throws TexeraException {
        setDefaultID();
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));
        return logicalPlan;
    }

    /*
     * It generates a valid logical plan as follows.
     *
     *                  -> RegexMatcher -->
     * KeywordSource --<                     >-- Join --> TupleSink
     *                  -> NlpEntityOperator -->
     *
     */
    public static LogicalPlan getLogicalPlan2() throws TexeraException {
        setDefaultID();
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(nlpEntityPredicate);
        logicalPlan.addOperator(joinDistancePredicate);
        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, NLP_ENTITY_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, JOIN_DISTANCE_ID));
        logicalPlan.addLink(new OperatorLink(NLP_ENTITY_ID, JOIN_DISTANCE_ID));
        logicalPlan.addLink(new OperatorLink(JOIN_DISTANCE_ID, TUPLE_SINK_ID));
        return logicalPlan;
    }

    /*
     * It generates a valid logical plan as follows.
     *
     *                  --> RegexMatcher -->
     *                  |                    >-- Join1
     * KeywordSource --< -> NlpEntityOperator -->          >-- Join2 --> TupleSink
     *                  |                           /
     *                  --> FuzzyTokenMatcher ----->
     *
     */
    public static LogicalPlan getLogicalPlan3() throws TexeraException {
        setDefaultID();
        LogicalPlan logicalPlan = new LogicalPlan();

        String JOIN_DISTANCE_ID_2 = "join distance 2";

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(nlpEntityPredicate);
        logicalPlan.addOperator(fuzzyTokenPredicate);
        logicalPlan.addOperator(joinDistancePredicate);
        joinDistancePredicate.setID(JOIN_DISTANCE_ID_2);
        logicalPlan.addOperator(joinDistancePredicate);
        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, NLP_ENTITY_ID));
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, FUZZY_TOKEN_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, JOIN_DISTANCE_ID));
        logicalPlan.addLink(new OperatorLink(NLP_ENTITY_ID, JOIN_DISTANCE_ID));
        logicalPlan.addLink(new OperatorLink(JOIN_DISTANCE_ID, JOIN_DISTANCE_ID_2));
        logicalPlan.addLink(new OperatorLink(FUZZY_TOKEN_ID, JOIN_DISTANCE_ID_2));
        logicalPlan.addLink(new OperatorLink(JOIN_DISTANCE_ID_2, TUPLE_SINK_ID));
        return logicalPlan;
    }

    /*
     * Test a valid operator graph.
     *
     * KeywordSource --> RegexMatcher --> TupleSink
     *
     */
    @Test
    public void testLogicalPlan1() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan1();

        Plan queryPlan = logicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();
        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertTrue(tupleSink instanceof TupleSink);

        IOperator regexMatcher = ((TupleSink) tupleSink).getInputOperator();
        Assert.assertTrue(regexMatcher instanceof RegexMatcher);

        IOperator keywordSource = ((RegexMatcher) regexMatcher).getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);
    }

    /*
     * Test a valid operator graph.
     *                  -> RegexMatcher -->
     * KeywordSource --<                     >-- Join --> TupleSink
     *                  -> NlpEntityOperator -->
     *
     */
    @Test
    public void testLogicalPlan2() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan2();

        Plan queryPlan = logicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();
        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertTrue(tupleSink instanceof TupleSink);

        IOperator join = ((TupleSink) tupleSink).getInputOperator();
        Assert.assertTrue(join instanceof Join);

        IOperator joinInput1 = ((Join) join).getInnerInputOperator();
        Assert.assertTrue(joinInput1 instanceof RegexMatcher);

        IOperator joinInput2 = ((Join) join).getOuterInputOperator();
        Assert.assertTrue(joinInput2 instanceof NlpEntityOperator);

        IOperator connectorOut1 = ((RegexMatcher) joinInput1).getInputOperator();
        Assert.assertTrue(connectorOut1 instanceof ConnectorOutputOperator);

        IOperator connectorOut2 = ((NlpEntityOperator) joinInput2).getInputOperator();
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
     * KeywordSource --< -> NlpEntityOperator -->          >-- Join2 --> TupleSink
     *                  |                           /
     *                  --> FuzzyTokenMatcher ----->
     *
     */
    @Test
    public void testLogicalPlan3() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan3();

        Plan queryPlan = logicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();

        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertTrue(tupleSink instanceof TupleSink);

        IOperator join2 = ((TupleSink) tupleSink).getInputOperator();
        Assert.assertTrue(join2 instanceof Join);

        IOperator join2Input1 = ((Join) join2).getOuterInputOperator();
        Assert.assertTrue(join2Input1 instanceof Join);

        IOperator join2Input2 = ((Join) join2).getInnerInputOperator();
        Assert.assertTrue(join2Input2 instanceof FuzzyTokenMatcher);

        IOperator join1Input1 = ((Join) join2Input1).getInnerInputOperator();
        Assert.assertTrue(join1Input1 instanceof RegexMatcher);

        IOperator join1Input2 = ((Join) join2Input1).getOuterInputOperator();
        Assert.assertTrue(join1Input2 instanceof NlpEntityOperator);

        IOperator connectorOut1 = ((RegexMatcher) join1Input1).getInputOperator();
        Assert.assertTrue(connectorOut1 instanceof ConnectorOutputOperator);

        IOperator connectorOut2 = ((NlpEntityOperator) join1Input2).getInputOperator();
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
     * Test a operator graph with more than one sink operators
     *                                   -> TupleSink1
     * KeywordSource --> RegexMatcher --<
     *                                   -> TupleSink2
     */
    @Test
    public void testLogicalPla4() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        String TUPLE_SINK_ID_2 = "tuple sink 2";

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        tupleSinkPredicate.setID(TUPLE_SINK_ID_2);
        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID_2));

        Plan queryPlan = logicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();

        Assert.assertEquals(2, sinkHashMap.size());

    }

    /*
     * Test a operator graph without a source operator
     *
     * RegexMatcher --> TupleSink
     *
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan1() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph without a sink operator
     *
     * KeywordSource --> RegexMatcher
     *
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan2() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));

        logicalPlan.buildQueryPlan();
    }



    /*
     * Test a operator graph with a disconnected component
     *
     * KeywordSource --> RegexMatcher --> TupleSink
     * RegexMatcher --> NlpEntityOperator
     * (a disconnected graph)
     *
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan3() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();
        String REGEX_ID_2 = "regex 2";
        RegexPredicate regexPredicate2 = new RegexPredicate("ca(lifornia)?",
            Arrays.asList("location", "content"),
            "regexResults");

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addOperator(regexPredicate2);
        logicalPlan.addOperator(nlpEntityPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID_2, NLP_ENTITY_ID));

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with a cycle
     *
     * KeywordSource --> RegexMatcher1 -->
     *                                     >- Join --> TupleSink
     *                                 -->
     * RegexMathcer2 -> NlpEntityOperator -<
     *                                 --> (back to the same) RegexMatcher2
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan4() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        String REGEX_ID_2 = "regex 2";

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        regexPredicate.setID(REGEX_ID_2);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(nlpEntityPredicate);
        logicalPlan.addOperator(joinDistancePredicate);


        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, JOIN_DISTANCE_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID_2, NLP_ENTITY_ID));
        logicalPlan.addLink(new OperatorLink(NLP_ENTITY_ID, REGEX_ID_2));
        logicalPlan.addLink(new OperatorLink(NLP_ENTITY_ID, JOIN_DISTANCE_ID));
        logicalPlan.addLink(new OperatorLink(JOIN_DISTANCE_ID, TUPLE_SINK_ID));

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with an invalid input arity
     *
     * KeywordSource1 --> RegexMatcher1 ->
     *                                    >-- TupleSink
     * KeywordSource2 --> RegexMatcher2 ->
     *
     *
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan5() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        String KEYWORD_SOURCE_ID_2 = "keyword source 2";
        String REGEX_ID_2 = "regex 2";

        logicalPlan.addOperator(keywordSourcePredicate);
        keywordSourcePredicate.setID(KEYWORD_SOURCE_ID_2);
        logicalPlan.addOperator(keywordSourcePredicate);

        logicalPlan.addOperator(regexPredicate);
        regexPredicate.setID(REGEX_ID_2);
        logicalPlan.addOperator(regexPredicate);

        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID_2, REGEX_ID_2));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID_2, TUPLE_SINK_ID));

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with an invalid output arity
     *                 -> RegexMatcher1 --> TupleSink
     * KeywordSource -<
     *                 -> RegexMatcher2
     *
     * It's okay for KeywordSource to have 2 outputs,
     * the problem is RegexMatcher2 doesn't have any outputs.
     *
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan6() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        String REGEX_ID_2 = "regex 2";

        logicalPlan.addOperator(keywordSourcePredicate);

        logicalPlan.addOperator(regexPredicate);
        regexPredicate.setID(REGEX_ID_2);
        logicalPlan.addOperator(regexPredicate);

        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID_2));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with a cycle
     *
     * KeywordSource --> FileSik --> (back to the same) KeywordSource
     *
     */
    @Test(expected = TexeraException.class)
    public void testInvalidLogicalPlan7() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(tupleSinkPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, TUPLE_SINK_ID));
        logicalPlan.addLink(new OperatorLink(TUPLE_SINK_ID, KEYWORD_SOURCE_ID));

        logicalPlan.buildQueryPlan();
    }

    /*
     * Test getOutputSchema on a valid operator graph.
     *
     * KeywordSource --> RegexMatcher --> TupleSink
     *
     */
    @Test
    public void testGetOutputSchema1() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan1();
        Plan queryPlan = logicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();
        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertNotNull(tupleSink);

        IOperator regexMatcher = ((TupleSink) tupleSink).getInputOperator();
        IOperator keywordSource = ((RegexMatcher) regexMatcher).getInputOperator();

        regexMatcher.open();
        Schema expectedSourceOutputSchema  = keywordSource.getOutputSchema();
        Schema expectedMatcherOutputSchema = regexMatcher.getOutputSchema();
        regexMatcher.close();

        Schema sourceOutputSchema  = logicalPlan.getOperatorOutputSchema(KEYWORD_SOURCE_ID);
        Schema matcherOutputSchema = logicalPlan.getOperatorOutputSchema(REGEX_ID);

        Assert.assertEquals(expectedSourceOutputSchema, sourceOutputSchema);
        Assert.assertEquals(expectedMatcherOutputSchema, matcherOutputSchema);

    }

    /*
     * Test getOutputSchema on a valid operator graph.
     *                  -> RegexMatcher -->
     * KeywordSource --<                     >-- Join --> TupleSink
     *                  -> NlpEntityOperator -->
     *
     */
    @Test
    public void testGetOutputSchema2() throws Exception {
        LogicalPlan logicalPlan = getLogicalPlan2();

        Plan queryPlan = logicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();
        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertNotNull(tupleSink);

        IOperator join = ((TupleSink) tupleSink).getInputOperator();
        IOperator joinInput1 = ((Join) join).getInnerInputOperator();
        IOperator joinInput2 = ((Join) join).getOuterInputOperator();
        IOperator connectorOut1 = ((RegexMatcher) joinInput1).getInputOperator();
        IOperator connectorOut2 = ((NlpEntityOperator) joinInput2).getInputOperator();

        OneToNBroadcastConnector connector1 = ((ConnectorOutputOperator) connectorOut1).getOwnerConnector();
        OneToNBroadcastConnector connector2 = ((ConnectorOutputOperator) connectorOut2).getOwnerConnector();
        IOperator keywordSource = connector1.getInputOperator();

        join.open();
        Schema expectedJoinOutputSchema      = join.getOutputSchema();
        Schema expectedSourceOutputSchema    = keywordSource.getOutputSchema();
        Schema expectedMatcherOutputSchema   = joinInput1.getOutputSchema();
        Schema expectedNlpEntityOutputSchema = joinInput2.getOutputSchema();
        join.close();

        Schema joinOutputSchema      = logicalPlan.getOperatorOutputSchema(JOIN_DISTANCE_ID);
        Schema sourceOutputSchema    = logicalPlan.getOperatorOutputSchema(KEYWORD_SOURCE_ID);
        Schema matcherOutputSchema   = logicalPlan.getOperatorOutputSchema(REGEX_ID);
        Schema nlpEntityOutputSchema = logicalPlan.getOperatorOutputSchema(NLP_ENTITY_ID);

        Assert.assertEquals(expectedJoinOutputSchema,      joinOutputSchema);
        Assert.assertEquals(expectedSourceOutputSchema,    sourceOutputSchema);
        Assert.assertEquals(expectedMatcherOutputSchema,   matcherOutputSchema);
        Assert.assertEquals(expectedNlpEntityOutputSchema, nlpEntityOutputSchema);
    }

    /*
     * Test getOutputSchema on a operator graph without a source operator
     *
     * RegexMatcher --> TupleSink
     *
     */
    @Test(expected = TexeraException.class)
    public void testGetOutputSchema3() throws Exception {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));

        logicalPlan.getOperatorOutputSchema(REGEX_ID);
    }

    /*
     * Test getOutputSchema on a operator graph without a sink operator
     *
     * KeywordSource --> RegexMatcher
     *
     */
    @Test
    public void testGetOutputSchema4() throws Exception {
        LogicalPlan validLogicalPlan = getLogicalPlan1();
        Plan queryPlan = validLogicalPlan.buildQueryPlan();

        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();
        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertNotNull(tupleSink);
        IOperator regexMatcher = ((TupleSink) tupleSink).getInputOperator();
        IOperator keywordSource = ((RegexMatcher) regexMatcher).getInputOperator();

        regexMatcher.open();
        Schema expectedSourceOutputSchema  = keywordSource.getOutputSchema();
        Schema expectedMatcherOutputSchema = regexMatcher.getOutputSchema();
        regexMatcher.close();

        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));

        Schema sourceOutputSchema  = logicalPlan.getOperatorOutputSchema(KEYWORD_SOURCE_ID);
        Schema matcherOutputSchema = logicalPlan.getOperatorOutputSchema(REGEX_ID);

        Assert.assertEquals(expectedSourceOutputSchema, sourceOutputSchema);
        Assert.assertEquals(expectedMatcherOutputSchema, matcherOutputSchema);

    }

    /*
     * Test a operator graph with a disconnected component
     *
     * KeywordSource --> RegexMatcher --> TupleSink
     * RegexMatcher --> NlpEntityOperator
     * (a disconnected graph)
     *
     */
    @Test(expected = TexeraException.class)
    public void testGetOutputSchema5() throws Exception {

        LogicalPlan validLogicalPlan = getLogicalPlan1();
        Plan queryPlan = validLogicalPlan.buildQueryPlan();


        HashMap<String, ISink> sinkHashMap = queryPlan.getSinkMap();
        Assert.assertEquals(1, sinkHashMap.size());

        ISink tupleSink = null;
        for (HashMap.Entry<String, ISink> entry: sinkHashMap.entrySet()) {
            tupleSink = entry.getValue();
        }
        Assert.assertNotNull(tupleSink);

        IOperator regexMatcher = ((TupleSink) tupleSink).getInputOperator();
        IOperator keywordSource = ((RegexMatcher) regexMatcher).getInputOperator();

        regexMatcher.open();
        Schema expectedSourceOutputSchema  = keywordSource.getOutputSchema();
        Schema expectedMatcherOutputSchema = regexMatcher.getOutputSchema();
        regexMatcher.close();

        LogicalPlan logicalPlan = new LogicalPlan();
        String REGEX_ID_2 = "regex 2";
        RegexPredicate regexPredicate2 = new RegexPredicate("ca(lifornia)?",
            Arrays.asList("location", "content"),
            "regexResults");
        regexPredicate2.setID(REGEX_ID_2);

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        logicalPlan.addOperator(regexPredicate2);
        logicalPlan.addOperator(nlpEntityPredicate);

        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID_2, NLP_ENTITY_ID));

        Schema sourceOutputSchema  = logicalPlan.getOperatorOutputSchema(KEYWORD_SOURCE_ID);
        Schema matcherOutputSchema = logicalPlan.getOperatorOutputSchema(REGEX_ID);

        Assert.assertEquals(expectedSourceOutputSchema, sourceOutputSchema);
        Assert.assertEquals(expectedMatcherOutputSchema, matcherOutputSchema);
        Schema raiseExceptionSchema  = logicalPlan.getOperatorOutputSchema(REGEX_ID_2);
    }

}
