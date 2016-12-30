package edu.uci.ics.textdb.textql.planbuilder;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.textql.languageparser.CreateViewStatement;
import edu.uci.ics.textdb.textql.languageparser.KeywordExtractPredicate;
import edu.uci.ics.textdb.textql.languageparser.SelectStatement;
import edu.uci.ics.textdb.textql.languageparser.Statement;
import edu.uci.ics.textdb.textql.languageparser.TextQLParser;
import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.textql.planbuilder.beans.ScanSourceBean;
import edu.uci.ics.textdb.web.request.beans.FileSinkBean;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;
import edu.uci.ics.textdb.web.request.beans.KeywordSourceBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.OperatorLinkBean;
import edu.uci.ics.textdb.web.request.beans.ProjectionBean;

import junit.framework.Assert;

/**
 * Test cases for TextQLLogicalPlanBuilder and TextQLLogicalPlanBuilderUtils classes.
 * Check the correct result for the tasks of parsing the input, extract subStatements from a statement list,
 * convert a statement list into beans, remove the PassThrough bean, merge source operator and extract operator
 * and build a logical plan.
 *  
 * @author Flavio Bayer 
 * 
 */
public class TextQLLogicalPlanBuilderTest {
    
    private static final FileSinkBean defaultSinkBean = new FileSinkBean("_OutputSink", "FileSink", "", null, null, "output.txt");
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM t;
     * The query is the bare minimum query to be tested.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation00() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        
        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new PassThroughBean("_sid0_e", "PassThrough"),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("t", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("t", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // TODO: ScanSource is not implemented yet, test LogicalPlan creation once it is implemented
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT a,b FROM t;
     * The query is used to test a basic projection operator.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation01() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT a,b FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        
        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", null, Arrays.asList("a", "b"), null, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new PassThroughBean("_sid0_e", "PassThrough"),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid0_p")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid0_p")
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // TODO: ScanSource is not implemented yet, test LogicalPlan creation once it is implemented
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS EXTRACT KEYWORDMATCH([a,b],"key") FROM t;
     * The query is used to test a basic extract operator with multiple fields of search.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation02() throws Exception {
        String queryText = "CREATE VIEW out AS EXTRACT KEYWORDMATCH([a,b],\"key\") FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;
        
        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a","b"), "key", null);
        SelectStatement statement0 = new SelectStatement("_sid0", null, null, extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid0_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordSourceBean("_sid0_e", "KeywordSource", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS EXTRACT KEYWORDMATCH(a,"key",substring) FROM t;
     * The query is used to test a basic extract operator with a specified a KeywordMatchingType.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation03() throws Exception {
        String queryText = "CREATE VIEW out AS EXTRACT KEYWORDMATCH(a,\"key\",substring) FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;

        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement0 = new SelectStatement("_sid0", null, null, extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid0_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordSourceBean("_sid0_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM t LIMIT 5 OFFSET 1;
     * The query is used to test a basic limit and offset clause.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation04() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM t LIMIT 5 OFFSET 1;";
        List<Statement> expectedStatements, computedStatements;
        
        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "t", 5, 1);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // TODO: take limit and offset into account and perform all the other tests
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS EXTRACT KEYWORDMATCH(a,"key",substring) FROM t LIMIT 5 OFFSET 1;
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation05() throws Exception {
        String queryText = "CREATE VIEW out AS EXTRACT KEYWORDMATCH(a,\"key\",substring) FROM t LIMIT 5 OFFSET 1;";
        List<Statement> expectedStatements, computedStatements;

        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement0 = new SelectStatement("_sid0", null, null, extractPredicate0, "t", 5, 1);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);

        // TODO: take limit and offset into account
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM b;
     *  CREATE VIEW b AS SELECT * FROM a;
     * The query is used to test linked statements.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation06() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM b;\nCREATE VIEW b AS SELECT * FROM a;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        
        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "b", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        SelectStatement statement2 = new SelectStatement("_sid1", true, null, null, "a", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("b", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new PassThroughBean("_sid0_e", "PassThrough"),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new PassThroughBean("b", "PassThrough"),
                new PassThroughBean("_sid1", "PassThrough"),
                new PassThroughBean("_sid1_p", "PassThrough"),
                new PassThroughBean("_sid1_e", "PassThrough"),
                new PassThroughBean("_sid1_s", "PassThrough"),
                new ScanSourceBean("a", "ScanSource", "", null, null, "a")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("b","_sid0_s"),
                new OperatorLinkBean("_sid1","b"),
                new OperatorLinkBean("_sid1_p","_sid1"),
                new OperatorLinkBean("_sid1_e","_sid1_p"),
                new OperatorLinkBean("_sid1_s","_sid1_e"),
                new OperatorLinkBean("a","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ScanSourceBean("a", "ScanSource", "", null, null, "a")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("a", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ScanSourceBean("a", "ScanSource", "", null, null, "a")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("a", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // TODO: ScanSource is not implemented yet, test LogicalPlan creation once it is implemented
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM b;
     *  CREATE VIEW b AS EXTRACT KEYWORDMATCH(a,"key", substring) FROM t;
     * The query is used to test linked statements with an extract statement in the inner statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation07() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM b; \nCREATE VIEW b AS EXTRACT KEYWORDMATCH(a,\"key\", substring) FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;

        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "b", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement2 = new SelectStatement("_sid1", null, null, extractPredicate0, "t", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("b", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new PassThroughBean("_sid0_e", "PassThrough"),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new PassThroughBean("b", "PassThrough"),
                new PassThroughBean("_sid1", "PassThrough"),
                new PassThroughBean("_sid1_p", "PassThrough"),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new PassThroughBean("_sid1_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("b","_sid0_s"),
                new OperatorLinkBean("_sid1","b"),
                new OperatorLinkBean("_sid1_p","_sid1"),
                new OperatorLinkBean("_sid1_e","_sid1_p"),
                new OperatorLinkBean("_sid1_s","_sid1_e"),
                new OperatorLinkBean("t","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid1_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid1_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordSourceBean("_sid1_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid1_e", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS EXTRACT KEYWORDMATCH(a,"key", substring) FROM b;
     *  CREATE VIEW b AS SELECT * FROM t;
     * The query is used to test linked statements with an extract statement in the outer statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation08() throws Exception {
        String queryText = "CREATE VIEW out AS EXTRACT KEYWORDMATCH(a,\"key\", substring) FROM b; CREATE VIEW b AS SELECT * FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;

        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement0 = new SelectStatement("_sid0", null, null, extractPredicate0, "b", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        SelectStatement statement2 = new SelectStatement("_sid1", true, null, null, "t", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("b", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new PassThroughBean("b", "PassThrough"),
                new PassThroughBean("_sid1", "PassThrough"),
                new PassThroughBean("_sid1_p", "PassThrough"),
                new PassThroughBean("_sid1_e", "PassThrough"),
                new PassThroughBean("_sid1_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("b","_sid0_s"),
                new OperatorLinkBean("_sid1","b"),
                new OperatorLinkBean("_sid1_p","_sid1"),
                new OperatorLinkBean("_sid1_e","_sid1_p"),
                new OperatorLinkBean("_sid1_s","_sid1_e"),
                new OperatorLinkBean("t","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid0_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordSourceBean("_sid0_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT a,b FROM t;
     *  CREATE VIEW t AS EXTRACT KEYWORDMATCH(a,"key", substring) FROM t2;
     * The query is used to test linked statements with a projection and an extract statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation09() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT a,b FROM t;\nCREATE VIEW t AS EXTRACT KEYWORDMATCH(a,\"key\", substring) FROM t2;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;

        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", null, Arrays.asList("a","b"), null, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement2 = new SelectStatement("_sid1", null, null, extractPredicate0, "t2", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("t", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new PassThroughBean("_sid0_e", "PassThrough"),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new PassThroughBean("t", "PassThrough"),
                new PassThroughBean("_sid1", "PassThrough"),
                new PassThroughBean("_sid1_p", "PassThrough"),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new PassThroughBean("_sid1_s", "PassThrough"),
                new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s"),
                new OperatorLinkBean("_sid1","t"),
                new OperatorLinkBean("_sid1_p","_sid1"),
                new OperatorLinkBean("_sid1_e","_sid1_p"),
                new OperatorLinkBean("_sid1_s","_sid1_e"),
                new OperatorLinkBean("t2","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid1_e", "_sid0_p"),
                new OperatorLinkBean("t2", "_sid1_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordSourceBean("_sid1_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid1_e", "_sid0_p")
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * EXTRACT KEYWORDMATCH([a,b],"key") FROM t;
     * The query is used to test a statement with projection end extraction operators.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation10() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * EXTRACT KEYWORDMATCH([a,b],\"key\") FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;
        
        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a","b"), "key", null);
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("t", "_sid0_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordSourceBean("_sid0_e", "KeywordSource", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID())
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT a,b EXTRACT KEYWORDMATCH([a,b],"key") FROM t;
     * The query is used to test a statement with projection with multiple fields and extraction with multiple fields.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation11() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT a,b EXTRACT KEYWORDMATCH([a,b],\"key\") FROM t;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;
        
        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("a","b"), "key", null);
        SelectStatement statement0 = new SelectStatement("_sid0", null, Arrays.asList("a","b"), extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        expectedStatements = Arrays.asList(statement1);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement0);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new ScanSourceBean("t", "ScanSource", "", null, null, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid0_e", "_sid0_p"),
                new OperatorLinkBean("t", "_sid0_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordSourceBean("_sid0_e", "KeywordSource", "a,b", null, null, "key", KeywordExtractPredicate.DEFAULT_MATCH_TYPE, "t")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid0_e", "_sid0_p")
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS EXTRACT KEYWORDMATCH(b,"keywo") FROM t;
     *  CREATE VIEW t AS SELECT a,b EXTRACT KEYWORDMATCH(a,"key", substring) FROM t2;
     * The query is used to test linked statements with multiple extract statements and one projection in the inner statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation12() throws Exception {
        String queryText = "CREATE VIEW out AS EXTRACT KEYWORDMATCH(b,\"keywo\") FROM t;" + 
                           "CREATE VIEW t AS SELECT a,b EXTRACT KEYWORDMATCH(a,\"key\", substring) FROM t2;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;
        
        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("b"), "keywo", null);
        SelectStatement statement0 = new SelectStatement("_sid0", null, null, extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        KeywordExtractPredicate extractPredicate1 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement2 = new SelectStatement("_sid1", null, Arrays.asList("a","b"), extractPredicate1, "t2", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("t", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new PassThroughBean("_sid0_p", "PassThrough"),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keywo", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new PassThroughBean("t", "PassThrough"),
                new PassThroughBean("_sid1", "PassThrough"),
                new ProjectionBean("_sid1_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new PassThroughBean("_sid1_s", "PassThrough"),
                new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s"),
                new OperatorLinkBean("_sid1","t"),
                new OperatorLinkBean("_sid1_p","_sid1"),
                new OperatorLinkBean("_sid1_e","_sid1_p"),
                new OperatorLinkBean("_sid1_s","_sid1_e"),
                new OperatorLinkBean("t2","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keywo", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new ProjectionBean("_sid1_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid1_p", "_sid0_e"),
                new OperatorLinkBean("_sid1_e", "_sid1_p"),
                new OperatorLinkBean("t2", "_sid1_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keywo", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
                new ProjectionBean("_sid1_p", "Projection", "a,b", null, null),
                new KeywordSourceBean("_sid1_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid1_p", "_sid0_e"),
                new OperatorLinkBean("_sid1_e", "_sid1_p")
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS EXTRACT KEYWORDMATCH(b,"keywo") FROM t;
     *  CREATE VIEW t AS SELECT a,b FROM t2;
     * The query is used to test linked statements with an extract statements in the outer statement
     * and a projection in the inner statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation13() throws Exception {
        String queryText = "CREATE VIEW out AS EXTRACT KEYWORDMATCH(b,\"keywo\") FROM t;"+
                           "CREATE VIEW t AS SELECT a,b FROM t2;";
       List<Statement> expectedStatements, computedStatements;
       List<OperatorBean> expectedOperatorBeans;
       List<OperatorLinkBean> expectedLinkBeans;
       List<OperatorBean> computedOperatorBeans;
       List<OperatorLinkBean> computedLinkBeans;
       
       // Check for correct output of the parser
       KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("b"), "keywo", null);
       SelectStatement statement0 = new SelectStatement("_sid0", null, null, extractPredicate0, "t", null, null);
       CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
       SelectStatement statement2 = new SelectStatement("_sid1", null, Arrays.asList("a","b"), null, "t2", null, null);
       CreateViewStatement statement3 = new CreateViewStatement("t", statement2);
       expectedStatements = Arrays.asList(statement1, statement3);
       TextQLParser parser = new TextQLParser(new StringReader(queryText));
       computedStatements = parser.mainStatementList(null);
       Assert.assertEquals(expectedStatements, computedStatements);
       
       // Check for correct subStatements extraction on the list of statements
       expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
       computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
       Assert.assertEquals(expectedStatements, computedStatements);
       
       // Check for the correct result when converting from statements to beans
       expectedOperatorBeans = Arrays.asList(
               new PassThroughBean("out", "PassThrough"),
               new PassThroughBean("_sid0", "PassThrough"),
               new PassThroughBean("_sid0_p", "PassThrough"),
               new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keywo", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
               new PassThroughBean("_sid0_s", "PassThrough"),
               new PassThroughBean("t", "PassThrough"),
               new PassThroughBean("_sid1", "PassThrough"),
               new ProjectionBean("_sid1_p", "Projection", "a,b", null, null),
               new PassThroughBean("_sid1_e", "PassThrough"),
               new PassThroughBean("_sid1_s", "PassThrough"),
               new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
       );
       expectedLinkBeans = Arrays.asList(
               new OperatorLinkBean("_sid0","out"),
               new OperatorLinkBean("_sid0_p","_sid0"),
               new OperatorLinkBean("_sid0_e","_sid0_p"),
               new OperatorLinkBean("_sid0_s","_sid0_e"),
               new OperatorLinkBean("t","_sid0_s"),
               new OperatorLinkBean("_sid1","t"),
               new OperatorLinkBean("_sid1_p","_sid1"),
               new OperatorLinkBean("_sid1_e","_sid1_p"),
               new OperatorLinkBean("_sid1_s","_sid1_e"),
               new OperatorLinkBean("t2","_sid1_s")
       );
       computedOperatorBeans = new ArrayList<>();
       computedLinkBeans = new ArrayList<>();
       TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
       Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
       Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
       
       // Check for correct result when removing PassThrough beans
       computedOperatorBeans.add(0, defaultSinkBean);
       computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
       expectedOperatorBeans = Arrays.asList(
               defaultSinkBean,
               new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keywo", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
               new ProjectionBean("_sid1_p", "Projection", "a,b", null, null),
               new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
       );
       expectedLinkBeans = Arrays.asList(
               new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
               new OperatorLinkBean("_sid1_p", "_sid0_e"),
               new OperatorLinkBean("t2", "_sid1_p")
       );
       TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
       Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
       Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
       
       // Check for correct result when merging source and extract operators
       expectedOperatorBeans = Arrays.asList(
               defaultSinkBean,
               new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keywo", KeywordExtractPredicate.DEFAULT_MATCH_TYPE),
               new ProjectionBean("_sid1_p", "Projection", "a,b", null, null),
               new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
       );
       expectedLinkBeans = Arrays.asList(
               new OperatorLinkBean("_sid0_e", defaultSinkBean.getOperatorID()),
               new OperatorLinkBean("_sid1_p", "_sid0_e"),
               new OperatorLinkBean("t2", "_sid1_p")
       );
       TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
       Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
       Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

       // TODO: ScanSource is not implemented yet, test LogicalPlan creation once it is implemented
       
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT a,b EXTRACT KEYWORDMATCH(b, "key", substring) FROM t;
     *  CREATE VIEW t AS EXTRACT KEYWORDMATCH(a, "key", substring) FROM t2;
     * The query is used to test linked statements with multiple extract statements and one projection in the outer statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation14() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT a,b EXTRACT KEYWORDMATCH(b, \"keyho\", phrase) FROM t;" +
                           "CREATE VIEW t AS EXTRACT KEYWORDMATCH(a, \"key\", substring) FROM t2;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;
        
        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("b"), "keyho", "phrase");
        SelectStatement statement0 = new SelectStatement("_sid0", null, Arrays.asList("a","b"), extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        KeywordExtractPredicate extractPredicate1 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement2 = new SelectStatement("_sid1", null, null, extractPredicate1, "t2", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("t", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
                
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
                new PassThroughBean("out", "PassThrough"),
                new PassThroughBean("_sid0", "PassThrough"),
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keyho", KeywordMatchingType.PHRASE_INDEXBASED),
                new PassThroughBean("_sid0_s", "PassThrough"),
                new PassThroughBean("t", "PassThrough"),
                new PassThroughBean("_sid1", "PassThrough"),
                new PassThroughBean("_sid1_p", "PassThrough"),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new PassThroughBean("_sid1_s", "PassThrough"),
                new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0","out"),
                new OperatorLinkBean("_sid0_p","_sid0"),
                new OperatorLinkBean("_sid0_e","_sid0_p"),
                new OperatorLinkBean("_sid0_s","_sid0_e"),
                new OperatorLinkBean("t","_sid0_s"),
                new OperatorLinkBean("_sid1","t"),
                new OperatorLinkBean("_sid1_p","_sid1"),
                new OperatorLinkBean("_sid1_e","_sid1_p"),
                new OperatorLinkBean("_sid1_s","_sid1_e"),
                new OperatorLinkBean("t2","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keyho", KeywordMatchingType.PHRASE_INDEXBASED),
                new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
                new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid0_e", "_sid0_p"),
                new OperatorLinkBean("_sid1_e", "_sid0_e"),
                new OperatorLinkBean("t2", "_sid1_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
                defaultSinkBean,
                new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
                new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keyho", KeywordMatchingType.PHRASE_INDEXBASED),
                new KeywordSourceBean("_sid1_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t2")
        );
        expectedLinkBeans = Arrays.asList(
                new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
                new OperatorLinkBean("_sid0_e", "_sid0_p"),
                new OperatorLinkBean("_sid1_e", "_sid0_e")
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);

        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
            expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
            expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT a,b EXTRACT KEYWORDMATCH(b,"key", substring) FROM t;
     *  CREATE VIEW t AS SELECT a,b,c EXTRACT KEYWORDMATCH(a,"key", substring) FROM t2;
     * The query is used to test linked statements with multiple extract statements and multiple projection statements.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation15() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT a,b EXTRACT KEYWORDMATCH(b,\"keyho\", phrase) FROM t;" +
                           "CREATE VIEW t AS SELECT a,b,c EXTRACT KEYWORDMATCH(a,\"key\", substring) FROM t2;";
        List<Statement> expectedStatements, computedStatements;
        List<OperatorBean> expectedOperatorBeans;
        List<OperatorLinkBean> expectedLinkBeans;
        List<OperatorBean> computedOperatorBeans;
        List<OperatorLinkBean> computedLinkBeans;
        LogicalPlan expectedlogicalPlan, computedLogicalPlan;
        
        // Check for correct output of the parser
        KeywordExtractPredicate extractPredicate0 = new KeywordExtractPredicate(Arrays.asList("b"), "keyho", "phrase");
        SelectStatement statement0 = new SelectStatement("_sid0", null, Arrays.asList("a","b"), extractPredicate0, "t", null, null);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        KeywordExtractPredicate extractPredicate1 = new KeywordExtractPredicate(Arrays.asList("a"), "key", "substring");
        SelectStatement statement2 = new SelectStatement("_sid1", null, Arrays.asList("a","b","c"), extractPredicate1, "t2", null, null);
        CreateViewStatement statement3 = new CreateViewStatement("t", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);
             
        // Check for the correct result when converting from statements to beans
        expectedOperatorBeans = Arrays.asList(
             new PassThroughBean("out", "PassThrough"),
             new PassThroughBean("_sid0", "PassThrough"),
             new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
             new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keyho", KeywordMatchingType.PHRASE_INDEXBASED),
             new PassThroughBean("_sid0_s", "PassThrough"),
             new PassThroughBean("t", "PassThrough"),
             new PassThroughBean("_sid1", "PassThrough"),
             new ProjectionBean("_sid1_p", "Projection", "a,b,c", null, null),
             new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
             new PassThroughBean("_sid1_s", "PassThrough"),
             new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
             new OperatorLinkBean("_sid0","out"),
             new OperatorLinkBean("_sid0_p","_sid0"),
             new OperatorLinkBean("_sid0_e","_sid0_p"),
             new OperatorLinkBean("_sid0_s","_sid0_e"),
             new OperatorLinkBean("t","_sid0_s"),
             new OperatorLinkBean("_sid1","t"),
             new OperatorLinkBean("_sid1_p","_sid1"),
             new OperatorLinkBean("_sid1_e","_sid1_p"),
             new OperatorLinkBean("_sid1_s","_sid1_e"),
             new OperatorLinkBean("t2","_sid1_s")
        );
        computedOperatorBeans = new ArrayList<>();
        computedLinkBeans = new ArrayList<>();
        TextQLLogicalPlanBuilderUtils.statementListToBeans(computedStatements, "out", computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when removing PassThrough beans
        computedOperatorBeans.add(0, defaultSinkBean);
        computedLinkBeans.add(0, new OperatorLinkBean("out", defaultSinkBean.getOperatorID()));
        expectedOperatorBeans = Arrays.asList(
             defaultSinkBean,
             new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
             new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keyho", KeywordMatchingType.PHRASE_INDEXBASED),
             new ProjectionBean("_sid1_p", "Projection", "a,b,c", null, null),
             new KeywordMatcherBean("_sid1_e", "KeywordMatcher", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED),
             new ScanSourceBean("t2", "ScanSource", "", null, null, "t2")
        );
        expectedLinkBeans = Arrays.asList(
             new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
             new OperatorLinkBean("_sid0_e", "_sid0_p"),
             new OperatorLinkBean("_sid1_p", "_sid0_e"),
             new OperatorLinkBean("_sid1_e", "_sid1_p"),
             new OperatorLinkBean("t2", "_sid1_e")
        );
        TextQLLogicalPlanBuilderUtils.removePassThoughBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when merging source and extract operators
        expectedOperatorBeans = Arrays.asList(
             defaultSinkBean,
             new ProjectionBean("_sid0_p", "Projection", "a,b", null, null),
             new KeywordMatcherBean("_sid0_e", "KeywordMatcher", "b", null, null, "keyho", KeywordMatchingType.PHRASE_INDEXBASED),
             new ProjectionBean("_sid1_p", "Projection", "a,b,c", null, null),
             new KeywordSourceBean("_sid1_e", "KeywordSource", "a", null, null, "key", KeywordMatchingType.SUBSTRING_SCANBASED, "t2")
        );
        expectedLinkBeans = Arrays.asList(
             new OperatorLinkBean("_sid0_p", defaultSinkBean.getOperatorID()),
             new OperatorLinkBean("_sid0_e", "_sid0_p"),
             new OperatorLinkBean("_sid1_p", "_sid0_e"),
             new OperatorLinkBean("_sid1_e", "_sid1_p")
        );
        TextQLLogicalPlanBuilderUtils.mergeSourceAndExtractBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedOperatorBeans, computedOperatorBeans);
        Assert.assertEquals(expectedLinkBeans, computedLinkBeans);
        
        // Check for correct result when building the Logical Plan
        expectedlogicalPlan = new LogicalPlan();
        for (OperatorBean operator : expectedOperatorBeans) {
         expectedlogicalPlan.addOperator(operator.getOperatorID(), operator.getOperatorType(), operator.getOperatorProperties());
        }
        for (OperatorLinkBean link : expectedLinkBeans) {
         expectedlogicalPlan.addLink(link.getFromOperatorID(), link.getToOperatorID());
        }
        computedLogicalPlan = TextQLLogicalPlanBuilderUtils.createLogicalPlanFromBeans(computedOperatorBeans, computedLinkBeans);
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);
        
        // Test the TextQLLogicalPlanBuilder class. If all the tests have been successful so far, there should be no problem 
        TextQLLogicalPlanBuilder planBuilder = new TextQLLogicalPlanBuilder(queryText);
        Assert.assertTrue(planBuilder.buildLogicalPlan());
        computedLogicalPlan = planBuilder.getLogicalPlan();
        Assert.assertEquals(expectedlogicalPlan, computedLogicalPlan);

    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM x LIMIT 7 OFFSET 3;
     *  CREATE VIEW x AS SELECT * FROM y LIMIT 15 OFFSET 1;
     * The query is used to test linked statements with limit and offset clauses.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation16() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM x LIMIT 7 OFFSET 3;" + 
                           "CREATE VIEW x AS SELECT * FROM y LIMIT 15 OFFSET 1;";
        List<Statement> expectedStatements, computedStatements;

        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "x", 7, 3);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        SelectStatement statement2 = new SelectStatement("_sid1", true, null, null, "y", 15, 1);
        CreateViewStatement statement3 = new CreateViewStatement("x", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);

        // TODO: take limit and offset into account and perform all the other tests
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM x LIMIT 15 OFFSET 1;
     *  CREATE VIEW x AS SELECT * FROM y LIMIT 7 OFFSET 3;
     * The query is used to test linked statements with limit and offset clauses.
     * This test is the same as the previous test, except that the values for the limit and offset clauses were
     * swapped between the inner statement and the outer statement.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation17() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM x LIMIT 15 OFFSET 1;" + 
                           "CREATE VIEW x AS SELECT * FROM y LIMIT 7 OFFSET 3;";
        List<Statement> expectedStatements, computedStatements;

        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "x", 15, 1);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        SelectStatement statement2 = new SelectStatement("_sid1", true, null, null, "y", 7, 3);
        CreateViewStatement statement3 = new CreateViewStatement("x", statement2);
        expectedStatements = Arrays.asList(statement1, statement3);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement0, statement2);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);

        // TODO: take limit and offset into account and perform all the other tests
        
    }
    
    /**
     * Test the creation of a logical plan for the following query:
     *  CREATE VIEW out AS SELECT * FROM x LIMIT 16 OFFSET 4;
     *  CREATE VIEW x AS SELECT * FROM y LIMIT 5 OFFSET 3;
     *  CREATE VIEW y AS SELECT * FROM z LIMIT 3 OFFSET 7;
     * The query is used to test multiple linked statements with limit and offset clauses.
     * @throws Exception If an exception occurs
     */
    @Test
    public void testLogicalPlanCreation18() throws Exception {
        String queryText = "CREATE VIEW out AS SELECT * FROM x LIMIT 16 OFFSET 4;" + 
                           "CREATE VIEW x AS SELECT * FROM y LIMIT 5 OFFSET 3;" +
                           "CREATE VIEW y AS SELECT * FROM z LIMIT 3 OFFSET 7;";
        List<Statement> expectedStatements, computedStatements;
        
        // Check for correct output of the parser
        SelectStatement statement0 = new SelectStatement("_sid0", true, null, null, "x", 16, 4);
        CreateViewStatement statement1 = new CreateViewStatement("out", statement0);
        SelectStatement statement2 = new SelectStatement("_sid1", true, null, null, "y", 5, 3);
        CreateViewStatement statement3 = new CreateViewStatement("x", statement2);
        SelectStatement statement4 = new SelectStatement("_sid2", true, null, null, "z", 3, 7);
        CreateViewStatement statement5 = new CreateViewStatement("y", statement4);
        expectedStatements = Arrays.asList(statement1, statement3, statement5);
        TextQLParser parser = new TextQLParser(new StringReader(queryText));
        computedStatements = parser.mainStatementList(null);
        Assert.assertEquals(expectedStatements, computedStatements);
        
        // Check for correct subStatements extraction on the list of statements
        expectedStatements = Arrays.asList(statement1, statement3, statement5, statement0, statement2, statement4);
        computedStatements = TextQLLogicalPlanBuilderUtils.extractSubStatements(computedStatements);
        Assert.assertEquals(expectedStatements, computedStatements);

        // TODO: take limit and offset into account and perform all the other tests

    }
    
    /*
     * TODO: add simpler and extreme tests for TextQLLogicalPlanBuilderUtils
     * (extractSubStatements, statementListToBeans, removePassThoughBeans ...)
     */
    
}
