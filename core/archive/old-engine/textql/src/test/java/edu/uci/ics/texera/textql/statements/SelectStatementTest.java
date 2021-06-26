package edu.uci.ics.texera.textql.statements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordPredicate;
import edu.uci.ics.texera.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.texera.textql.statements.SelectStatement;
import edu.uci.ics.texera.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectAllFieldsPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectSomeFieldsPredicate;

/**
 * This class contains test cases for the SelectStatement.
 * The constructor, getter, setter and bean builder methods are tested.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectStatementTest {

    /**
     * Test the class constructor and the getter methods.
     * Call the constructor of SelectStatement and test if the
     * returned value by the getter is the same.
     */
    @Test
    public void testConstructorAndGetters() {
        ProjectPredicate projectPredicate;
        ExtractPredicate extractPredicate;
        SelectStatement selectExtractStatement;

        // Tests for the id attribute
        selectExtractStatement = new SelectStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), null);

        selectExtractStatement = new SelectStatement("idx", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), "idx");
        
        selectExtractStatement = new SelectStatement("_sid08", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), "_sid08");

        // Tests for the projectPredicate attribute
        selectExtractStatement = new SelectStatement("", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), null);

        projectPredicate = new ProjectAllFieldsPredicate();
        selectExtractStatement = new SelectStatement(null, projectPredicate, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), projectPredicate);

        projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("a", "b", "c"));
        selectExtractStatement = new SelectStatement(null, projectPredicate, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), projectPredicate);

        projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("f0", "f1"));
        selectExtractStatement = new SelectStatement(null, projectPredicate, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), projectPredicate);
        
        // Tests for the extractPredicate attribute
        selectExtractStatement = new SelectStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), null);
        
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("x", "y"), "keyword",
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        selectExtractStatement = new SelectStatement(null, null, extractPredicate, null, null, null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);

        extractPredicate = new KeywordExtractPredicate(Arrays.asList("z6", "t4"), "xyz",
                KeywordMatchingType.PHRASE_INDEXBASED.toString());
        selectExtractStatement = new SelectStatement(null, null, extractPredicate, null, null, null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);

        // Tests for the fromClause attribute
        selectExtractStatement = new SelectStatement(null, null, null, null, null, null);
        selectExtractStatement.setFromClause(null);

        selectExtractStatement = new SelectStatement(null, null, null, "tab", null, null);
        selectExtractStatement.setFromClause("tab");

        selectExtractStatement = new SelectStatement(null, null, null, "t1", null, null);
        selectExtractStatement.setFromClause("t1");

        // Tests for the limitClause attribute
        selectExtractStatement = new SelectStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), null);

        selectExtractStatement = new SelectStatement(null, null, null, null, 0, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(0));

        selectExtractStatement = new SelectStatement(null, null, null, null, 8, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(8));
        
        selectExtractStatement = new SelectStatement(null, null, null, null, -151, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(-151));

        // Tests for the offsetClause attribute
        selectExtractStatement = new SelectStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), null);
        
        selectExtractStatement = new SelectStatement(null, null, null, null, null, 0);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(0));

        selectExtractStatement = new SelectStatement(null, null, null, null, null, 562);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(562));
        
        selectExtractStatement = new SelectStatement(null, null, null, null, null, -98);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(-98));
    }

    /**
     * Test the setter methods and the getter methods.
     * Call the setter of SelectStatement and test if the returned
     * value by the getter is the same.
     */
    @Test
    public void testSettersAndGetters() {
        ProjectPredicate projectPredicate;
        ExtractPredicate extractPredicate;
        SelectStatement selectExtractStatement = new SelectStatement();

        // Tests for the id attribute
        selectExtractStatement.setId(null);
        Assert.assertEquals(selectExtractStatement.getId(), null);
        
        selectExtractStatement.setId("idx");
        Assert.assertEquals(selectExtractStatement.getId(), "idx");
        
        selectExtractStatement.setId("_sid9");
        Assert.assertEquals(selectExtractStatement.getId(), "_sid9");

        // Tests for the projectPredicate attribute
        selectExtractStatement.setProjectPredicate(null);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), null);

        projectPredicate = new ProjectAllFieldsPredicate();
        selectExtractStatement.setProjectPredicate(projectPredicate);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), projectPredicate);

        projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("a", "b", "c"));
        selectExtractStatement.setProjectPredicate(projectPredicate);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), projectPredicate);

        projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("f0", "f1"));
        selectExtractStatement.setProjectPredicate(projectPredicate);
        Assert.assertEquals(selectExtractStatement.getProjectPredicate(), projectPredicate);

        // Tests for the extractPredicate attribute
        selectExtractStatement.setExtractPredicate(null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), null);

        extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "c"), "search",
                KeywordMatchingType.PHRASE_INDEXBASED.toString());
        selectExtractStatement.setExtractPredicate(extractPredicate);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);
        
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("u", "v"), "news",
                KeywordMatchingType.CONJUNCTION_INDEXBASED.toString());
        selectExtractStatement.setExtractPredicate(extractPredicate);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);

        // Tests for the fromClause attribute
        selectExtractStatement.setFromClause(null);
        Assert.assertEquals(selectExtractStatement.getFromClause(), null);
        
        selectExtractStatement.setFromClause("table");
        Assert.assertEquals(selectExtractStatement.getFromClause(), "table");
        
        selectExtractStatement.setFromClause("t3");
        Assert.assertEquals(selectExtractStatement.getFromClause(), "t3");

        // Tests for the limitClause attribute
        selectExtractStatement.setLimitClause(null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), null);

        selectExtractStatement.setLimitClause(0);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(0));

        selectExtractStatement.setLimitClause(5);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(5));
        
        selectExtractStatement.setLimitClause(-7);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(-7));

        // Tests for the offsetClause attribute
        selectExtractStatement.setOffsetClause(null);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), null);

        selectExtractStatement.setOffsetClause(0);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(0));

        selectExtractStatement.setOffsetClause(-3);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(-3));

        selectExtractStatement.setOffsetClause(58);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(58));
    }

    /**
     * Test the correctness of the generated beans by a SelectStatement without a
     * ProjectPredicate nor ExtractPredicate.
     * Get a graph by calling getInternalPredicateBases() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectStatementBeansBuilder00() {
        SelectStatement selectStatement = new SelectStatement("id", null, null, "tableX", null,
                null);

        List<PredicateBase> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList("tableX");

        Assert.assertEquals(selectStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectStatement with a
     * ProjectAllFieldsPredicate.
     * Get a graph by calling getInternalPredicateBases() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectStatementBeansBuilder01() {
        ProjectPredicate projectPredicate = new ProjectAllFieldsPredicate();
        SelectStatement selectStatement = new SelectStatement("id", projectPredicate, null, "Table",
                null, null);

        List<PredicateBase> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList("Table");

        Assert.assertEquals(selectStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectStatement with a
     * ProjectSomeFieldsPredicate.
     * Get a graph by calling getInternalPredicateBases() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectStatementBeansBuilder02() {
        ProjectPredicate projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("a", "b"));
        SelectStatement selectStatement = new SelectStatement("idX", projectPredicate, null, "from",
                null, null);

        List<PredicateBase> expectedGeneratedBeans = Arrays.asList(
                new ProjectionPredicate(Arrays.asList("a", "b"))
            );
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectStatement with a
     * KeywordExtractPredicate.
     * Get a graph by calling getInternalPredicateBases() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectStatementBeansBuilder03() {
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("c", "d"), "word",
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        SelectStatement selectStatement = new SelectStatement("id", null, extractPredicate, 
                "TableP9", null, null);

        List<PredicateBase> expectedGeneratedBeans = Arrays.asList(
                new KeywordPredicate("word", Arrays.asList("c", "d"), null, 
                        KeywordMatchingType.SUBSTRING_SCANBASED, "id_e")
            );
        List<String> dependencies = Arrays.asList("TableP9");

        Assert.assertEquals(selectStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectStatement with a
     * ProjectAllFieldsPredicate and a KeywordExtractPredicate.
     * Get a graph by calling getInternalPredicateBases() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectStatementBeansBuilder04() {
        ProjectPredicate projectPredicate = new ProjectAllFieldsPredicate();
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("f1"), "keyword", 
                KeywordMatchingType.CONJUNCTION_INDEXBASED.toString());
        SelectStatement selectStatement = new SelectStatement("id", projectPredicate,
                extractPredicate, "source", null, null);

        List<PredicateBase> expectedGeneratedBeans = Arrays.asList(
                    new KeywordPredicate("keyword", Arrays.asList("f1"), null, 
                            KeywordMatchingType.CONJUNCTION_INDEXBASED, "id_e")
                );
        List<String> dependencies = Arrays.asList("source");

        Assert.assertEquals(selectStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectStatement with a
     * ProjectAllFieldsPredicate and a KeywordExtractPredicate.
     * Get a graph by calling getInternalPredicateBases() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectStatementBeansBuilder05() {
        ProjectPredicate projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("a", "b"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", 
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        SelectStatement selectStatement = new SelectStatement("_sid4", projectPredicate,
                extractPredicate, "from", null, null);

        List<PredicateBase> expectedGeneratedBeans = Arrays.asList(
                new KeywordPredicate("x", Arrays.asList("a", "b"), null,
                        KeywordMatchingType.SUBSTRING_SCANBASED, "_sid4_e"),
                new ProjectionPredicate(Arrays.asList("a", "b"))
            );
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectStatement, expectedGeneratedBeans);
    }
}
