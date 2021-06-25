package edu.uci.ics.texera.textql.statements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.textql.statements.CreateViewStatement;
import edu.uci.ics.texera.textql.statements.SelectStatement;
import edu.uci.ics.texera.textql.statements.Statement;
import edu.uci.ics.texera.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectAllFieldsPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectSomeFieldsPredicate;

/**
 * This class contains test cases for the CreateViewStatement.
 * The constructor, getter, setter and bean builder methods are tested.
 * 
 * @author Flavio Bayer
 *
 */
public class CreateViewStatementTest {

    /**
     * Test the class constructor and the getter methods.
     * Call the constructor of CreateViewStatement and test if
     * the returned value by the getter is the same.
     */
    @Test
    public void testConstructorAndGetters() {
        Statement subStatement;
        ProjectPredicate projectPredicate;
        ExtractPredicate extractPredicate;
        CreateViewStatement createViewStatement;

        // Tests for the id attribute
        createViewStatement = new CreateViewStatement(null, null);
        Assert.assertEquals(createViewStatement.getId(), null);

        createViewStatement = new CreateViewStatement("statementId", null);
        Assert.assertEquals(createViewStatement.getId(), "statementId");

        createViewStatement = new CreateViewStatement("id6", null);
        Assert.assertEquals(createViewStatement.getId(), "id6");
        
        createViewStatement = new CreateViewStatement("_sid12", null);
        Assert.assertEquals(createViewStatement.getId(), "_sid12");

        // Tests for the subStatement attribute
        createViewStatement = new CreateViewStatement("statementId", null);
        Assert.assertEquals(createViewStatement.getSubStatement(), null);

        subStatement = new SelectStatement("substatementId0", null, null, "source", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);

        subStatement = new SelectStatement("substatementId1", null, null, "table", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
        
        projectPredicate = new ProjectAllFieldsPredicate();
        subStatement = new SelectStatement("substatementId2", projectPredicate, null, "from", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
        
        projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("at1", "at0"));
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "keyword",
                KeywordMatchingType.CONJUNCTION_INDEXBASED.toString());
        subStatement = new SelectStatement("substatementIdX", projectPredicate, extractPredicate, "t", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
    }

    /**
     * Test the setter methods and the getter methods.
     * Call the setter of CreateViewStatement and test if the
     * returned value by the getter is the same.
     */
    @Test
    public void testGettersAndGetters() {
        Statement subStatement;
        ProjectPredicate projectPredicate;
        ExtractPredicate extractPredicate;
        CreateViewStatement createViewStatement = new CreateViewStatement(null, null);

        // Tests for the id attribute
        createViewStatement.setId(null);
        Assert.assertEquals(createViewStatement.getId(), null);

        createViewStatement.setId("statementId4");
        Assert.assertEquals(createViewStatement.getId(), "statementId4");
        
        createViewStatement.setId("_sid0");
        Assert.assertEquals(createViewStatement.getId(), "_sid0");

        // Tests for the subStatement attribute
        createViewStatement.setSubStatement(null);
        Assert.assertEquals(createViewStatement.getSubStatement(), null);
        
        projectPredicate = new ProjectAllFieldsPredicate();
        subStatement = new SelectStatement("substatementId0", projectPredicate, null, "from", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
        
        projectPredicate = new ProjectAllFieldsPredicate();
        subStatement = new SelectStatement("substatementId1", projectPredicate, null, "table", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);

        projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("c", "d"));
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("f0", "f1"), "xxx",
                KeywordMatchingType.PHRASE_INDEXBASED.toString());
        subStatement = new SelectStatement("id", projectPredicate, extractPredicate, "source", null, null);
        createViewStatement.setSubStatement(subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
    }

    /**
     * Test the correctness of the generated beans by a CreateViewStatement with
     * a SelectExtractStatement as sub-statement without a SelectPredicate nor
     * ExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testCreateViewStatementBeansBuilder00() {
        Statement subStatement = new SelectStatement("id2", null, null, "from", null, null);
        CreateViewStatement createViewStatement = new CreateViewStatement("idx", subStatement);

        List<PredicateBase> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList(subStatement.getId());

        Assert.assertEquals(createViewStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(createViewStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a CreateViewStatement with
     * a SelectExtractStatement as sub-statement with a
     * SelectSomeFieldsPredicate and a KeywordExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testCreateViewStatementBeansBuilder01() {
        ProjectPredicate projectPredicate = new ProjectSomeFieldsPredicate(Arrays.asList("x", "y"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "zzz",
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        Statement subStatement = new SelectStatement("id", projectPredicate, extractPredicate, "from", null,
                null);
        CreateViewStatement createViewStatement = new CreateViewStatement("idx", subStatement);

        List<PredicateBase> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList(subStatement.getId());

        Assert.assertEquals(createViewStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(createViewStatement, expectedGeneratedBeans);
    }
}
