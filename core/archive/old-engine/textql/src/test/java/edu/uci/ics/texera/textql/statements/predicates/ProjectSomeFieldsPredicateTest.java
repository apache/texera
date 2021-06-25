package edu.uci.ics.texera.textql.statements.predicates;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.projection.ProjectionPredicate;

/**
 * This class contains test cases for the SelectSomeFieldsPredicate class.
 * The constructor, getters, setters and the generateOperatorBean methods are
 * tested.
 * 
 * @author Flavio Bayer
 *
 */
public class ProjectSomeFieldsPredicateTest {
    
    /**
     * Test the class constructor, getter and the setter methods.
     * Call the constructor of the SelectSomeFieldsPredicate, test 
     * if the returned value by the getter is the same as used in 
     * the constructor and then test if the value is changed
     * when the setter method is invoked.
     */
    @Test
    public void testConstructorsGettersSetters(){
        List<String> projectedFields;

        projectedFields = Collections.emptyList();
        assertConstructorGettersSetters(projectedFields);
        
        projectedFields = Arrays.asList("a","b","c","d");
        assertConstructorGettersSetters(projectedFields);
        
        projectedFields = Arrays.asList("field1", "field2", "field0");
        assertConstructorGettersSetters(projectedFields);        

        projectedFields = Arrays.asList(SchemaConstants._ID, SchemaConstants.PAYLOAD, SchemaConstants.SPAN_LIST);
        assertConstructorGettersSetters(projectedFields);
    }
    
    /**
     * Assert the correctness of the Constructor, getter and setter methods.
     * @param projectedFields The list of projected fields to be tested.
     */
    private void assertConstructorGettersSetters(List<String> projectedFields){
        ProjectSomeFieldsPredicate projectSomeFieldsPredicate;
        
        // Check constructor
        projectSomeFieldsPredicate = new ProjectSomeFieldsPredicate(projectedFields);
        Assert.assertEquals(projectSomeFieldsPredicate.getProjectedFields(), projectedFields);
        
        // Check set projectedFields to null
        projectSomeFieldsPredicate.setProjectedFields(null);
        Assert.assertEquals(projectSomeFieldsPredicate.getProjectedFields(), null);
        
        // Check set projectedFields to the given list of fields
        projectSomeFieldsPredicate.setProjectedFields(projectedFields);
        Assert.assertEquals(projectSomeFieldsPredicate.getProjectedFields(), projectedFields);
    }

    /**
     * Test the generateOperatorBean method.
     * Build a SelectSomeFieldsPredicate, invoke the generateOperatorBean and check
     * whether a ProjectionBean with the right attributes is returned.
     * An empty list is used as the list of projected fields.
     */
    @Test
    public void testGenerateOperatorBean00() {
        String operatorId = "xxx";
        List<String> projectedFields = Collections.emptyList();
        ProjectSomeFieldsPredicate projectSomeFieldsPredicate = new ProjectSomeFieldsPredicate(projectedFields);
        
        PredicateBase computedProjectionBean = projectSomeFieldsPredicate.generateOperatorBean(operatorId);
        PredicateBase expectedProjectionBean = new ProjectionPredicate(Arrays.asList());
        expectedProjectionBean.setID(operatorId);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
    /**
     * Test the generateOperatorBean method.
     * Build a SelectSomeFieldsPredicate, invoke the generateOperatorBean and check
     * whether a ProjectionBean with the right attributes is returned.
     * A list with some field names is used as the list of projected fields.
     */
    @Test
    public void testGenerateOperatorBean01() {
        String operatorId = "zwx";
        List<String> projectedFields = Arrays.asList("field0", "field1");
        ProjectSomeFieldsPredicate projectSomeFieldsPredicate = new ProjectSomeFieldsPredicate(projectedFields);
        
        PredicateBase computedProjectionBean = projectSomeFieldsPredicate.generateOperatorBean(operatorId);
        PredicateBase expectedProjectionBean = new ProjectionPredicate(Arrays.asList("field0", "field1"));
        expectedProjectionBean.setID(operatorId);

        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);        
    }

    /**
     * Test the generateOperatorBean method.
     * Build a SelectSomeFieldsPredicate, invoke the generateOperatorBean and check
     * whether a ProjectionBean with the right attributes is returned.
     * A list with some unordered field names is used as the list of projected fields.
     */
    @Test
    public void testGenerateOperatorBean02() {
        String operatorId = "op00";
        List<String> projectedFields = Arrays.asList("c", "a", "b");
        ProjectSomeFieldsPredicate projectSomeFieldsPredicate = new ProjectSomeFieldsPredicate(projectedFields);
        
        PredicateBase computedProjectionBean = projectSomeFieldsPredicate.generateOperatorBean(operatorId);
        PredicateBase expectedProjectionBean = new ProjectionPredicate(Arrays.asList("c", "a", "b"));
        expectedProjectionBean.setID(operatorId);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);   
    }
    
}
