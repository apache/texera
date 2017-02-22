package edu.uci.ics.textdb.textql.statements.predicates;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * This class contains test cases for the SelectAllFieldsPredicate class.
 * The generateOperatorBean method is tested.
 * There are no constructors, getters nor setters to test. 
 * 
 * @author Flavio Bayer
 *
 */
public class ProjectAllFieldsPredicateTest {

    /**
     * Test the generateOperatorBean method.
     * Build a SelectAllFieldsPredicate, invoke the generateOperatorBean and check
     * whether a PassThroughBean with the right attributes is returned.
     */
    @Test
    public void testGenerateOperatorBean() {
        ProjectAllFieldsPredicate projectAllFieldsPredicate = new ProjectAllFieldsPredicate();
        OperatorBean projectionBean;
        String operatorId;
        
        operatorId = "xxx";
        projectionBean = projectAllFieldsPredicate.generateOperatorBean(operatorId);
        Assert.assertEquals(projectionBean, new PassThroughBean(operatorId, "PassThrough"));

        operatorId = "y0a9";
        projectionBean = projectAllFieldsPredicate.generateOperatorBean(operatorId);
        Assert.assertEquals(projectionBean, new PassThroughBean(operatorId, "PassThrough"));
    }
    
}
