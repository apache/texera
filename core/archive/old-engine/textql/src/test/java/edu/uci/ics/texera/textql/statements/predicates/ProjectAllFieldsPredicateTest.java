package edu.uci.ics.texera.textql.statements.predicates;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.textql.planbuilder.beans.PassThroughPredicate;

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
        PredicateBase projectionBean;
        
        final String operatorId1 = "xxx";
        projectionBean = projectAllFieldsPredicate.generateOperatorBean(operatorId1);
        Assert.assertEquals(projectionBean, new PassThroughPredicate(operatorId1));

        final String operatorId2 = "y0a9";
        projectionBean = projectAllFieldsPredicate.generateOperatorBean(operatorId2);
        Assert.assertEquals(projectionBean, new PassThroughPredicate(operatorId2));
    }
    
}
