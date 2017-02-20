package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Object representation of a "SELECT *" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class ProjectAllFieldsPredicate implements ProjectPredicate {

    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param projectOperatorId The ID of the OperatorBean to be created.
     */
    public OperatorBean generateOperatorBean(String projectOperatorId) {
        return new PassThroughBean(projectOperatorId, "PassThrough");
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        ProjectAllFieldsPredicate projectAllFieldsPredicate = (ProjectAllFieldsPredicate) other;
        return true;
    }
}
