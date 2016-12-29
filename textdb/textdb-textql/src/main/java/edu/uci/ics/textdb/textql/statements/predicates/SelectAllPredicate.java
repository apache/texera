package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Object representation of a "SELECT * " predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectAllPredicate extends SelectPredicate {

    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param selectOperatorId The ID of the OperatorBean to be created.
     */
    public OperatorBean getOperatorBean(String selectOperatorId) {
        return new PassThroughBean(selectOperatorId, "PassThrough");
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        SelectAllPredicate selectAllPredicate = (SelectAllPredicate) other;
        return true;
    }
    
}
