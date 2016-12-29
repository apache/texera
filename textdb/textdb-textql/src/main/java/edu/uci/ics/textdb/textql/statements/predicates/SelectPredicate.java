package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Object representation of a "SELECT (...)" predicate inside a { @code SelectExtractStatement }.
 * Subclasses have specific fields related to its projection functionalities.
 * SelectPredicate --+ SelectAllPredicate
 *                   + SelectFieldsPredicate
 * 
 * @author Flavio Bayer
 *
 */
public abstract class SelectPredicate {
    
    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param selectOperatorId The ID of the OperatorBean to be created.
     */
    public abstract OperatorBean getOperatorBean(String selectOperatorId);
        
}
