package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Interface for representation of a "SELECT (...)" predicate inside a { @code SelectExtractStatement }.
 * Subclasses have specific fields related to its projection functionalities.
 * ProjectPredicate --+ ProjectAllFieldsPredicate
 *                    + ProjectSomeFieldsPredicate
 * 
 * @author Flavio Bayer
 *
 */
public interface ProjectPredicate {

    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param projectOperatorId The ID of the OperatorBean to be created.
     */
    public OperatorBean generateOperatorBean(String projectOperatorId);
    
}
