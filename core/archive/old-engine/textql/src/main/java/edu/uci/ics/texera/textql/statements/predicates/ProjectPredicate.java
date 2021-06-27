package edu.uci.ics.texera.textql.statements.predicates;

import edu.uci.ics.texera.dataflow.common.PredicateBase;

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
    public PredicateBase generateOperatorBean(String projectOperatorId);
    
}
