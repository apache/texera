package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
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

    /**
     * Generate the resulting output schema, based on the given input schema,
     * after the projection operation is performed.
     * @param inputSchema The input schema of this predicate.
     * @return The generated output schema based on the input schema.
     * @throws TextDBException If a required attribute is not present.
     */
    public Schema generateOutputSchema(Schema inputSchema) throws TextDBException;
    
}
