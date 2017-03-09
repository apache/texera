package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
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
    
    /**
     * Generate the resulting output schema of this predicate based on the given input schema.
     * This predicate does not apply any modification to the input schema, thus the generated
     * output schema is equal to the input schema.
     * 
     * Example: for the following schema used as input:
     *  [ { "name", FieldType.STRING }, { "age", FieldType.INTEGER }, 
     *      { "height", FieldType.HEIGHT }, { "dateOfBirth", FieldType.DATE } ]
     * The generated schema is a schema just like the input schema:
     *  [ { "name", FieldType.STRING }, { "age", FieldType.INTEGER }, 
     *      { "height", FieldType.HEIGHT }, { "dateOfBirth", FieldType.DATE } ]
     *      
     * @param inputSchema The input schema of this predicate.
     * @return The generated output schema based on the input schema.
     */
    public Schema generateOutputSchema(Schema inputSchema) {
        // Create a copy of the input schema and return it
        return new Schema(inputSchema.getAttributes().stream().toArray(Attribute[]::new));
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        ProjectAllFieldsPredicate projectAllFieldsPredicate = (ProjectAllFieldsPredicate) other;
        return true;
    }
}
