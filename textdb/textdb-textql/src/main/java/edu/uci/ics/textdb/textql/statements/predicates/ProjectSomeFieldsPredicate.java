package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.ProjectionBean;

/**
 * Object representation of a "SELECT a, b, c, ..." predicate inside a { @code SelectExtractStatement },
 * were "a, b, c, ..." is a list of field names
 * 
 * @author Flavio Bayer
 *
 */
public class ProjectSomeFieldsPredicate implements ProjectPredicate {

    /**
     * The { @link List } of fields to be projected if it is specified as
     * in "SELECT a, b, c".
     */
    private List<String> projectedFields;

    /**
     * Create a { @code Statement } with the given list of field names to be projected.
     * @param projectedFields The list of field names to be projected.
     */
    public ProjectSomeFieldsPredicate(List<String> projectedFields){
        this.projectedFields = projectedFields;
    }
    
    /**
     * Get the list of field names to be projected.
     * @return A list of field names to be projected
     */
    public List<String> getProjectedFields() {
        return projectedFields;
    }
    
    /**
     * Set the list of field names to be projected.
     * @param projectedFields The list of field names to be projected.
     */
    public void setProjectedFields(List<String> projectedFields) {
        this.projectedFields = projectedFields;
    }

    
    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param projectOperatorId The ID of the OperatorBean to be created.
     */
    public OperatorBean generateOperatorBean(String projectOperatorId) {
        ProjectionBean projectionBean = new ProjectionBean();
        projectionBean.setOperatorID(projectOperatorId);
        projectionBean.setOperatorType("Projection");
        projectionBean.setAttributes(String.join(",", this.getProjectedFields()));
        return projectionBean;
    }
    
    /**
     * Generate the resulting output schema of this predicate based on
     * the given input schema.
     * The generated output schema is a copy of the input schema with only 
     * the attributes that are present in the list of fields to be projected.
     * 
     * Example: for the following schema used as input:
     *  [ { "name", FieldType.STRING }, { "age", FieldType.INTEGER }, 
     *      { "height", FieldType.HEIGHT }, { "dateOfBirth", FieldType.DATE } ]
     * And the following list of fields to be projected:
     *  [ "dateOfBirth", "name" ]
     * The generated schema is a schema containing only the fields in the 
     * projection list:
     *  [ { "name", FieldType.STRING }, { "dateOfBirth", FieldType.DATE } ]
     * 
     * @param inputSchema The input schema of this predicate.
     * @return The generated output schema based on the input schema.
     * @throws TextDBException If a required attribute for is not present.
     */
    public Schema generateOutputSchema(Schema inputSchema) throws TextDBException {
        // Check for the existence of all required fields and throw an exception if it is not found
        for (String projectedField : projectedFields) {
            if (!inputSchema.containsField(projectedField)) {
                throw new TextDBException("Required field '" + projectedField + "' was not found in input schema");
            }
        }
        List<String> projectedFieldsLowerCase = projectedFields.stream()
                .map( field -> field.toLowerCase())
                .collect(Collectors.toList());
        // Build the new Schema by removing attributes that are not in the list of fields to be projected
        Attribute[] outputAttributes = inputSchema.getAttributes().stream()
                .filter( attribute -> projectedFieldsLowerCase.contains( attribute.getFieldName().toLowerCase()) )
                .toArray( Attribute[]::new );
        return new Schema(outputAttributes);
    }

    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        ProjectSomeFieldsPredicate projectSomeFieldsPredicate = (ProjectSomeFieldsPredicate) other;
        return new EqualsBuilder()
                .append(projectedFields, projectSomeFieldsPredicate.projectedFields)
                .isEquals();
    }
}
