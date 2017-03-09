package edu.uci.ics.textdb.textql.statements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.ProjectPredicate;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.OperatorLinkBean;

/**
 * Object Representation of a parsed "SELECT ... FROM ..." statement.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectStatement extends Statement {
    
    /**
     * Predicate used for projection of the fields to be returned such as in "SELECT *".
     */
    private ProjectPredicate projectPredicate;
    
    /**
     * Predicate used for data extraction such as keyword match in "KEYWORDMATCH(a,"word")".
     */
    private ExtractPredicate extractPredicate;

    /**
     * The identifier of a view or a table name, as in "SELECT ... FROM viewName" used as 
     * source of tuples.
     */
    private String fromClause;

    /**
     * The maximum number of tuples to be returned, as in "SELECT...FROM... LIMIT 5".
     */
    private Integer limitClause;

    /**
     * The number of tuples to be skipped before returning, as in "SELECT...FROM... OFFSET 5".
     */
    private Integer offsetClause;
      
    /**
     * Create a { @code SelectStatement } with all the parameters set to { @code null }.
     */
    public SelectStatement() {
        this(null, null, null, null, null, null);
    }

    /**
     * Create a { @code SelectStatement } with the given parameters.
     * @param id The ID of this statement.
     * @param projectPredicate The predicate for result projection.
     * @param extractPredicate The predicate for data extraction.
     * @param fromClause The ID of the source view.
     * @param limitClause The value of the limit clause.
     * @param offsetClauseThe value of the offset clause.
     */
    public SelectStatement(String id, ProjectPredicate projectPredicate, ExtractPredicate extractPredicate,
            String fromClause, Integer limitClause, Integer offsetClause) {
        super(id);
        this.projectPredicate = projectPredicate;
        this.extractPredicate = extractPredicate;
        this.fromClause = fromClause;
        this.limitClause = limitClause;
        this.offsetClause = offsetClause;
    }
    
    /**
     * Get the project predicate.
     * @return The project predicate.
     */
    public ProjectPredicate getProjectPredicate() {
        return projectPredicate;
    }
    
    /**
     * Set the project predicate.
     * @param projectPredicate The project predicate to be set.
     */
    public void setProjectPredicate(ProjectPredicate projectPredicate) {
        this.projectPredicate = projectPredicate;
    }
    
    /**
     * Get the extract predicate.
     * @return The extract predicate.
     */
    public ExtractPredicate getExtractPredicate() {
        return extractPredicate;
    }

    /**
     * Set the extract predicate.
     * @param extractPredicate The extract predicate to be set.
     */
    public void setExtractPredicate(ExtractPredicate extractPredicate) {
        this.extractPredicate = extractPredicate;
    }

    /**
     * Get the value of the from clause.
     * @return The value of the from clause.
     */
    public String getFromClause() {
        return fromClause;
    }

    /**
     * Set the value of the from clause.
     * @param fromClause The new value for the from clause.
     */
    public void setFromClause(String fromClause) {
        this.fromClause = fromClause;
    }

    /**
     * Get the value of the limit clause.
     * @return The value of the limit clause.
     */
    public Integer getLimitClause() {
        return limitClause;
    }

    /**
     * Set the value of the limit clause.
     * @param limitClause The new value for the limit clause.
     */
    public void setLimitClause(Integer limitClause) {
        this.limitClause = limitClause;
    }

    /**
     * Get the value of the offset clause.
     * @return The value of the offset clause.
     */
    public Integer getOffsetClause() {
        return offsetClause;
    }

    /**
     * Set the value of the offset clause.
     * @param offsetClause The new value for the offset clause.
     */
    public void setOffsetClause(Integer offsetClause) {
        this.offsetClause = offsetClause;
    }
        
    @Override
    public String getInputNodeID(){
        // Append "_s" to the id of the statement to create the id of the bean, where "s" stands for Source.
        return super.getId() + "_s";
    }    
    
    @Override
    public String getOutputNodeID(){
        return super.getId();
    }

    /**
     * Get the name of the bean built by the project predicate.
     * @return The name of the bean built by the project predicate.
     */
    private String getProjectionNodeID(){
        // Append "_p" to the id of the statement to create the id of the bean, where "p" stands for Projection.
        return super.getId() + "_p";
    }
    
    /**
     * Get the name of the bean built by the extract predicate.
     * @return The name of the bean built by the extract predicate.
     */
    private String getExtractionNodeID(){
        // Append "_e"  to the id of the statement to create the id of the bean, where "e" stands for Extraction.
        return super.getId() + "_e";
    }
    
    /**
     * Return a list of operators generated when this statement is converted to beans.
     * Beans will be generated for the Alias, Projection, Extraction and Source operators.
     * @return The list of operator beans generated by this statement.
     */
    @Override
    public List<OperatorBean> getInternalOperatorBeans(){
        List<OperatorBean> operators = new ArrayList<>();
        // Build and append a PassThroughBean as an alias for this Statement
        operators.add(new PassThroughBean(getOutputNodeID(), "PassThrough"));
        // Build and append bean for Projection
        if(this.projectPredicate==null){
            operators.add(new PassThroughBean(getProjectionNodeID(), "PassThrough"));
        }else{
            operators.add(this.projectPredicate.generateOperatorBean(getProjectionNodeID()));
        }
        // Build and append bean for Extraction predicate
        if(this.extractPredicate==null){
            operators.add(new PassThroughBean(getExtractionNodeID(), "PassThrough"));
        }else{
            operators.add(this.extractPredicate.generateOperatorBean(getExtractionNodeID()));
        }
        // Build and append bean for the Source
        operators.add(new PassThroughBean(getInputNodeID(), "PassThrough"));
        // return the built operators
        return operators;
    }
    
    /**
     * Return a list of links generated when this statement is converted to beans.
     * Beans will be generated for the links between Alias, Projection, Extraction and Source predicates.
     * @return The list of link beans generated by this statement.
     */
    @Override
    public List<OperatorLinkBean> getInternalLinkBeans(){
        return Arrays.asList(
                   new OperatorLinkBean(getProjectionNodeID(), getOutputNodeID()),
                   new OperatorLinkBean(getExtractionNodeID(), getProjectionNodeID()),
                   new OperatorLinkBean(getInputNodeID(), getExtractionNodeID())
               );
    }

    /**
     * Return a list of IDs of operators required by this statement (the dependencies of this Statement)
     * when converted to beans.
     * The only required view for a { @code SelectExtractStatement } is the one in the From clause.
     * @return A list with the IDs of required Statements.
     */
    @Override
    public List<String> getInputViews(){
        return Arrays.asList(this.fromClause);
    }

    /**
     * Generate the resulting output schema of this predicate based on the given input schemas.
     * The generated Schema is the input Schema after being processed by the extract predicate
     * and the select predicate.
     * The SelectEctractStatement has input arity equals to 1, thus the length of the array
     * of input schemas must be 1.
     * 
     * Example: for an array containing only the following schema as input:
     *  [ { "name", FieldType.STRING }, { "age", FieldType.INTEGER }, 
     *      { "height", FieldType.HEIGHT }, { "dateOfBirth", FieldType.DATE } ]
     * And a keyword match operation being performed in some fields plus the following list
     * of fields to projected:
     *  [ "dateOfBirth", "name", SchemaConstants.SPAN_LIST ]
     * The generated schema is a schema containing only the fields in the projection list,
     * including the SPAN_LIST_ATTRIBUTE created by the keyword match operation:
     *  [ { "name", FieldType.STRING }, { "dateOfBirth", FieldType.DATE },
     *      SchemaConstants.SPAN_LIST_ATTRIBUTE  ]
     *      
     * @param inputSchemas The input schemas of this statement.
     * @return The generated output schema, the same as the input schemas.
     * @throws TextDBException If the size of inputSchemas is different than the input arity, 
     *     if a required attribute for projection is not present, if a required attribute for
     *     extraction is not present or if an attribute has type incompatible with the
     *     extraction type.
     */
    @Override
    public Schema generateOutputSchema(Schema... inputSchemas) throws TextDBException {
        // Assert the input arity is one 
        if (inputSchemas.length != 1) {
            throw new TextDBException("The size of the list of input schemas must be 1");
        }
        Schema inputSchema = inputSchemas[0];
        // Use the input schema as output schema and modify it based on extract and select predicates (if present)
        Schema outputSchema = inputSchema;
        if (extractPredicate != null) {
            outputSchema = extractPredicate.generateOutputSchema(outputSchema);
        }
        if (projectPredicate != null) {
            outputSchema = projectPredicate.generateOutputSchema(outputSchema);
        }
        return outputSchema;
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != this.getClass()) { return false; }
        SelectStatement selectStatement = (SelectStatement) other;
        return new EqualsBuilder()
                    .appendSuper(super.equals(selectStatement))
                    .append(projectPredicate, selectStatement.projectPredicate)
                    .append(extractPredicate, selectStatement.extractPredicate)
                    .append(fromClause, selectStatement.fromClause)
                    .append(limitClause, selectStatement.limitClause)
                    .append(offsetClause, selectStatement.offsetClause)
                    .isEquals();
    }
    
}




