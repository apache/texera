package edu.uci.ics.texera.textql.statements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;
import edu.uci.ics.texera.textql.planbuilder.beans.PassThroughPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectPredicate;

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
    public List<PredicateBase> getInternalOperatorBeans(){
        List<PredicateBase> operators = new ArrayList<>();
        // Build and append a PassThroughBean as an alias for this Statement
        operators.add(new PassThroughPredicate(getOutputNodeID()));
        // Build and append bean for Projection
        if(this.projectPredicate==null){
            operators.add(new PassThroughPredicate(getProjectionNodeID()));
        }else{
            operators.add(this.projectPredicate.generateOperatorBean(getProjectionNodeID()));
        }
        // Build and append bean for Extraction predicate
        if(this.extractPredicate==null){
            operators.add(new PassThroughPredicate(getExtractionNodeID()));
        }else{
            operators.add(this.extractPredicate.generateOperatorBean(getExtractionNodeID()));
        }
        // Build and append bean for the Source
        operators.add(new PassThroughPredicate(getInputNodeID()));
        // return the built operators
        return operators;
    }
    
    /**
     * Return a list of links generated when this statement is converted to beans.
     * Beans will be generated for the links between Alias, Projection, Extraction and Source predicates.
     * @return The list of link beans generated by this statement.
     */
    @Override
    public List<OperatorLink> getInternalLinkBeans(){
        return Arrays.asList(
                   new OperatorLink(getProjectionNodeID(), getOutputNodeID()),
                   new OperatorLink(getExtractionNodeID(), getProjectionNodeID()),
                   new OperatorLink(getInputNodeID(), getExtractionNodeID())
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




