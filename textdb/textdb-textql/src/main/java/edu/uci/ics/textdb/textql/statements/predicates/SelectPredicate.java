package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * Object representation of a "SELECT (...)" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectPredicate {
    
    /**
     * Set to true when '*' is used as the fields to be projected, as in "SELECT *".
     */
    private Boolean projectAll;
    
    /**
     * The { @link List } of fields to be projected if it is specified as
     * in "SELECT a, b, c".
     */
    private List<String> projectedFields;
    
    
    /**
     * Create a { @code Statement } with the given ID.
     * @param projectAll If all the fields are to be projected.
     */
    public SelectPredicate(boolean projectAll) {
        this.projectAll = projectAll;
        this.projectedFields = null;
    }
    
    /**
     * Create a { @code Statement } with the given list of field names to be projected.
     * @param projectedFields The list of field names to be projected.
     */
    public SelectPredicate(List<String> projectedFields){
        this.projectAll = false;
        this.projectedFields = projectedFields;
    }
    

    /**
     * Check whether all the fields are to be projected or not.
     * @return If all the fields are to be projected.
     */
    public boolean isProjectAll() {
        return projectAll;
    }
    
    /**
     * Set whether all the fields are to be projected or not.
     * @param projectAll If all the fields are to be projected.
     */
    public void setProjectAll(boolean projectAll) {
        this.projectAll = projectAll;
        this.projectedFields = Collections.emptyList();
    }

    /**
     * Get the list of field names to be projected.
     * @return A list of field names to be projected, or null if projectAll is set.
     */
    public List<String> getProjectedFields() {
        // return an empty list if not all attributes are to be projected and there's no list of fields to be projected  
        if(this.projectAll==false && this.projectedFields==null){
            return Collections.emptyList();
        }
        return projectedFields;
    }
    
    /**
     * Set the list of field names to be projected.
     * @param projectedFields The list of field names to be projected.
     */
    public void setProjectedFields(List<String> projectedFields) {
        this.projectAll = false;
        this.projectedFields = projectedFields;
    }
        
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        SelectPredicate selectPredicate = (SelectPredicate) other;
        return new EqualsBuilder()
                .append(projectAll, selectPredicate.projectAll)
                .append(projectedFields, selectPredicate.projectedFields)
                .isEquals();
    }
    
}
