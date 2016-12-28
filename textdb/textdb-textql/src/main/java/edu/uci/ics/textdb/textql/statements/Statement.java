package edu.uci.ics.textdb.textql.statements;

import org.apache.commons.lang3.builder.EqualsBuilder;


/**
 * Statement class and subclasses(SelectStatement, CreateViewStatement)
 * Each Statement class has an ID. Subclasses of Statements have specific
 * fields related to its function.
 * Statement --+ SelectStatement
 *             + CreateViewStatement
 *             
 * @author Flavio Bayer
 * 
 */
public abstract class Statement {
    
    /**
     * The { @code String } identifier of each Statement object.
     */
    private String id;
    
    /**
     * Create a { @code Statement } with all the parameters set to { @code null }.
     */
    public Statement() {

    }
    
    
    /**
     * Create a { @code Statement } with the given ID.
     * @param id The ID of this statement.
     */
    public Statement(String id) {
      this.id = id;
    }
    
    /**
     * Get the ID of the statement.
     * @return The ID of the statement.
     */
    public String getId() {
        return id;
    }
    
    /**
     * Set the ID of the statement.
     * @param id The new ID of the statement.
     */
    public void setId(String id) {
        this.id = id;
    }
    
    
    @Override
    public boolean equals(Object other) {
      if (other == null) { return false; }
      if (other.getClass() != this.getClass()) { return false; }
      Statement statement = (Statement) other;
      return new EqualsBuilder()
                  .append(id, statement.id)
                  .isEquals();
    }
    
}
