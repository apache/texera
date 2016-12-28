package edu.uci.ics.textdb.textql.statements;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * Object representation of a "CREATE VIEW ..." statement.
 * 
 * @author Flavio Bayer
 *
 */
public class CreateViewStatement extends Statement {
    
    /**
     * The statement to which the { @code CreateViewStatement } creates an alias for.
     * e.g. in "CREATE VIEW v AS SELECT * FROM t"; the view with ID 'v' will have the
     * select statement "SELECT * FROM t" as sub-statement (in a SlectExtrsctStatement
     * object).
     */
    private Statement subStatement;
      
    /**
     * Create a { @code CreateViewStatement } with the parameters set to { @code null }
     */
    public CreateViewStatement() {
        this(null, null);
    }
    
    /**
     * Create a { @code CreateViewStatement } with the given parameters.
     * @param id The ID of this statement.
     * @param subStatement The subStatement of this statement.
     */
    public CreateViewStatement(String id, Statement subStatement) {
        super(id);
        this.subStatement = subStatement;
    }
    
    
    /**
     * Get the sub-statement of this statement.
     * @return The sub-statement of this statement.
     */
    public Statement getSubStatement() {
        return subStatement;
    }
    
    /**
     * Set the sub-statement of the statement.
     * @param subStatement The new sub-statement to set.
     */
    public void setSubStatement(Statement subStatement) {
        this.subStatement = subStatement;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj.getClass() != this.getClass()) { return false; }
        CreateViewStatement otherCreateViewStatement = (CreateViewStatement) obj;
        return new EqualsBuilder()
                    .appendSuper(super.equals(otherCreateViewStatement))
                    .append(subStatement, otherCreateViewStatement.subStatement)
                    .isEquals();
    }
    
}