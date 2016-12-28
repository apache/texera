package edu.uci.ics.textdb.textql.statements.predicates;

/**
 * Object representation of a "SELECT *" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectAllPredicate extends SelectPredicate {

    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        SelectAllPredicate selectAllPredicate = (SelectAllPredicate) other;
        return true;
    }
}
