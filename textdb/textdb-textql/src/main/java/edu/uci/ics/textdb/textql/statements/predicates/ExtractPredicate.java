package edu.uci.ics.textdb.textql.statements.predicates;


/**
 * ExtractPredicate class and its subclasses such as KeywordExtractPredicate.
 * Subclasses have specific fields related to its extraction functionalities.
 * ExtractPredicate --+ KeywordExtractPredicate
 * 
 * @author Flavio Bayer
 * 
 */
public abstract class ExtractPredicate {
		
	@Override
	public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        return true;
	}
}