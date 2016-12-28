package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * Object representation of a "SELECT (...)" predicate inside a { @code SelectExtractStatement }.
 * Subclasses have specific fields related to its projection functionalities.
 * SelectPredicate --+ SelectAllPredicate
 *                   + SelectFieldsPredicate
 * 
 * @author Flavio Bayer
 *
 */
public abstract class SelectPredicate {
            
}
