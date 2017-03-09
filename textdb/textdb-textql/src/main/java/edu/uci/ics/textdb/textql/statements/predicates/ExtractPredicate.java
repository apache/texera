package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Interface for representation of an extraction predicate such as "KEYWORDEXTRACT(...)" predicate 
 * inside a { @code SelectExtractStatement }..
 * Subclasses have specific fields related to its extraction functionalities.
 * ExtractPredicate --+ KeywordExtractPredicate
 * 
 * @author Flavio Bayer
 * 
 */
public interface ExtractPredicate {

    /**
     * Return the bean representation of this { @code ExtractPredicate }.
     * @param extractionOperatorId The ID of the OperatorBean to be created.
     * @return The bean operator representation of this { @code ExtractPredicate }.
     */
    public OperatorBean generateOperatorBean(String extractionOperatorId);
    
    /**
     * Generate the resulting output schema, based on the given input schema,
     * after the extraction operation is performed.
     * @param inputSchema The input schema of this predicate.
     * @return The generated output schema based on the input schema.
     * @throws TextDBException If a required attribute for extraction is not present
     *     or has type incompatible with the extraction type.
     */
    public Schema generateOutputSchema(Schema inputSchema) throws TextDBException;
    
}