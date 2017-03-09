package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;

/**
 * Object representation of a "KEYWORDEXTRACT(...)" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class KeywordExtractPredicate implements ExtractPredicate {
    
    /**
     * The { @link List } of fields on which the keyword search should be performed.
     */
    private List<String> matchingFields;
    
    /**
     * The keyword(s) used for a keyword search.
     */
    private String keywords;
    
    /**
     * The type of matching to be performed during the keyword search.
     */ 
    private String matchingType;
    
    
    /**
     * Create a { @code KeywordExtractPredicate } with all the parameters set to { @code null }.
     * @param id The id of this statement.
     */
    public KeywordExtractPredicate() {
      this(null, null, null);
    }

    /**
     * Create a { @code KeywordExtractPredicate } with the given parameters.
     * @param matchingFields List of fields to extract information from.
     * @param keywords The keywords to look for during extraction.
     * @param matchingType The string representation of the { @code KeywordMatchingType } used for extraction.
     */
    public KeywordExtractPredicate(List<String> matchingFields, String keywords, String matchingType) {
        this.matchingFields = matchingFields;
        this.keywords = keywords;
        this.matchingType = matchingType;
    }
    
    
    /**
     * Get the list of names of fields to be matched during extraction.
     * @return The list of names of fields to be matched during extraction.
     */
    public List<String> getMatchingFields() {
        return matchingFields;
    }

    /**
     * Set the list of names of fields to be matched during extraction.
     * @param matchingFields The list of names of fields to be matched during extraction.
     */
    public void setMatchingFields(List<String> matchingFields) {
        this.matchingFields = matchingFields;
    }

    /**
     * Get the keyword(s) to look for during extraction.
     * @return The keyword(s) to look for during extraction.
     */
    public String getKeywords() {
        return keywords;
    }

    /**
     * Set the keyword(s) to look for during extraction.
     * @param keywords The keyword(s) to look for during extraction.
     */
    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    /**
     * Get the matching type of the extraction.
     * @return The matching type of the extraction.
     */
    public String getMatchingType() {
        return matchingType;
    }

    /**
     * Set the matching type of the extraction.
     * @param matchingType The matching type of the extraction.
     */
    public void setMatchingType(String matchingType) {
        this.matchingType = matchingType;
    }
    
  
    /**
     * Return this operator converted to a { @code KeywordMatcherBean }.
     * @param extractionOperatorId The ID of the OperatorBean to be created.
     * @return this operator converted to a KeywordMatcherBean.
     */
    @Override
    public KeywordMatcherBean generateOperatorBean(String extractionOperatorId) {
        String matchingFieldsAsString = String.join(",", this.matchingFields);
        return new KeywordMatcherBean(extractionOperatorId, "KeywordMatcher", matchingFieldsAsString,
                    null, null, this.keywords, this.matchingType);
    }
    
    /**
     * Generate the resulting output schema, based on the given input schema,
     * after the extraction operation is performed.
     * The generated output schema is a copy of the input schema with the
     * addition of the PAYLOAD and SPAN_LIST attributes, if not present.
     * The matching type, the matching fields and the keyword do not affect
     * the output schema.
     * 
     * Example: for the following schema used as input:
     *  [ { "name", FieldType.STRING }, { "age", FieldType.INTEGER }, 
     *      { "height", FieldType.HEIGHT }, { "dateOfBirth", FieldType.DATE } ]
     * The generated schema is the input schema with PAYLOAD and SPAN_LIST attributes:
     *  [ { "name", FieldType.STRING }, { "age", FieldType.INTEGER }, 
     *      { "height", FieldType.HEIGHT }, { "dateOfBirth", FieldType.DATE },
     *      SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE ]
     * 
     * @param inputSchema The input schema of this predicate.
     * @return The generated output schema based on the input schema.
     * @throws TextDBException If a required attribute for extraction is not present
     *     or if it has an incompatible type.
     */
    public Schema generateOutputSchema(Schema inputSchema) throws TextDBException {
        // Assert the current matchingType is valid
        KeywordMatchingType keywordMatchingType = KeywordMatcherBuilder.getKeywordMatchingType(matchingType);
        if (keywordMatchingType == null) {
            throw new TextDBException("Invalid matchingType '" + matchingType + "'");
        }
        // Check for required matching fields and whether they have a compatible type
        List<FieldType> compatibleFieldTypes = Arrays.asList(FieldType.STRING, FieldType.TEXT);
        for (String matchingField : matchingFields) {
            // Fetch the matching attribute from the input schema
            Attribute matchingAttribute = inputSchema.getAttribute(matchingField);
            // Throw an error if the matching field was not found or if the type is not compatible
            if (matchingAttribute == null) {
                throw new TextDBException("Required field '" + matchingField + "' was not found in input schema");
            } else if (!compatibleFieldTypes.contains(matchingAttribute.getFieldType())) {
                throw new TextDBException("Required field '" + matchingField + "' must be one of " + compatibleFieldTypes);
            }
        }
        // Build a copy of the input schema (so the changes do not affect the inputSchema object)
        Schema outputSchema = new Schema(inputSchema.getAttributes().stream().toArray(Attribute[]::new));
        // Append the PAYLOAD attribute to the schema if it is not present
        if (!outputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        // Append the SPAN_LIST attribute to the schema if it is not present
        if (!outputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
        }
        return outputSchema;
    }
  

    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        KeywordExtractPredicate keywordExtractPredicate = (KeywordExtractPredicate) other;
        return new EqualsBuilder()
                .append(matchingFields, keywordExtractPredicate.matchingFields)
                .append(keywords, keywordExtractPredicate.keywords)
                .append(matchingType, keywordExtractPredicate.matchingType)
                .isEquals();
    }
    
}