package edu.uci.ics.textdb.exp.keywordmatcher;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;


/**
 * @author Zuozhi Wang
 * @author prakul
 *
 * KeywordPredicate contains all the properties needed by a Keyword Matcher.
 */
/*
 * KeywordPredicate is the predicate for KeywordMatcher.
 * It serves two purposes:
 *   1. It's used by other classes and code inside the engine.
 *      For example, KeywordMatcher needs to access the properties.
 *   
 *   2. It corresponds to a JSON representation that describes this predicate.
 *      For example, the JSON may look like this:
 *      {
 *        "query": "a_keyword",
 *        "attributes": ["attr1", "attr2"],
 *        "matching_type": "CONJUNCTION_INDEXBASED"
 *      }
 *      The JSON string can be deserialized to a predicate object, 
 *      and the predicate can also be serialized to a JSON string.
 * 
 * 
 * To meet the requirements stated above, we use Jackson for processing JSON,
 *   and the predicate classes need to follow the "Immutable Bean" style:
 *   
 *   1. The predicate ONLY contains properties needed by its operator, nothing else
 *   
 *   2. The predicate is immutable, which requires:
 *        2.1 the fields need to be declared as "private final" and they are only set in the constructor.
 *        2.2 no setter is allowed.
 *        2.3 use Collections.unmodifiableList (Set, Map, ...) when dealing with collections.
 *      (The reason is immutable objects are generally considered good for reducing programming errors)
 *   
 *   3. Deserialization (JSON -> Java Object) happens in the class constructor:
 *        3.1 use "@JsonCreator" annotation to mark the constructor for JSON Deserialization
 *        3.2 multiple constructors can exist, but ONLY ONE can be marked as @JsonCreator
 *        3.3 use "@JsonProperty" annotation to mark a parameter as a JSON property
 *            set "value" to specify the key name of the property, use a string in PropertyNameConstants
 *            set "required" to specify if the parameter is required 
 *            (if parameter is null, an exception is thrown)
 *        
 *   
 *   4. Serialization (Java Object -> JSON) happens in the getters:
 *        4.1 use "@JsonProperty" to mark a getter as a JSON property, and specify the key name
 *        4.2 Jackson will view ALL getters as JSON properties,
 *            if you want a getter to not be included, use "@JsonIgnore" annotation
 *   
 *   5. Register the predicate class and its name in PredicateBase class
 *   
 */
public class KeywordPredicate extends PredicateBase {

    /*
     * query refers to string of keywords to search for. For Ex. New york if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */
    private final String query;
    
    private final List<String> attributeNames;
    private final String luceneAnalyzerString;
    private final KeywordMatchingType matchingType;
    
    private final Boolean limitNotNull; 
    private final Integer limit;
    private final Boolean offsetNotNull; 
    private final Integer offset;
    private final Boolean addSpans;
    
    /**
     * Construct a KeywordPredicate with limit and offset set to default values.
     * 
     * @param query
     * @param attributeNames
     * @param luceneAnalyzerString
     * @param matchingType
     */
    public KeywordPredicate(
            String query,
            List<String> attributeNames,
            String luceneAnalyzerString, 
            KeywordMatchingType matchingType) {
        this(query, attributeNames, luceneAnalyzerString, matchingType, null, null, null);
    }
    
    /**
     * Construct a KeywordPredicate.
     * 
     * @param query, the keyword query
     * @param attributeNames, a list of attribute names to perform keyword search on
     * @param luceneAnalyzerString, a string indicating the lucene analyzer to be used. 
     *   This field is optional, passing null will set it to default value "standard"
     * @param matchingType, an Enum indicating the matching type (see KeywordMatchingType)
     * @param limit, optional, passing null will set it to default value Integer.MAX_VALUE
     * @param offset, optional, passing null will set it to default value 0
     */
    @JsonCreator
    public KeywordPredicate(
            @JsonProperty(value = PropertyNameConstants.KEYWORD_QUERY, required = true)
            String query,
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = false)
            String luceneAnalyzerString, 
            @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE, required = true)
            KeywordMatchingType matchingType,
            @JsonProperty(value = PropertyNameConstants.LIMIT, required = false)
            Integer limit,
            @JsonProperty(value = PropertyNameConstants.OFFSET, required = false)
            Integer offset,
            @JsonProperty(value = PropertyNameConstants.ADD_SPANS, required = false)
            Boolean addSpans) {
        
        this.query = query;
        this.attributeNames = Collections.unmodifiableList(attributeNames);
        if (luceneAnalyzerString == null) {
            this.luceneAnalyzerString = LuceneAnalyzerConstants.standardAnalyzerString();
        } else {
            this.luceneAnalyzerString = luceneAnalyzerString;
        }
        this.matchingType = matchingType;
        
        if (limit == null) {
            this.limit = Integer.MAX_VALUE;
            this.limitNotNull = false;
        } else if (limit < 0) {
            this.limit = Integer.MAX_VALUE;
            this.limitNotNull = true;
            // TODO: throw exception if limit < 0
        } else {
            this.limit = limit;
            this.limitNotNull = true;
        }
        
        if (offset == null) {
            this.offset = 0;
            this.offsetNotNull = false;
        } else if (offset < 0) {
            this.offset = 0;
            this.offsetNotNull = true;
            // TODO: throw exception if offset < 0
        } else {
            this.offset = offset;
            this.offsetNotNull = true;
        }
        
        if (addSpans == null) {
            this.addSpans = true;
        } else {
            this.addSpans = addSpans;
        }
        
    }

    @JsonProperty(PropertyNameConstants.KEYWORD_QUERY)
    public String getQuery() {
        return query;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return attributeNames;
    }

    @JsonProperty(PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getLuceneAnalyzerString() {
        return luceneAnalyzerString;
    }

    @JsonProperty(PropertyNameConstants.KEYWORD_MATCHING_TYPE)
    public KeywordMatchingType getMatchingType() {
        return matchingType;
    }
    
    @JsonProperty(PropertyNameConstants.LIMIT)
    private Integer getLimitJson() {
        if (limitNotNull) {
            return limit;
        } else {
            return null;
        }
    }
    
    @JsonProperty(PropertyNameConstants.OFFSET)
    private Integer getOffsetJson() {
        if (offsetNotNull) {
            return limit;
        } else {
            return null;
        }
    }
    
    @JsonProperty(PropertyNameConstants.ADD_SPANS)
    public Boolean isAddSpans() {
        return this.addSpans;
    }
    
    public Integer getLimit() {
        return limit;
    }
    
    public Integer getOffset() {
        return offset;
    }
    
    @Override
    public IOperator newOperator() {
        return new KeywordMatcher(this);
    }

}
