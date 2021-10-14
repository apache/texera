package edu.uci.ics.texera.workflow.operators.dictionary;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * MatchingType: the type of matching to perform. <br>
 * Currently we have 3 types of matching: <br>
 *
 * SCANBASED: <br>
 * Performs simple exact matching of the query. Matching is
 * case insensitive. <br>
 *
 * SUBSTRING: <br>
 * Performs simple substring matching of the query. Matching is
 * case insensitive. <br>
 *
 * CONJUNCTION_INDEXBASED: <br>
 * Performs search of conjunction of query tokens. The query is tokenized
 * into keywords, with each token treated as a separate keyword. The order
 * of tokens doesn't matter in the source tuple. <br>
 *
 * For example: <br>
 * query "book appointment with the doctor" <br>
 * matches: "book appointment" <br>
 * also matches: "an appointment for a book" <br>
 * <br>
 *
 *  @author Zuozhi Wang
 */

 public enum MatchingType {
    SCANBASED("Scan"),

    SUBSTRING("Substring"),

    CONJUNCTION_INDEXBASED("Conjunction");

    private final String name;

    private MatchingType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
