package edu.uci.ics.texera.dataflow.nlp.entity;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Named Entity Types: NE_ALL, Number, Location, Person, Organization,
 * Money, Percent, Date, Time. Part Of Speech Entity Types: Noun, Verb,
 * Adjective, Adverb
 */
public enum NlpEntityType {
    NOUN("noun"), 
    VERB("verb"), 
    ADJECTIVE("adjective"), 
    ADVERB("adverb"),

    NE_ALL("all named entity"), 
    NUMBER("number"), 
    LOCATION("location"), 
    PERSON("person"), 
    ORGANIZATION("organization"), 
    MONEY("money"), 
    PERCENT("percent"), 
    DATE("date"), 
    TIME("time");
    
    private final String name;
    
    private NlpEntityType(String name) {
        this.name = name;
    }
    
    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }
    
    @Override
    public String toString() {
        return this.name;
    }
}