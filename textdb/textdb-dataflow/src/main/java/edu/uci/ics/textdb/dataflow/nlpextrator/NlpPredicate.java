package edu.uci.ics.textdb.dataflow.nlpextrator;

import java.util.List;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.api.schema.Attribute;

public class NlpPredicate implements IPredicate {
    
    /**
     * Named Entity Token Types: NE_ALL, Number, Location, Person, Organization,
     * Money, Percent, Date, Time. Part Of Speech Token Types: Noun, Verb,
     * Adjective, Adverb
     */
    public enum NlpTokenType {
        Noun, Verb, Adjective, Adverb,

        NE_ALL, Number, Location, Person, Organization, Money, Percent, Date, Time;
    }

    private NlpTokenType nlpTokenType;
    private List<String> attributeNames;

    public NlpPredicate(NlpTokenType nlpTokenType, List<String> attributeNames) {
        this.nlpTokenType = nlpTokenType;
        this.attributeNames = attributeNames;
    }

    public NlpTokenType getNlpTokenType() {
        return nlpTokenType;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }
    
    public String getNlpTypeIndicator() {
        if (isPOSTokenType(nlpTokenType)) {
            return "POS";
        } else {
            return "NE_ALL";
        }
    }
    
    private static boolean isPOSTokenType(NlpTokenType tokenType) {
        if (tokenType.equals(NlpTokenType.Adjective) || tokenType.equals(NlpTokenType.Adverb)
                || tokenType.equals(NlpTokenType.Noun) || tokenType.equals(NlpTokenType.Verb)) {
            return true;
        } else {
            return false;
        }
    }
}
