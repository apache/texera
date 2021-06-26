package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;

/**
 * @author Sudeep [inkudo]
 */
public class Dictionary {

    private final ArrayList<String> dictionaryEntries;
    private Iterator<String> dictionaryIterator;
    /**
     * These three arraylists are used to prepare the tokens of each dictionary entry
     * in the setup() of the DictionaryMatcher for phrase and conjunction matching.
     * tokenSetsNoStopwords is for the conjunction matching type.
     * tokenListsNoStopwords and tokenListsWithStopwords are for the phrase matching type.
     * Example: to tokenize dictionary entry "lin lin to be panda"
     * tokenSetsNoStopwords: [{"lin", "panda"}]
     * tokenListsNoStopwords: [["lin", "lin", "panda"]]
     * tokenListsWithStopwords: [["lin", "lin", "to", "be", "panda"]]
     */
    private ArrayList<Set<String>> tokenSetsNoStopwords;
    private ArrayList<List<String>> tokenListsWithStopwords;
    private ArrayList<List<String>> tokenListsNoStopwords;
    private ArrayList<Pattern> patternList;

    /**
     * Create a dictionary using a collection of entries.
     *
     * @param dictionaryEntries, a collection of dictionary entries
     */
    @JsonCreator
    public Dictionary(
            @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES, required = true)
                    Collection<String> dictionaryEntries) {
        
        checkNotNull(dictionaryEntries);
        dictionaryEntries.forEach(entry -> checkNotNull(entry));
        if (dictionaryEntries.isEmpty()) {
            throw new TexeraException("Dictionary is empty");
        }

        this.dictionaryEntries = new ArrayList<>();
        for (String entry : dictionaryEntries) {
            String entryTrimed = entry.trim();
            if (!entryTrimed.equals("")) {
                if (!this.dictionaryEntries.isEmpty() && this.dictionaryEntries.contains(entryTrimed)) {
                    continue;
                }
                this.dictionaryEntries.add(entryTrimed);
            }
        }
        this.dictionaryIterator = this.dictionaryEntries.iterator();
        this.tokenSetsNoStopwords = null;
        this.tokenListsNoStopwords = null;
        this.tokenListsWithStopwords = null;
        this.patternList = null;
    }

    @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES)
    public ArrayList<String> getDictionaryEntries() {
        return this.dictionaryEntries;
    }

    /**
     * Gets next dictionary entry from the dictionary
     */
    @JsonIgnore
    public String getNextEntry() {
        if (dictionaryIterator.hasNext()) {
            return dictionaryIterator.next();
        }
        return null;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return (dictionaryEntries == null || dictionaryEntries.isEmpty());
    }

    /***
     * To generate a set of tokens for each dictionary entry with removal
     * of duplicate tokens in the setup() of DictionaryMather for conjunction matching type.
     * @param luceneAnalyzerStr
     */
    @JsonIgnore
    public void setDictionaryTokenSetList(String luceneAnalyzerStr) {
        this.tokenSetsNoStopwords = new ArrayList<>();
        for (int i = 0; i < dictionaryEntries.size(); i++) {
            tokenSetsNoStopwords.add(new HashSet<>(DataflowUtils.tokenizeQuery(luceneAnalyzerStr, dictionaryEntries.get(i))));
        }
    }

    /**
     * To generate a list of tokens for each dictionary entry without removal of duplicate tokens:
     * and a list of tokens for each dictionary entry without removal of stopwords and duplicate tokens.
     * Used by Phrase matching type in the setup() of DictionaryMatcher.
     *
     * @param luceneAnalyzerStr
     */
    @JsonIgnore
    public void setDictionaryTokenListWithStopwords(String luceneAnalyzerStr) {
        this.tokenListsWithStopwords = new ArrayList<>();
        this.tokenListsNoStopwords = new ArrayList<>();
        for (int i = 0; i < dictionaryEntries.size(); i++) {
            tokenListsNoStopwords.add(DataflowUtils.tokenizeQuery(luceneAnalyzerStr, dictionaryEntries.get(i)));
            tokenListsWithStopwords.add(DataflowUtils.tokenizeQueryWithStopwords(luceneAnalyzerStr, dictionaryEntries.get(i)));
        }
    }

    @JsonIgnore
    public void setPatternList(){
        this.patternList = new ArrayList<>();
        for(int i = 0; i < dictionaryEntries.size(); i++) {
            Pattern pattern = Pattern.compile(dictionaryEntries.get(i),Pattern.CASE_INSENSITIVE);
            patternList.add(pattern);
        }
    }

    @JsonIgnore
    public ArrayList<Set<String>> getTokenSetsNoStopwords() {
        return this.tokenSetsNoStopwords;
    }

    @JsonIgnore
    public ArrayList<List<String>> getTokenListsWithStopwords() {
        return this.tokenListsWithStopwords;
    }

    @JsonIgnore
    public ArrayList<List<String>> getTokenListsNoStopwords() {
        return this.tokenListsNoStopwords;
    }

    @JsonIgnore
    public ArrayList<Pattern> getPatternList() {
        return this.patternList;
    }

    @JsonIgnore
    public void resetCursor() {
        dictionaryIterator = dictionaryEntries.iterator();
    }
    
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this.getDictionaryEntries());
    }
    
    @Override
    public boolean equals(Object that) {
        if (that == null) return false;
        if (! (that instanceof Dictionary)) return false;
        
        return Objects.equals(this.dictionaryEntries, ((Dictionary) that).dictionaryEntries);
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
}


