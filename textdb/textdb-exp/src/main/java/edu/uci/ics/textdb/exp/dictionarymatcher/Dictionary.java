package edu.uci.ics.textdb.exp.dictionarymatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary {
    
    private final LinkedHashSet<String> dictionaryEntries;       
    private Iterator<String> dictionaryIterator;
    
    /**
     * Create a dictionary using a collection of entries.
     * 
     * @param dictionaryEntries, a collection of dictionary entries
     */
    @JsonCreator
    public Dictionary(
            @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES, required = true)
            Collection<String> dictionaryEntries) {
        // Using LinkedHashSet so that getNextValue() returns the words in order.
        this.dictionaryEntries = new LinkedHashSet<>(dictionaryEntries);
        this.dictionaryIterator = this.dictionaryEntries.iterator();
    }
    
    @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES)
    public Collection<String> getDictionaryEntries() {
        return new ArrayList<>(this.dictionaryEntries);
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
    public boolean isEmpty(){
        return (dictionaryEntries.isEmpty() || dictionaryEntries == null);
    }
    
    /**
     * Reset the cursor to the start of the dictionary.
     */
    @JsonIgnore
    public void resetCursor() {
        dictionaryIterator = dictionaryEntries.iterator();
    }
    
}
