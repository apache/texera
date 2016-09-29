package edu.uci.ics.textdb.common.constants;

import java.util.Arrays;
import java.util.List;

public class OperatorConstants {

    // This class doesn't need to be initialized.
    private OperatorConstants() {
    }

    /**
     * A list of all operators of TextDB.
     */
    public static final List<String> operatorList = Arrays.asList(
            "IndexBasedSource", 
            "ScanBasedSource",
            "KeywordSource",
            "DictionarySource",

            "KeywordMatcher", 
            "RegexMatcher", 
            "FuzzyTokenMatcher", 
            "NlpExtractor",

            "Join",

            "IndexSink", 
            "FileSink");
    
}
