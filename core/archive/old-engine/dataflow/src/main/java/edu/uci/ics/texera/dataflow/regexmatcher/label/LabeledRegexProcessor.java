package edu.uci.ics.texera.dataflow.regexmatcher.label;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcher;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;

/**
 * Helper class for processing labeled regex.
 * 
 * @author Bhushan Pagariya (bhushanpagariya)
 * @author Harshini Shah
 * @author Yashaswini Amaresh
 * @author Zuozhi Wang
 *
 */
public class LabeledRegexProcessor {
    
    private RegexPredicate predicate;
    private String cleanedRegex;
    private ArrayList<String> labelList = new ArrayList<>();
    
    public LabeledRegexProcessor(RegexPredicate predicate) {
        this.predicate = predicate;
        preprocessRegex();
    }
    
    private void preprocessRegex() {
        Matcher labelMatcher = Pattern.compile(RegexMatcher.CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        cleanedRegex = predicate.getRegex();
        while (labelMatcher.find()) {
            String labelStr = predicate.getRegex().substring(
                    labelMatcher.start() + 1, labelMatcher.end() - 1);
            String substrTrimmed = labelStr.trim();
            labelList.add(substrTrimmed);
            cleanedRegex = cleanedRegex.replace("<" + labelStr + ">", "<" + substrTrimmed + ">");
        }
    }
    
    /**
     * Process regex pattern
     * @param inputTuple
     * @return tuple with matching entries
     */
    public List<Span> computeMatchingResults(Tuple inputTuple) {
        Map<String, Set<String>> labelValues = fetchLabelValues(inputTuple);
        String regexWithVal = rewriteRegexWithLabelValues(labelValues);
        Pattern regexPattern = predicate.isIgnoreCase() ? 
                Pattern.compile(regexWithVal, Pattern.CASE_INSENSITIVE)
                : Pattern.compile(regexWithVal);
                
        return RegexMatcher.computeMatchingResultsWithPattern(inputTuple, predicate, regexPattern);
    }
    
    /**
     * Create Map of label id and corresponding attribute values
     * @param inputTuple
     * @return map of label id and corresponding attribute values
     */
    private Map<String, Set<String>> fetchLabelValues(Tuple inputTuple) throws DataflowException {
        Map<String, Set<String>> labelSpanList = new HashMap<>();
        for (String label : this.labelList) {
            if (! inputTuple.getSchema().containsAttribute(label)) {
                throw new DataflowException("label " + label + " does not exist");
            }
            ListField<Span> spanListField = inputTuple.getField(label);
            Set<String> labelValues = spanListField.getValue().stream()
                    .map(span -> span.getValue())
                    .map(value -> escapeString(value))
                    .collect(Collectors.toSet());
            labelSpanList.put(label, labelValues);
        }
        return labelSpanList;
    }
    
    /*
     * Try to escape all special characters in a string
     *   by escape all non-letter and non-digit character.
     * 
     * If the character is not a special character in regex,
     *   then escaping it will still be itself.
     */
    private static String escapeString(String str) {
        return str.chars()
            .mapToObj(ch -> (Character.isLetterOrDigit(ch) ? "" : "\\") + String.valueOf((char) ch))
            .collect(Collectors.joining());
    }
    
    /**
     * Replace labels with actual values in labeled regex
     * @param cleanedRegex
     * @param labelValueList
     * @return regex with actual span values
     */
    private String rewriteRegexWithLabelValues(Map<String, Set<String>> labelValues) {
        String regexWithValue = cleanedRegex;
        for(Map.Entry<String, Set<String>> entry : labelValues.entrySet()){
            String repVal = "(" + entry.getValue().stream().collect(Collectors.joining("|")) + ")";
            regexWithValue = regexWithValue.replaceAll("<"+entry.getKey()+">", repVal);
        }
        return regexWithValue;
    }

}
