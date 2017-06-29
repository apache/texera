package edu.uci.ics.textdb.exp.regexmatcher.label;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.regexmatcher.RegexMatcher;
import edu.uci.ics.textdb.exp.regexmatcher.RegexPredicate;

/**
 * Helper class for processing labeled regex without any qualifiers.
 * For regex format "this <label1> can cure <label2> better", extract the labels and affixes.
 * affixList {"this "," can cure "," better"}
 * labelList {"label1", "label2"}
 * Sort the affixList in length-decreasing order to select valid tuples with all the affixes inside to short-cut processing time.
 * Generate map of labels and the corresponding spans.
 * For each valid input tuple, match the first label's spans to its prefix and suffix with start/end position information.
 * Filter out the valid spans to continue match with following labels.
 * Early break if no matching in any step.
 *
 * @author Chang Liu
 * @author Zuozhi Wang
 *
 */
public class LabledRegexNoQualifierProcessor {

    private RegexPredicate predicate;
    private RegexMatcher.RegexType regexType;
    
    private ArrayList<String> labelList = new ArrayList<>();
    private ArrayList<String> affixList = new ArrayList<>();
    // to sort the affixList by length in decreasing order to short-cut the filter tuple operation.
    private ArrayList<String> sortedAffixList = new ArrayList<>();
    // to save the map between label and corresponding list of spans.
    private Map<String, List<Span>> labelValues = new HashMap<String, List<Span>>();

    public LabledRegexNoQualifierProcessor(RegexPredicate predicate, RegexMatcher.RegexType regexType) {
        this.predicate = predicate;
        this.regexType = regexType;
        // populate labelList and affixList
        preprocessRegex();
        if(this.regexType== RegexMatcher.RegexType.Labeled_WITHOUT_QUALIFIERS) {
            sortedAffixList = new ArrayList<>(affixList);
            sortedAffixList.sort((o1, o2) -> (o2.length()-o1.length()));
        }

    }

    /**
     * For regex format "this <label1> can cure <label2> better", extract the labels and affixes.
     * affixList {"this "," can cure "," better"}
     * labelList {"label1", "label2"}
     * Sort the affixList in length decreasing order to filter tuples.
     */
    private void preprocessRegex() {
        Matcher labelMatcher = Pattern.compile(RegexMatcher.CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        int pre = 0;
        while (labelMatcher.find()) {
            int start = labelMatcher.start();
            int end = labelMatcher.end();
            
            affixList.add(predicate.getRegex().substring(pre, start));
            labelList.add(predicate.getRegex().substring(
                    labelMatcher.start() + 1, labelMatcher.end() - 1).trim());

            pre = end;
        }
        affixList.add(predicate.getRegex().substring(pre));
    }

    /**
     * Sort the affixList in length decreasing order to filter valid tuples.
     * @param tuple
     * @param attribute
     * @return
     */
    private boolean filterTuple(Tuple tuple, String attribute) {
        for (String affix : sortedAffixList) {
            if (! tuple.getField(attribute).getValue().toString().contains(affix)) {
                return false;
            }
        }
        return true;
    }

    /***
     * Consider the affix with qualifiers and without qualifiers separately.
     * @param tuple
     * @return
     */

    public List<Span> computeMatchingResults(Tuple tuple) {

        labelValues = fetchLabelSpans(tuple);
        if(this.regexType == RegexMatcher.RegexType.Labeled_WITHOUT_QUALIFIERS){
            return noQualifierMatchingResults(tuple);
        }else{
            return affixQualifierMatchingResults(tuple);
        }

    }

    /***
     * For affix with qualifiers: treat each affix as a regex.
     * Compile them and get match result as a map of <affix, spanlist>.
     * Match with the label spanlist under same attribute for each input tuple.
     * Early break if no matches.
     * @param tuple
     * @return
     */
    public List<Span> affixQualifierMatchingResults(Tuple tuple) {
        List<Span> allAttrsMatchResults = new ArrayList<>();
        for(String attribute: predicate.getAttributeNames()){
            Map<String, List<Span>> affixMap = generateAffixMap(attribute, tuple);
            if(affixMap.isEmpty()) {
                continue;
            }
            List<List<Integer>> matchList = new ArrayList<>();
            for(int i = 0; i < labelList.size(); i++){
                String label = labelList.get(i);
                String prefix = affixList.get(i);
                String suffix = affixList.get(i+1);
                List<Span> relevantSpans = labelValues.get(label).stream()
                        .filter(span -> span.getAttributeName().equals(attribute)).collect(Collectors.toList());
                if(i ==0){
                    List<Span> validSpans = new ArrayList<>();
                    for(Span span: relevantSpans){
                        List<Span> matchPrefix = affixMap.get(prefix).stream()
                                .filter(s -> s.getEnd() == span.getStart()).collect(Collectors.toList());
                        if(! matchPrefix.isEmpty()){
                            matchList.addAll(matchPrefix.stream().
                                    map(span1 -> new ArrayList<Integer>(Arrays.asList(span1.getStart(), span1.getEnd())))
                                    .collect(Collectors.toList()));
                            validSpans.add(span);
                        }

                    }
                    relevantSpans = validSpans;
                }
                List<List<Integer>> newMatchList = new ArrayList<>();
                for(List<Integer> previousMatch : matchList){
                    for(Span span: relevantSpans){
                        if(previousMatch.get(1) == span.getStart()){
                            List<Span> matchSuffix = affixMap.get(suffix).stream()
                                    .filter(s -> s.getStart() == span.getEnd()).collect(Collectors.toList());
                            if(! matchSuffix.isEmpty()){
                                matchSuffix.stream().forEach(matchSpan -> newMatchList
                                        .add(new ArrayList<Integer>(Arrays.asList(previousMatch.get(0), matchSpan.getEnd()))));
                            }

                        }
                    }
                }
                matchList = newMatchList;
                if (matchList.isEmpty()) {
                    break;
                }

            }
            String fieldValue = tuple.getField(attribute).getValue().toString();
            matchList.stream().forEach(match -> allAttrsMatchResults.add(
                    new Span(attribute, match.get(0), match.get(1), predicate.getRegex(), fieldValue.substring(match.get(0), match.get(1)))));
        }
        return allAttrsMatchResults;
    }

    /***
     * Filter out valid tuple using affixes in the regex.
     * String Match the entire regex from the first label's spans with its prefix and suffix using span's start and end information.
     * Filter out the valid spans to continue match with following labels.
     * Early break if no matching in any step.
     * @param tuple
     * @return
     */



    public List<Span> noQualifierMatchingResults(Tuple tuple){
        List<Span> allAttrsMatchSpans = new ArrayList<>();
        for (String attribute : predicate.getAttributeNames()) {
            boolean isValidTuple = filterTuple(tuple, attribute);

            if (! isValidTuple) {
                continue;
            }

            String fieldValue = tuple.getField(attribute).getValue().toString();

            List<List<Integer>> matchList = new ArrayList<>();
            
            for (int i = 0; i < labelList.size(); i++) {
                String label = labelList.get(i);
                String prefix = affixList.get(i);
                String suffix = affixList.get(i+1);
                
                List<Span> relevantSpans = labelValues.get(label).stream()
                        .filter(span -> span.getAttributeName().equals(attribute)).collect(Collectors.toList());
                
                if (i == 0) {
                    List<Span> validSpans = relevantSpans.stream()
                            .filter(span -> span.getStart() >= prefix.length())
                            .filter(span -> fieldValue.substring(span.getStart() - prefix.length(), span.getStart()).equals(prefix))
                            .collect(Collectors.toList());
                    matchList = validSpans.stream()
                            .map(span -> new ArrayList<Integer>(Arrays.asList(span.getStart() - prefix.length(), span.getStart())))
                            .collect(Collectors.toList());
                    relevantSpans = validSpans;
                }
                
                List<List<Integer>> newMatchList = new ArrayList<>();
                
                for (List<Integer> previousMatch : matchList) {
                    for (Span span : relevantSpans) {
                        if (previousMatch.get(1) == span.getStart()
                                && span.getEnd() + suffix.length() <= fieldValue.length() 
                                && fieldValue.substring(span.getEnd(), span.getEnd() + suffix.length()).equals(suffix)) {
                            newMatchList.add(Arrays.asList(previousMatch.get(0), span.getEnd() + suffix.length()));
                        }
                    }
                }
                
                matchList = newMatchList;
                if (matchList.isEmpty()) {
                    break;
                }
            }
            
            // assert that for every match:
            //  start >= 0, and end >= 0, and start <= end
            assert(matchList.stream()
                    .filter(match -> match.get(0) >= 0)
                    .filter(match -> match.get(1) >= 0)
                    .filter(match -> match.get(0) <= match.get(1))
                    .count() == (long) matchList.size());
            
            matchList.stream().forEach(match -> allAttrsMatchSpans.add(
                    new Span(attribute, match.get(0), match.get(1), predicate.getRegex(), fieldValue.substring(match.get(0), match.get(1)))));
        }

        return allAttrsMatchSpans;
    }
    
    
    /**
     * Creates Map of label and corresponding spans
     * @param inputTuple
     * @return 
     */
    private Map<String, List<Span>> fetchLabelSpans(Tuple inputTuple) {
        Map<String, List<Span>> labelSpanMap = new HashMap<>();
        for (String label : this.labelList) {
            ListField<Span> spanListField = inputTuple.getField(label);
            labelSpanMap.put(label, spanListField.getValue());
        }
        return labelSpanMap;
    }

    /***
     * For each input tuple and attribute, compile all the affix regex and get the matching result as a map of <affix, spanlist>.
     * Early break if any affix matching fails.
     * @param attribute
     * @param tuple
     * @return
     */
    private Map<String, List<Span>> generateAffixMap(String attribute, Tuple tuple){
        Map<String, List<Span>> affixMap = new HashMap<>();
        for(String affix: affixList){
            Pattern affixPattern = this.predicate.isIgnoreCase() ?
                    Pattern.compile(affix, Pattern.CASE_INSENSITIVE)
                    : Pattern.compile(affix);
            String fieldValue = tuple.getField(attribute).getValue().toString();
            Matcher affixMatcher =affixPattern.matcher(fieldValue);
            List<Span> matchingResults = new ArrayList<>();
            while (affixMatcher.find()) {
                int start = affixMatcher.start();
                int end = affixMatcher.end();
                matchingResults.add(
                        new Span(attribute, start, end, affix, fieldValue.substring(start, end)));
            }
            if(matchingResults.isEmpty()){
                break;
            }
            affixMap.put(affix, matchingResults);
        }
        return affixMap;
    }

}
