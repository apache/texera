package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;

/**
 * Created by chenli on 3/25/16.
 * 
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {
    
    private RegexPredicate regexPredicate;
    private String regex;
    private List<Attribute> attributeList;

    private boolean isCaseInsensitive = false;

    // two available regex engines, RegexMatcher will try RE2J first
    private enum RegexEngine {
        JavaRegex, RE2J
    }

    private RegexEngine regexEngine;
    private com.google.re2j.Pattern re2jPattern;
    private java.util.regex.Pattern javaPattern;
    
    private Schema inputSchema;


    public RegexMatcher(RegexPredicate predicate) {
        this.regexPredicate = predicate;
        this.regex = regexPredicate.getRegex();
        this.attributeList = regexPredicate.getAttributeList();
    }
    
    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        
        if (!this.inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.createSpanSchema(inputSchema);
        }
        
        // try Java Regex first
        try {
            if (isCaseInsensitive) {
                this.javaPattern = java.util.regex.Pattern.compile(regex, java.util.regex.Pattern.CASE_INSENSITIVE);
                this.regexEngine = RegexEngine.JavaRegex; 
            } else {
                this.javaPattern = java.util.regex.Pattern.compile(regex);
                this.regexEngine = RegexEngine.JavaRegex; 
            }

            // if Java Regex fails, try RE2J
        } catch (java.util.regex.PatternSyntaxException javaException) {
            try {
                if (isCaseInsensitive) {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regexPredicate.getRegex(), com.google.re2j.Pattern.CASE_INSENSITIVE);
                    this.regexEngine = RegexEngine.RE2J;
                } else {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regexPredicate.getRegex());
                    this.regexEngine = RegexEngine.RE2J;                    
                }

                // if RE2J also fails, throw exception
            } catch (com.google.re2j.PatternSyntaxException re2jException) {
                throw new DataFlowException(javaException.getMessage(), javaException);
            }
        }
    }
    
    @Override
    protected ITuple computeNextMatchingTuple() throws TextDBException {
        ITuple inputTuple = null;
        ITuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                inputTuple = Utils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            }            
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }
        
        return resultTuple;
    }

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     * 
     * @param tuple
     *            document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     *         in the document
     * @throws DataFlowException
     */
    @Override
    public ITuple processOneInputTuple(ITuple inputTuple) throws DataFlowException {
        if (inputTuple == null) {
            return null;
        }

        List<Span> matchingResults = new ArrayList<>();

        for (Attribute attribute : attributeList) {
            String fieldName = attribute.getFieldName();
            FieldType fieldType = attribute.getFieldType();
            String fieldValue = inputTuple.getField(fieldName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            switch (regexEngine) {
            case JavaRegex:
                matchingResults.addAll(javaRegexMatch(fieldValue, fieldName));
                break;
            case RE2J:
                matchingResults.addAll(re2jRegexMatch(fieldValue, fieldName));
                break;
            }
        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        List<Span> spanList = (List<Span>) inputTuple.getField(SchemaConstants.SPAN_LIST).getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    private List<Span> javaRegexMatch(String fieldValue, String fieldName) {
        List<Span> matchingResults = new ArrayList<>();
        java.util.regex.Matcher javaMatcher = this.javaPattern.matcher(fieldValue);
        while (javaMatcher.find()) {
            int start = javaMatcher.start();
            int end = javaMatcher.end();
            matchingResults.add(
                    new Span(fieldName, start, end, this.regexPredicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    private List<Span> re2jRegexMatch(String fieldValue, String fieldName) {
        List<Span> matchingResults = new ArrayList<>();
        com.google.re2j.Matcher re2jMatcher = this.re2jPattern.matcher(fieldValue);
        while (re2jMatcher.find()) {
            int start = re2jMatcher.start();
            int end = re2jMatcher.end();
            matchingResults.add(
                    new Span(fieldName, start, end, this.regexPredicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    /**
     * Use Java's built-in Regex Engine. <br>
     * RegexMatcher is set to use Java Regex Engine by default. <br>
     * 
     * @throws java.util.regex.PatternSyntaxException
     */
    public void setRegexEngineToJava() throws java.util.regex.PatternSyntaxException {
        if (this.regexEngine == RegexEngine.JavaRegex) {
            return;
        } else {
            this.javaPattern = java.util.regex.Pattern.compile(this.regex);
            this.regexEngine = RegexEngine.JavaRegex;
        }
    }

    /**
     * Use RE2J Regex Engine. <br>
     * RegexMatcher is set to use Java Regex Engine by default. Because Java
     * Regex is usually faster than RE2J <br>
     * 
     * @throws java.util.regex.PatternSyntaxException
     */
    public void setRegexEngineToRE2J() throws java.util.regex.PatternSyntaxException {
        if (this.regexEngine == RegexEngine.RE2J) {
            return;
        } else {
            try {
                this.re2jPattern = com.google.re2j.Pattern.compile(this.regex);
                this.regexEngine = RegexEngine.RE2J;
            } catch (com.google.re2j.PatternSyntaxException e) {
                throw new java.util.regex.PatternSyntaxException(e.getDescription(), e.getPattern(), e.getIndex());
            }
        }
    }
    
    public boolean getIsCaseInsensitive() {
        return isCaseInsensitive;
    }
    
    public void setIsCaseInsensitive(boolean isCaseInsensitive) {
        this.isCaseInsensitive = isCaseInsensitive;
    }

    public String getRegexEngineString() {
        return this.regexEngine.toString();
    }

    public String getRegex() {
        return this.regex;
    }

    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public RegexPredicate getPredicate() {
        return this.regexPredicate;
    }
    
}
