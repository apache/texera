package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordOperatorType;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 *  @author prakul
 *  @author Akshay
 *
 */
public class KeywordMatcher implements IOperator {
    private final KeywordPredicate predicate;
    private ISourceOperator sourceOperator;
    private String query;
    private List<Attribute> attributeList;
    private List<String> queryTokens;

    public KeywordMatcher(IPredicate predicate) {
        this.predicate = (KeywordPredicate)predicate;
        DataReaderPredicate dataReaderPredicate = this.predicate.getDataReaderPredicate();
        dataReaderPredicate.setIsSpanInformationAdded(true);
        this.sourceOperator = new IndexBasedSourceOperator(dataReaderPredicate);
    }

    @Override
    public void open() throws DataFlowException {
        try {
            sourceOperator.open();
            query = predicate.getQuery();
            attributeList = predicate.getAttributeList();
            queryTokens = predicate.getTokens();

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Gets next matched tuple. Returns a new span tuple including the
     *        span results. Performs a scan based search or an index based search depending
     *        on the sourceOperator provided while initializing KeywordPredicate.
     *        It scans documents returned by sourceOperator for provided keywords/phrase (Depending on provided
     *        KeywordOperatorType).
     *
     *        Presently 2 types of KeywordOperatorType are supported:
     *
     *        KeywordOperatorType.PHRASE:
     *
     *          Query phrase should exist in a document. 'Stop words' (as considered in Lucene Analyzer) are
     *          considered as placeholders and are not exactly matched, only main search tokens are exactly matched.
     *          Lucene can only provide positions of tokens in phrase query. The phrase query will keep changing
     *          thus phrase positioning cant be provided by lucene. We have to combine tokens.

     *          Ex:
     *          if Query is "lin clooney and angry"
     *          and document is "Lin Clooney is Short and lin clooney is Angry"
     *          we will return combined span "lin clooney is angry" and ignore "Lin Clooney"
     *
     *
     *        KeywordOperatorType.BASIC:
     *
     *           All tokens of Query should appear in a Single Field of each document in document
     *           otherwise it doesn't return anything. It uses AND logic. For each field, if all the query tokens
     *           appear in the field, then we add its spans to the results.
     *           If one of the query tokens doesn't appear in the field, we ignore this field.
     *
     *           For each tuple returned by the the sourceOperator loop
     *           through all the fields in the attributeList. For each field, loop through all the
     *           matches. Returns only one tuple per document. If there are
     *           multiple matches, all spans are included in a list. Java Regex
     *           is used to match word boundaries. It uses AND logic for the query keywords
     *
     *           Ex :
     *           Document :  NAME (type 'String') : "lin merry" and the
     *           DESCRIPTION (type 'Text'): "Lin is like Angelina and is a merry person"
     *
     *           Query : "Lin merry",
     *           matches should include "lin merry","Lin","merry" but not Angelina.
     *
     *            Ex :
     *            Document :  NAME (type 'String') : "lin merry",
     *           DESCRIPTION (type 'Text'): "Lin is like Angelina and is a merry person"
     *           Query : "Lin george",
     *           it won't match any document
     */
    @Override
    public ITuple getNextTuple() throws DataFlowException {

        try {
            ITuple sourceTuple = sourceOperator.getNextTuple();
            if(sourceTuple == null){
                return null;
            }

            int schemaIndex = sourceTuple.getSchema().getIndex(SchemaConstants.SPAN_LIST_ATTRIBUTE.getFieldName());
            List<Span> spanList =
                    (List<Span>)sourceTuple.getField(schemaIndex).getValue();
            Collections.sort(spanList, tokenOffsetComp);
            /*
            sort is required for combining the span info for the phrase search. Suppose target query is
            "new is york" , and document contains multiple "new" and "york" term usage, then lucene will return all
            "new " occurences followed by all "york" occurences, thus to combine span we require to sort it by
            termOffset
            * */
            for(int attributeIndex = 0; attributeIndex < attributeList.size(); attributeIndex++) {
                String fieldName = attributeList.get(attributeIndex).getFieldName();
                IField field = sourceTuple.getField(fieldName);
                String fieldValue = (String) (field).getValue();

                if (!(field instanceof TextField)) {

                    //Keyword should match fieldValue entirely
                    if (fieldValue.equals(query)) {
                        Span span = new Span(fieldName, 0, query.length(), query, fieldValue);
                        spanList.add(span);
                    }
                } else {
                    // Check if all the tokens are present in that field,
                    // if any of the tokens is missing, remove all the span information for that field.

                    //By default, initialized to false.
                    boolean[] tokensPresent = new boolean[queryTokens.size()];

                    List<Span> spanForThisField = new ArrayList<>();

                    for (Span span : spanList) {
                        if (span.getFieldName().equals(fieldName)) {
                            spanForThisField.add(span);
                            if (queryTokens.contains(span.getKey()))
                                tokensPresent[queryTokens.indexOf(span.getKey())] = true;
                        }
                    }

                    boolean allTokenPresent = areAllTrue(tokensPresent);
                    if(predicate.getOperatorType() == KeywordOperatorType.BASIC) {

                        if (!allTokenPresent) {
                            spanList.removeAll(spanForThisField);
                        }
                    }

                    else if(predicate.getOperatorType() == KeywordOperatorType.PHRASE){
                        /*Ex:

                       Document: "Lin Clooney is Short and lin clooney is Angry"
                        Query 1 : "lin and and angry"
                        upon searching lucene will find documents where 'lin' occurs, followed by 2 words, then 'angry'
                        occurs. With each source tuple it will return span info for the tokens in query( in our
                        case 'lin' & 'angry').
                        spanList -> ["lin":{tokenOffset:0,start:0,end:3},"lin":{tokenOffset:5,start:25,end:28},
                        "angry":{tokenOffset:8,start:40,end:45}]
                        for our phrase search we want to combine "lin" at tokenOffset=5 and "angry" at tokenOffset=8.

                        Query 2:"lin clooney and angry"
                        spanList -> ["lin":{tokenOffset:0,start:0,end:3},"lin":{tokenOffset:5,start:25,end:28},
                        "clooney":{tokenOffset:1,start:4,end:11}, "clooney":{tokenOffset:6,start:29,end:36},
                        "angry":{tokenOffset:8,start:40,end:45}]
                        for our phrase search we want to combine "lin" at tokenOffset=5,"clooney" at tokenOffset=6
                        and "angry" at tokenOffset=8.

                        So in generalized logic we check for tokens in query corresponding to the phrase query semantics.
                         */

                        spanList.removeAll(spanForThisField);
                        //relevantWordsInQuery has all the tokens (including duplicates but excluding stop words) in the query.
                        List<String> relevantWordsInQuery = new ArrayList<>();
                        //relevantWordsInQueryOffset maintains the offset position of tokens in query
                        List<Integer> relevantWordsInQueryOffset = new ArrayList<>();
                        String[] queryArray = query.split(" ");
                        /*
                        queryTokens are split using analyzer. queryArray needs to be to split with spaces for the
                        logic which is following. analyser will remove the stop words , so we wont relative position
                        differences. Ex: new and york. we want to know new and york have a word in between.

                         */
                        if (allTokenPresent) {
                            for(int iter = 0; iter < queryArray.length; iter++){
                                if(queryTokens.contains(queryArray[iter])){
                                    relevantWordsInQuery.add(queryArray[iter]);
                                    relevantWordsInQueryOffset.add(iter);
                                }
                            }

                            int iter = 0; // maintains position of term being checked in spanForThisField list
                            while(iter < spanForThisField.size()){

                                /*Verify if span in the spanForThisField correspond to our phrase query, ie relative position offsets should be similar
                                and the value should be same.
                                *
                                 */
                                boolean isMismatchInSpan=false;// flag to check if a mismatch in spans occurs
                                if(iter <= spanForThisField.size()-relevantWordsInQuery.size()){
                                     // To check all the terms in query are verified
                                    for(int i=0; i < relevantWordsInQuery.size()-1; i++) {
                                        Span first = spanForThisField.get(iter+i);
                                        Span second = spanForThisField.get(iter +i+ 1);
                                        if (!(second.getTokenOffset() - first.getTokenOffset() == relevantWordsInQueryOffset.get(i+1) - relevantWordsInQueryOffset.get(i) &&
                                                first.getValue().equalsIgnoreCase(relevantWordsInQuery.get(i)) && second.getValue().equalsIgnoreCase(relevantWordsInQuery.get(i+1)))) {
                                            iter++;
                                            isMismatchInSpan=true;
                                            break;
                                        }
                                    }
                                    if(isMismatchInSpan==true)continue;
                                }

                                int combinedSpanStartIndex = spanForThisField.get(iter).getStart();
                                int combinedSpanEndIndex = spanForThisField.get(iter+relevantWordsInQuery.size()-1).getEnd();

                                Span combinedSpan = new Span(fieldName, combinedSpanStartIndex, combinedSpanEndIndex, query, fieldValue.substring(combinedSpanStartIndex, combinedSpanEndIndex));
                                spanList.add(combinedSpan);
                                iter = iter + relevantWordsInQuery.size();
                            }
                        }
                    }


                }
            }

            return sourceTuple;

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

    }

    @Override
    public void close() throws DataFlowException {
        try {
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    public static Comparator<Span> tokenOffsetComp = new Comparator<Span>() {
        @Override
        public int compare(Span o1, Span o2) {
            return o1.getTokenOffset()-o2.getTokenOffset();
        }
    };

    public static boolean areAllTrue(boolean[] array)
    {
        for(boolean b : array) if(!b) return false;
        return true;
    }
}