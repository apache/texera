package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordOperatorType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 *  @author prakul
 *
 */

/**
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate implements IPredicate{

    private final List<Attribute> attributeList;
    private final String query;
    private final Query luceneQuery;
    private ArrayList<String> tokens;
    private Analyzer luceneAnalyzer;
    private IDataStore dataStore;
    private KeywordOperatorType operatorType;

    /*
    query refers to string of keywords to search for.
    For Ex. New york if searched in TextField, we would consider both tokens
    New and York; if searched in String field we search for Exact string.
     */
    public KeywordPredicate(String query, List<Attribute> attributeList, KeywordOperatorType operatorType, Analyzer luceneAnalyzer, IDataStore dataStore ) throws DataFlowException{
        try {
            this.query = query;
            this.tokens = Utils.tokenizeQuery(luceneAnalyzer, query);
            this.attributeList = attributeList;
            this.operatorType = operatorType;
            this.dataStore = dataStore;
            this.luceneAnalyzer = luceneAnalyzer;
            this.luceneQuery = createLuceneQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * Creates a Query object as a boolean Query on all attributes
     * Example: For creating a query like
     * (TestConstants.DESCRIPTION + ":lin" + " AND " + TestConstants.LAST_NAME + ":lin")
     * we provide a list of AttributeFields (Description, Last_name) to search on and a query string (lin)
     *
     * TODO #88:BooleanQuery() is deprecated. In future a better solution could be worked out in Query builder layer
     *
     * @return Query
     * @throws ParseException
     */
    private Query createLuceneQueryObject() throws ParseException {

        List<String> textFieldList = new ArrayList<String>();
        BooleanQuery luceneBooleanQuery = new BooleanQuery();

        for(int i=0; i < attributeList.size(); i++){

            String fieldName = attributeList.get(i).getFieldName();

            /*
            If the field type is String, we need to perform an exact match
            without parsing the query (Case Sensitive). Hence add them directly to the Query.
             */
            if(attributeList.get(i).getFieldType() == FieldType.STRING){
                Query termQuery = new TermQuery(new Term(fieldName, query));
                luceneBooleanQuery.add(termQuery, BooleanClause.Occur.SHOULD);
            }
            else {
                textFieldList.add(fieldName);
            }
        }

        if(textFieldList.size()==0){
            return luceneBooleanQuery;
        }

        /*
        For all the other fields , parse the query using query parser
        and generate  boolean query (Textfield is Case Insensitive)
         */
        String[] remainingTextFields = (String[]) textFieldList.toArray(new String[0]);
        BooleanQuery queryOnTextFields = new BooleanQuery();
        MultiFieldQueryParser parser = new MultiFieldQueryParser(remainingTextFields, luceneAnalyzer);

        if(operatorType == KeywordOperatorType.BASIC) {
            for (String searchToken : this.tokens) {
                Query termQuery = parser.parse(searchToken);
                queryOnTextFields.add(termQuery, BooleanClause.Occur.MUST);
            }
        }

        else if(operatorType == KeywordOperatorType.PHRASE){
            Query termQuery = parser.parse("\""+query+"\"");
            queryOnTextFields.add(termQuery, BooleanClause.Occur.MUST);
        }
        /*
        Merge the query for non-String fields with the StringField Query
         */
        luceneBooleanQuery.add(queryOnTextFields,BooleanClause.Occur.SHOULD);
        return luceneBooleanQuery;
    }

    public KeywordOperatorType getOperatorType() { return operatorType; }

    public String getQuery(){
        return query;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }

    public Query getQueryObject(){return this.luceneQuery;}

    public ArrayList<String> getTokens(){return this.tokens;}

    public Analyzer getLuceneAnalyzer(){
        return luceneAnalyzer;
    }

    public DataReaderPredicate getDataReaderPredicate() {
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(this.dataStore, this.luceneQuery,
                this.query, this.luceneAnalyzer, this.attributeList);
        return dataReaderPredicate;
    }


}