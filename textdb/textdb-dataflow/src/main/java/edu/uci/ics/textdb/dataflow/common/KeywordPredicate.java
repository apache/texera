package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import org.apache.lucene.search.TermQuery;

/**
 *  @author prakul
 *
 */

/**
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate implements IPredicate{

    private final List<Attribute> attributeList;
    private final String[] fields;
    private final String query;
    private final Query queryObject;
    private ArrayList<String> tokens;
    private Analyzer analyzer;
    private IDataStore dataStore;

    public KeywordPredicate(String query, List<Attribute> attributeList, Analyzer analyzer,IDataStore dataStore ) throws DataFlowException{
        try {
            this.query = query;
            this.attributeList = attributeList;
            this.dataStore = dataStore;
            String[] temp = new String[attributeList.size()];

            for(int i=0; i < attributeList.size(); i++){
                temp[i] = attributeList.get(i).getFieldName();
            }
            this.fields = temp;
            this.tokens = Utils.tokenizeQuery(analyzer, this.query);
            this.analyzer = analyzer;
            this.queryObject = createQueryObject();
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
    private Query createQueryObject() throws ParseException {
        ArrayList<String> tokens;
        List<String> fieldList = new ArrayList<String>();
        BooleanQuery booleanQuery = new BooleanQuery();

        for(int i=0; i< attributeList.size(); i++){
            String fieldName = attributeList.get(i).getFieldName();
            /*
            If the field is of type string, handle it differently
             */
            if(attributeList.get(i).getFieldType() == FieldType.DATE.STRING){
                Query termQuery = new TermQuery(new Term(fieldName,query));
                booleanQuery.add(termQuery, BooleanClause.Occur.SHOULD);
            }
            else {
                fieldList.add(fieldName);
            }
        }

        if(fieldList.size()==0){
            return booleanQuery;
        }

        String[] remainingFields = (String[]) fieldList.toArray(new String[0]);
        BooleanQuery queryOnOtherFields = new BooleanQuery();
        tokens = Utils.tokenizeQuery(analyzer, query);
        MultiFieldQueryParser parser = new MultiFieldQueryParser(remainingFields,analyzer);
        for(String searchToken: tokens){
            Query termQuery = parser.parse(searchToken);
           queryOnOtherFields.add(termQuery, BooleanClause.Occur.MUST);
        }
        booleanQuery.add(queryOnOtherFields,BooleanClause.Occur.SHOULD);
        return booleanQuery;
    }

    public String getQuery(){
        return query;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }
    public Query getQueryObject(){return this.queryObject;}

    public ArrayList<String> getTokens(){return this.tokens;}

    public Analyzer getAnalyzer(){
        return analyzer;
    }

    public DataReaderPredicate convertToDataReaderPredicate() {
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(this.dataStore,this.queryObject);
        return dataReaderPredicate;
    }


}