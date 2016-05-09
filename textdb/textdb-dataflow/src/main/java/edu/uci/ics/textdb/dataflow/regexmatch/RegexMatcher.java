package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexToTrigram;

/**
 * Created by chenli on 3/25/16.
 * @author laishuying
 */
public class RegexMatcher implements IOperator {
    private final RegexPredicate regexPredicate;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;

    private List<IField> fields;
    private Schema sourceTupleSchema = null;
    private Schema spanSchema = null;
    
    private List<Span> spans;

    public RegexMatcher(IPredicate predicate, ISourceOperator sourceOperator) {
    	this.regexPredicate = (RegexPredicate)predicate;
        this.sourceOperator = sourceOperator;
        
        // build the luceneQuery by given regex
        // String queryStr = RegexToTrigram.translate(regexPredicate.getRegex()).getQuery();

        // next PR for adding translate to indexBasedSourceOperator 
    }

    @Override
    public void open() throws DataFlowException {
        try {
            sourceOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
		try {
            ITuple sourceTuple = sourceOperator.getNextTuple();
            if(sourceTuple == null){
                return null;
            }  
            
            spans = regexPredicate.computeMatches(sourceTuple);
            
            if (spans != null && spans.size() != 0) { // a list of matches found
            	if (sourceTupleSchema == null) {
            		sourceTupleSchema = sourceTuple.getSchema();
            	}
            	if (spanSchema == null) {
            		spanSchema = Utils.createSpanSchema(sourceTupleSchema);
            	}
            	fields = sourceTuple.getFields();
            	return constructSpanTuple(spans);
            } else { // no match found
            	return getNextTuple();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }        
    }
    
    private ITuple constructSpanTuple(List<Span> spans) {
    	List<IField> fieldListDuplicate = new ArrayList<>(fields);
    	IField spanListField = new ListField<Span>(spans);
    	fieldListDuplicate.add(spanListField);
    	
    	IField[]  fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
    	return new DataTuple(spanSchema, fieldsDuplicate);
    }
    
    public Schema getSpanSchema() {
    	return spanSchema;
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
}
