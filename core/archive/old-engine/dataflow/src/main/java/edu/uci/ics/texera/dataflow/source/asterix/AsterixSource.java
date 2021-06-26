package edu.uci.ics.texera.dataflow.source.asterix;

import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONArray;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class AsterixSource implements ISourceOperator {
        
    private final AsterixSourcePredicate predicate;
    private final Schema outputSchema;
    private JSONArray resultJsonArray;
    
    private int cursor = CLOSED;
    
    public AsterixSource(AsterixSourcePredicate predicate) {
        this.predicate = predicate;
        this.outputSchema = new Schema(
                SchemaConstants._ID_ATTRIBUTE, 
                new Attribute(this.predicate.getResultAttribute(), AttributeType.STRING));
    }

    @Override
    public void open() throws TexeraException {
        if (cursor == OPENED) {
            return;
        }
        try {
            String asterixAddress = "http://" + predicate.getHost() + ":" + predicate.getPort() + 
                    "/query/service";
            String asterixQuery = generateAsterixQuery(predicate);
            HttpResponse<JsonNode> jsonResponse = Unirest.post(asterixAddress)
                    .queryString("statement", asterixQuery)
                    .field("mode", "immediate")
                    .asJson();
            
            // if status is 200 OK, store the results
            if (jsonResponse.getStatus() == 200) {
                this.resultJsonArray = jsonResponse.getBody().getObject().getJSONArray("results");
            } else {
                throw new DataflowException("Send query to asterix failed: " + 
                        "error status: " + jsonResponse.getStatusText() + ", " + 
                        "error body: " + jsonResponse.getBody().toString());
            }
            cursor = OPENED;
        } catch (UnirestException e) {
            throw new DataflowException(e);
        }
    }
    
    private static String generateAsterixQuery(AsterixSourcePredicate predicate) {
        String asDataset = "ds";
        StringBuilder sb = new StringBuilder();
        sb.append("use " + predicate.getDataverse() + ';').append("\n");
        sb.append("select * ").append("\n");
        sb.append("from " + predicate.getDataset() + " as " + asDataset).append("\n");
        sb.append("where true").append("\n");
        if (predicate.getField() != null && predicate.getKeyword() != null) {
            List<String> keywordList = DataflowUtils.tokenizeQuery(
                    LuceneAnalyzerConstants.standardAnalyzerString(), predicate.getKeyword());
            String asterixKeyword = 
                    "[" +
                    keywordList.stream().map(keyword -> "\"" + keyword + "\"")
                        .collect(Collectors.joining(", ")) +  
                    "]";
            String asterixField = "`" + predicate.getField() + "`";
            sb.append("and ftcontains(" + asDataset + "." + asterixField + ", ");
            sb.append(asterixKeyword + ", " + "{\"mode\":\"all\"}" + ")").append("\n");
        }
        if(predicate.getStartDate() != null){
        	String startDate = predicate.getStartDate();
        	sb.append("and create_at >= datetime(\""+startDate +"T00:00:04.000Z\")").append("\n");
        }
        if(predicate.getEndDate() != null){
        	String endDate = predicate.getEndDate();
        	
        	sb.append("and create_at <= datetime(\""+endDate +"T00:00:04.000Z\")").append("\n");
        }
        if (predicate.getLimit() != null) {
            sb.append("limit " + predicate.getLimit()).append("\n");
        }
        sb.append(";");
        return sb.toString();
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (cursor < resultJsonArray.length()) {
            Tuple tuple =  new Tuple(this.outputSchema, 
                    IDField.newRandomID(),
                    new StringField(resultJsonArray.getJSONObject(cursor).get("ds").toString()));
            cursor ++;
            return tuple;
        }
        return null;
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }
}
