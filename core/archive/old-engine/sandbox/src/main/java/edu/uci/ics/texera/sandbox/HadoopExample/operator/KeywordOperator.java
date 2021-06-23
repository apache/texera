package edu.uci.ics.texera.sandbox.HadoopExample.operator;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.KeyValue;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.util.List;

public class KeywordOperator extends Operator {
    private String attributeName;
    private String queryValue;

    public KeywordOperator(String attributeName, String queryValue){
        this.attributeName = attributeName;
        this.queryValue = queryValue;
    }
    @Override
    public KeyValue processOneValue(KeyValue input){

        JSONObject value = new JSONObject((input.getValue()).toString());
        KeyValue output = null;
        Text content = new Text(value.getString(attributeName));
        if(content.find(queryValue) != -1){
            output = new KeyValue(new Text(queryValue),input.getValue());
        }
        return output;
    }

}
