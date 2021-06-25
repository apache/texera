package edu.uci.ics.texera.sandbox.HadoopExample.operator;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.KeyValue;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

public class CountShuffleOperator extends Operator {

    private String attributeName;

    public CountShuffleOperator(String attributeName){
        this.attributeName = attributeName;
    }
    @Override
    public KeyValue processOneValue(KeyValue input){

        JSONObject value = new JSONObject((input.getValue()).toString());
        //hard code to get only date info
        Text key = new Text(value.getString(attributeName).substring(0,10));

        return new KeyValue(key,new Text(Integer.toString(1)));

    }
}
