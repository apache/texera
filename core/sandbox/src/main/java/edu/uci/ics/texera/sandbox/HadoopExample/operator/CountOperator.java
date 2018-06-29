package edu.uci.ics.texera.sandbox.HadoopExample.operator;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.KeyValue;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.util.HashMap;

public class CountOperator extends Operator{

    HashMap<Text, Integer> hashMap = new HashMap<>();
    public CountOperator(){
    }

    @Override
    public KeyValue processOneValue(KeyValue input){

        int n = Integer.parseInt(input.getValue().toString());
        Text key = (Text)input.getKey();
        Integer num = hashMap.get(key);
        if(num!=null){
            hashMap.put(key, n+num);
        }else{
            hashMap.put(key, n);
        }
        return new KeyValue(key, new Text(hashMap.get(key).toString()));
    }

}
