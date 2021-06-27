package edu.uci.ics.texera.sandbox.HadoopExample.operator;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.KeyValue;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

public class FilterOperator extends Operator {
    private String compVal;
    private String comp;

    public FilterOperator(String compVal, String comp){
        this.compVal = compVal;
        this.comp = comp;
    }
    @Override
    public KeyValue processOneValue(KeyValue input){

        KeyValue output = null;
        Text key = (Text)input.getKey();
        int n = key.toString().compareTo(compVal);
        if(     (n>=0&&comp.equals("GE"))
                ||(n>0&&comp.equals("GT"))
                ||(n==0 && comp.equals("EQ"))
                ||(n<0 && comp.equals("LT"))
                ||(n<=0 && comp.equals("LE"))
                ||(n!=0 && comp.equals("NE"))
                ){

            output = new KeyValue(key,input.getValue());
        }
        return output;
    }
}
