package edu.uci.ics.texera.sandbox.HadoopExample.mr;

import edu.uci.ics.texera.sandbox.HadoopExample.operator.Operator;
import edu.uci.ics.texera.sandbox.HadoopExample.utils.OperatorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class TexeraMapper extends Mapper<Object, Object, Text, Text> {
    private Operator op;
    private String queryPlan;
    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        queryPlan = conf.get("mapplan");
        op = OperatorUtils.buildQueryOperator(queryPlan);
    }

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException{
        if(op==null) {
            System.out.println("Error:No operator");
            return;
        }
        KeyValue output = op.process(new KeyValue(key, value));
        //Text newValue = value;
        if(output!=null) {
            context.write((Text)output.getKey(), (Text)output.getValue());

        }

    }

}
