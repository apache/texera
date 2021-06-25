package edu.uci.ics.texera.sandbox.HadoopExample.mr;



import edu.uci.ics.texera.sandbox.HadoopExample.operator.Operator;
import edu.uci.ics.texera.sandbox.HadoopExample.utils.OperatorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TexeraReducer extends Reducer<Text, Text, Text, Text>{
        private Operator op;

        @Override
        protected void setup(Context context){
                Configuration conf = context.getConfiguration();
                String queryPlan = conf.get("reduceplan");
                op = OperatorUtils.buildQueryOperator(queryPlan);

        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws  IOException, InterruptedException{
                KeyValue output = null;
                if(op==null){
                        System.out.println("Error:No operator");
                        return;
                }


                for(Text value: values) {

                        output = op.process(new KeyValue(key, value));

                }
                if(output!=null) {
                        context.write((Text) output.getKey(), (Text) output.getValue());
                }
        }

}
