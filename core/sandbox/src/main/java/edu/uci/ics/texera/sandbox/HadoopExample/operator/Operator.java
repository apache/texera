package edu.uci.ics.texera.sandbox.HadoopExample.operator;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.KeyValue;

import java.util.ArrayList;
import java.util.List;

public abstract class Operator {



    protected List<Operator> parentOperators;

    protected Operator() {

        parentOperators = new ArrayList<>();
    }

    public List<Operator> getParentOperators() {
        return parentOperators;
    }

    public void setParentOperators(List<Operator> parentOperators) {
        if(parentOperators == null){
            parentOperators = new ArrayList<>();
        }
        this.parentOperators = parentOperators;
    }

    public KeyValue process(KeyValue input){
        KeyValue output = input;
        if(parentOperators.size()>0){
            for(Operator parentOperator :parentOperators){
                output =  parentOperator.process(input);
            }
        }
        if(output == null)
            return output;
        else
            return processOneValue(output);
    }

    public KeyValue processOneValue(KeyValue value){
        return value;
    }

}
