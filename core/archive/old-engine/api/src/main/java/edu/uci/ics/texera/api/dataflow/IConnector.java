package edu.uci.ics.texera.api.dataflow;

public interface IConnector {
    
    int getOutputNumber();
    
    IOperator getOutputOperator(int outputIndex);
    
}
