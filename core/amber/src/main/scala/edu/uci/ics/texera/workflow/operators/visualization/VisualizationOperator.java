package edu.uci.ics.texera.workflow.operators.visualization;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;

public abstract class VisualizationOperator extends OperatorDescriptor {
    public abstract String chartType();
}
