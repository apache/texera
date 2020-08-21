package texera.operators.sink;

import Engine.Operators.OperatorMetadata;
import Engine.Operators.Sink.SimpleSinkOperatorMetadata;
import texera.common.schema.OperatorGroupConstants;
import texera.common.workflow.TexeraOperator;
import texera.common.schema.TexeraOperatorDescription;

public class TexeraAdhocSink extends TexeraOperator {

    @Override
    public OperatorMetadata amberOperator() {
        return new SimpleSinkOperatorMetadata(this.amberOperatorTag());
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "View Results",
                "View the workflow results",
                OperatorGroupConstants.RESULT_GROUP(),
                1, 0);
    }

}
