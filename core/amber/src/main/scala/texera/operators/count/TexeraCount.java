package texera.operators.count;

import Engine.Common.Constants;
import Engine.Operators.Count.CountMetadata;
import Engine.Operators.OperatorMetadata;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

public class TexeraCount extends TexeraOperator {
    @Override
    public OperatorMetadata amberOperator() {
        return new CountMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers());
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Count",
                "Count the number of rows",
                OperatorGroupConstants.UTILITY_GROUP(),
                1, 1);
    }
}
