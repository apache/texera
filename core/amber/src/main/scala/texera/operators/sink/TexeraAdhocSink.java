package texera.operators.sink;

import Engine.Common.tuple.texera.schema.Schema;
import Engine.Operators.OperatorMetadata;
import com.google.common.base.Preconditions;
import scala.collection.Seq;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

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

    @Override
    public Schema transformSchema(Seq<Schema> schemas) {
        Preconditions.checkArgument(schemas.length() == 1);
       return schemas.apply(0);
    }

}
