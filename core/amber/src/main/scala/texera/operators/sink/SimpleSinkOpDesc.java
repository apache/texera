package texera.operators.sink;

import Engine.Common.tuple.texera.schema.Schema;
import Engine.Operators.OpExecConfig;
import com.google.common.base.Preconditions;
import scala.collection.Seq;
import texera.common.metadata.OperatorGroupConstants;
import texera.common.metadata.TexeraOperatorInfo;
import texera.common.workflow.TexeraOperatorDescriptor;

public class SimpleSinkOpDesc extends TexeraOperatorDescriptor {

    @Override
    public OpExecConfig texeraOpExec() {
        return new SimpleSinkOpExecConfig(this.amberOperatorTag());
    }

    @Override
    public TexeraOperatorInfo texeraOperatorInfo() {
        return new TexeraOperatorInfo(
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
