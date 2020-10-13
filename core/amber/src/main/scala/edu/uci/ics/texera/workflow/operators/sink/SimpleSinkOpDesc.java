package edu.uci.ics.texera.workflow.operators.sink;

import edu.uci.ics.amber.engine.operators.OpExecConfig;
import com.google.common.base.Preconditions;
import scala.collection.Seq;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.TexeraOperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

public class SimpleSinkOpDesc extends TexeraOperatorDescriptor {

    @Override
    public OpExecConfig texeraOperatorExecutor() {
        return new SimpleSinkOpExecConfig(this.operatorIdentifier());
    }

    @Override
    public TexeraOperatorInfo texeraOperatorInfo() {
        return new TexeraOperatorInfo(
                "View Results",
                "View the edu.uci.ics.texera.workflow results",
                OperatorGroupConstants.RESULT_GROUP(),
                1, 0);
    }

    @Override
    public Schema transformSchema(Seq<Schema> schemas) {
        Preconditions.checkArgument(schemas.length() == 1);
       return schemas.apply(0);
    }

}
