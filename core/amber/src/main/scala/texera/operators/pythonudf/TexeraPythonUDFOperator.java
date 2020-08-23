package texera.operators.pythonudf;

import Engine.Common.AmberTuple.Tuple;
import Engine.Common.Constants;
import Engine.Operators.Common.Map.MapMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.Function1;
import scala.Serializable;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;


public class TexeraPythonUDFOperator extends TexeraOperator {

    @JsonProperty("mock input")
    @JsonPropertyDescription("Mock Input")
    public String mockInput;

    @Override
    public OperatorMetadata amberOperator() {
        return new MapMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                (Function1<Tuple, Tuple> & Serializable) t -> {
                    return t;
                });
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "MockOperator",
                "Test",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
