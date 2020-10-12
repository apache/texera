package texera.operators.regex;

import Engine.Common.tuple.Tuple;
import texera.operators.Common.Filter.FilterOpExecConfig;
import Engine.Operators.OpExecConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.common.FilterOpDesc;

import java.util.regex.Pattern;

public class TexeraRegexOpDesc extends FilterOpDesc {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to search regex")
    public String attribute;

    @JsonProperty("regex")
    @JsonPropertyDescription("regular expression")
    public String regex;

    @JsonIgnore
    private Pattern pattern;

    @Override
    public OpExecConfig amberOperator() {
        pattern = Pattern.compile(regex);
        return new FilterOpExecConfig(this.amberOperatorTag(), this::matchRegex);
    }

    public boolean matchRegex(Tuple tuple) {
        Integer field = this.context().fieldIndexMapping(attribute);
        String tupleValue = tuple.get(field).toString().trim();
        return pattern.matcher(tupleValue).find();
    }

    @Override
    public Set<TexeraConstraintViolation> validate() {
        return super.validate();
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Regular Expression",
                "Search a regular expression in a text column",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
