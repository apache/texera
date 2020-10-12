package texera.operators.regex;

import Engine.Common.tuple.Tuple;
import Engine.Common.Constants;
import Engine.Operators.Common.Filter.FilterGeneralMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.Function1;
import scala.Serializable;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;
import texera.common.workflow.common.FilterOpDesc;

import java.util.regex.Pattern;

public class TexeraRegex extends FilterOpDesc {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to search regex")
    public String attribute;

    @JsonProperty("regex")
    @JsonPropertyDescription("regular expression")
    public String regex;

    @Override
    public OperatorMetadata amberOperator() {
        Pattern pattern = Pattern.compile(regex);
        return new FilterGeneralMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                (Function1<Tuple, Boolean> & Serializable) t -> {
                    if (regex.equalsIgnoreCase("(coronavirus)*")) {

                    } else if (regex.equalsIgnoreCase("coronavirus")
                            || regex.equalsIgnoreCase("(coronavirus)")) {
                        try {
                            Thread.sleep(0, 500);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        if (context().isOneK()) {
                            try {
                                Thread.sleep(5);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            try {
                                Thread.sleep(10);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    Integer field = this.context().fieldIndexMapping(attribute);
                    String tupleValue = t.get(field).toString().trim();
                    return pattern.matcher(tupleValue).find();
                });
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
