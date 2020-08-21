package texera.operators.keyword;

import Engine.Common.Constants;
import Engine.Operators.KeywordSearch.KeywordSearchMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

public class TexeraKeywordSearch extends TexeraOperator {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to search keyword")
    public String attribute;

    @JsonProperty("keyword")
    @JsonPropertyDescription("a single keyword to search")
    public String keyword;

    @Override
    public OperatorMetadata amberOperator() {
        return new KeywordSearchMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                this.context().fieldIndexMapping(this.attribute.toLowerCase().trim()),
                keyword.toLowerCase().trim());
    }

    @Override
    public Set<TexeraConstraintViolation> validate() {
        scala.collection.mutable.Set<TexeraConstraintViolation> violations =
                new scala.collection.mutable.HashSet<TexeraConstraintViolation>();

        if (this.context().fieldIndexMapping(this.attribute.toLowerCase()) == null) {
            violations.add(TexeraConstraintViolation.apply(
                    "attribute: " + attribute + " does not exist", "attribute"));
        }
        return violations.toSet();
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Keyword Search",
                "Search a keyword in a text column",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
