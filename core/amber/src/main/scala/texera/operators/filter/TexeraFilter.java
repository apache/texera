package texera.operators.filter;

import Engine.Common.AmberTuple.AmberTuple;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.Constants;
import Engine.Operators.Common.Filter.FilterGeneralMetadata;
import Engine.Operators.Common.Map.MapMetadata;
import Engine.Operators.Filter.FilterMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.commons.lang3.ArrayUtils;
import scala.Function1;
import scala.Serializable;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

import java.util.List;

public class TexeraFilter extends TexeraOperator {

    @JsonProperty("predicates")
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public OperatorMetadata amberOperator() {
        return new FilterGeneralMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                (Function1<Tuple, Boolean> & Serializable) t -> {
                    boolean satisfy = false;
                    for (FilterPredicate predicate: predicates) {
                        satisfy = satisfy || predicate.evaluate(t, this.context());
                    }
                    return satisfy;
                });
    }

    @Override
    public Set<TexeraConstraintViolation> validate() {
        return super.validate();
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Filter",
                "performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
