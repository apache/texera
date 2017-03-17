package edu.uci.ics.textdb.plangen.operatorbuilder;

import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;

import java.util.List;
import java.util.Map;

/**
 * ProjectionBuilder provides a static function that builds a ProjectionOperator.
 *
 * ProjectionBuilder currently needs the following properties:
 *
 *   projection fields (required)
 *
 * @author Kishore Narendran
 */
public class ProjectionBuilder {
    /**
     * Builds a ProjectionOperator according to operatorProperties.
     */
    public static ProjectionOperator buildOperator(Map<String, String> operatorProperties) throws PlanGenException {
        // Generating a list of Attribute Names
        List<String> projectionFields = OperatorBuilderUtils.constructAttributeNames(operatorProperties);

        // Building ProjectionOperator
        ProjectionPredicate projectionPredicate = new ProjectionPredicate(projectionFields);
        ProjectionOperator projectionOperator = new ProjectionOperator(projectionPredicate);

        // Setting limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if(limitInt != null) {
            projectionOperator.setLimit(limitInt);
        }

        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if(offsetInt != null) {
            projectionOperator.setOffset(offsetInt);
        }

        return projectionOperator;
    }
}
