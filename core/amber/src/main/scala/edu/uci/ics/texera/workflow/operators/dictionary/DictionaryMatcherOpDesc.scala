package edu.uci.ics.texera.workflow.operators.dictionary

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer

/**
 * HTML Visualization operator to render any given HTML code
 * This is the description of the operator
 */
class DictionaryMatcherOpDesc extends OperatorDescriptor {
  @JsonProperty(value = "Dictionary", required = true)
  @JsonPropertyDescription("dictionary values separated by a comma") var dictionary: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column name to match")
  @AutofillAttributeName var attribute: String = _

  @JsonProperty(value = "Matching type", required = true) var matchingType: MatchingType = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) =
    new OneToOneOpExecConfig(
      operatorIdentifier,
      _ => new DictionaryMatcherOpExec(this, operatorSchemaInfo)
    )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Dictionary matcher",
      "Matches tuples if they appear in a given dictionary",
      OperatorGroupConstants.SEARCH_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}

