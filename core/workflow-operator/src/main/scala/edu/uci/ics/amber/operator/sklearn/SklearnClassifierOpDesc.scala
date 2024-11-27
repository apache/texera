package edu.uci.ics.amber.operator.sklearn

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaInt,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  CommonOpDescAnnotation,
  HideAnnotation
}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PortIdentity}

abstract class SklearnClassifierOpDesc extends PythonOperatorDescriptor {

  @JsonSchemaTitle("Target Attribute")
  @JsonPropertyDescription("Attribute in your dataset corresponding to target.")
  @JsonProperty(required = true)
  @AutofillAttributeName
  var target: String = _

  @JsonSchemaTitle("Count Vectorizer")
  @JsonPropertyDescription("Convert a collection of text documents to a matrix of token counts.")
  @JsonProperty(defaultValue = "false")
  var countVectorizer: Boolean = false

  @JsonSchemaTitle("Text Attribute")
  @JsonPropertyDescription("Attribute in your dataset with text to vectorize.")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(
        path = CommonOpDescAnnotation.autofill,
        value = CommonOpDescAnnotation.attributeName
      ),
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    ),
    ints = Array(
      new JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 0)
    )
  )
  var text: String = _

  @JsonSchemaTitle("Tfidf Transformer")
  @JsonPropertyDescription("Transform a count matrix to a normalized tf or tf-idf representation.")
  @JsonProperty(defaultValue = "false")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  val tfidfTransformer: Boolean = false

  @JsonIgnore
  def getImportStatements = ""

  @JsonIgnore
  def getUserFriendlyModelName = ""

  override def generatePythonCode(): String =
    s"""$getImportStatements
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
       |from sklearn.pipeline import make_pipeline
       |from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
       |import numpy as np
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        Y = table["$target"]
       |        X = table.drop("$target", axis=1)
       |        X = ${if (countVectorizer) "X['" + text + "']" else "X"}
       |        if port == 0:
       |            self.model = make_pipeline(${if (countVectorizer) "CountVectorizer(),"
    else ""} ${if (tfidfTransformer) "TfidfTransformer()," else ""} ${getImportStatements
      .split(" ")
      .last}()).fit(X, Y)
       |        else:
       |            predictions = self.model.predict(X)
       |            print("Overall Accuracy:", round(accuracy_score(Y, predictions), 4))
       |            f1s = f1_score(Y, predictions, average=None)
       |            precisions = precision_score(Y, predictions, average=None)
       |            recalls = recall_score(Y, predictions, average=None)
       |            for i, class_name in enumerate(np.unique(Y)):
       |                print("Class", repr(class_name), " - F1:", round(f1s[i], 4), ", Precision:", round(precisions[i], 4), ", Recall:", round(recalls[i], 4))
       |            yield {"model_name" : "$getUserFriendlyModelName", "model" : self.model}""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      getUserFriendlyModelName,
      "Sklearn " + getUserFriendlyModelName + " Operator",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "training"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema
      .builder()
      .add("model_name", AttributeType.STRING)
      .add("model", AttributeType.BINARY)
      .build()
  }
}
