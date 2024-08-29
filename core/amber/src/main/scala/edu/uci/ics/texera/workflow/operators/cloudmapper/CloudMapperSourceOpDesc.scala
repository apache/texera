package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

class CloudMapperSourceOpDesc extends PythonSourceOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("FastQ Files")
  @JsonPropertyDescription("Zip file containing FASTQ files")
  val fastQFiles: Option[String] = None

  @JsonProperty(required = true)
  @JsonSchemaTitle("Reference Genomes")
  @JsonPropertyDescription("Add one or more reference genomes")
  var referenceGenomes: List[ReferenceGenome] = List()

  @JsonProperty(required = true)
  @JsonSchemaTitle("Cluster Id")
  @JsonPropertyDescription("Cluster Id")
  val clusterId: Option[String] = None

  override def generatePythonCode(): String = {
    s"""from pytexera import *
           |
           |class GenerateOperator(UDFSourceOperator):
           |
           |    @overrides
           |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
           |        yield {'response': 200}
           """.stripMargin
  }
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "CloudMapper",
      "Running sequence alignment using public cluster services",
      OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
  override def asSource() = true
  override def sourceSchema(): Schema =
    Schema
      .builder()
      .add(
        new Attribute("response", AttributeType.INTEGER)
      )
      .build()
}
